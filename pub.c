/*
 * publisher.c
 *
 * 수정 내역:
 *   1. on_disconnect → 플래그 방식 (deadlock 방지)
 *   2. failover 재연결 → loop_stop → disconnect → connect 방식 (민준이 방식)
 *   3. 모션 감지 → 연속 프레임 임계값 방식 (오감지 감소)
 *
 * Build:
 *   gcc -Wall -O2 -o publisher publisher.c -lmosquitto -lsqlite3 -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sqlite3.h>
#include <mosquitto.h>

/* ── 카메라 ID ───────────────────────────────────────────────── */
#define CAM_ID  "pi1"   /* publisher 장비마다 변경 */

/* ── 토픽 ────────────────────────────────────────────────────── */
#define TOPIC_FRAME   "camera/" CAM_ID "/frame"
#define TOPIC_EVENT   "camera/" CAM_ID "/event"
#define TOPIC_STATUS  "camera/" CAM_ID "/status"

/* ── 브로커 failover 목록 ────────────────────────────────────── */
typedef struct {
    const char *host;
    int         port;
    const char *label;
} broker_t;

static const broker_t BROKERS[] = {
    { "192.168.0.7",  1883, "B1" },   /* Primary 브로커 */
    { "192.168.0.11", 1883, "B2" },   /* Backup 입구 브로커 */
    { "192.168.0.13", 1883, "B3" },   /* Primary 출구 브로커 (직접 연결 fallback) */
    { "192.168.0.9",  1883, "B4" },  // 추가
};
#define BROKER_COUNT  (int)(sizeof(BROKERS)/sizeof(BROKERS[0]))

/* ── 카메라 설정 ─────────────────────────────────────────────── */
#define CAM_WIDTH    640
#define CAM_HEIGHT   480
#define CAM_FPS      15
#define CAM_QUALITY  80

/* ── 프레임 버퍼 ─────────────────────────────────────────────── */
#define PIPE_CHUNK   (4 * 1024)
#define FRAME_MAX    (512 * 1024)

/* ── SQLite 경로 ─────────────────────────────────────────────── */
#define DB_PATH  "/tmp/event_queue.db"

/* ── 모션 감지 설정 ──────────────────────────────────────────── */
#define MOTION_RATIO_THRESHOLD  1.20  /* 크기 변화 임계값: 이전 대비 20% 이상 증가 */
#define MOTION_CONFIRM_FRAMES   3     /* 연속 N프레임 동안 변화가 지속돼야 모션으로 인식 */

/* ─────────────────────────────────────────────────────────────
 * 전역 상태
 * ───────────────────────────────────────────────────────────── */
static int              g_broker_idx     = 0;
static int              g_connected      = 0;
static volatile int     g_need_failover  = 0;  /* on_disconnect → main 루프 통신용 플래그 */
static pthread_mutex_t  g_mutex          = PTHREAD_MUTEX_INITIALIZER;
static struct mosquitto *g_mosq          = NULL;
static sqlite3         *g_db             = NULL;

/* ─────────────────────────────────────────────────────────────
 * SQLite 큐
 * 브로커가 전부 다운됐을 때 이벤트를 로컬에 저장해두고,
 * 재연결 후 flush하는 방식으로 메시지 유실을 방지한다.
 * ───────────────────────────────────────────────────────────── */
static int db_init(void)
{
    if (sqlite3_open(DB_PATH, &g_db) != SQLITE_OK) {
        fprintf(stderr, "[DB] Open failed: %s\n", sqlite3_errmsg(g_db));
        return -1;
    }
    const char *sql =
        "CREATE TABLE IF NOT EXISTS queue ("
        "  id      INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  topic   TEXT    NOT NULL,"
        "  payload TEXT    NOT NULL,"
        "  ts      INTEGER NOT NULL,"
        "  sent    INTEGER DEFAULT 0);";
    char *err = NULL;
    sqlite3_exec(g_db, sql, NULL, NULL, &err);
    if (err) {
        fprintf(stderr, "[DB] %s\n", err);
        sqlite3_free(err);
        return -1;
    }
    return 0;
}

/* 이벤트를 SQLite에 저장 */
static void db_enqueue(const char *topic, const char *payload)
{
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO queue (topic,payload,ts) VALUES(?,?,?);", -1, &s, NULL);
    sqlite3_bind_text(s, 1, topic,   -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 2, payload, -1, SQLITE_STATIC);
    sqlite3_bind_int64(s, 3, (sqlite3_int64)time(NULL));
    sqlite3_step(s);
    sqlite3_finalize(s);
    printf("[DB] Queued: %s\n", payload);
}

/* 재연결 후 미전송 이벤트를 브로커로 전송 */
static void db_flush(struct mosquitto *mosq)
{
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(g_db,
        "SELECT id,topic,payload FROM queue"
        " WHERE sent=0 ORDER BY id LIMIT 50;",
        -1, &s, NULL);
    while (sqlite3_step(s) == SQLITE_ROW) {
        int         id      = sqlite3_column_int(s, 0);
        const char *topic   = (const char *)sqlite3_column_text(s, 1);
        const char *payload = (const char *)sqlite3_column_text(s, 2);
        int rc = mosquitto_publish(mosq, NULL, topic,
                                   (int)strlen(payload), payload, 2, false);
        if (rc == MOSQ_ERR_SUCCESS) {
            char upd[64];
            snprintf(upd, sizeof(upd),
                     "UPDATE queue SET sent=1 WHERE id=%d;", id);
            sqlite3_exec(g_db, upd, NULL, NULL, NULL);
            printf("[DB] Flushed id=%d\n", id);
        } else break;
    }
    sqlite3_finalize(s);
}

/* ─────────────────────────────────────────────────────────────
 * MQTT 콜백
 * ───────────────────────────────────────────────────────────── */

/* 브로커 연결 성공 시 호출 */
static void on_connect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud;
    if (rc == 0) {
        pthread_mutex_lock(&g_mutex);
        g_connected = 1;
        pthread_mutex_unlock(&g_mutex);

        printf("[MQTT] Connected to %s (%s)\n",
               BROKERS[g_broker_idx].host,
               BROKERS[g_broker_idx].label);

        /* 온라인 상태 발행 (retain) */
        mosquitto_publish(mosq, NULL, TOPIC_STATUS,
                          strlen("online"), "online", 1, true);

        /* 재연결 시 SQLite에 쌓인 미전송 이벤트 flush */
        db_flush(mosq);
    } else {
        fprintf(stderr, "[MQTT] Connect failed rc=%d\n", rc);
    }
}

/*
 * 브로커 연결 끊김 시 호출
 *
 * ⚠️  이 콜백은 libmosquitto 내부 네트워크 스레드에서 실행된다.
 *     여기서 mosquitto_connect()를 직접 호출하면 deadlock 위험이 있다.
 *     플래그(g_need_failover)만 세우고 실제 재연결은 메인 루프에서 처리한다.
 */
static void on_disconnect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud;
    (void)mosq;

    pthread_mutex_lock(&g_mutex);
    g_connected = 0;
    pthread_mutex_unlock(&g_mutex);

    if (rc != 0) {
        printf("[MQTT] Disconnected from %s. Triggering failover...\n",
               BROKERS[g_broker_idx].label);
        /* 메인 루프에 failover 요청 플래그만 세움 */
        g_need_failover = 1;
    }
}

/* ─────────────────────────────────────────────────────────────
 * failover 실행
 *
 * 민준이 방식: loop_stop → disconnect → 인덱스 이동 → connect → loop_start
 * 기존 루프를 완전히 정리한 뒤 새 브로커로 깔끔하게 재연결한다.
 * mosquitto_connect()만 바꾸는 방식보다 내부 상태 충돌 위험이 없다.
 * ───────────────────────────────────────────────────────────── */
static void do_failover(struct mosquitto *mosq)
{
    /* 다음 브로커로 인덱스 순환 */
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] Switching to %s (%s)\n",
           BROKERS[g_broker_idx].host,
           BROKERS[g_broker_idx].label);

    /* 기존 루프 완전 정리 */
    mosquitto_loop_stop(mosq, true);
    mosquitto_disconnect(mosq);

    /* 새 브로커로 연결 시도 */
    int rc = mosquitto_connect(mosq,
                               BROKERS[g_broker_idx].host,
                               BROKERS[g_broker_idx].port,
                               60);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] Connect failed to %s, will retry\n",
                BROKERS[g_broker_idx].label);
        sleep(1);
        g_need_failover = 1;  /* 다음 루프에서 재시도 */
        return;
    }

    /* 루프 재시작 */
    mosquitto_loop_start(mosq);
    g_need_failover = 0;
}

/* ─────────────────────────────────────────────────────────────
 * MQTT 초기화
 * ───────────────────────────────────────────────────────────── */
static struct mosquitto *mqtt_init(void)
{
    mosquitto_lib_init();
    struct mosquitto *mosq = mosquitto_new(
        "cctv_pub_" CAM_ID, false, NULL);
    if (!mosq) { perror("mosquitto_new"); return NULL; }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);

    /* reconnect delay 설정 (민준이 방식) */
    mosquitto_reconnect_delay_set(mosq, 1, 5, false);

    /* LWT: 비정상 종료 시 status = "offline" 자동 발행 */
    mosquitto_will_set(mosq, TOPIC_STATUS,
                       strlen("offline"), "offline", 1, true);

    /* B1부터 순서대로 연결 시도 */
    for (int i = 0; i < BROKER_COUNT; i++) {
        if (mosquitto_connect(mosq,
                              BROKERS[i].host,
                              BROKERS[i].port,
                              60) == MOSQ_ERR_SUCCESS) {
            g_broker_idx = i;
            printf("[MQTT] Initial connect to %s (%s)\n",
                   BROKERS[i].host, BROKERS[i].label);
            break;
        }
        fprintf(stderr, "[MQTT] %s (%s) unreachable\n",
                BROKERS[i].host, BROKERS[i].label);
    }

    /* 백그라운드 네트워크 스레드 시작 */
    mosquitto_loop_start(mosq);
    return mosq;
}

/* ─────────────────────────────────────────────────────────────
 * 이벤트 발행 (QoS 2 + SQLite fallback)
 * 브로커가 살아있으면 바로 전송, 아니면 SQLite에 저장
 * ───────────────────────────────────────────────────────────── */
static void publish_event(struct mosquitto *mosq, const char *payload)
{
    /* 일단 SQLite에 저장 (전송 실패 시 보험) */
    db_enqueue(TOPIC_EVENT, payload);

    pthread_mutex_lock(&g_mutex);
    int connected = g_connected;
    pthread_mutex_unlock(&g_mutex);

    /* 연결 중이면 바로 flush */
    if (connected) db_flush(mosq);
}

/* ─────────────────────────────────────────────────────────────
 * rpicam-vid 파이프 열기
 * ───────────────────────────────────────────────────────────── */
static FILE *open_camera_pipe(void)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd),
        "rpicam-vid -t 0 --codec mjpeg"
        " --width %d --height %d"
        " --framerate %d -q %d"
        " --nopreview -o - 2>/dev/null",
        CAM_WIDTH, CAM_HEIGHT, CAM_FPS, CAM_QUALITY);
    FILE *p = popen(cmd, "r");
    if (!p) fprintf(stderr, "[Camera] popen failed\n");
    return p;
}

/* ─────────────────────────────────────────────────────────────
 * MJPEG 스트림에서 JPEG 프레임 1개 파싱
 *   SOI 마커: 0xFF 0xD8  (JPEG 시작)
 *   EOI 마커: 0xFF 0xD9  (JPEG 끝)
 * ───────────────────────────────────────────────────────────── */
static uint8_t *read_jpeg_frame(FILE *pipe, size_t *out_size)
{
    static uint8_t buf[FRAME_MAX];
    static size_t  buf_len = 0;
    static uint8_t chunk[PIPE_CHUNK];

    while (1) {
        size_t n = fread(chunk, 1, sizeof(chunk), pipe);
        if (n == 0) return NULL;

        if (buf_len + n > FRAME_MAX) buf_len = 0;
        memcpy(buf + buf_len, chunk, n);
        buf_len += n;

        /* SOI 마커 찾기 */
        size_t soi = SIZE_MAX;
        for (size_t i = 0; i + 1 < buf_len; i++) {
            if (buf[i] == 0xFF && buf[i+1] == 0xD8) { soi = i; break; }
        }
        if (soi == SIZE_MAX) { buf_len = 0; continue; }
        if (soi > 0) {
            memmove(buf, buf + soi, buf_len - soi);
            buf_len -= soi;
        }

        /* EOI 마커 찾기 */
        for (size_t i = 2; i + 1 < buf_len; i++) {
            if (buf[i] == 0xFF && buf[i+1] == 0xD9) {
                size_t flen = i + 2;
                uint8_t *frame = malloc(flen);
                if (!frame) return NULL;
                memcpy(frame, buf, flen);
                *out_size = flen;
                memmove(buf, buf + flen, buf_len - flen);
                buf_len -= flen;
                return frame;
            }
        }
    }
}

/* ─────────────────────────────────────────────────────────────
 * 모션 감지 — 연속 프레임 임계값 방식
 *
 * 동작 원리:
 *   JPEG 파일 크기는 화면 복잡도(정보량)에 비례한다.
 *   움직임이 생기면 압축률이 낮아져 파일 크기가 커지는 원리를 이용한다.
 *
 * 개선 포인트:
 *   단순 1프레임 비교 → MOTION_CONFIRM_FRAMES(3프레임) 연속 변화 감지
 *   1프레임 노이즈(조명 변화, 자동 화이트밸런스 등)에 의한 오감지 방지
 *
 * 반환값:
 *   1 = 모션 감지 (MOTION_CONFIRM_FRAMES 연속으로 임계값 초과)
 *   0 = 모션 없음
 * ───────────────────────────────────────────────────────────── */
static int detect_motion(size_t cur, size_t prev)
{
    static int consecutive_count = 0;

    if (prev == 0) {
        consecutive_count = 0;
        return 0;
    }

    double ratio = (double)cur / (double)prev;

    if (ratio > MOTION_RATIO_THRESHOLD) {
        consecutive_count++;
        printf("[Motion] ratio=%.2f count=%d/%d\n",
               ratio, consecutive_count, MOTION_CONFIRM_FRAMES);

        if (consecutive_count >= MOTION_CONFIRM_FRAMES) {
            consecutive_count = 0;
            return 1;
        }
    } else {
        if (consecutive_count > 0)
            printf("[Motion] Reset (ratio=%.2f)\n", ratio);
        consecutive_count = 0;
    }

    return 0;
}

/* ─────────────────────────────────────────────────────────────
 * main
 * ───────────────────────────────────────────────────────────── */
int main(void)
{
    if (db_init() != 0) return -1;

    g_mosq = mqtt_init();
    if (!g_mosq) return -1;

    printf("[Publisher] CAM_ID=%s  %dx%d @%dfps\n",
           CAM_ID, CAM_WIDTH, CAM_HEIGHT, CAM_FPS);

    size_t prev_size  = 0;
    int    frame_cnt  = 0;
    time_t last_fps_t = time(NULL);

retry:;
    FILE *pipe = open_camera_pipe();
    if (!pipe) { sleep(3); goto retry; }
    printf("[Camera] Pipe opened\n");

    while (1) {

        /*
         * ── failover 처리 ─────────────────────────────────────
         * on_disconnect가 g_need_failover 플래그를 세우면
         * do_failover()를 호출해 다음 브로커로 전환한다.
         * loop_stop → disconnect → connect → loop_start 순서로
         * 내부 상태를 완전히 정리한 뒤 재연결하므로 안전하다.
         */
        if (g_need_failover) {
            do_failover(g_mosq);
        }

        /* ── 프레임 읽기 ── */
        size_t   fsize = 0;
        uint8_t *frame = read_jpeg_frame(pipe, &fsize);

        if (!frame) {
            fprintf(stderr, "[Camera] Pipe broken, restarting...\n");
            pclose(pipe);
            sleep(2);
            goto retry;
        }

        /* ── 프레임 발행 QoS 0 (손실 허용) ── */
        pthread_mutex_lock(&g_mutex);
        int connected = g_connected;
        pthread_mutex_unlock(&g_mutex);

        if (connected)
            mosquitto_publish(g_mosq, NULL, TOPIC_FRAME,
                              (int)fsize, frame, 0, false);

        /* ── 모션 감지 → 이벤트 발행 QoS 2 (무손실) ── */
        if (detect_motion(fsize, prev_size)) {
            char payload[128];
            snprintf(payload, sizeof(payload),
                     "{\"cam\":\"%s\",\"event\":\"motion\",\"ts\":%ld}",
                     CAM_ID, (long)time(NULL));
            publish_event(g_mosq, payload);
            printf("[Event] Motion detected  frame=%zu bytes\n", fsize);
        }
        prev_size = fsize;

        /* ── FPS 출력 (10초마다) ── */
        frame_cnt++;
        time_t now = time(NULL);
        if (now - last_fps_t >= 10) {
            printf("[Stats] %.1f fps  broker=%s\n",
                   (double)frame_cnt / (double)(now - last_fps_t),
                   BROKERS[g_broker_idx].label);
            frame_cnt  = 0;
            last_fps_t = now;
        }

        free(frame);
    }

    pclose(pipe);
    mosquitto_loop_stop(g_mosq, true);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    sqlite3_close(g_db);
    return 0;
}