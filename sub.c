/*
 * subscriber_ws.c
 *
 * 수정 내역:
 *   1. on_disconnect → 플래그 방식 (deadlock 방지)
 *   2. failover 재연결 → loop_stop → disconnect → connect 방식 (민준이 방식)
 *   3. dedup → 해시 테이블 기반 O(1) 중복 제거
 *   4. mosquitto_loop_forever → 수동 루프로 교체 (플래그 체크 가능)
 *
 * Build:
 *   gcc -Wall -O2 -o subscriber_ws subscriber_ws.c \
 *       -lmosquitto -lws -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <mosquitto.h>
#include <wsserver/ws.h>

/* ─────────────────────────────────────────────────────────────
 * dedup 자료구조 — 해시 테이블 기반 O(1)
 *
 * B3↔B4 양방향 브릿지 구조상 동일한 메시지가 두 경로로
 * Sub에 도달할 수 있다. message-id를 해시 테이블에 기록해
 * 중복 수신 시 버린다.
 *
 * 적용 대상: event / status 토픽 (QoS 1/2, 중복 처리 필요)
 * 미적용:    frame 토픽 (QoS 0, 손실 허용 + 중복 무방)
 * ───────────────────────────────────────────────────────────── */
#define DEDUP_SIZE  1024
#define TTL_SECONDS 60
#define ID_MAX_LEN  64

typedef struct {
    char   id[ID_MAX_LEN];
    time_t timestamp;
    int    occupied;
} SeenMsg;

static SeenMsg seen[DEDUP_SIZE];

static unsigned int hash_id(const char *id) {
    unsigned int h = 0;
    while (*id) h = h * 31 + (unsigned char)*id++;
    return h % DEDUP_SIZE;
}

static void cleanup_expired(void) {
    time_t now = time(NULL);
    for (int i = 0; i < DEDUP_SIZE; i++) {
        if (seen[i].occupied && now - seen[i].timestamp > TTL_SECONDS)
            seen[i].occupied = 0;
    }
}

static int is_duplicate(const char *msg_id) {
    cleanup_expired();
    unsigned int slot = hash_id(msg_id);
    if (seen[slot].occupied &&
        strncmp(seen[slot].id, msg_id, ID_MAX_LEN) == 0)
        return 1;
    strncpy(seen[slot].id, msg_id, ID_MAX_LEN - 1);
    seen[slot].id[ID_MAX_LEN - 1] = '\0';
    seen[slot].timestamp = time(NULL);
    seen[slot].occupied  = 1;
    return 0;
}

/*
 * event/status payload에서 dedup용 고유 id 생성
 * publisher 형식: {"cam":"pi1","event":"motion","ts":1713000000}
 * → "pi1-1713000000" 형태로 id 생성
 */
static void extract_msg_id(const char *cam_id, const char *payload,
                            char *out, size_t out_len) {
    const char *ts_start = strstr(payload, "\"ts\":");
    if (ts_start) {
        ts_start += 5;
        snprintf(out, out_len, "%s-%.*s", cam_id, 12, ts_start);
    } else {
        snprintf(out, out_len, "%s-%ld", cam_id, (long)time(NULL));
    }
}

/* ── WebSocket 포트 ──────────────────────────────────────────── */
#define WS_PORT   8765

/* ── 브로커 failover 목록 ────────────────────────────────────── */
typedef struct {
    const char *host;
    int         port;
    const char *label;
} broker_t;

static const broker_t BROKERS[] = {
    { "192.168.0.13", 1883, "B3" },  /* Primary 출구 브로커 */
    { "192.168.0.9",  1883, "B4" },  /* Backup 출구 브로커  */
    { "192.168.0.7",  1883, "B1" },  // 추가
    { "192.168.0.11", 1883, "B2" },  // 추가
};
#define BROKER_COUNT (int)(sizeof(BROKERS)/sizeof(BROKERS[0]))

/* ── 구독 토픽 ───────────────────────────────────────────────── */
#define TOPIC_FRAME   "camera/+/frame"
#define TOPIC_EVENT   "camera/+/event"
#define TOPIC_STATUS  "camera/+/status"

/* ── 최신 프레임 캐시 (카메라별) ────────────────────────────── */
#define MAX_CAMS 4
typedef struct {
    char     cam_id[16];
    uint8_t *data;
    size_t   size;
} FrameCache;

static FrameCache       g_frames[MAX_CAMS];
static int              g_frame_count    = 0;
static pthread_mutex_t  g_mutex          = PTHREAD_MUTEX_INITIALIZER;
static int              g_broker_idx     = 0;
static int              g_connected      = 0;
static volatile int     g_need_failover  = 0;  /* on_disconnect → main 루프 통신용 플래그 */
static struct mosquitto *g_mosq          = NULL;

static FrameCache *get_cache(const char *cam_id) {
    for (int i = 0; i < g_frame_count; i++)
        if (strcmp(g_frames[i].cam_id, cam_id) == 0)
            return &g_frames[i];
    if (g_frame_count >= MAX_CAMS) return NULL;
    FrameCache *c = &g_frames[g_frame_count++];
    strncpy(c->cam_id, cam_id, sizeof(c->cam_id) - 1);
    c->data = NULL; c->size = 0;
    return c;
}

static void extract_cam_id(const char *topic, char *out, size_t out_len) {
    const char *p = strchr(topic, '/');
    if (!p) { strncpy(out, "unknown", out_len); return; }
    p++;
    const char *q = strchr(p, '/');
    if (!q) { strncpy(out, "unknown", out_len); return; }
    size_t len = (size_t)(q - p);
    if (len >= out_len) len = out_len - 1;
    strncpy(out, p, len);
    out[len] = '\0';
}

/* ─────────────────────────────────────────────────────────────
 * WebSocket 콜백
 * ───────────────────────────────────────────────────────────── */
void onopen(ws_cli_conn_t client) {
    printf("[WS] Connected: %s\n", ws_getaddress(client));
    pthread_mutex_lock(&g_mutex);
    for (int i = 0; i < g_frame_count; i++) {
        if (g_frames[i].data && g_frames[i].size > 0)
            ws_sendframe_bin(client,
                (const char *)g_frames[i].data,
                g_frames[i].size);
    }
    pthread_mutex_unlock(&g_mutex);
}

void onclose(ws_cli_conn_t client) {
    printf("[WS] Disconnected: %s\n", ws_getaddress(client));
}

void onmessage(ws_cli_conn_t client,
               const unsigned char *msg, uint64_t size, int type) {
    (void)client; (void)msg; (void)size; (void)type;
}

/* ─────────────────────────────────────────────────────────────
 * MQTT 콜백
 * ───────────────────────────────────────────────────────────── */

/* 브로커 연결 성공 시 — 세 토픽 모두 재구독
 * 재연결 시에도 호출되므로 구독을 여기서 등록하면
 * 최초 연결 / failover 재연결 모두 보장된다. */
static void on_mqtt_connect(struct mosquitto *mosq, void *ud, int rc) {
    (void)ud;
    if (rc != 0) {
        fprintf(stderr, "[MQTT] Connect failed rc=%d\n", rc);
        return;
    }
    pthread_mutex_lock(&g_mutex);
    g_connected = 1;
    pthread_mutex_unlock(&g_mutex);

    printf("[MQTT] Connected to %s (%s)\n",
           BROKERS[g_broker_idx].host,
           BROKERS[g_broker_idx].label);

    mosquitto_subscribe(mosq, NULL, TOPIC_FRAME,  0);
    mosquitto_subscribe(mosq, NULL, TOPIC_EVENT,  2);
    mosquitto_subscribe(mosq, NULL, TOPIC_STATUS, 1);
}

/*
 * 브로커 연결 끊김 시 호출
 *
 * ⚠️  콜백은 libmosquitto 내부 네트워크 스레드에서 실행된다.
 *     여기서 mosquitto_connect()를 직접 호출하면 deadlock 위험.
 *     플래그만 세우고 실제 재연결은 메인 루프에서 처리한다.
 */
static void on_mqtt_disconnect(struct mosquitto *mosq, void *ud, int rc) {
    (void)ud;
    (void)mosq;
    pthread_mutex_lock(&g_mutex);
    g_connected = 0;
    pthread_mutex_unlock(&g_mutex);

    if (rc != 0) {
        printf("[MQTT] Disconnected from %s. Triggering failover...\n",
               BROKERS[g_broker_idx].label);
        g_need_failover = 1;
    }
}

/* 메시지 수신 시 처리 */
static void on_mqtt_message(struct mosquitto *mosq, void *ud,
                             const struct mosquitto_message *msg) {
    (void)mosq; (void)ud;
    if (!msg->payload || msg->payloadlen <= 0) return;

    char cam_id[16];
    extract_cam_id(msg->topic, cam_id, sizeof(cam_id));

    /* ── frame 토픽: dedup 없이 binary broadcast ─────────────
     * QoS 0, 손실 허용. 중복 수신돼도 화면 갱신만 되므로 무방. */
    if (strstr(msg->topic, "/frame")) {
        pthread_mutex_lock(&g_mutex);
        FrameCache *c = get_cache(cam_id);
        if (c) {
            free(c->data);
            c->data = malloc(msg->payloadlen);
            if (c->data) {
                memcpy(c->data, msg->payload, msg->payloadlen);
                c->size = (size_t)msg->payloadlen;
            }
        }
        pthread_mutex_unlock(&g_mutex);
        ws_sendframe_bcast(WS_PORT,
            (const char *)msg->payload,
            (uint64_t)msg->payloadlen,
            WS_FR_OP_BIN);
        return;
    }

    /* ── event / status 토픽: dedup 적용 후 text broadcast ───
     * B3↔B4 양방향 브릿지로 인해 동일 메시지가 두 경로로 도달 가능.
     * cam_id + ts 조합으로 id를 만들어 중복 차단. */
    char msg_id[ID_MAX_LEN];
    extract_msg_id(cam_id, (const char *)msg->payload,
                   msg_id, sizeof(msg_id));

    if (is_duplicate(msg_id)) {
        printf("[DEDUP] DROP  topic=%s id=%s\n", msg->topic, msg_id);
        return;
    }
    printf("[DEDUP] ACCEPT topic=%s id=%s\n", msg->topic, msg_id);

    char wrapper[1024];
    const char *type = strstr(msg->topic, "/event") ? "event" : "status";
    snprintf(wrapper, sizeof(wrapper),
             "{\"type\":\"%s\",\"cam\":\"%s\",\"data\":%.*s}",
             type, cam_id,
             msg->payloadlen, (char *)msg->payload);

    ws_sendframe_bcast(WS_PORT,
        wrapper, (uint64_t)strlen(wrapper),
        WS_FR_OP_TXT);

    printf("[MQTT] %s/%s → WS: %s\n", type, cam_id, wrapper);
}

/* ─────────────────────────────────────────────────────────────
 * failover 실행
 *
 * 민준이 방식: loop_stop → disconnect → 인덱스 이동 → connect → loop_start
 * 내부 상태를 완전히 정리한 뒤 새 브로커로 재연결한다.
 * ───────────────────────────────────────────────────────────── */
static void do_failover(struct mosquitto *mosq)
{
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] Switching to %s (%s)\n",
           BROKERS[g_broker_idx].host,
           BROKERS[g_broker_idx].label);

    mosquitto_loop_stop(mosq, true);
    mosquitto_disconnect(mosq);

    int rc = mosquitto_connect(mosq,
                               BROKERS[g_broker_idx].host,
                               BROKERS[g_broker_idx].port,
                               60);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] Connect failed to %s, will retry\n",
                BROKERS[g_broker_idx].label);
        sleep(1);
        g_need_failover = 1;
        return;
    }

    mosquitto_loop_start(mosq);
    g_need_failover = 0;
}

/* ─────────────────────────────────────────────────────────────
 * main
 * ───────────────────────────────────────────────────────────── */
int main(void) {
    /* 1. WebSocket 서버 (백그라운드 스레드) */
    ws_socket(&(struct ws_server){
        .host          = "0.0.0.0",
        .port          = WS_PORT,
        .thread_loop   = 1,
        .timeout_ms    = 1000,
        .evs.onopen    = &onopen,
        .evs.onclose   = &onclose,
        .evs.onmessage = &onmessage,
    });
    printf("[WS] Server started on port %d\n", WS_PORT);

    /* 2. MQTT 초기화 */
    mosquitto_lib_init();
    g_mosq = mosquitto_new("cctv_sub_edge_c", false, NULL);
    if (!g_mosq) { perror("mosquitto_new"); return -1; }

    mosquitto_connect_callback_set(g_mosq, on_mqtt_connect);
    mosquitto_disconnect_callback_set(g_mosq, on_mqtt_disconnect);
    mosquitto_message_callback_set(g_mosq, on_mqtt_message);

    /* reconnect delay 설정 (민준이 방식) */
    mosquitto_reconnect_delay_set(g_mosq, 1, 5, false);

    if (mosquitto_connect(g_mosq,
                          BROKERS[g_broker_idx].host,
                          BROKERS[g_broker_idx].port,
                          60) != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Initial connect failed\n");
        return -1;
    }

    mosquitto_loop_start(g_mosq);

    /*
     * 3. 수동 메인 루프
     * loop_forever 대신 usleep(100ms) 루프를 사용해
     * g_need_failover 플래그를 주기적으로 확인한다.
     */
    while (1) {
        if (g_need_failover) {
            do_failover(g_mosq);
        }
        usleep(100 * 1000);  /* 100ms */
    }

    /* 정상 종료 시 cleanup */
    mosquitto_loop_stop(g_mosq, true);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    for (int i = 0; i < g_frame_count; i++)
        free(g_frames[i].data);
    return 0;
}