/*
 * subscriber_ws.c  (개선판)
 *
 * 변경점:
 *   1. 토픽 분리 수신
 *        camera/+/frame   → WebSocket binary  (영상)
 *        camera/+/event   → WebSocket text    (이벤트 JSON)
 *        camera/+/status  → WebSocket text    (온·오프라인)
 *   2. 브로커 failover  → localhost(Edge C) → Core A → Core B → Core C
 *   3. 최신 프레임 캐시 유지 (신규 브라우저 접속 시 즉시 전송)
 *
 * Build:
 *   gcc -Wall -O2 -o subscriber_ws subscriber_ws.c \
 *       -lmosquitto -lws -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <mosquitto.h>
#include <wsserver/ws.h>

/* ── WebSocket 포트 ──────────────────────────────────────────── */
#define WS_PORT   8765

/* ── 브로커 failover 목록 ────────────────────────────────────── */
static const char *BROKERS[] = {
    "localhost",       /* Edge C (로컬 Mosquitto) */
    "192.168.1.101",   /* Core A */
    "192.168.1.102",   /* Core B */
    "192.168.1.103",   /* Core C */
};
#define BROKER_COUNT (int)(sizeof(BROKERS)/sizeof(BROKERS[0]))
#define MQTT_PORT    1883

/* ── 구독 토픽 ───────────────────────────────────────────────── */
#define TOPIC_FRAME   "camera/+/frame"
#define TOPIC_EVENT   "camera/+/event"
#define TOPIC_STATUS  "camera/+/status"

/* ── 최신 프레임 캐시 (pi1, pi2 각각) ───────────────────────── */
#define MAX_CAMS 4
typedef struct {
    char     cam_id[16];
    uint8_t *data;
    size_t   size;
} FrameCache;

static FrameCache       g_frames[MAX_CAMS];
static int              g_frame_count = 0;
static pthread_mutex_t  g_mutex       = PTHREAD_MUTEX_INITIALIZER;
static int              g_broker_idx  = 0;

/* cam_id 로 캐시 슬롯 찾기 (없으면 생성) */
static FrameCache *get_cache(const char *cam_id)
{
    for (int i = 0; i < g_frame_count; i++)
        if (strcmp(g_frames[i].cam_id, cam_id) == 0)
            return &g_frames[i];
    if (g_frame_count >= MAX_CAMS) return NULL;
    FrameCache *c = &g_frames[g_frame_count++];
    strncpy(c->cam_id, cam_id, sizeof(c->cam_id) - 1);
    c->data = NULL; c->size = 0;
    return c;
}

/* MQTT 토픽에서 cam_id 추출: "camera/pi1/frame" → "pi1" */
static void extract_cam_id(const char *topic, char *out, size_t out_len)
{
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
void onopen(ws_cli_conn_t client)
{
    printf("[WS] Connected: %s\n", ws_getaddress(client));

    /* 접속 즉시 캐시된 최신 프레임 전송 (카메라별) */
    pthread_mutex_lock(&g_mutex);
    for (int i = 0; i < g_frame_count; i++) {
        if (g_frames[i].data && g_frames[i].size > 0)
            ws_sendframe_bin(client,
                (const char *)g_frames[i].data,
                g_frames[i].size);
    }
    pthread_mutex_unlock(&g_mutex);
}

void onclose(ws_cli_conn_t client)
{
    printf("[WS] Disconnected: %s\n", ws_getaddress(client));
}

void onmessage(ws_cli_conn_t client,
               const unsigned char *msg, uint64_t size, int type)
{
    (void)client; (void)msg; (void)size; (void)type;
}

/* ─────────────────────────────────────────────────────────────
 * MQTT 콜백
 * ───────────────────────────────────────────────────────────── */
static void on_mqtt_connect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud;
    if (rc != 0) {
        fprintf(stderr, "[MQTT] Connect failed rc=%d\n", rc);
        return;
    }
    printf("[MQTT] Connected to %s\n", BROKERS[g_broker_idx]);

    /* 세 토픽 모두 구독 */
    mosquitto_subscribe(mosq, NULL, TOPIC_FRAME,  0);
    mosquitto_subscribe(mosq, NULL, TOPIC_EVENT,  2);
    mosquitto_subscribe(mosq, NULL, TOPIC_STATUS, 1);
}

static void on_mqtt_disconnect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud;
    if (rc != 0) {
        g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
        printf("[MQTT] Disconnected. Trying %s...\n",
               BROKERS[g_broker_idx]);
        sleep(2);
        mosquitto_connect(mosq, BROKERS[g_broker_idx], MQTT_PORT, 60);
    }
}

static void on_mqtt_message(struct mosquitto *mosq, void *ud,
                             const struct mosquitto_message *msg)
{
    (void)mosq; (void)ud;
    if (!msg->payload || msg->payloadlen <= 0) return;

    char cam_id[16];
    extract_cam_id(msg->topic, cam_id, sizeof(cam_id));

    /* ── frame 토픽: binary broadcast ───────────────────────── */
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

        /* 모든 브라우저에 JPEG binary 전송 */
        ws_sendframe_bcast(WS_PORT,
            (const char *)msg->payload,
            (uint64_t)msg->payloadlen,
            WS_FR_OP_BIN);

        return;
    }

    /* ── event / status 토픽: JSON text broadcast ───────────── */
    /* cam_id 를 포함한 래퍼 JSON 으로 감싸서 전송
       예) {"type":"event","cam":"pi1","data":{"event":"motion",...}} */
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
 * main
 * ───────────────────────────────────────────────────────────── */
int main(void)
{
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
    struct mosquitto *mosq = mosquitto_new(
        "cctv_sub_edge_c", false, NULL);
    if (!mosq) { perror("mosquitto_new"); return -1; }

    mosquitto_connect_callback_set(mosq, on_mqtt_connect);
    mosquitto_disconnect_callback_set(mosq, on_mqtt_disconnect);
    mosquitto_message_callback_set(mosq, on_mqtt_message);

    if (mosquitto_connect(mosq, BROKERS[g_broker_idx],
                          MQTT_PORT, 60) != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Initial connect failed\n");
        return -1;
    }

    /* 3. MQTT 루프 (메인 스레드) */
    mosquitto_loop_forever(mosq, -1, 1);

    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();

    for (int i = 0; i < g_frame_count; i++)
        free(g_frames[i].data);
    return 0;
}
