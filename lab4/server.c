#include "common.h"
#include <pthread.h>

#define THREAD_POOL_SIZE 10
#define QUEUE_SIZE       128
#define MAX_CLIENTS      64

/* ------------------------------------------------------------------ */
/* OSI-layer logging                                                    */
/* ------------------------------------------------------------------ */
static void osi_log(int layer, const char *label, const char *msg)
{
    printf("[Layer %d - %s] %s\n", layer, label, msg);
}

/* ------------------------------------------------------------------ */
/* Connection queue                                                     */
/* ------------------------------------------------------------------ */
typedef struct {
    int              fds[QUEUE_SIZE];
    struct sockaddr_in addrs[QUEUE_SIZE];
    int              head, tail, count;
    pthread_mutex_t  lock;
    pthread_cond_t   not_empty;
    pthread_cond_t   not_full;
} ConnQueue;

static ConnQueue queue;

static void queue_init(ConnQueue *q)
{
    q->head = q->tail = q->count = 0;
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

static void queue_push(ConnQueue *q, int fd, struct sockaddr_in *addr)
{
    pthread_mutex_lock(&q->lock);
    while (q->count == QUEUE_SIZE)
        pthread_cond_wait(&q->not_full, &q->lock);
    q->fds[q->tail]   = fd;
    q->addrs[q->tail] = *addr;
    q->tail = (q->tail + 1) % QUEUE_SIZE;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

static void queue_pop(ConnQueue *q, int *fd, struct sockaddr_in *addr)
{
    pthread_mutex_lock(&q->lock);
    while (q->count == 0)
        pthread_cond_wait(&q->not_empty, &q->lock);
    *fd   = q->fds[q->head];
    *addr = q->addrs[q->head];
    q->head = (q->head + 1) % QUEUE_SIZE;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);
}

/* ------------------------------------------------------------------ */
/* Client registry                                                      */
/* ------------------------------------------------------------------ */
typedef struct {
    int  sock;
    char nickname[NICK_MAX];
    int  authenticated;
} Client;

static Client          clients[MAX_CLIENTS];
static int             client_count = 0;
static pthread_mutex_t clients_lock = PTHREAD_MUTEX_INITIALIZER;

/* Returns 1 if nick is not in use (must be called with clients_lock held) */
static int nick_available(const char *nick)
{
    for (int i = 0; i < client_count; i++) {
        if (clients[i].authenticated &&
            strcmp(clients[i].nickname, nick) == 0)
            return 0;
    }
    return 1;
}

static int add_unauthenticated(int sock)
{
    pthread_mutex_lock(&clients_lock);
    if (client_count >= MAX_CLIENTS) {
        pthread_mutex_unlock(&clients_lock);
        return -1;
    }
    clients[client_count].sock          = sock;
    clients[client_count].nickname[0]   = '\0';
    clients[client_count].authenticated = 0;
    client_count++;
    pthread_mutex_unlock(&clients_lock);
    return 0;
}

/* Try to authenticate a client; returns -1 if nick is taken */
static int authenticate(int sock, const char *nick)
{
    pthread_mutex_lock(&clients_lock);
    if (!nick_available(nick)) {
        pthread_mutex_unlock(&clients_lock);
        return -1;
    }
    for (int i = 0; i < client_count; i++) {
        if (clients[i].sock == sock) {
            strncpy(clients[i].nickname, nick, NICK_MAX - 1);
            clients[i].nickname[NICK_MAX - 1] = '\0';
            clients[i].authenticated = 1;
            pthread_mutex_unlock(&clients_lock);
            return 0;
        }
    }
    pthread_mutex_unlock(&clients_lock);
    return -1;
}

static void remove_client(int sock)
{
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].sock == sock) {
            clients[i] = clients[--client_count];
            break;
        }
    }
    pthread_mutex_unlock(&clients_lock);
}

/* Broadcast to all authenticated clients except sender */
static void broadcast_except(const Message *msg, int sender_sock)
{
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].sock != sender_sock && clients[i].authenticated)
            send_msg(clients[i].sock, msg);
    }
    pthread_mutex_unlock(&clients_lock);
}

/* Route a private message; returns -1 if recipient not found */
static int send_to_nick(const char *nick, const Message *msg)
{
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].authenticated &&
            strcmp(clients[i].nickname, nick) == 0) {
            send_msg(clients[i].sock, msg);
            pthread_mutex_unlock(&clients_lock);
            return 0;
        }
    }
    pthread_mutex_unlock(&clients_lock);
    return -1;
}

/* ------------------------------------------------------------------ */
/* Helpers: build and send control messages                            */
/* ------------------------------------------------------------------ */
static void send_error(int fd, const char *text)
{
    Message out;
    memset(&out, 0, sizeof(out));
    out.type = MSG_ERROR;
    size_t tlen = strlen(text);
    if (tlen > MAX_PAYLOAD) tlen = MAX_PAYLOAD;
    out.length = sizeof(out.type) + (uint32_t)tlen;
    memcpy(out.payload, text, tlen);

    osi_log(7, "Application",   "prepare MSG_ERROR");
    osi_log(6, "Presentation",  "serialize Message");
    osi_log(4, "Transport",     "send()");
    send_msg(fd, &out);
}

static void send_server_info(int fd, const char *text)
{
    Message out;
    memset(&out, 0, sizeof(out));
    out.type = MSG_SERVER_INFO;
    size_t tlen = strlen(text);
    if (tlen > MAX_PAYLOAD) tlen = MAX_PAYLOAD;
    out.length = sizeof(out.type) + (uint32_t)tlen;
    memcpy(out.payload, text, tlen);

    osi_log(7, "Application",   "prepare MSG_SERVER_INFO");
    osi_log(6, "Presentation",  "serialize Message");
    osi_log(4, "Transport",     "send()");
    send_msg(fd, &out);
}

static void broadcast_server_info(const char *text, int exclude_sock)
{
    Message out;
    memset(&out, 0, sizeof(out));
    out.type = MSG_SERVER_INFO;
    size_t tlen = strlen(text);
    if (tlen > MAX_PAYLOAD) tlen = MAX_PAYLOAD;
    out.length = sizeof(out.type) + (uint32_t)tlen;
    memcpy(out.payload, text, tlen);
    broadcast_except(&out, exclude_sock);
}

/* ------------------------------------------------------------------ */
/* Per-connection handler                                               */
/* ------------------------------------------------------------------ */
static void handle_client(int conn_fd, struct sockaddr_in *cli_addr)
{
    char addr_str[64];
    format_addr(cli_addr, addr_str, sizeof(addr_str));
    char nickname[NICK_MAX] = {0};

    printf("Client connected: [%s]\n", addr_str);

    Message in, out;

    /* ---- Step 1: HELLO / WELCOME handshake (from ЛР3) ---- */
    osi_log(4, "Transport",    "recv()");
    if (recv_msg(conn_fd, &in) < 0 || in.type != MSG_HELLO) {
        fprintf(stderr, "[%s] Expected MSG_HELLO\n", addr_str);
        close(conn_fd);
        return;
    }
    osi_log(6, "Presentation", "deserialize Message");
    osi_log(5, "Session",      "MSG_HELLO received");

    memset(&out, 0, sizeof(out));
    out.type   = MSG_WELCOME;
    out.length = sizeof(out.type);
    osi_log(7, "Application",   "prepare MSG_WELCOME");
    osi_log(6, "Presentation",  "serialize Message");
    osi_log(4, "Transport",     "send()");
    if (send_msg(conn_fd, &out) < 0) {
        close(conn_fd);
        return;
    }

    /* Register as unauthenticated */
    if (add_unauthenticated(conn_fd) < 0) {
        send_error(conn_fd, "Server is full");
        close(conn_fd);
        return;
    }

    /* ---- Step 2: Wait for MSG_AUTH ---- */
    osi_log(4, "Transport", "recv()");
    if (recv_msg(conn_fd, &in) < 0) {
        fprintf(stderr, "[%s] Lost connection before AUTH\n", addr_str);
        remove_client(conn_fd);
        close(conn_fd);
        return;
    }
    osi_log(6, "Presentation", "deserialize Message");

    if (in.type != MSG_AUTH) {
        fprintf(stderr, "[%s] Expected MSG_AUTH, got %d\n", addr_str, in.type);
        send_error(conn_fd, "Authentication required");
        remove_client(conn_fd);
        close(conn_fd);
        return;
    }
    osi_log(6, "Presentation", "parsed MSG_AUTH");

    int nick_len = (int)in.length - (int)sizeof(in.type);
    if (nick_len <= 0 || nick_len >= NICK_MAX) {
        send_error(conn_fd, "Invalid nickname length");
        remove_client(conn_fd);
        close(conn_fd);
        return;
    }
    memcpy(nickname, in.payload, (size_t)nick_len);
    nickname[nick_len] = '\0';

    if (authenticate(conn_fd, nickname) < 0) {
        send_error(conn_fd, "Nickname already taken");
        remove_client(conn_fd);
        close(conn_fd);
        return;
    }
    osi_log(5, "Session", "authentication success");
    printf("User [%s] connected\n", nickname);

    /* Notify the new client */
    char info[64];
    snprintf(info, sizeof(info), "Welcome, %s!", nickname);
    send_server_info(conn_fd, info);

    /* Announce to everyone else */
    snprintf(info, sizeof(info), "User [%s] connected", nickname);
    broadcast_server_info(info, conn_fd);

    /* ---- Step 3: Message loop ---- */
    bool running = true;
    while (running) {
        osi_log(4, "Transport", "recv()");
        if (recv_msg(conn_fd, &in) < 0) {
            printf("User [%s] disconnected (connection lost)\n", nickname);
            break;
        }
        osi_log(6, "Presentation", "deserialize Message");
        osi_log(5, "Session",      "client authenticated");

        int payload_len = (int)in.length - (int)sizeof(in.type);

        switch (in.type) {

        case MSG_TEXT: {
            osi_log(7, "Application", "handle MSG_TEXT");
            char text[MAX_PAYLOAD + 1] = {0};
            if (payload_len > 0 && payload_len <= MAX_PAYLOAD)
                memcpy(text, in.payload, (size_t)payload_len);

            char bcast[MAX_PAYLOAD + 1];
            int blen = snprintf(bcast, sizeof(bcast), "[%s]: %s", nickname, text);
            if (blen < 0) blen = 0;
            if (blen > MAX_PAYLOAD) blen = MAX_PAYLOAD;

            printf("%s\n", bcast);

            Message bmsg;
            memset(&bmsg, 0, sizeof(bmsg));
            bmsg.type   = MSG_TEXT;
            bmsg.length = sizeof(bmsg.type) + (uint32_t)blen;
            memcpy(bmsg.payload, bcast, (size_t)blen);

            osi_log(7, "Application",  "broadcast message");
            osi_log(6, "Presentation", "serialize Message");
            osi_log(4, "Transport",    "send()");
            broadcast_except(&bmsg, conn_fd);
            break;
        }

        case MSG_PING:
            osi_log(7, "Application", "handle MSG_PING");
            memset(&out, 0, sizeof(out));
            out.type   = MSG_PONG;
            out.length = sizeof(out.type);
            osi_log(7, "Application",  "prepare MSG_PONG");
            osi_log(6, "Presentation", "serialize Message");
            osi_log(4, "Transport",    "send()");
            if (send_msg(conn_fd, &out) < 0)
                running = false;
            break;

        case MSG_PRIVATE: {
            osi_log(7, "Application", "handle MSG_PRIVATE");
            /* payload format: "target_nick:message" */
            char buf[MAX_PAYLOAD + 1] = {0};
            if (payload_len > 0 && payload_len <= MAX_PAYLOAD)
                memcpy(buf, in.payload, (size_t)payload_len);

            char *colon = strchr(buf, ':');
            if (!colon) {
                send_error(conn_fd, "Bad format. Use: /w <nick> <message>");
                break;
            }
            *colon = '\0';
            char *target  = buf;
            char *message = colon + 1;

            char priv[MAX_PAYLOAD + 1];
            int plen = snprintf(priv, sizeof(priv),
                                "[PRIVATE][%s]: %s", nickname, message);
            if (plen < 0) plen = 0;
            if (plen > MAX_PAYLOAD) plen = MAX_PAYLOAD;

            Message pmsg;
            memset(&pmsg, 0, sizeof(pmsg));
            pmsg.type   = MSG_PRIVATE;
            pmsg.length = sizeof(pmsg.type) + (uint32_t)plen;
            memcpy(pmsg.payload, priv, (size_t)plen);

            osi_log(7, "Application",  "route private message");
            osi_log(6, "Presentation", "serialize Message");
            osi_log(4, "Transport",    "send()");
            if (send_to_nick(target, &pmsg) < 0)
                send_error(conn_fd, "User not found");
            break;
        }

        case MSG_BYE:
            osi_log(7, "Application", "handle MSG_BYE");
            printf("User [%s] disconnected\n", nickname);
            running = false;
            break;

        default:
            fprintf(stderr, "[%s] Unknown type %d\n", addr_str, in.type);
            break;
        }
    }

    /* Announce departure */
    char leave[64];
    snprintf(leave, sizeof(leave), "User [%s] disconnected", nickname);
    broadcast_server_info(leave, conn_fd);

    remove_client(conn_fd);
    close(conn_fd);
}

/* ------------------------------------------------------------------ */
/* Worker thread                                                        */
/* ------------------------------------------------------------------ */
static void *worker(void *arg)
{
    (void)arg;
    while (1) {
        int fd;
        struct sockaddr_in addr;
        queue_pop(&queue, &fd, &addr);
        handle_client(fd, &addr);
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Main                                                                 */
/* ------------------------------------------------------------------ */
int main(void)
{
    queue_init(&queue);

    pthread_t threads[THREAD_POOL_SIZE];
    for (int i = 0; i < THREAD_POOL_SIZE; i++)
        pthread_create(&threads[i], NULL, worker, NULL);

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in srv_addr;
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family      = AF_INET;
    srv_addr.sin_addr.s_addr = INADDR_ANY;
    srv_addr.sin_port        = SERVER_PORT;

    if (bind(sockfd, (struct sockaddr *)&srv_addr, sizeof(srv_addr)) < 0) {
        perror("bind"); close(sockfd); return 1;
    }
    if (listen(sockfd, 16) < 0) {
        perror("listen"); close(sockfd); return 1;
    }

    printf("TCP server listening on port %d (thread pool: %d)\n",
           ntohs(SERVER_PORT), THREAD_POOL_SIZE);

    while (1) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int conn_fd = accept(sockfd, (struct sockaddr *)&cli_addr, &cli_len);
        if (conn_fd < 0) { perror("accept"); continue; }
        queue_push(&queue, conn_fd, &cli_addr);
    }

    close(sockfd);
    return 0;
}
