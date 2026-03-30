#include "common.h"
#include <pthread.h>

#define THREAD_POOL_SIZE 10
#define QUEUE_SIZE       128
#define MAX_CLIENTS      128

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
/* Connected-client list (for broadcast)                               */
/* ------------------------------------------------------------------ */
typedef struct {
    int  sock;
    char addr[64];
    char nickname[MAX_PAYLOAD + 1];
} Client;

static Client         clients[MAX_CLIENTS];
static int            client_count = 0;
static pthread_mutex_t clients_lock = PTHREAD_MUTEX_INITIALIZER;

static void add_client(int sock, const char *addr, const char *nick)
{
    pthread_mutex_lock(&clients_lock);
    if (client_count < MAX_CLIENTS) {
        clients[client_count].sock = sock;
        strncpy(clients[client_count].addr,     addr, 63);
        strncpy(clients[client_count].nickname, nick, MAX_PAYLOAD);
        clients[client_count].addr[63]         = '\0';
        clients[client_count].nickname[MAX_PAYLOAD] = '\0';
        client_count++;
    }
    pthread_mutex_unlock(&clients_lock);
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

/* Sends msg to every client except sender_sock */
static void broadcast(const Message *msg, int sender_sock)
{
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].sock != sender_sock)
            send_msg(clients[i].sock, msg);
    }
    pthread_mutex_unlock(&clients_lock);
}

/* ------------------------------------------------------------------ */
/* Per-connection handler (runs inside a worker thread)                */
/* ------------------------------------------------------------------ */
static void handle_client(int conn_fd, struct sockaddr_in *cli_addr)
{
    char addr_str[64];
    format_addr(cli_addr, addr_str, sizeof(addr_str));

    Message in, out;
    char nickname[MAX_PAYLOAD + 1] = {0};

    /* Step 1: expect MSG_HELLO with nickname */
    if (recv_msg(conn_fd, &in) < 0 || in.type != MSG_HELLO) {
        fprintf(stderr, "[%s] Expected MSG_HELLO, closing.\n", addr_str);
        close(conn_fd);
        return;
    }

    int nick_len = (int)in.length - (int)sizeof(in.type);
    if (nick_len > 0 && nick_len <= MAX_PAYLOAD) {
        memcpy(nickname, in.payload, (size_t)nick_len);
        nickname[nick_len] = '\0';
    } else {
        strcpy(nickname, "anonymous");
    }

    printf("Client connected: %s [%s]\n", nickname, addr_str);

    /* Step 2: send MSG_WELCOME */
    memset(&out, 0, sizeof(out));
    out.type   = MSG_WELCOME;
    out.length = sizeof(out.type);
    if (send_msg(conn_fd, &out) < 0) {
        close(conn_fd);
        return;
    }

    add_client(conn_fd, addr_str, nickname);

    /* Step 3: message loop */
    bool running = true;
    while (running) {
        if (recv_msg(conn_fd, &in) < 0) {
            printf("Client disconnected: %s [%s]\n", nickname, addr_str);
            break;
        }

        int payload_len = (int)in.length - (int)sizeof(in.type);

        switch (in.type) {
        case MSG_TEXT: {
            char text[MAX_PAYLOAD + 1] = {0};
            if (payload_len > 0 && payload_len <= MAX_PAYLOAD)
                memcpy(text, in.payload, (size_t)payload_len);

            /* Log on server */
            printf("%s [%s]: %s\n", nickname, addr_str, text);

            /* Build broadcast payload: "nickname [addr]: text" */
            char bcast[MAX_PAYLOAD + 1];
            int blen = snprintf(bcast, sizeof(bcast), "%s [%s]: %s",
                                nickname, addr_str, text);
            if (blen < 0) blen = 0;
            if (blen > MAX_PAYLOAD) blen = MAX_PAYLOAD;

            memset(&out, 0, sizeof(out));
            out.type   = MSG_TEXT;
            out.length = sizeof(out.type) + (uint32_t)blen;
            memcpy(out.payload, bcast, (size_t)blen);
            broadcast(&out, conn_fd);
            break;
        }

        case MSG_PING:
            memset(&out, 0, sizeof(out));
            out.type   = MSG_PONG;
            out.length = sizeof(out.type);
            if (send_msg(conn_fd, &out) < 0)
                running = false;
            break;

        case MSG_BYE:
            printf("Client disconnected: %s [%s]\n", nickname, addr_str);
            running = false;
            break;

        default:
            fprintf(stderr, "[%s] Unknown message type: %d\n", addr_str, in.type);
            break;
        }
    }

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

    /* Create thread pool */
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
