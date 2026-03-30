#include "common.h"
#include <pthread.h>

static int              g_sockfd    = -1;
static volatile int     g_connected = 0;
static pthread_mutex_t  g_lock      = PTHREAD_MUTEX_INITIALIZER;

static char             g_nick[256];
static struct sockaddr_in g_srv_addr;

/* ------------------------------------------------------------------ */
/* Receiver thread: prints incoming messages                           */
/* ------------------------------------------------------------------ */
static void *receiver(void *arg)
{
    (void)arg;
    Message in;

    while (1) {
        /* Wait until connected */
        pthread_mutex_lock(&g_lock);
        int fd = g_sockfd;
        int connected = g_connected;
        pthread_mutex_unlock(&g_lock);

        if (!connected || fd < 0) {
            struct timespec ts = {0, 100000000L}; /* 100 ms */
            nanosleep(&ts, NULL);
            continue;
        }

        if (recv_msg(fd, &in) < 0) {
            pthread_mutex_lock(&g_lock);
            if (g_sockfd == fd)   /* still the same socket? */
                g_connected = 0;
            pthread_mutex_unlock(&g_lock);
            continue;
        }

        int payload_len = (int)in.length - (int)sizeof(in.type);

        switch (in.type) {
        case MSG_TEXT:
            if (payload_len > 0)
                printf("\r%.*s\n> ", payload_len, in.payload);
            else
                printf("\r[empty]\n> ");
            fflush(stdout);
            break;

        case MSG_PONG:
            printf("\rPONG\n> ");
            fflush(stdout);
            break;

        default:
            break;
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Connect + HELLO/WELCOME handshake                                   */
/* ------------------------------------------------------------------ */
static int do_connect(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    if (connect(fd, (struct sockaddr *)&g_srv_addr, sizeof(g_srv_addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }

    Message out, in;
    size_t nick_len = strlen(g_nick);

    memset(&out, 0, sizeof(out));
    out.type   = MSG_HELLO;
    out.length = sizeof(out.type) + (uint32_t)nick_len;
    memcpy(out.payload, g_nick, nick_len);
    if (send_msg(fd, &out) < 0) { close(fd); return -1; }

    if (recv_msg(fd, &in) < 0 || in.type != MSG_WELCOME) {
        fprintf(stderr, "Handshake failed\n");
        close(fd);
        return -1;
    }

    return fd;
}

/* ------------------------------------------------------------------ */
/* Main                                                                 */
/* ------------------------------------------------------------------ */
int main(void)
{
    printf("Enter your nickname: ");
    fflush(stdout);
    if (!fgets(g_nick, sizeof(g_nick), stdin)) return 1;
    size_t nl = strlen(g_nick);
    if (nl > 0 && g_nick[nl - 1] == '\n') g_nick[--nl] = '\0';
    if (nl == 0) strcpy(g_nick, "user");

    memset(&g_srv_addr, 0, sizeof(g_srv_addr));
    g_srv_addr.sin_family      = AF_INET;
    g_srv_addr.sin_port        = SERVER_PORT;
    g_srv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    /* Initial connection */
    g_sockfd = do_connect();
    if (g_sockfd < 0) {
        fprintf(stderr, "Could not connect to server\n");
        return 1;
    }
    g_connected = 1;
    printf("Connected. Welcome, %s!\n", g_nick);
    printf("Commands: /ping  /quit\n");

    /* Start receiver thread */
    pthread_t recv_tid;
    pthread_create(&recv_tid, NULL, receiver, NULL);
    pthread_detach(recv_tid);

    /* Main input loop */
    char buf[MAX_PAYLOAD];
    while (1) {
        /* Reconnect if disconnected */
        pthread_mutex_lock(&g_lock);
        int connected = g_connected;
        pthread_mutex_unlock(&g_lock);

        if (!connected) {
            printf("Disconnected. Reconnecting in 2 seconds...\n");
            sleep(2);

            pthread_mutex_lock(&g_lock);
            if (g_sockfd >= 0) {
                shutdown(g_sockfd, SHUT_RDWR);
                close(g_sockfd);
                g_sockfd = -1;
            }
            pthread_mutex_unlock(&g_lock);

            int fd = do_connect();
            if (fd < 0) {
                printf("Reconnect failed, retrying...\n");
                continue;
            }

            pthread_mutex_lock(&g_lock);
            g_sockfd    = fd;
            g_connected = 1;
            pthread_mutex_unlock(&g_lock);
            printf("Reconnected as %s\n> ", g_nick);
            fflush(stdout);
            continue;
        }

        printf("> ");
        fflush(stdout);
        if (!fgets(buf, sizeof(buf), stdin)) break;

        size_t len = strlen(buf);
        if (len > 0 && buf[len - 1] == '\n') buf[--len] = '\0';
        if (len == 0) continue;

        pthread_mutex_lock(&g_lock);
        int fd = g_sockfd;
        pthread_mutex_unlock(&g_lock);

        Message out;
        memset(&out, 0, sizeof(out));

        if (strcmp(buf, "/ping") == 0) {
            out.type   = MSG_PING;
            out.length = sizeof(out.type);
            if (send_msg(fd, &out) < 0) {
                pthread_mutex_lock(&g_lock);
                g_connected = 0;
                pthread_mutex_unlock(&g_lock);
            }
        } else if (strcmp(buf, "/quit") == 0) {
            out.type   = MSG_BYE;
            out.length = sizeof(out.type);
            send_msg(fd, &out);
            break;
        } else {
            out.type   = MSG_TEXT;
            out.length = sizeof(out.type) + (uint32_t)len;
            memcpy(out.payload, buf, len);
            if (send_msg(fd, &out) < 0) {
                pthread_mutex_lock(&g_lock);
                g_connected = 0;
                pthread_mutex_unlock(&g_lock);
            }
        }
    }

    pthread_mutex_lock(&g_lock);
    if (g_sockfd >= 0) close(g_sockfd);
    pthread_mutex_unlock(&g_lock);

    printf("Disconnected\n");
    return 0;
}
