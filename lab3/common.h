#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <errno.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define SERVER_PORT htons(5052)
#define MAX_PAYLOAD 1024

typedef struct {
    uint32_t length;
    uint8_t  type;
    char     payload[MAX_PAYLOAD];
} __attribute__((packed)) Message;

enum {
    MSG_HELLO   = 1,
    MSG_WELCOME = 2,
    MSG_TEXT    = 3,
    MSG_PING    = 4,
    MSG_PONG    = 5,
    MSG_BYE     = 6
};

static inline int send_msg(int fd, const Message *msg)
{
    size_t total = sizeof(msg->length) + msg->length;
    ssize_t n = send(fd, msg, total, 0);
    if (n < 0) {
        perror("send");
        return -1;
    }
    return 0;
}

static inline int recv_exact(int fd, void *buf, size_t count)
{
    size_t received = 0;
    while (received < count) {
        ssize_t n = recv(fd, (char *)buf + received, count - received, 0);
        if (n == 0)
            return -1;
        if (n < 0) {
            perror("recv");
            return -1;
        }
        received += (size_t)n;
    }
    return 0;
}

static inline int recv_msg(int fd, Message *msg)
{
    if (recv_exact(fd, &msg->length, sizeof(msg->length)) < 0)
        return -1;

    if (msg->length > MAX_PAYLOAD + sizeof(msg->type)) {
        fprintf(stderr, "Message too long: %u\n", msg->length);
        return -1;
    }

    if (recv_exact(fd, &msg->type, msg->length) < 0)
        return -1;

    return 0;
}

static inline void format_addr(const struct sockaddr_in *addr, char *out, size_t out_len)
{
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
    snprintf(out, out_len, "%s:%d", ip, ntohs(addr->sin_port));
}

#endif
