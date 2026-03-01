#include "common.h"

static void handle_client(int conn_fd, struct sockaddr_in *cli_addr)
{
    char addr_str[64];
    format_addr(cli_addr, addr_str, sizeof(addr_str));

    Message in, out;
    char nickname[MAX_PAYLOAD + 1] = {0};

    // Step 1: expect MSG_HELLO
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

    // Step 2: send MSG_WELCOME
    memset(&out, 0, sizeof(out));
    out.type   = MSG_WELCOME;
    out.length = sizeof(out.type);
    if (send_msg(conn_fd, &out) < 0) {
        close(conn_fd);
        return;
    }

    // Step 3: message loop
    bool running = true;
    while (running) {
        if (recv_msg(conn_fd, &in) < 0) {
            printf("[%s] Connection lost.\n", addr_str);
            break;
        }

        int payload_len = (int)in.length - (int)sizeof(in.type);

        switch (in.type) {
        case MSG_TEXT:
            if (payload_len > 0)
                printf("[%s]: %.*s\n", addr_str, payload_len, in.payload);
            break;

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
            running = false;
            break;
        }
    }

    close(conn_fd);
}

int main(void)
{
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in srv_addr;
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family      = AF_INET;
    srv_addr.sin_addr.s_addr = INADDR_ANY;
    srv_addr.sin_port        = SERVER_PORT;

    if (bind(sockfd, (struct sockaddr *)&srv_addr, sizeof(srv_addr)) < 0) {
        perror("bind");
        close(sockfd);
        return 1;
    }

    if (listen(sockfd, 1) < 0) {
        perror("listen");
        close(sockfd);
        return 1;
    }

    printf("TCP server listening on port %d\n", ntohs(SERVER_PORT));

    while (1) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int conn_fd = accept(sockfd, (struct sockaddr *)&cli_addr, &cli_len);
        if (conn_fd < 0) {
            perror("accept");
            continue;
        }
        handle_client(conn_fd, &cli_addr);
    }

    close(sockfd);
    return 0;
}
