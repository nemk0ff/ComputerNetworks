#include "common.h"

int main(void)
{
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_in srv_addr;
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family      = AF_INET;
    srv_addr.sin_port        = SERVER_PORT;
    srv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(sockfd, (struct sockaddr *)&srv_addr, sizeof(srv_addr)) < 0) {
        perror("connect");
        close(sockfd);
        return 1;
    }

    Message out, in;

    /* Ask for nickname */
    char nick[256];
    printf("Enter your nickname: ");
    fflush(stdout);
    if (!fgets(nick, sizeof(nick), stdin)) {
        close(sockfd);
        return 1;
    }
    size_t nick_len = strlen(nick);
    if (nick_len > 0 && nick[nick_len - 1] == '\n')
        nick[--nick_len] = '\0';
    if (nick_len == 0) {
        strcpy(nick, "user");
        nick_len = 4;
    }

    /* Send MSG_HELLO with nickname */
    memset(&out, 0, sizeof(out));
    out.type   = MSG_HELLO;
    out.length = sizeof(out.type) + (uint32_t)nick_len;
    memcpy(out.payload, nick, nick_len);

    if (send_msg(sockfd, &out) < 0)
        goto EXIT;

    /* Expect MSG_WELCOME */
    if (recv_msg(sockfd, &in) < 0)
        goto EXIT;
    if (in.type != MSG_WELCOME) {
        fprintf(stderr, "Expected MSG_WELCOME, got %d\n", in.type);
        goto EXIT;
    }

    char addr_str[64];
    format_addr(&srv_addr, addr_str, sizeof(addr_str));
    printf("Connected\nWelcome %s\n", addr_str);

    /* Main loop */
    char buf[MAX_PAYLOAD];
    while (1) {
        printf("> ");
        fflush(stdout);

        if (!fgets(buf, sizeof(buf), stdin))
            break;

        size_t len = strlen(buf);
        if (len > 0 && buf[len - 1] == '\n')
            buf[--len] = '\0';
        if (len == 0)
            continue;

        if (strcmp(buf, "/ping") == 0) {
            memset(&out, 0, sizeof(out));
            out.type   = MSG_PING;
            out.length = sizeof(out.type);
            if (send_msg(sockfd, &out) < 0) break;
            if (recv_msg(sockfd, &in) < 0) break;
            if (in.type != MSG_PONG) {
                fprintf(stderr, "Expected MSG_PONG, got %d\n", in.type);
                break;
            }
            printf("PONG\n");
        } else if (strcmp(buf, "/quit") == 0) {
            memset(&out, 0, sizeof(out));
            out.type   = MSG_BYE;
            out.length = sizeof(out.type);
            send_msg(sockfd, &out);
            break;
        } else {
            memset(&out, 0, sizeof(out));
            out.type   = MSG_TEXT;
            out.length = sizeof(out.type) + (uint32_t)len;
            memcpy(out.payload, buf, len);
            if (send_msg(sockfd, &out) < 0) break;
        }
    }

EXIT:
    close(sockfd);
    printf("Disconnected\n");
    return 0;
}
