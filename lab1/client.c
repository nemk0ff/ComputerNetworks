#include "common.h"

int main(void)
{
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    struct timeval tv = { .tv_sec = 3, .tv_usec = 0 };
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in srv_addr;
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family      = AF_INET;
    srv_addr.sin_port        = SERVER_PORT;
    srv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    char message[BUFSIZE];
    char reply[BUFSIZE];

    printf("UDP echo client (server 127.0.0.1:%d)\n", ntohs(SERVER_PORT));
    printf("Type a message or /quit to exit.\n");

    while (1) {
        printf("> ");
        fflush(stdout);

        if (!fgets(message, sizeof(message), stdin))
            break;

        size_t len = strlen(message);
        if (len > 0 && message[len - 1] == '\n') {
            message[--len] = '\0';
        }
        if (len == 0)
            continue;

        if (strcmp(message, "/quit") == 0)
            break;

        ssize_t sent = sendto(sockfd, message, len, 0,
                              (struct sockaddr *)&srv_addr, sizeof(srv_addr));
        if (sent < 0) {
            perror("sendto");
            continue;
        }

        socklen_t addr_len = sizeof(srv_addr);
        ssize_t n = recvfrom(sockfd, reply, sizeof(reply) - 1, 0,
                             (struct sockaddr *)&srv_addr, &addr_len);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                fprintf(stderr, "Timeout: no response from server\n");
            else
                perror("recvfrom");
            continue;
        }
        reply[n] = '\0';
        printf("< %s\n", reply);
    }

    close(sockfd);
    printf("Disconnected.\n");
    return 0;
}
