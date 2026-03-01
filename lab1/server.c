#include "common.h"

int main(void)
{
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
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

    printf("UDP echo server listening on port %d\n", ntohs(SERVER_PORT));

    char buf[BUFSIZE];
    struct sockaddr_in cli_addr;
    char addr_str[64];

    while (1) {
        socklen_t cli_len = sizeof(cli_addr);
        ssize_t n = recvfrom(sockfd, buf, sizeof(buf) - 1, 0,
                             (struct sockaddr *)&cli_addr, &cli_len);
        if (n < 0) {
            perror("recvfrom");
            continue;
        }
        buf[n] = '\0';

        format_addr(&cli_addr, addr_str, sizeof(addr_str));
        print_timestamp();
        printf("[%s]: %s\n", addr_str, buf);

        ssize_t sent = sendto(sockfd, buf, (size_t)n, 0,
                              (struct sockaddr *)&cli_addr, cli_len);
        if (sent < 0)
            perror("sendto");
    }

    close(sockfd);
    return 0;
}
