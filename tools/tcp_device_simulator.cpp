#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <iostream>
#include <string>

#include <nlohmann/json.hpp>

bool sendAll(int fd,const void* data,std::size_t size)
{
    const auto* buffer =static_cast<const std::uint8_t*>(data);

    std::size_t sent = 0;
    while (sent < size)
    {
        ssize_t n = send(fd,buffer + sent,size - sent,0);
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }

        if (n == 0)
        {
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

int main()
{
    int sockfd =socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0)
    {
        std::cerr<< "socket() failed"<< std::endl;
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(9000);

    if (inet_pton(AF_INET,"127.0.0.1",&server_addr.sin_addr) != 1)
    {
        std::cerr<< "inet_pton() failed"<< std::endl;
        close(sockfd);
        return 1;
    }
    if (connect(sockfd,reinterpret_cast<sockaddr*>(&server_addr),sizeof(server_addr)) < 0)
    {
        std::cerr<< "connect() failed"<< std::endl;
        close(sockfd);
        return 1;
    }

    std::cout<< "Connected to gateway"<< std::endl;

    nlohmann::json root;
    root["device_id"] = "sensor-001";
    root["timestamp_ms"] = 1757250000000ULL;
    root["sequence"] = 1;

    root["metrics"] = nlohmann::json::array({
        {
            {"name", "temperature"},
            {"value", 25.6},
            {"unit", "C"}
        },
        {
            {"name", "pressure"},
            {"value", 2.1},
            {"unit", "bar"}
        }
    });

    std::string payload = root.dump();

    std::uint32_t payload_length =static_cast<std::uint32_t>(payload.size());
    std::uint32_t network_length =htonl(payload_length);

    if (!sendAll(sockfd,&network_length,sizeof(network_length)))
    {
        std::cerr<< "Failed to send header"<< std::endl;

        close(sockfd);
        return 1;
    }

    if (!sendAll(sockfd,payload.data(),payload.size()))
    {
        std::cerr<< "Failed to send payload"<< std::endl;

        close(sockfd);
        return 1;
    }

    std::cout<< "Sent telemetry frame, payload="<< payload.size()<< " bytes"<< std::endl;

    close(sockfd);

    return 0;
}