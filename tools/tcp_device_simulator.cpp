#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <thread>
#include <chrono>
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

std::vector<std::uint8_t> buildFrame(const std::string& payload)
{
    std::uint32_t payload_length =static_cast<std::uint32_t>(payload.size());
    std::uint32_t network_length =htonl(payload_length);

    std::vector<std::uint8_t> frame(sizeof(network_length)+ payload.size());
    std::memcpy(frame.data(),&network_length,sizeof(network_length));
    
    std::memcpy(frame.data() + sizeof(network_length),payload.data(),payload.size());
    return frame;
}

std::string buildTelemetryJson(std::uint64_t sequence,double temperature)
{
    nlohmann::json root;

    root["device_id"] = "sensor-001";
    root["timestamp_ms"] = 1757250000000ULL;
    root["sequence"] = sequence;

    root["metrics"] = nlohmann::json::array({
        {
            {"name", "temperature"},
            {"value", temperature},
            {"unit", "C"}
        },
        {
            {"name", "pressure"},
            {"value", 2.1},
            {"unit", "bar"}
        }
    });

    return root.dump();
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
    std::string payload1 =buildTelemetryJson(1, 25.1);
    std::string payload2 =buildTelemetryJson(2, 25.2);
    std::string payload3 =buildTelemetryJson(3, 25.3);

    std::vector<std::uint8_t> frame1 =buildFrame(payload1);
    std::vector<std::uint8_t> frame2 =buildFrame(payload2);
    std::vector<std::uint8_t> frame3 =buildFrame(payload3);

    std::vector<std::uint8_t> all_frames;
    all_frames.insert(all_frames.end(),frame1.begin(),frame1.end());
    all_frames.insert(all_frames.end(),frame2.begin(),frame2.end());
    all_frames.insert(all_frames.end(),frame3.begin(),frame3.end());

    if (!sendAll(sockfd,all_frames.data(),all_frames.size()))
    {
        std::cerr<< "Failed to send sticky frames"<< std::endl;

        close(sockfd);
        return 1;
    }
    std::cout
        << "Sent 3 sticky frames, total="
        << all_frames.size()
        << " bytes"
        << std::endl;
    
    // std::vector<std::uint8_t> frame =buildFrame(payload);
    // std::cout<< "Frame total size="<< frame.size() << " bytes, payload="<< payload.size()<< " bytes"<< std::endl;

    // if (!sendAll(sockfd,frame.data(),2))
    // {
    //     std::cerr<< "Failed to send part 1"<< std::endl;

    //     close(sockfd);
    //     return 1;
    // }

    // std::cout<< "Sent part 1: 2 bytes"<< std::endl;
    // std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // // 第二次：发送接下来的3字节
    // if (!sendAll(sockfd,frame.data() + 2,3))
    // {
    //     std::cerr<< "Failed to send part 2"<< std::endl;

    //     close(sockfd);
    //     return 1;
    // }

    // std::cout<< "Sent part 2: 3 bytes"<< std::endl;

    // std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // // 第三次：发送剩余全部字节
    // if (!sendAll(sockfd,frame.data() + 5,frame.size() - 5))
    // {
    //     std::cerr<< "Failed to send part 3"<< std::endl;

    //     close(sockfd);
    //     return 1;
    // }
    // std::cout<< "Sent part 3: "<< frame.size() - 5<< " bytes"<< std::endl;

    close(sockfd);

    return 0;
}