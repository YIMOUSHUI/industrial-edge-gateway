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

/**
 * @brief 从TCP连接中持续接收，直到收满指定字节数。
 *
 * TCP是字节流协议，一次recv()不保证拿到我们需要的全部数据。
 * 因此这里循环recv()，直到累计收到size字节。
 *
 * @param fd   TCP socket文件描述符。
 * @param data 接收数据存放的位置。
 * @param size 本次必须完整接收的字节数。
 *
 * @return true  成功收满size字节。
 * @return false 对端关闭连接或发生真正的recv错误。
 */
bool recvAll(int fd,void* data,std::size_t size)
{
    /*
     * data是void*，不能直接进行：
     *
     * data + received
     *
     * 所以先转换成“按字节访问”的指针。
     */
    auto* buffer = static_cast<std::uint8_t*>(data);

    /*
     * received表示目前已经成功收到多少字节。
     */
    std::size_t received = 0;
    while (received < size)
    {
        /*
         * 从“还没收到的位置”继续接收。
         *
         * 例如：
         *
         * 目标4字节
         * 第一次已经收到2字节
         *
         * 那下一次：
         *
         * buffer + 2
         * size - 2
         *
         * 从第2字节后面继续收剩余2字节。
         */
        const ssize_t n = recv(fd,buffer + received,size - received,0);
        if (n > 0)
        {
            received += static_cast<std::size_t>(n);
            continue;
        }

        /*
         * recv() == 0：
         *
         * TCP对端已经正常关闭连接。
         */
        if (n == 0)
        {
            return false;
        }

        /*
         * EINTR：
         *
         * 当前recv()被系统信号临时打断，
         * socket本身没有坏，
         * 重新执行recv()即可。
         */
        if (errno == EINTR)
        {
            continue;
        }

        /*
         * 其他错误认为接收失败。
         */
        return false;
    }
    return true;
}

/**
 * @brief 按当前Gateway TCP协议接收一条完整Frame。
 *
 * 协议格式：
 *
 * [4字节Payload长度][JSON Payload]
 *
 * @param fd      TCP socket文件描述符。
 * @param payload 输出参数，成功后保存完整JSON字符串。
 *
 * @return true  成功收到一条完整Frame。
 * @return false 连接关闭、长度非法或接收失败。
 */
bool receiveFrame(int fd,std::string& payload)
{
    /*
     * 第一步：
     * 读取固定4字节长度头。
     *
     * 这里保存的是“网络字节序”的32位整数。
     */
    std::uint32_t network_length = 0;
    if (!recvAll(fd,&network_length,sizeof(network_length)))
    {
        return false;
    }

    /*
     * ntohl：
     *
     * network to host long
     *
     * 网络字节序 -> 当前主机字节序。
     *
     * 与发送时的htonl()正好相反。
     */
    const std::uint32_t payload_length = ntohl(network_length);
    /*
     * 和Gateway接收上行Frame一样，
     * 给单条消息设置最大长度限制。
     *
     * 这里限制为1MiB，
     * 防止错误长度值导致模拟器申请异常大的内存。
     */
    constexpr std::uint32_t MAX_PAYLOAD_SIZE = 1024 * 1024;
    if (payload_length > MAX_PAYLOAD_SIZE)
    {
        std::cerr
            << "Invalid command payload length: "
            << payload_length
            << std::endl;

        return false;
    }

    /*
     * 根据长度头申请正好大小的缓冲区。
     */
    std::vector<std::uint8_t> buffer(payload_length);
    /*
     * 如果Payload长度不是0，
     * 就继续完整接收正文。
     */
    if (payload_length > 0)
    {
        if (!recvAll(fd,buffer.data(),buffer.size()))
        {
            return false;
        }
    }

    /*
     * 把收到的字节转换成std::string，
     * 后面就可以交给nlohmann::json解析。
     */
    payload.assign(buffer.begin(),buffer.end());
    return true;
}

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

std::string buildTelemetryJson(const std::string& device_id, std::uint64_t sequence, double temperature)
{
    nlohmann::json root;

    root["device_id"] = device_id;
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

int main(int argc, char* argv[])
{
    std::string device_id = "sensor-001";
    if (argc >= 2)
    {
        device_id = argv[1];
    }

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
    std::string payload1 =buildTelemetryJson(device_id, 1, 25.1);
    //std::string payload2 =buildTelemetryJson(device_id, 2, 25.2);
    //std::string payload3 =buildTelemetryJson(device_id, 3, 25.3);

    std::vector<std::uint8_t> frame1 =buildFrame(payload1);
    //std::vector<std::uint8_t> frame2 =buildFrame(payload2);
    //std::vector<std::uint8_t> frame3 =buildFrame(payload3);
    if (!sendAll(sockfd,frame1.data(),frame1.size()))
    {
        std::cerr<< "Failed to send frame" << std::endl;

        close(sockfd);
        return 1;
    }

    std::cout<< device_id << " telemetry sent, waiting for gateway command..." << std::endl;
    /*
    * 阻塞等待Gateway下发一条完整TCP Frame。
    */
    std::string command_payload;
    if (!receiveFrame(sockfd,command_payload))
    {
        std::cerr
            << "Failed to receive command from gateway"
            << std::endl;

        close(sockfd);
        return 1;
    }

    /*
    * 当前TCP Payload约定使用JSON。
    *
    * 这里解析以后主要为了测试时
    * 能够清晰打印Gateway真正下发的Command。
    */
    try
    {
        const nlohmann::json command =
            nlohmann::json::parse(
                command_payload
            );

        std::cout
            << "Received command from gateway:"
            << std::endl;

        /*
        * dump(4)：
        * 以4个空格缩进的形式格式化打印JSON，
        * 方便人工查看。
        */
        std::cout
            << command.dump(4)
            << std::endl;
    }
    catch (const nlohmann::json::exception& e)
    {
        std::cerr
            << "Failed to parse command JSON: "
            << e.what()
            << std::endl;

        close(sockfd);
        return 1;
    }
    close(sockfd);
    return 0;
}