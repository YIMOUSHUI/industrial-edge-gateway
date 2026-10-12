#include <modbus/modbus.h>

#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <iostream>

#include <sys/socket.h>
#include <unistd.h>

namespace
{
    /*
    * SIGINT（Ctrl+C）到来时置为0。
    *
    * 使用sig_atomic_t是因为它适合在简单的
    * signal handler中进行读写。
    */
    volatile std::sig_atomic_t g_running = 1;

    /**
     * @brief 处理Ctrl+C退出。
     */
    void signalHandler(int)
    {
        g_running = 0;
    }

} // namespace

int main(int argc, char* argv[])
{
    /*
    * 默认不注入异常，保持原有模拟器行为。
    *
    * 指定 --spike-at-read 12 时，
    * 在第12次Holding Register读取时注入一次40℃。
    */
    int spike_at_read = 0;
    if (argc != 1)
    {
        if (argc != 3 ||std::strcmp(argv[1], "--spike-at-read") != 0)
        {
            std::cerr
                << "Usage: modbus_device_simulator "
                << "[--spike-at-read N]\n";
            return 1;
        }

        char* end = nullptr;
        errno = 0;

        const long value =std::strtol(argv[2], &end, 10);

        /*
        * 至少第9次才允许注入，
        * 确保M3的8条历史样本先完成预热。
        */
        if (errno != 0 ||end == argv[2] ||*end != '\0' ||value < 9 ||value > 100000)
        {
            std::cerr<< "Invalid spike-at-read value\n";
            return 1;
        }

        spike_at_read = static_cast<int>(value);
    }

    /*
    * 使用 sigaction 注册退出信号。
    *
    * 不设置 SA_RESTART：
    * Ctrl+C 到来时，让 accept()/recv() 这类阻塞系统调用
    * 返回 EINTR，而不是自动重新进入阻塞状态。
    */
    struct sigaction sa{};
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGINT, &sa, nullptr) < 0 ||sigaction(SIGTERM, &sa, nullptr) < 0)
    {
        std::cerr << "Failed to install signal handler" << std::endl;
        return 1;
    }

    /*
     * 创建一个Modbus TCP Server上下文。
     *
     * 127.0.0.1：
     *   当前只供Ubuntu本机开发测试。
     *
     * 1502：
     *   Modbus TCP标准端口是502，
     *   但502属于低端口，普通用户使用不方便。
     *
     *   所以开发阶段使用1502。
     */
    modbus_t* ctx = modbus_new_tcp("0.0.0.0",1502);
    if (ctx == nullptr)
    {
        std::cerr<< "Failed to create Modbus TCP context"<< std::endl;
        return 1;
    }

    /*
     * 创建模拟设备的寄存器空间。
     *
     * 参数依次表示：
     *
     * Coil数量
     * Discrete Input数量
     * Holding Register数量
     * Input Register数量
     *
     * 当前只使用Holding Register，
     * 所以申请10个寄存器即可。
     */
    modbus_mapping_t* mapping =modbus_mapping_new(0,0,10,0);
    if (mapping == nullptr)
    {
        std::cerr<< "Failed to allocate Modbus mapping"<< std::endl;
        modbus_free(ctx);
        return 1;
    }


    /*
     * 初始化模拟设备的数据。
     *
     * Register 0：
     *   250 → 25.0 ℃
     *
     * Register 1：
     *   600 → 60.0 %
     *
     * Register 2：
     *   1500 rpm
     */
    mapping->tab_registers[0] = 250;
    mapping->tab_registers[1] = 600;
    mapping->tab_registers[2] = 1500;

    /*
     * 创建TCP监听socket。
     *
     * 参数1表示监听队列长度。
     *
     * 当前只需要一个Gateway客户端，
     * 因此1已经足够。
     */
    int server_socket = modbus_tcp_listen(ctx,1);
    if (server_socket < 0)
    {
        std::cerr<< "Failed to listen on Modbus TCP port 1502: "<< modbus_strerror(errno)<< std::endl;

        modbus_mapping_free(mapping);
        modbus_free(ctx);
        return 1;
    }


    std::cout<< "Modbus device simulator started"<< std::endl;
    std::cout<< "Listening on 0.0.0.0:1502"<< std::endl;
    std::cout<< "Holding registers:"<< std::endl;
    std::cout<< "  HR0 temperature = "<< mapping->tab_registers[0]<< " (25.0 C)"<< std::endl;
    std::cout<< "  HR1 humidity    = "<< mapping->tab_registers[1]<< " (60.0 %)"<< std::endl;
    std::cout<< "  HR2 speed       = "<< mapping->tab_registers[2]<< " rpm"<< std::endl;

    /*
     * 等待Modbus Client连接。
     *
     * 后面的ModbusConnector就会扮演Client角色。
     */
    int client_socket = modbus_tcp_accept(ctx,&server_socket);
    if (client_socket < 0)
    {
            /*
        * Ctrl+C 会中断底层 accept()。
        *
        * 此时 errno == EINTR，并且 signalHandler()
        * 已经把 g_running 设置为0。
        * 这是正常退出，不属于服务器错误。
        */
        if (!g_running && errno == EINTR)
        {
            close(server_socket);
            modbus_mapping_free(mapping);
            modbus_free(ctx);

            std::cout << "Modbus device simulator stopped" << std::endl;
            return 0;
        }

        std::cerr
            << "Failed to accept Modbus TCP client: "
            << std::strerror(errno)
            << std::endl;

        close(server_socket);
        modbus_mapping_free(mapping);
        modbus_free(ctx);
        return 1;
    }


    std::cout<< "Modbus client connected"<< std::endl;

    /*
     * Modbus TCP最大ADU报文缓冲区。
     *
     * ADU可以先简单理解为：
     * 一条完整的Modbus TCP请求报文。
     */
    std::uint8_t query[MODBUS_TCP_MAX_ADU_LENGTH]{};

    /*
    * 记录Modbus Holding Register读取请求次数。
    *
    * 只统计从地址0开始的读请求，
    * 不统计写寄存器命令。
    */
    int holding_read_count = 0;
    while (g_running)
    {
        /*
         * 阻塞等待Client发来的Modbus请求。
         *
         * 例如：
         *
         * 读取Holding Register
         * 写单个Holding Register
         * 写多个Holding Register
         */
        const int request_length = modbus_receive(ctx,query);

        if (request_length > 0)
        {
            /*
            * Modbus TCP报文：
            *
            * 前7字节是MBAP Header。
            * query[7]是Modbus Function Code。
            *
            * 0x03表示读取Holding Registers。
            *
            * query[8]、query[9]是起始寄存器地址。
            *
            * 这里只统计从地址0开始的读取。
            */
            const bool is_holding_read =request_length >= 12 &&query[7] == 0x03 &&query[8] == 0x00 &&query[9] == 0x00;

            if (is_holding_read)
            {
                ++holding_read_count;

                if (spike_at_read > 0 &&holding_read_count == spike_at_read)
                {
                    /*
                    * 第N次读取之前，将HR0改成400。
                    *
                    * ModbusConnector按10倍缩放：
                    * 400 → 40.0℃。
                    */
                    mapping->tab_registers[0] = 400;

                    std::cout
                        << "[M3 TEST] Inject 40.0C at read "
                        << holding_read_count
                        << std::endl;
                }
                else if (spike_at_read > 0 &&holding_read_count == spike_at_read + 1)
                {
                    /*
                    * 下一次读取之前恢复25℃。
                    *
                    * 仅注入一个异常采样，
                    * 避免把持续高温和突发异常混在一起。
                    */
                    mapping->tab_registers[0] = 250;

                    std::cout
                        << "[M3 TEST] Restore 25.0C at read "
                        << holding_read_count
                        << std::endl;
                }
            }

            /*
             * libmodbus根据收到的Function Code
             * 自动访问mapping中的寄存器，
             * 并生成正确的Modbus响应。
             *
             * 如果Client执行写寄存器，
             * mapping->tab_registers中的值
             * 也会被自动更新。
             */
            if (modbus_reply(ctx,query,request_length,mapping) < 0)
            {
                std::cerr<< "Failed to send Modbus reply"<< std::endl;
                break;
            }

            /*
             * 每处理完一条请求打印当前寄存器，
             * 方便后续观察读写效果。
             */
            std::cout<< "Registers: "<< "HR0="<< mapping->tab_registers[0]<< ", HR1="<< mapping->tab_registers[1]<< ", HR2="<< mapping->tab_registers[2]<< std::endl;
            continue;
        }


        /*
        * Ctrl+C 中断了底层 recv()/select()。
        * 这是正常退出，不打印“客户端掉线”。
        */
        if (!g_running && errno == EINTR)
        {
            break;
        }

        /*
         * modbus_receive()返回-1，
         * 一般意味着客户端断开或socket发生错误。
         *
         * 当前第一版模拟器直接结束，
         * 后续如有需要再增加重新accept逻辑。
         */
        if (g_running)
        {
            std::cout<< "Modbus client disconnected"<< std::endl;
        }

        break;
    }

    /*
     * 按照与创建相反的顺序释放资源。
     */
    if (client_socket >= 0)
    {
        close(client_socket);
    }

    close(server_socket);
    modbus_mapping_free(mapping);
    modbus_close(ctx);
    modbus_free(ctx);

    std::cout<< "Modbus device simulator stopped"<< std::endl;
    return 0;
}



