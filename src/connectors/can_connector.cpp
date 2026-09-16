#include "connectors/can_connector.h"

#include "common/logger.h"

#include <linux/can.h>
#include <linux/can/raw.h>
#include <net/if.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <iomanip>
#include <sstream>
#include <sys/time.h>
#include <cstring>
#include <utility>
#include <chrono>
#include <cstdint>
#include <exception>

CanConnector::CanConnector(std::string interface_name): interface_name_(std::move(interface_name))
{
}

CanConnector::~CanConnector()
{
    /*
     * 析构时统一调用 stop()，
     * 确保接收线程已经退出、CAN socket已经关闭。
     *
     * stop()本身支持重复调用，
     * 因此即使 GatewayContext 已经主动 stop()，
     * 这里再次调用也是安全的。
     */
    stop();
}

std::string CanConnector::name() const
{
    return "can";
}

bool CanConnector::start()
{
    if (running_)
    {
        Logger::warn(
            "CanConnector is already running"
        );

        return true;
    }

    can_fd_ = socket(PF_CAN,SOCK_RAW,CAN_RAW);
    if (can_fd_ < 0)
    {
        Logger::error(
            "Failed to create CAN socket"
        );
        return false;
    }

    const unsigned int interface_index = if_nametoindex(interface_name_.c_str());
    //if_nametoindex把网络接口名称转换成对应的接口索引（interface index）
    if (interface_index == 0)
    {
        Logger::error("CAN interface not found: " + interface_name_);

        close(can_fd_);
        can_fd_ = -1;
        return false;
    }

    //准备 CAN 地址
    sockaddr_can address{};
    address.can_family = AF_CAN;
    address.can_ifindex =static_cast<int>(interface_index);

    if (bind(can_fd_,reinterpret_cast<sockaddr*>(&address),sizeof(address)) < 0)
    {
        Logger::error(
            "Failed to bind CAN socket to interface: " + interface_name_
        );

        close(can_fd_);
        can_fd_ = -1;
        return false;
    }

    /*
    * 给CAN socket设置接收超时。
    *
    * receiveLoop()后续会阻塞在read()等待CAN帧。
    * 如果永远没有CAN数据，而外部调用stop()，
    * 接收线程就没有机会重新检查running_。
    *
    * 因此这里设置500ms超时：
    *
    * 500ms没有数据
    *      ↓
    * read()返回EAGAIN/EWOULDBLOCK
    *      ↓
    * receiveLoop重新检查running_
    *
    * 这样stop()最多等待约500ms即可让线程正常退出。
    */
    timeval timeout{};
    timeout.tv_sec = 0;
    timeout.tv_usec = 500000;

    if (setsockopt(can_fd_,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout)) < 0)
    {
        Logger::error("Failed to set CAN receive timeout");

        close(can_fd_);
        can_fd_ = -1;

        return false;
    }

    /*
    * socket已经完成创建、绑定和超时配置，
    * 现在才正式进入运行状态。
    */
    running_ = true;

    /*
    * 启动CAN接收线程。
    *
    * receive_thread_以后专门阻塞等待CAN帧，
    * 不占用Gateway主线程。
    */
    receive_thread_ = std::thread(&CanConnector::receiveLoop,this);
    Logger::info("CanConnector started on interface: " + interface_name_);

    return true;
}

void CanConnector::receiveLoop()
{
    while (running_)
    {
        /*
         * SocketCAN直接以can_frame为单位读取。
         *
         * 和TCP不同，这里不需要：
         *
         * 长度头
         * recv_buffer
         * 粘包拆包
         *
         * 一次成功read()得到的就是一整个CAN Frame。
         */
        can_frame frame{};
        const ssize_t n =read(can_fd_,&frame,sizeof(frame));

        /*
         * 正常收到一整个经典CAN帧。
         */
        if (n == static_cast<ssize_t>(sizeof(frame)))
        {
            /*
             * can_id字段除了真正的CAN ID，
             * 高位还可能带有一些SocketCAN标志位。
             *
             * 第一版只处理标准11位CAN ID，
             * 所以使用CAN_SFF_MASK取出真正的ID。
             *
             * 例如：
             *
             * cansend vcan0 123#11223344
             *
             * 最终得到：
             *
             * can_id = 0x123
             */
            const canid_t can_id = frame.can_id & CAN_SFF_MASK;


            /*
             * 第一版把CAN ID转换成统一device_id。
             *
             * 例如：
             *
             * CAN ID 0x101
             *
             *      ↓
             *
             * device_id = "can-101"
             */
            std::ostringstream device_stream;
            device_stream << "can-"<< std::hex<< std::uppercase<< can_id;
            const std::string device_id = device_stream.str();

            /*
             * CAN总线不存在TCP connect/disconnect。
             *
             * 第一版规定：
             * 只要收到过某个CAN ID的帧，
             * 就把它记录为已经发现的CAN设备。
             *
             * receive_thread_负责写，
             * gatewayctl线程以后可能读取，
             * 因此必须加锁。
             */
            {
                std::lock_guard<std::mutex> lock(devices_mutex_);
                seen_devices_.insert(device_id);
            }

            /*
             * 当前这一小步先只确认：
             * CanConnector确实收到了CAN帧。
             *
             * 下一步再把frame.data[]
             * 转换成TelemetryMessage。
             */
            std::ostringstream log_stream;

            log_stream<< "CAN frame received: id=0x"<< std::hex<< std::uppercase<< can_id<< ", dlc="<< std::dec<< static_cast<int>(frame.can_dlc);

            Logger::info(log_stream.str());

            /*
            * 第一版CAN应用层协议：
            *
            * CAN ID 0x101
            *   DATA[0..1] = 温度值 × 10
            *
            * 示例：
            *
            *   cansend vcan0 101#00FA
            *
            *   0x00FA = 250
            *   250 / 10.0 = 25.0 ℃
            *
            * 注意：
            * 这不是CAN协议本身规定的格式，
            * 而是当前网关为了验证完整链路定义的测试协议。
            */
            if (can_id == 0x101)
            {
                /*
                * 温度需要两个字节。
                *
                * 如果收到：
                *
                * 101#FA
                *
                * 只有1个字节，就不能继续解析。
                */
                if (frame.can_dlc < 2)
                {
                    Logger::warn(
                        "CAN 0x101 frame has invalid DLC"
                    );

                    continue;
                }


                /*
                * 两个字节按照大端序组成16位整数。
                *
                * DATA:
                *
                *   data[0] = 0x00
                *   data[1] = 0xFA
                *
                * 计算：
                *
                *   (0x00 << 8) | 0xFA
                *
                * 得到：
                *
                *   0x00FA = 250
                */
                const std::uint16_t raw_temperature = (static_cast<std::uint16_t>(frame.data[0]) << 8)|static_cast<std::uint16_t>(frame.data[1]);

                /*
                * 当前协议规定原始值扩大了10倍：
                *
                * raw = 250
                *
                *      ↓
                *
                * temperature = 25.0 ℃
                */
                const double temperature = static_cast<double> (raw_temperature) / 10.0;

                TelemetryMessage message{};
                /*
                * 使用CAN ID构造统一device_id。
                *
                * CAN ID 0x101
                *
                *      ↓
                *
                * "can-101"
                */
                message.device_id = device_id;

                /*
                * 标记这条Telemetry来自CAN协议。
                */
                message.protocol = "can";

                /*
                * 获取Unix时间戳，单位毫秒。
                *
                * system_clock代表系统真实时间，
                * 适合放进遥测消息并上传。
                */
                const auto now = std::chrono::system_clock::now();
                message.timestamp_ms =static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count());

                /*
                * 每生成一条CAN遥测消息，
                * 分配一个递增序列号。
                */
                message.sequence = sequence_.fetch_add(1) + 1;


                /*
                * 把CAN二进制数据转换成统一业务指标。
                *
                * 后面的Queue / MQTT / SQLite
                * 不需要再知道0x00FA是什么意思。
                */
                MetricValue metric{};

                metric.name = "temperature";
                metric.value = temperature;
                metric.unit = "C";

                message.metrics.push_back(std::move(metric));

                /*
                * Connector只负责协议转换。
                *
                * 如果GatewayContext设置了回调，
                * 就把统一TelemetryMessage交给上层。
                *
                * 后续：
                *
                * callback
                *   ↓
                * TelemetryQueue
                *   ↓
                * Worker
                *   ↓
                * MQTT / SQLite
                */
                if (message_callback_)
                {
                    message_callback_(message);
                }
                continue;
            }
        }

        /*
         * read()被系统信号临时打断，
         * CAN socket本身没有问题，
         * 直接重新read即可。
         */
        if (n < 0 && errno == EINTR)
        {
            continue;
        }

        /*
         * SO_RCVTIMEO导致的正常接收超时。
         *
         * 500ms没有收到CAN帧时会到这里。
         *
         * 这不是故障，
         * 下一轮while会重新检查running_。
         */
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            continue;
        }

        /*
         * 如果外部已经要求停止，
         * 不再把退出过程中的read异常当作故障。
         */
        if (!running_)
        {
            break;
        }

        /*
         * 运行过程中出现其他read错误。
         */
        Logger::error("Failed to read CAN frame");
        break;
    }
}

void CanConnector::stop()
{
    /*
     * 即使没有完整启动成功，
     * 只要还有有效fd也允许继续清理。
     */
    if (!running_ && can_fd_ < 0)
    {
        return;
    }

    /*
     * 告诉receiveLoop停止。
     *
     * 如果线程此时正阻塞在read()，
     * SO_RCVTIMEO保证它最多约500ms后返回，
     * 然后看到running_ == false退出。
     */
    running_ = false;

    /*
     * 等接收线程彻底结束以后，
     * 才释放它正在使用的can_fd_。
     */
    if (receive_thread_.joinable())
    {
        receive_thread_.join();
    }

    if (can_fd_ >= 0)
    {
        close(can_fd_);
        can_fd_ = -1;
    }

    Logger::info(
        "CanConnector stopped"
    );
}

void CanConnector::setMessageCallback(MessageCallback callback)
{
    /*
     * GatewayContext会在Connector启动前设置回调，
     * 后续receiveLoop收到CAN遥测后，
     * 就通过这个回调把统一TelemetryMessage交出去。
     */
    message_callback_ = std::move(callback);
}

std::vector<std::string> CanConnector::getOnlineDevices()
{
    /*
     * receiveLoop可能正在向seen_devices_插入新设备，
     * gatewayctl又可能同时查询设备列表，
     * 因此读取时同样必须持有同一把mutex。
     */
    std::lock_guard<std::mutex> lock(devices_mutex_);

    return std::vector<std::string>(seen_devices_.begin(),seen_devices_.end());
}

bool CanConnector::sendCommand(const DeviceCommand& command)
{
    /*
     * Connector没有正常启动时不能发送CAN帧。
     *
     * running_ == false：
     *   当前Connector可能正在停止或尚未启动。
     *
     * can_fd_ < 0：
     *   当前没有有效SocketCAN socket。
     */
    if (!running_ || can_fd_ < 0)
    {
        Logger::warn("Cannot send CAN command: connector is not running");
        return false;
    }

    /*
     * 当前CanConnector使用统一设备ID格式：
     *
     *   can-101
     *
     * 其中：
     *
     *   "can-" → 协议前缀
     *   "101"  → 十六进制CAN ID，即0x101
     *
     * 因此收到下行命令后，
     * 首先需要把业务层device_id重新转换成CAN ID。
     */
    const std::string prefix = "can-";
    if (command.device_id.size() <= prefix.size() || command.device_id.compare(0,prefix.size(),prefix) != 0)
    {
        Logger::warn("Invalid CAN device_id: " + command.device_id);
        return false;
    }

    /*
     * 去掉"can-"前缀。
     *
     * 例如：
     *
     *   "can-101"
     *
     *       ↓
     *
     *   "101"
     */
    const std::string can_id_text =command.device_id.substr(prefix.size());
    unsigned long parsed_can_id = 0;
    try
    {
        /*
         * 按16进制解析CAN ID。
         *
         * "101"
         *
         *   ↓ base = 16
         *
         * 0x101
         */
        std::size_t parsed_length = 0;
        parsed_can_id = std::stoul(can_id_text,&parsed_length,16);

        /*
         * 要求整个字符串都必须是合法的16进制CAN ID。
         *
         * 例如：
         *
         *   can-101abcxyz
         *
         * 不能只成功解析前面的101abc，
         * 然后忽略后面的非法字符。
         */
        if (parsed_length!= can_id_text.size())
        {
            Logger::warn("Invalid CAN device_id: "+ command.device_id);
            return false;
        }
    }
    catch (const std::exception&)
    {
        Logger::warn("Invalid CAN device_id: " + command.device_id);
        return false;
    }

     /*
     * 当前第一版只支持标准11位CAN ID。
     *
     * 标准帧范围：
     *
     *   0x000 ~ 0x7FF
     *
     * CAN_SFF_MASK就是：
     *
     *   0x7FF
     */
    if (parsed_can_id> CAN_SFF_MASK)
    {
        Logger::warn("CAN ID exceeds standard 11-bit range: " + command.device_id);
        return false;
    }

    /*
     * 当前测试协议第一版只支持：
     *
     *   action = "set_value"
     *
     * 后续如果增加：
     *
     *   start
     *   stop
     *   reset
     *
     * 可以继续在CanConnector内部增加
     * 对应的CAN命令编码规则。
     */
    if (command.action != "set_value")
    {
        Logger::warn("Unsupported CAN command action: " + command.action);
        return false;
    }

    /*
     * set_value命令必须携带：
     *
     * {
     *     "value": 300
     * }
     */
    if (!command.parameters.contains("value")|| (!command.parameters["value"].is_number_integer()&& !command.parameters["value"].is_number_unsigned()))
    {
        Logger::warn(
            "CAN set_value command requires integer parameter: value"
        );
        return false;
    }

    std::uint64_t value = 0;

    /*
     * JSON可能保存的是：
     *
     * 有符号整数
     * 或
     * 无符号整数
     *
     * 所以这里分别处理。
     */
    if (command.parameters["value"].is_number_unsigned())
    {
        value = command.parameters["value"].get<std::uint64_t>();
    }
    else
    {
        const std::int64_t signed_value = command.parameters["value"].get<std::int64_t>();
        /*
         * 当前协议value只允许非负数。
         */
        if (signed_value < 0)
        {
            Logger::warn(
                "CAN set_value must not be negative"
            );
            return false;
        }

        value = static_cast<std::uint64_t>(signed_value);
    }

    /*
     * 当前协议使用两个字节保存value：
     *
     * DATA[1]
     * DATA[2]
     *
     * 因此最大值只能是：
     *
     * 0xFFFF = 65535
     */
    if (value > 0xFFFF)
    {
        Logger::warn("CAN set_value exceeds 16-bit range");
        return false;
    }

     /*
     * 构造经典CAN Frame。
     */
    can_frame frame{};

    /*
     * 目标CAN ID。
     *
     * 例如：
     *
     *   device_id = "can-101"
     *
     *      ↓
     *
     *   frame.can_id = 0x101
     */
    frame.can_id =static_cast<canid_t>(parsed_can_id);

    /*
     * 当前命令一共使用3个数据字节：
     *
     * DATA[0] = 命令码
     * DATA[1] = value高8位
     * DATA[2] = value低8位
     */
    frame.can_dlc = 3;

     /*
     * 0x01约定为set_value命令码。
     */
    frame.data[0] = 0x01;

    /*
     * 将16位value拆成两个8位字节。
     *
     * 例如：
     *
     * value = 300
     *       = 0x012C
     *
     * DATA[1] = 0x01
     * DATA[2] = 0x2C
     */
    frame.data[1] =static_cast<std::uint8_t>((value >> 8) & 0xFF);
    frame.data[2] =static_cast<std::uint8_t>(value & 0xFF);

    /*
     * SocketCAN发送一个经典CAN Frame。
     *
     * 和TCP不同：
     *
     * 这里不是发送任意长度字节流，
     * 而是一次写入完整can_frame。
     */
    const ssize_t written = write(can_fd_,&frame,sizeof(frame));
 
     /*
     * 正常情况下应该完整写入一个can_frame。
     *
     * 如果返回值不是sizeof(frame)，
     * 当前命令认为发送失败。
     */
    if (written!= static_cast<ssize_t>(sizeof(frame)))
    {
        Logger::error("Failed to send CAN command to device: "+ command.device_id);
        return false;
    }

    Logger::info("CAN command sent successfully: device_id=" + command.device_id + ", action=" + command.action + ", value=" + std::to_string(value));

    return true;
}
