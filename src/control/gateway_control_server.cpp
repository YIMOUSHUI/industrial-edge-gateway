#include "gateway_control_server.h"

#include "core/gateway_context.h"
#include "common/logger.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <utility>
#include <cerrno>
#include <cstring>

GatewayControlServer::GatewayControlServer(std::string socket_path,GatewayContext& gateway): socket_path_(std::move(socket_path)),gateway_(gateway)
{
}

bool GatewayControlServer::start()
{
    /*
     * 防止重复start()。
     *
     * 如果已经在运行，就不应该重新创建
     * 第二个listen_fd_。
     */
    if (running_)
    {
        Logger::warn(
            "GatewayControlServer is already running"
        );

        return true;
    }

    /*
     * 第一步：
     *
     * 创建Unix Domain Socket。
     *
     * AF_UNIX：
     *   使用Unix域地址，不使用IP地址。
     *
     * SOCK_STREAM：
     *   使用可靠、有序的字节流连接。
     */
    listen_fd_ =socket(AF_UNIX,SOCK_STREAM,0);

    if (listen_fd_ < 0)
    {
        Logger::error(
            "Failed to create control UDS socket"
        );

        return false;
    }

    /*
     * 第二步：
     *
     * 清理上一次运行可能遗留的旧socket路径。
     *
     * 如果路径不存在，
     * unlink()失败也没有关系，
     * 因为我们本来就是想确保它不存在。
     */
    unlink(socket_path_.c_str());

    /*
     * 第三步：
     *
     * 准备UDS专用地址结构。
     */
    sockaddr_un address{};

    /*
     * 告诉Linux：
     * 这是一个Unix Domain Socket地址。
     */
    address.sun_family = AF_UNIX;

    /*
     * 把：
     *
     * /tmp/gateway.sock
     *
     * 复制到：
     *
     * address.sun_path
     *
     * sun_path是一个固定长度的字符数组。
     */
    if (socket_path_.size() >= sizeof(address.sun_path))
    {
        Logger::error(
            "Control socket path is too long: "
            + socket_path_
        );

        close(listen_fd_);
        listen_fd_ = -1;

        return false;
    }

    std::strncpy(address.sun_path,socket_path_.c_str(),sizeof(address.sun_path) - 1);

    /*
     * 第四步：
     *
     * 把当前listen socket绑定到：
     *
     * /tmp/gateway.sock
     *
     * 从这一步成功以后，
     * Linux文件系统里就会出现对应的
     * Unix Domain Socket路径。
     */
    if (bind(listen_fd_,reinterpret_cast<sockaddr*>(&address),sizeof(address)) < 0)
    {
        Logger::error(
            "Failed to bind control UDS socket: "
            + socket_path_
        );

        close(listen_fd_);
        listen_fd_ = -1;

        /*
         * bind失败时顺便清理可能产生的路径。
         */
        unlink(socket_path_.c_str());

        return false;
    }


    /*
     * 第五步：
     *
     * 将socket设置成监听socket，
     * 准备接受gatewayctl连接。
     *
     * 8表示等待accept的连接队列
     * 最多允许积压若干个连接。
     *
     * 对于低频本地管理接口，
     * 这个数已经完全足够。
     */
    if (listen(listen_fd_,8) < 0)
    {
        Logger::error(
            "Failed to listen on control UDS socket"
        );
        close(listen_fd_);listen_fd_ = -1;

        unlink(socket_path_.c_str());

        return false;
    }


    /*
     * 到这里说明：
     *
     * socket
     * bind
     * listen
     *
     * 全部成功。
     *
     * 下一步才会真正启动control_thread_
     * 并进入accept()等待gatewayctl。
     */
    running_ = true;

    /*
    * 启动专门的本地管理线程。
    *
    * &GatewayControlServer::controlLoop
    * 表示：
    *   线程要执行GatewayControlServer成员函数
    *   controlLoop()。
    *
    * this表示：
    *   controlLoop()属于当前这个
    *   GatewayControlServer对象。
    */
    control_thread_ = std::thread(&GatewayControlServer::controlLoop,this);

    Logger::info("GatewayControlServer listening on "+ socket_path_);

    return true;
}

void GatewayControlServer::controlLoop()
{
    /*
     * running_为true时，
     * Control Server持续等待gatewayctl连接。
     */
    while (running_)
    {
        /*
         * accept()等待一个gatewayctl连接。
         *
         * 这里暂时不关心客户端地址，
         * 所以后两个参数直接传nullptr。
         *
         * 没有gatewayctl连接时，
         * 当前control_thread_会阻塞在这里。
         *
         * 这不会影响gatewayd的其他线程。
         */
        const int client_fd = accept(listen_fd_,nullptr,nullptr);

        /*
         * accept()失败。
         */
        if (client_fd < 0)
        {
            /*
             * 最重要的是先检查：
             * Gateway是不是正在正常stop。
             *
             * stop()会：
             *
             * running_ = false;
             * shutdown(listen_fd_, SHUT_RDWR);
             *
             * shutdown会让阻塞中的accept()返回。
             *
             * 所以如果此时running_已经是false，
             * 这不是业务故障，
             * 而是正常退出流程。
             */
            if (!running_)
            {
                break;
            }

            /*
             * EINTR：
             * accept()被系统信号临时打断。
             *
             * socket本身没有坏，
             * 下一轮重新accept即可。
             */
            if (errno == EINTR)
            {
                continue;
            }
            /*
             * Gateway仍然处于运行状态，
             * 但accept()发生了其他错误。
             */
            Logger::error(
                "GatewayControlServer accept() failed"
            );

            continue;
        }


        /*
         * 能走到这里说明：
         *
         * 有一个gatewayctl成功连接进来了。
         *
         * listen_fd_
         *   -> 一直负责等待新的客户端
         *
         * client_fd
         *   -> 只负责当前这一次gatewayctl连接
         */
        Logger::info(
            "gatewayctl connected, fd="
            + std::to_string(client_fd)
        );

        /*
        * 接收gatewayctl发送的一整条管理请求。
        */
        std::string request;
        if (!receiveLine(client_fd,request))
        {
            Logger::warn(
                "Failed to receive gatewayctl request"
            );

            close(client_fd);
            continue;
        }

        /*
        * 当前这一小步只验证：
        *
        * UDS连接建立以后，
        * GatewayControlServer能够完整收到一条以'\n'结尾的请求。
        *
        * 下一步才处理：
        *
        * status
        * devices
        */
        Logger::info( "Received gatewayctl request: "+ request);

        /*
        * 根据管理命令生成响应文本。
        *
        * 例如：
        *
        * request = "status"
        *
        * response =
        * "Gateway: running\n"
        * "MQTT: connected\n"
        * "Online devices: 1\n"
        */
        const std::string response = handleRequest(request);

       /*
        * 将完整响应发送给gatewayctl。
        */
        if (!sendAll(client_fd,response))
        {
            Logger::warn(
                "Failed to send gatewayctl response"
            );

            close(client_fd);
            continue;
        }

        /*
        * 一次UDS连接只处理：
        *
        * 一条请求
        * 一条响应
        *
        * 响应发送完成以后关闭连接。
        *
        * gatewayctl那边以后会通过：
        *
        * recv() == 0
        *
        * 判断整个响应已经结束。
        */
        close(client_fd);
    }
    Logger::info(
        "GatewayControlServer control thread stopped"
    );
}

void GatewayControlServer::stop()
{
    /*
     * 如果本来就没有运行，
     * 不需要重复执行停止流程。
     */
    if (!running_)
    {
        return;
    }

    /*
     * 第一步：
     * 告诉control_thread_：
     * 服务已经进入停止状态。
     */
    running_ = false;

    /*
     * 第二步：
     * control_thread_此时很可能正在：
     *
     * accept(listen_fd_, ...)
     *
     * 里面阻塞等待gatewayctl。
     *
     * 仅仅把running_改成false，
     * 并不能让accept()自己醒来。
     *
     * shutdown(..., SHUT_RDWR)
     * 用来让这个socket停止读写，
     * 从而让阻塞中的accept()返回。
     */
    if (listen_fd_ >= 0)
    {
        shutdown(
            listen_fd_,
            SHUT_RDWR
        );
    }

    /*
     * 第三步：
     * 等待Control Server线程真正退出。
     *
     * controlLoop()中的accept()返回后，
     * 会看到running_已经是false，
     * 然后break退出。
     */
    if (control_thread_.joinable())
    {
        control_thread_.join();
    }

    /*
     * 第四步：
     * control_thread_已经完全结束，
     * 不会再使用listen_fd_。
     *
     * 现在才真正关闭文件描述符。
     */
    if (listen_fd_ >= 0)
    {
        close(listen_fd_);
        listen_fd_ = -1;
    }

    /*
     * 第五步：
     * 删除UDS在文件系统中的socket路径。
     *
     * 这样下次gatewayd启动时
     * 不会留下旧的 /tmp/gateway.sock。
     */
    unlink(socket_path_.c_str());
    Logger::info(
        "GatewayControlServer stopped"
    );
}

bool GatewayControlServer::sendAll(int client_fd,const std::string& data)
{
    /*
     * offset表示：
     *
     * data前面已经有多少字节成功交给内核。
     */
    std::size_t offset = 0;

    while (offset < data.size())
    {
        /*
         * 从尚未发送的位置继续发送。
         *
         * data.data() + offset
         *     -> 当前应该从哪里开始发
         *
         * data.size() - offset
         *     -> 还剩多少字节没有发送
         */
        const ssize_t n = send(client_fd,data.data() + offset,data.size() - offset,MSG_NOSIGNAL);

        /*
         * 成功发送了一部分数据。
         */
        if (n > 0)
        {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        /*
         * send()被信号临时中断。
         *
         * socket本身没有坏，
         * 重新尝试即可。
         */
        if (n < 0 && errno == EINTR)
        {
            continue;
        }

        /*
         * 其他情况都认为发送失败。
         */
        return false;
    }
    return true;
}

bool GatewayControlServer::receiveLine(
    int client_fd,
    std::string& line
)
{
    /*
     * 每次调用都从一条新的空命令开始。
     */
    line.clear();

    /*
     * 管理命令很短。
     *
     * 这里设置一个最大长度，
     * 防止异常客户端无限发送却始终不发'\n'，
     * 导致line不断增长。
     */
    constexpr std::size_t MAX_LINE_SIZE = 1024;

    while (line.size() < MAX_LINE_SIZE)
    {
        char ch = '\0';

        /*
         * 每次读取1个字节。
         *
         * 对gatewayctl这种极低频、很短的本地管理命令，
         * 这种写法非常直观，性能完全足够。
         */
        const ssize_t n =recv(client_fd,&ch,1,0);

        /*
         * 成功读到1个字节。
         */
        if (n > 0)
        {
            /*
             * '\n'表示这一条管理请求结束。
             *
             * '\n'本身不保存进line。
             */
            if (ch == '\n')
            {
                return true;
            }

            line.push_back(ch);
            continue;
        }
        /*
         * recv() == 0：
         *
         * gatewayctl主动关闭了连接，
         * 但我们还没有收到完整一行。
         */
        if (n == 0)
        {
            return false;
        }
        /*
         * EINTR：
         * recv()被信号临时中断，
         * socket本身没有坏，重新recv即可。
         */
        if (errno == EINTR)
        {
            continue;
        }
        /*
         * 其他recv错误。
         */
        return false;
    }

    /*
     * 超过最大命令长度仍然没有遇到'\n'，
     * 认为这不是合法管理请求。
     */
    return false;
}

std::string GatewayControlServer::handleRequest(const std::string& request)
{
    /*
     * status：
     *
     * 返回Gateway整体的基础运行状态。
     */
    if (request == "status")
    {
        /*
         * MQTT真实连接状态由GatewayContext
         * 继续向MqttPublisher查询。
         */
        const bool mqtt_connected =gateway_.isMqttConnected();

        /*
         * 获取整个Gateway当前在线设备快照。
         */
        const std::vector<std::string> devices = gateway_.getOnlineDevices();
        std::string response;

        /*
         * GatewayControlServer能够正常处理请求，
         * 本身就说明当前gatewayd正在运行。
         */
        response += "Gateway: running\n";
        response += "MQTT: ";

        if (mqtt_connected)
        {
            response += "connected\n";
        }
        else
        {
            response += "disconnected\n";
        }

        response +=
            "Online devices: "
            + std::to_string(devices.size())
            + "\n";

        /*
        * TelemetryQueue运行状态。
        *
        * Queue size：
        *   当前仍然等待Worker消费的消息数量。
        *
        * Dropped telemetry：
        *   从本次gatewayd启动开始，
        *   因Queue达到容量上限而被拒绝的遥测总数。
        */
        response += "Telemetry queue: " + std::to_string(gateway_.getTelemetryQueueSize()) + "/" + std::to_string(gateway_.getTelemetryQueueCapacity()) + "\n";
        response += "Dropped telemetry: " + std::to_string(gateway_.getDroppedTelemetryCount()) + "\n";
        response +="Telemetry workers: " + std::to_string(gateway_.getTelemetryWorkerCount())+ "\n";
        response +="Uplink queue: "+ std::to_string(gateway_.getUplinkQueueSize())+ "/"+ std::to_string(gateway_.getUplinkQueueCapacity())+ "\n";
        response +="Rules enabled: "+ std::to_string(gateway_.getEnabledRuleCount())+ "\n";
        response +="Rule messages evaluated: "+ std::to_string(gateway_.getRuleMessagesEvaluated())+ "\n";
        response +="Rule triggers: "+ std::to_string(gateway_.getRuleTriggerCount())+ "\n";
        response +="Local rule actions: "+ std::to_string(gateway_.getLocalRuleActionCount())+ "\n";
        /*
        * M3：异常检测运行状态。
        */
        response +="Anomaly profiles enabled: "+ std::to_string(gateway_.getEnabledAnomalyProfileCount())+ "\n";
        response +="Anomaly windows scored: "+ std::to_string(gateway_.getAnomalyWindowsScored())+ "\n";
        response +="Anomalies detected: "+ std::to_string(gateway_.getAnomaliesDetected())+ "\n";
        /*
        * 获取gatewayd当前进程资源快照。
        */
        const ProcessMetrics process_metrics = gateway_.getProcessMetrics();
        if (process_metrics.valid)
        {
            response += "Process RSS: " + std::to_string(process_metrics.rss_kb) + " KB\n";
            response += "Open fd: " + std::to_string(process_metrics.open_fd_count) + "\n";
            response += "Threads: " + std::to_string(process_metrics.thread_count) + "\n";
        }
        else
        {
            /*
            * /proc读取异常不应该导致status命令本身失败。
            */
            response += "Process metrics: unavailable\n";
        }
        response += "Uptime: " + std::to_string(gateway_.getUptimeSeconds()) + " s\n";

        /*
        * Hardware Watchdog状态。
        *
        * Ubuntu开发环境：
        *   Watchdog: disabled
        *
        * i.MX6ULL正常运行：
        *   Watchdog: feeding
        *
        * Gateway核心健康检查失败：
        *   Watchdog: armed, feeding stopped
        */
        if (!gateway_.isWatchdogEnabled())
        {
            response += "Watchdog: disabled\n";
        }
        else if (gateway_.isWatchdogFeeding())
        {
            response += "Watchdog: feeding\n";
        }
        else
        {
            response += "Watchdog: armed, feeding stopped\n";
        }
        return response;
    }

    /*
     * devices：
     *
     * 返回当前所有Connector汇总后的在线设备ID。
     */
    if (request == "devices")
    {
        const std::vector<std::string> devices =gateway_.getOnlineDevices();

        /*
         * 当前没有任何在线设备。
         */
        if (devices.empty())
        {
            return "No online devices\n";
        }
        std::string response;

        /*
         * 每个device_id单独占一行。
         *
         * 例如：
         *
         * sensor-001
         * sensor-002
         */
        for (const std::string& device_id : devices)
        {
            response += device_id;
            response += '\n';
        }

        return response;
    }

    /*
    * health：
    *
    * 第一版只根据MQTT连接状态
    * 判断Gateway整体健康程度。
    *
    * MQTT正常：
    *   healthy
    *
    * MQTT断开：
    *   degraded
    *
    * 因为MQTT断开时Gateway仍然可以继续采集，
    * 并通过SQLite进行离线缓存，
    * 所以这里不认为整个Gateway已经不可用。
    */
    if (request == "health")
    {
        const bool mqtt_connected =
            gateway_.isMqttConnected();

        if (mqtt_connected)
        {
            return "Health: healthy\n";
        }

        return 
            "Health: degraded\n"
            "Reason: MQTT disconnected\n";
    }

    /*
     * 其他请求当前都不支持。
     */
    return "Unknown command: "+ request+ "\n";
}


GatewayControlServer::~GatewayControlServer()
{
    stop();
}