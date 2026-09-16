#include "connectors/modbus_connector.h"

#include "common/logger.h"

#include <modbus/modbus.h>

#include <chrono>
#include <cerrno>
#include <utility>

ModbusConnector::ModbusConnector(std::string host,int port,std::string device_id,std::uint32_t poll_interval_ms): host_(std::move(host)),port_(port),device_id_(std::move(device_id)),poll_interval_ms_(poll_interval_ms)
{
}

ModbusConnector::~ModbusConnector()
{
    /*
     * 析构时统一调用stop()。
     *
     * 即使GatewayContext之前已经主动stop()，
     * 再调用一次也应该是安全的。
     */
    stop();
}

std::string ModbusConnector::name() const
{
    return "modbus";
}

void ModbusConnector::setMessageCallback(MessageCallback callback)
{
    /*
     * 保存GatewayContext提供的统一遥测回调。
     *
     * 下一步轮询到Modbus寄存器数据后，
     * 会先转换为TelemetryMessage，
     * 再通过这个callback交给GatewayContext。
     */
    message_callback_ = std::move(callback);
}

bool ModbusConnector::start()
{
    /*
     * 防止重复启动同一个Connector。
     */
    if (running_)
    {
        Logger::warn("ModbusConnector is already running");
        return true;
    }

    /*
     * 创建Modbus TCP通信上下文。
     *
     * 这里只是创建libmodbus对象，
     * 还没有真正建立TCP连接。
     *
     * 当前测试：
     *
     * host_ = 127.0.0.1
     * port_ = 1502
     */
    context_ = modbus_new_tcp(host_.c_str(),port_);
    if (context_ == nullptr)
    {
        Logger::error("Failed to create Modbus TCP context");
        return false;
    }

    /*
     * 到这里说明TCP连接已经真正建立成功。
     */
    connected_ = true;
    running_ = true;

    /*
     * 启动Modbus后台线程。
     *
     * pollingLoop()负责：
     *
     * 1. PLC未连接时主动connect；
     * 2. 连接失败后周期重试；
     * 3. 已连接时周期读取寄存器；
     * 4. 连接断开后重新进入重连状态。
     */
    polling_thread_ = std::thread(&ModbusConnector::pollingLoop,this);

    Logger::info("ModbusConnector started: device_id="+ device_id_ + ", target="+ host_+ ":"+ std::to_string(port_));
    /*
     * 注意：
     *
     * 当前这一小步暂时不启动polling_thread_。
     *
     * 下一步实现pollingLoop()之后，
     * 再在这里创建轮询线程。
     *
     * 当前只验证Modbus连接生命周期。
     */
    return true;
}

void ModbusConnector::pollingLoop()
{
    while (running_)
    {
        /*
         * 第一阶段：
         * 当前PLC没有连接时，主动尝试建立连接。
         *
         * PLC暂时离线只是远端设备状态，
         * 不会导致整个Gateway退出。
         */
        if (!connected_)
        {   
            int connect_result = -1;
            /*
            * 建立连接同样会修改context_内部的socket状态，
            * 因此也和读/写操作使用同一把锁。
            */
            {
                std::lock_guard<std::mutex> lock(io_mutex_);
                if (running_&& context_ != nullptr)
                {
                    connect_result =modbus_connect(context_);
                }
            }

            if (connect_result== -1)
            {
                /*
                 * 连接失败：
                 *
                 * Gateway继续运行，
                 * ModbusConnector保持后台重试。
                 */
                Logger::warn("Modbus device unavailable: device_id=" + device_id_ + ", target=" + host_ + ":" + std::to_string(port_) + ", error="+ modbus_strerror(errno));

                /*
                 * 控制重连频率。
                 *
                 * 当前配置1000ms，
                 * 即大约每1秒尝试一次连接。
                 */
                std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms_));
                continue;
            }

            /*
             * modbus_connect()成功，
             * 当前PLC正式进入在线状态。
             */
            connected_ = true;
            Logger::info("Modbus device connected: device_id="+ device_id_+ ", target="+ host_+ ":"+ std::to_string(port_));
        }

        /*
         * 第二阶段：
         * 已经连接PLC，读取3个Holding Register。
         *
         * 当前模拟PLC约定：
         *
         * HR0 = 温度 × 10
         * HR1 = 湿度 × 10
         * HR2 = 转速
         */
        std::uint16_t registers[3]{};

        int read_count = -1;
        /*
        * polling_thread_和Command Worker共享同一个
        * libmodbus context_。
        *
        * 所以一次完整的读取事务期间，
        * 不允许sendCommand()同时发送写寄存器请求。
        */
        {
            std::lock_guard<std::mutex> lock(io_mutex_);

            if (running_&& connected_&& context_ != nullptr)
            {
                read_count = modbus_read_registers(context_,0,3,registers);
            }
        }


        if (read_count == 3)
        {
            /*
             * 将寄存器原始值转换成业务数据。
             *
             * HR0 = 250
             *      ↓
             * 25.0 ℃
             *
             * HR1 = 600
             *      ↓
             * 60.0 %
             *
             * HR2 = 1500
             *      ↓
             * 1500 rpm
             */
            const double temperature =static_cast<double>(registers[0]) / 10.0;
            const double humidity =static_cast<double>(registers[1]) / 10.0;
            const double speed =static_cast<double>(registers[2]);

            /*
             * 转换成Gateway内部统一遥测模型。
             *
             * 从这里开始，
             * 后面的Queue / MQTT / SQLite
             * 不再关心底层来自Modbus寄存器。
             */
            TelemetryMessage message{};

            message.device_id = device_id_;
            message.protocol =  "modbus";

            /*
             * 生成毫秒级Unix时间戳。
             */
            const auto now = std::chrono::system_clock::now();
            message.timestamp_ms = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count());

            /*
             * 每成功生成一条Modbus遥测，
             * 序列号递增一次。
             */
            message.sequence = sequence_.fetch_add(1) + 1;

            /*
             * HR0 → 温度。
             */
            message.metrics.push_back(MetricValue{"temperature",temperature, "C"});

            /*
             * HR1 → 湿度。
             */
            message.metrics.push_back(MetricValue{"humidity",humidity,"%"});

            /*
             * HR2 → 转速。
             */
            message.metrics.push_back(MetricValue{"speed",speed,"rpm"});

            /*
             * 交给GatewayContext统一处理。
             *
             * 后续链路继续复用：
             *
             * GatewayContext::handleMessage()
             *          ↓
             * TelemetryQueue
             *          ↓
             * Worker
             *          ↓
             * MQTT / SQLite
             */
            if (message_callback_)
            {
                message_callback_(message);
            }

            Logger::debug(
                "Modbus telemetry received: device_id="
                + device_id_
                + ", temperature="
                + std::to_string(temperature)
                + ", humidity="
                + std::to_string(humidity)
                + ", speed="
                + std::to_string(speed)
            );
        }
        else
        {
            /*
             * read失败通常说明：
             *
             * PLC退出；
             * TCP连接断开；
             * 网络故障；
             * 或远端设备异常。
             *
             * 这些都不应该导致Gateway退出。
             */
            if (!running_)
            {
                break;
            }

            Logger::warn(
                "Modbus connection lost: device_id="
                + device_id_
                + ", error="
                + modbus_strerror(errno)
            );

            /*
             * 当前TCP连接已经失效，
             * 关闭libmodbus内部socket。
             *
             * context_本身不释放，
             * 因为后续还需要使用同一个context_
             * 再次调用modbus_connect()进行重连。
             */
            {
                std::lock_guard<std::mutex> lock(io_mutex_);
                if (context_ != nullptr&& connected_)
                {
                    modbus_close(context_);
                }
                connected_ = false;
            }
        }
        /*
         * 控制正常采集频率。
         *
         * 当前poll_interval_ms_ = 1000，
         * 即约每1秒读取一次HR0~HR2。
         */
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms_));
    }
}

void ModbusConnector::stop()
{

    if (!running_ && context_ == nullptr)
    {
        return;
    }

    /*
     * 首先通知轮询线程停止。
     *
     * 当前虽然pollingLoop还没有正式启用，
     * 但生命周期按最终结构提前写完整。
     */
    running_ = false;

    /*
     * 如果后续polling_thread_已经启动，
     * 必须先等待线程退出，
     * 再释放它正在使用的context_。
     */
    if (polling_thread_.joinable())
    {
        polling_thread_.join();
    }

    /*
    * polling_thread_已经退出。
    *
    * 这里使用同一把io_mutex_保护context_释放，
    * 防止Command Worker恰好仍在结束一次sendCommand()。
    */
    {
        std::lock_guard<std::mutex> lock(io_mutex_);
        /*
        * context_非空表示libmodbus上下文已经创建。
        */
        if (context_ != nullptr)
        {
            /*
            * 如果之前已经成功建立TCP连接，
            * 先关闭底层Modbus连接。
            */
            if (connected_)
            {
                modbus_close(context_);
            }

            /*
            * 释放libmodbus上下文本身。
            */
            modbus_free(context_);
            context_ = nullptr;
        }
        connected_ = false;
    }

    Logger::info("ModbusConnector stopped");
}

std::vector<std::string> ModbusConnector::getOnlineDevices()
{
    /*
     * 第一版一个ModbusConnector对应一台设备。
     *
     * 已连接：
     *   返回该device_id
     *
     * 未连接：
     *   返回空列表
     */
    if (connected_)
    {
        return {device_id_};
    }

    return {};
}

bool ModbusConnector::sendCommand(const DeviceCommand& command)
{
    /*
     * 当前一个ModbusConnector对应一台设备。
     *
     * 正常情况下CommandRouter已经根据device_id
     * 找到了正确的Connector。
     *
     * 这里仍然进行一次边界校验，
     * 防止错误命令被直接交给当前Connector。
     */
    if (command.device_id != device_id_)
    {
        Logger::warn("Modbus command device mismatch: "+ command.device_id);
        return false;
    }

    /*
     * 当前第一版Modbus下行只支持：
     *
     * action = "set_speed"
     *
     * 它对应模拟PLC中的Holding Register 2。
     */
    if (command.action != "set_speed")
    {
        Logger::warn("Unsupported Modbus command action: "+ command.action);
        return false;
    }

    /*
     * set_speed必须携带：
     *
     * {
     *     "value": 1200
     * }
     */
    if (!command.parameters.contains("value")|| (!command.parameters["value"].is_number_integer()&& !command.parameters["value"].is_number_unsigned()))
    {
        Logger::warn(
            "Modbus set_speed requires integer parameter: value"
        );

        return false;
    }

    std::uint64_t speed = 0;

    /*
     * JSON中的整数可能被nlohmann/json保存为：
     *
     * signed integer
     * 或
     * unsigned integer
     *
     * 因此这里分别读取。
     */
    if (command.parameters["value"].is_number_unsigned())
    {
        speed =command.parameters["value"].get<std::uint64_t>();
    }
    else
    {
        const std::int64_t signed_speed =command.parameters["value"].get<std::int64_t>();
        /*
         * 当前转速不接受负值。
         */
        if (signed_speed < 0)
        {
            Logger::warn(
                "Modbus set_speed must not be negative"
            );
            return false;
        }

        speed =static_cast<std::uint64_t>(signed_speed);
    }

    /*
     * 一个Holding Register为16 bit。
     *
     * 当前HR2直接保存转速值，
     * 因此合法范围：
     *
     * 0 ~ 65535
     */
    if (speed > 0xFFFF)
    {
        Logger::warn("Modbus set_speed exceeds 16-bit range");
        return false;
    }

    /*
     * 当前Connector正在停止，或者PLC离线时，
     * 不能执行下行写操作。
     */
    if (!running_|| !connected_)
    {
        Logger::warn(
            "Cannot send Modbus command: device is offline"
        );
        return false;
    }

    /*
     * pollingLoop()也会使用同一个context_
     * 执行读寄存器操作。
     *
     * 所以这里必须使用相同的io_mutex_，
     * 保证整个写寄存器事务不会和读取事务交叉。
     */
    std::lock_guard<std::mutex> lock(io_mutex_);

    /*
     * 获取锁之前，设备可能刚好发生断线。
     *
     * 所以进入临界区后再检查一次状态。
     */
    if (!running_|| !connected_|| context_ == nullptr)
    {
        Logger::warn(
            "Cannot send Modbus command: connection is unavailable"
        );

        return false;
    }

    /*
     * 写Holding Register 2。
     *
     * 当前模拟PLC定义：
     *
     * HR0 → temperature
     * HR1 → humidity
     * HR2 → speed
     */
    const int result = modbus_write_register(context_,2,static_cast<int>(speed));
    /*
     * modbus_write_register()成功写一个寄存器时，
     * 返回1。
     */
    if (result != 1)
    {
        Logger::warn(
            "Failed to write Modbus speed register: "
            + device_id_
            + ", error="
            + modbus_strerror(errno)
        );

        /*
         * 写操作失败时，这条TCP连接可能已经失效。
         *
         * 关闭当前连接并标记离线。
         *
         * 后续pollingLoop()发现connected_ == false，
         * 会自动进入你Phase 5A已经实现好的重连逻辑。
         */
        modbus_close(context_);
        connected_ = false;

        return false;
    }

    Logger::info(
        "Modbus command sent successfully: device_id="
        + device_id_
        + ", action=set_speed"
        + ", value="
        + std::to_string(speed)
    );

    return true;
}

