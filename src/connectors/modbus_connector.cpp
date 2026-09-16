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
            if (modbus_connect(context_)== -1)
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

        const int read_count = modbus_read_registers(context_,0,3,registers);
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

            Logger::info(
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
            modbus_close(context_);
            connected_ = false;
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
     * Modbus下行还没有进入实现阶段。
     *
     * 后续这里会负责：
     *
     * DeviceCommand
     *      ↓
     * 选择目标Holding Register
     *      ↓
     * modbus_write_register()
     */
    (void)command;

    Logger::warn("Modbus command sending is not implemented yet");
    return false;
}

