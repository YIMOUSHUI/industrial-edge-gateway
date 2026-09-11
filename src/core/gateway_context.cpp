#include "core/gateway_context.h"
#include "connectors/mock_connector.h"
#include "connectors/tcp_connector.h"
#include "converters/telemetry_converter.h"
#include "common/logger.h"

#include <utility>
#include <memory>
#include <chrono>
#include <thread>

bool GatewayContext::start(const std::string& config_path)
{
    if (!config_.load(config_path))
    {
        return false;
    }
    const GatewayConfig& config =config_.data();

    Logger::init(config.log_level);
    Logger::info("Gateway starting");
    Logger::info("Gateway id=" + config.gateway_id);

    /*
    * 根据配置创建 MQTT Publisher。
    *
    * 注意：
    * MQTT 属于 Gateway 的北向输出通道。
    * 即使 Broker 暂时不可用，也不应该阻止南向 TCP Connector 启动。
    *
    * 因此这里第一次连接失败时只记录警告，
    * 后续由 Worker 线程在发送消息时负责重新连接。
    */
    if (config.mqtt.enabled)
    {
        mqtt_publisher_ =
            std::make_unique<MqttPublisher>(
                config.mqtt.server_uri,
                config.mqtt.client_id,
                config.mqtt.topic,
                config.mqtt.qos
            );
        /*
        * 启动阶段尝试进行第一次 MQTT 连接。
        *
        * 成功：
        *   Gateway 后续可以直接发布消息。
        *
        * 失败：
        *   Gateway 仍继续启动，
        *   Worker 后续发送数据时再次尝试连接。
        */
        if (!mqtt_publisher_->connect())
        {
            Logger::warn(
                "MQTT broker is unavailable at startup; "
                "gateway will continue running and retry later"
            );
        }
    }

    /*
    * 初始化 SQLite 离线存储。
    *
    * 当前阶段暂时使用 gateway.db 作为数据库文件，
    * 后续会将数据库路径移动到 gateway.yaml 配置中。
    *
    * OfflineStore 必须在 Worker 线程启动之前完成初始化，
    * 因为后续 Worker 会使用该组件保存和读取离线消息。
    */
    offline_store_ = std::make_unique<OfflineStore>("gateway.db");
    if (!offline_store_->open())
    {
        Logger::error(
            "Failed to initialize offline SQLite store"
        );
        return false;
    }
    Logger::info("Offline SQLite store initialized");


    for (const ConnectorConfig& connector_config :config.connectors)          
    {
        if (!connector_config.enabled)
        {
            continue;
        }

        if (connector_config.type == "mock")
        {
            auto connector = std::make_unique<MockConnector>();
            connector->setMessageCallback(
                [this](const TelemetryMessage& message)
                {
                    handleMessage(message);
                }
             );
            connectors_.push_back(std::move(connector));
        }
        else if(connector_config.type == "tcp")
        {
            auto connector = std::make_unique<TcpConnector>(connector_config.port);
            connector->setMessageCallback(
                [this](const TelemetryMessage& message)
                {
                    handleMessage(message);
                }
            );
             connectors_.push_back(std::move(connector));
        }
    }

    for (auto& connector : connectors_)
    {
        if (!connector->start())
        {
            Logger::error("Failed to start connector: "+ connector->name());
            return false;
        }
    }

    worker_thread_ =std::thread(&GatewayContext::workerLoop,this);//创建工作者线程

    Logger::info("Gateway started");
    return true;
}

/**
 * @brief 停止 Gateway 及其内部工作线程。
 *
 * 停止顺序：
 * 1. 停止所有 Connector，阻止新的 TelemetryMessage 继续产生；
 * 2. 设置 Gateway 停止标志；
 * 3. 唤醒可能正在等待 MQTT 重连的 Worker；
 * 4. 停止 TelemetryQueue；
 * 5. 等待 Worker 线程退出；
 * 6. Worker 完全退出后再断开 MQTT；
 * 7. Gateway 完成停止。
 */
void GatewayContext::stop()
{
    /*
     * 第一步：停止所有消息生产者。
     *
     * TcpConnector 停止后：
     * - I/O 线程退出；
     * - 不再接收新的设备数据；
     * - 不再向 TelemetryQueue push 新消息。
     */
    for (auto& connector : connectors_)
    {
        connector->stop();
    }

    /*
    * 第二步：通知 Worker，Gateway 正在退出。
    *
    * Worker 在 MQTT 重连过程中会检查该标志。
    */
    stop_requested_.store(true);

    /*
     * 第三步：主动唤醒正在等待下一次 MQTT 重连的 Worker。
     *
     * 如果 Worker 正在 wait_for(5 秒)，notify_all()
     * 会让它立即醒来，而不需要等满 5 秒。
     */
    mqtt_retry_condition_.notify_all();

    /*
     * 第四步：停止遥测队列。
     *
     * 该操作会唤醒可能正在
     * TelemetryQueue::waitAndPop() 中等待消息的 Worker。
     */
    telemetry_queue_.stop();

    /*
     * 第五步：等待 Worker 自己正常退出。
     *
     * join() 不会强制杀死 Worker，
     * 它只是等待 workerLoop() 返回。
     */
    if (worker_thread_.joinable())
    {
        worker_thread_.join();
    }
    
    /*
     * 第六步：Worker 已经不再使用 MQTT Publisher，
     * 此时可以安全断开 Broker 连接。
     */
    if (mqtt_publisher_)
    {
        mqtt_publisher_->disconnect();
    }

    /*
    * Worker 已经退出后再关闭 SQLite。
    *
    * 这样可以保证不会出现：
    * Worker仍在访问 OfflineStore，
    * 但数据库句柄已经被提前关闭的情况。
    */
    if (offline_store_)
    {
        offline_store_->close();
    }
    Logger::info("Gateway stopped");
}

void GatewayContext::handleMessage(const TelemetryMessage& message)
{
    if (!telemetry_queue_.push(message)) //“生产者”
    {
        Logger::warn("Telemetry queue is full or stopped");
    }
}

/**
 * @brief 等待 MQTT 下一次重连，并支持 Gateway 停止操作中断等待。
 *
 * Worker 在 MQTT 连接失败后调用本函数。
 *
 * 与 std::this_thread::sleep_for(5s) 不同：
 * wait_for() 最多等待 5 秒，但 GatewayContext::stop()
 * 可以通过 mqtt_retry_condition_.notify_all()
 * 将 Worker 提前唤醒。
 *
 * @return true  等待正常超时，可以继续尝试 MQTT 重连。
 * @return false Gateway 已请求停止，应结束 MQTT 重连流程。
 */
bool GatewayContext::waitForMqttRetry()
{
    /*
     * condition_variable::wait_for() 需要 unique_lock。
     *
     * Worker 进入等待状态时，wait_for() 会暂时释放 mutex；
     * Worker 被唤醒后，再重新获得 mutex 并检查停止条件。
    */
    std::unique_lock<std::mutex> lock(mqtt_retry_mutex_);//unique_lock 允许中途 unlock / lock。
    /*
     * wait_for() 有两种结束方式：
     *
     * 1. 正常等待满 5 秒：
     *    stop_requested_ 仍然是 false。
     *
     * 2. stop() 设置 stop_requested_=true，
     *    并调用 notify_all()：
     *    Worker 被提前唤醒。
     *
     * predicate 返回 true 表示：
     * “停止条件已经满足”。
    */
    const bool stopped =
        mqtt_retry_condition_.wait_for(//最长等待时间。要求搭配：互斥锁
            lock,
            std::chrono::seconds(5),
            [this]()
            {
                return stop_requested_.load(); //最多等 5 秒；但是如果 stop_requested_ == true，可以提前结束。
            }
        );
    
    /*
     * stopped == true：
     *   Gateway 请求停止，所以返回 false。
     *
     * stopped == false：
     *   只是正常等满了 5 秒，可以继续重连。
     */
    return !stopped;
}

/**
 * @brief Gateway 遥测业务 Worker 主循环。
 *
 * Worker 是 TelemetryQueue 的消费者，负责：
 * 1. 获取 TelemetryMessage；
 * 2. 将消息转换为 JSON；
 * 3. 检查 MQTT 连接状态；
 * 4. Broker 不可用时进行重连；
 * 5. Broker 恢复后发布当前消息。
 *
 * MQTT 网络操作全部发生在 Worker 线程中，
 * 不会直接阻塞 TcpConnector 的 epoll I/O 线程。
 */
void GatewayContext::workerLoop()//工作者线程 运行函数
{
    TelemetryMessage message;

    /*
     * waitAndPop()：
     * - Queue 有数据时，取出一条 TelemetryMessage；
     * - Queue 为空时，Worker 在 condition_variable 上睡眠；
     * - Queue stop 且没有剩余数据时返回 false，结束循环。
     */
    while (telemetry_queue_.waitAndPop(message))
    {
        Logger::info("Worker received message from "+ message.device_id+ ", sequence="+ std::to_string(message.sequence));

        /*
         * mqtt_publisher_ 为空表示 MQTT 功能没有启用。
         *
         * 例如 gateway.yaml：
         *
         * mqtt:
         *   enabled: false
         *
         * 此时不执行任何 MQTT 操作。
         */
        if (!mqtt_publisher_)
        {
            Logger::debug("MQTT publisher is disabled, skip telemetry publish");

            continue;
        }
        /*
         * 将内部统一数据模型转换成 JSON 字符串。
         *
         * 输入：
         *   TelemetryMessage
         *
         * 输出示例：
         *   {
         *     "device_id": "sensor-001",
         *     "protocol": "tcp",
         *     "sequence": 1,
         *     ...
         *   }
         */
        const std::string payload = TelemetryConverter::toJson(message);
        
        /*
         * 当前消息只有在成功发布后才离开这个重试循环。
         *
         * 这样 Broker 短时间离线时，
         * 当前已经从 TelemetryQueue 取出的消息不会立即丢失。
         */
        while(true)
        {
            /*
             * Gateway 正在退出时，不再继续 MQTT 重连。
             *
             * return 会直接结束整个 workerLoop()，
             * 因此 Worker 线程也会结束。
             */
            if (stop_requested_.load())
            {
                Logger::info(
                    "Worker stopping; cancel MQTT retry"
                );
                return;
            }

            /*
             * 如果 MQTT 当前没有连接 Broker，
             * 先尝试重新建立连接。
             */
            if (!mqtt_publisher_->isConnected())
            {
                Logger::warn(
                    "MQTT disconnected; attempting to reconnect"
                );
        
                /*
                 * 尝试重新连接 Broker。
                 */
                if (!mqtt_publisher_->connect())
                {
                    Logger::warn(
                        "MQTT reconnect failed; retrying in 5 seconds"
                    );
                    /*
                     * 当前采用最简单的固定重试间隔。
                     * 这里只暂停 Worker，不会阻塞 TcpConnector I/O 线程。
                     */
                    if (!waitForMqttRetry())
                    {
                        Logger::info(
                            "Worker stopping during MQTT reconnect wait"
                        );
                        return;
                    }
                    continue;
                }
                Logger::info(
                    "MQTT connection restored"
                );
            }
                /*
                * MQTT 已连接，尝试发布当前遥测消息。
                */ 
            if (mqtt_publisher_->publish(payload))
            {
                Logger::debug(
                    "Telemetry published via MQTT, device="
                    + message.device_id
                    + ", sequence="
                    + std::to_string(message.sequence)
                );
                /*
                * 当前消息发送成功。
                * break 只退出当前的 MQTT 重试循环，
                * 外层 while 会继续从 TelemetryQueue 获取下一条消息。
                */
                break;
            }

            /*
            * publish() 失败通常意味着连接刚刚发生异常。
            *
            * 等待一段时间后，下一轮循环会再次检查连接状态，
            * 必要时重新执行 connect()。
            */
            Logger::warn(
                "MQTT publish failed; retrying in 5 seconds"
            );
            std::this_thread::sleep_for(
                std::chrono::seconds(5)
            );
        }
    }
    Logger::info("Telemetry worker stopped");
}
   
