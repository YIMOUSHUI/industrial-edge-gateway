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
    * 通知 Worker：
    * Gateway 已经进入停止流程。
    *
    * Worker 后续不会再主动进行 MQTT 网络重连，
    * 剩余 Queue 消息将优先持久化后退出。
    */
    stop_requested_.store(true);

    /*
    * 停止 Queue，并唤醒当前可能正在
    * waitAndPopFor() 中等待消息的 Worker。
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
 * @brief Gateway 遥测业务 Worker 主循环。
 *
 * Worker 负责：
 * 1. 从 TelemetryQueue 获取新遥测消息；
 * 2. MQTT 在线时直接发布消息；
 * 3. MQTT 离线或发布失败时，将消息持久化到 SQLite；
 * 4. 即使 Queue 暂时没有新消息，也周期性醒来检查 MQTT 状态；
 * 5. MQTT 断开期间最多每 5 秒尝试一次重新连接。
 *
 * 当前阶段只完成 Store-and-Forward 中的 Store：
 * 离线数据能够写入 SQLite。
 *
 * SQLite 历史数据的自动补发将在下一阶段加入。
 */
void GatewayContext::workerLoop()//工作者线程 运行函数
{
    TelemetryMessage message;

    /*
     * 下一次允许尝试 MQTT 重连的时间点。
     *
     * 该变量只由 Worker 线程访问，因此不需要 mutex 或 atomic。
     *
     * 初始值设为 now()：
     * 如果 Gateway 启动时 MQTT 连接失败，
     * Worker 启动后可以立即再检查一次连接状态。
     */
    auto next_mqtt_reconnect_time = std::chrono::steady_clock::now();
    /*
     * waitAndPop()：
     * - Queue 有数据时，取出一条 TelemetryMessage；
     * - Queue 为空时，Worker 在 condition_variable 上睡眠；
     * - Queue stop 且没有剩余数据时返回 false，结束循环。
     */
    while (true)
    {
        /*
         * 最多等待 1 秒获取新的 TelemetryMessage。
         *
         * 与旧 waitAndPop() 不同：
         * 即使没有新设备数据，Worker 最多 1 秒也会醒一次，
         * 从而有机会检查 MQTT 是否需要重连。
         */
        const QueuePopResult pop_result =telemetry_queue_.waitAndPopFor(message,std::chrono::milliseconds(1000));
         
        /*
         * Queue 已 stop 且没有剩余消息。
         *
         * Worker 的所有输入已经处理完成，
         * 可以结束线程。
         */
        if (pop_result == QueuePopResult::Stopped)
        {
            break;
        }

        /*
         * MQTT 功能没有启用时：
         *
         * 当前项目不把“主动关闭 MQTT”视为网络故障，
         * 因此不会把所有消息写入离线数据库。
         *
         * Message：
         *   跳过 MQTT 发布。
         *
         * Timeout：
         *   没有消息，也没有 MQTT 维护工作。
         */
        if (!mqtt_publisher_)
        {
            if (pop_result == QueuePopResult::Message)
            {
                Logger::debug(
                    "MQTT publisher is disabled, skip telemetry publish"
                );
            }
            continue;
        }

        /*
         * Worker 每一轮都可以检查 MQTT 状态。
         *
         * 但即使 Worker 每 1 秒甚至更快运行一次，
         * 也不意味着每次都执行 connect()。
         *
         * 只有：
         * 1. MQTT 当前未连接；
         * 2. Gateway 没有正在退出；
         * 3. 已经到达下一次允许重连的时间；
         *
         * 才真正尝试连接 Broker。
         */
        if (!mqtt_publisher_->isConnected()&& !stop_requested_.load())
        {
            const auto now = std::chrono::steady_clock::now();
            if (now >= next_mqtt_reconnect_time)
            {
                Logger::warn(
                    "MQTT disconnected; attempting to reconnect"
                );

                if (mqtt_publisher_->connect())
                {
                    Logger::info(
                        "MQTT connection restored"
                    );
                }
                else
                {
                     /*
                     * 本次重连失败。
                     *
                     * 不让 Worker sleep 5 秒，
                     * 只是记录“下一次 5 秒以后再允许 connect”。
                     *
                     * 这 5 秒期间 Worker仍然能够继续消费 Queue，
                     * 并把离线消息写入 SQLite。
                     */
                    next_mqtt_reconnect_time = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                    Logger::warn(
                        "MQTT reconnect failed; "
                        "next attempt in 5 seconds"
                    );
                }


            }

        }

        /*
         * Timeout 只表示本轮没有新的设备消息。
         *
         * MQTT 状态检查已经在上面完成，
         * 因此本轮可以直接结束，
         * 回到 while 顶部继续等待。
        */
        if (pop_result == QueuePopResult::Timeout)
        {
            continue;
        }

        /*
         * 能运行到这里，说明本轮确实取到了一条新消息。
         */
        Logger::info(
            "Worker received message from " + message.device_id + ", sequence=" + std::to_string(message.sequence)
        );

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
         * Gateway 已经进入停止流程时，
         * 不再尝试新的网络发送。
         *
         * 当前 Queue 中已经存在的剩余消息
         * 直接持久化到 SQLite，
         * 防止进程退出导致内存数据丢失。
         */
        if (stop_requested_.load())
        {
            if (!offline_store_->save(message.device_id,payload,message.timestamp_ms))
            {
                Logger::error(
                    "Failed to persist telemetry during shutdown, device="
                    + message.device_id
                );
            }
            continue;
        }

        /*
         * MQTT 在线时优先直接发布实时消息。
         */
        if (mqtt_publisher_->isConnected())
        {
            if (mqtt_publisher_->publish(payload))
            {
                Logger::debug(
                    "Telemetry published via MQTT, device="
                    + message.device_id
                    + ", sequence="
                    + std::to_string(message.sequence)
                );
                continue;
            }
            /*
             * isConnected() 刚才虽然为 true，
             * 但 publish() 仍可能因为网络在此刻异常而失败。
             *
             * 当前消息不能丢，因此下面仍会保存到 SQLite。
             *
             * 同时将下一次重连安排在 5 秒以后，
             * 防止后续循环立即高频重连。
             */
            Logger::warn(
                "MQTT publish failed; storing telemetry offline"
            );

            mqtt_publisher_->disconnect();
            next_mqtt_reconnect_time = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        }
        else
        {
            Logger::warn(
                "MQTT unavailable; storing telemetry offline"
            );
        }

        /*
         * MQTT 当前不可用，或者刚才 publish 失败。
         *
         * 把已经转换好的 MQTT JSON Payload
         * 持久化到 SQLite。
         *
         * 保存完成后不阻塞当前消息，
         * Worker 下一轮继续消费 TelemetryQueue。
         */
        if (!offline_store_->save(message.device_id,payload,message.timestamp_ms))
        {
            /*
             * 当前阶段 SQLite 是最后一道持久化保障。
             *
             * 如果 save() 也失败，
             * 当前消息无法安全保留，因此必须明确记录错误。
             */
            Logger::error(
                "Failed to persist offline telemetry, device="
                + message.device_id
                + ", sequence="
                + std::to_string(message.sequence)
            );
        }
        else
        {
            Logger::info(
                "Telemetry stored offline, device="
                + message.device_id
                + ", sequence="
                + std::to_string(message.sequence)
            );
        }
    }
    Logger::info("Telemetry worker stopped");
}
   
