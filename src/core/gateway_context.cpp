#include "core/gateway_context.h"
#include "connectors/mock_connector.h"
#include "connectors/tcp_connector.h"
#include "converters/telemetry_converter.h"
#include "common/logger.h"

#include <utility>
#include <memory>

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

    if (config.mqtt.enabled)
    {
        mqtt_publisher_ =
            std::make_unique<MqttPublisher>(
                config.mqtt.server_uri,
                config.mqtt.client_id,
                config.mqtt.topic,
                config.mqtt.qos
            );

        if (!mqtt_publisher_->connect())
        {
            Logger::error("Failed to connect MQTT broker");
            return false;
        }
    }

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
 * @brief 停止 Gateway 及其内部所有工作模块。
 *
 * 停止顺序：
 * 1. 先停止 Connector，阻止继续产生新的 TelemetryMessage；
 * 2. 停止 TelemetryQueue，通知 Worker 不再等待新消息；
 * 3. 等待 Worker 将队列中剩余消息处理完成并退出；
 * 4. 最后断开 MQTT，保证 Worker 处理剩余消息时仍然可以 publish；
 * 5. Gateway 完全停止。
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
     * 第二步：停止遥测队列。
     *
     * stop() 会：
     * - 标记 Queue 已停止；
     * - 唤醒可能正在 waitAndPop() 中睡眠的 Worker。
     */
    telemetry_queue_.stop();

    /*
     * 第三步：等待 Worker 正常结束。
     *
     * Worker 会优先处理 Queue 中已经存在的剩余消息，
     * 处理完成后 workerLoop() 才会退出。
     */
    if (worker_thread_.joinable())
    {
        worker_thread_.join();
    }
    
    /*
     * 第四步：断开 MQTT。
     *
     * MQTT 必须放在 Worker join() 之后断开。
     *
     * 原因：
     * Worker 在退出之前可能仍然需要把 Queue 中最后几条消息
     * publish 到 Broker。如果提前 disconnect，剩余消息就无法发送。
     */
    if (mqtt_publisher_)
    {
        mqtt_publisher_->disconnect();
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
 * @brief Gateway 业务工作线程主循环。
 *
 * 该线程作为 TelemetryQueue 的消费者：
 * 1. 阻塞等待队列中的 TelemetryMessage；
 * 2. 将统一遥测模型转换为 JSON；
 * 3. 通过 MQTT Publisher 将 JSON 发布到 Broker。
 *
 * 注意：
 * - 本函数运行在独立 Worker 线程中，不运行在 TcpConnector 的 I/O 线程中。
 * - MQTT publish 即使发生阻塞，也不会直接阻塞 epoll 网络接收线程。
 * - 当 TelemetryQueue 被 stop() 且剩余消息处理完成后，
 *   waitAndPop() 返回 false，本循环结束。
 */
void GatewayContext::workerLoop()//工作者线程 运行函数
{
    TelemetryMessage message;

    // 持续从遥测队列中消费消息。
    // waitAndPop() 在队列为空时会阻塞等待，而不是占用 CPU 空转。
    while (telemetry_queue_.waitAndPop(message))
    {
        Logger::info("Worker received message from "+ message.device_id+ ", sequence="+ std::to_string(message.sequence));

        /*
         * MQTT Publisher 只有在 mqtt.enabled=true 时才会创建。
         *
         * unique_ptr 为空表示当前没有启用 MQTT，
         * 此时不能调用 mqtt_publisher_->publish()。
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
         * 将 JSON 发布到 MQTT Broker。
         *
         * MqttPublisher 内部会使用 Paho mqtt::client，
         * 发布到 gateway.yaml 中配置的 topic。
         *
         * 返回 false 表示当前未连接 Broker，
         * 或 publish 过程中发生 MQTT 异常。
         */
        if (!mqtt_publisher_->publish(payload))
        {
            Logger::warn("Failed to publish telemetry via MQTT, device=" + message.device_id + ", sequence=" + std::to_string(message.sequence));

            continue;
        }
        Logger::debug("Telemetry published via MQTT, device=" + message.device_id+ ", sequence="+ std::to_string(message.sequence));
    }
    Logger::info("Telemetry worker stopped");
}