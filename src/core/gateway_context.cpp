#include "core/gateway_context.h"
#include "core/command_queue.h"
#include "connectors/mock_connector.h"
#include "connectors/tcp_connector.h"
#include "connectors/can_connector.h"
#include "connectors/modbus_connector.h"
#include "converters/telemetry_converter.h"
#include "converters/command_converter.h"
#include "common/logger.h"

#include <utility>
#include <memory>
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cmath>

namespace
{

    /**
     * @brief 获取steady_clock当前毫秒值。
     *
     * 这里只用于比较“经过了多久”，
     * 不表示真实日期时间。
     */
    std::int64_t steadyNowMs()
    {
        return
            std::chrono::duration_cast<std::chrono::milliseconds>
                (std::chrono::steady_clock::now().time_since_epoch()).count();
    }
}

bool GatewayContext::start(const std::string& config_path)
{
    if (!config_.load(config_path))
    {
        return false;
    }
    const GatewayConfig& config =config_.data();

    /*
    * 保存Watchdog核心健康超时阈值。
    */
    watchdog_health_timeout_sec_ = config.watchdog.health_timeout_sec;

    /*
    * 在Worker真正启动以前先给初始心跳。
    *
    * 防止Watchdog刚启动时，
    * 由于Worker还未来得及第一次循环就被误判。
    */
    telemetry_worker_heartbeat_ms_.store(steadyNowMs());

    command_in_progress_.store(false);

    command_started_ms_.store(0);


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
                config.mqtt.command_topic,
                config.mqtt.qos
            );
        
        mqtt_publisher_->setCommandCallback(
            [this](const std::string& topic,const std::string& payload)
            {
                handleCommandPayload(topic,payload);
            }
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
        /*
        * MQTT可能已经创建甚至连接成功，
        * 所以启动失败前统一清理已经建立的资源。
        */
        stop();
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

            /*
            * connector.get() 取得 unique_ptr 当前管理对象的原始指针。
            *
            * unique_ptr 仍然拥有对象；
            * source_connector 只是用于标记消息来源，
            * 不参与对象释放。get() 不会转移所有权。
            */
            IConnector* source_connector =connector.get();

            connector->setMessageCallback(
                [this,source_connector](const TelemetryMessage& message)
                {
                    handleMessage(message,source_connector);
                }
             );
            connectors_.push_back(std::move(connector));
        }
        else if(connector_config.type == "tcp")
        {
            auto connector = std::make_unique<TcpConnector>(connector_config.port);
            IConnector* source_connector =connector.get();

            connector->setMessageCallback(
                [this, source_connector](const TelemetryMessage& message)
                {
                    handleMessage(message,source_connector);
                }
            );

            connectors_.push_back(std::move(connector));
        }
        else if (connector_config.type == "can")
        {
            /*
            * CAN Connector必须指定SocketCAN接口。
            *
            * 例如：
            *
            * Ubuntu开发环境：
            *   interface: vcan0
            *
            * 真实CAN硬件：
            *   interface: can0
            */
            if (connector_config.interface_name.empty())
            {
                Logger::error(
                    "CAN connector interface is empty: "
                    + connector_config.name
                );

                stop();
                return false;
            }

             /*
            * 根据gateway.yaml中的接口名称
            * 创建CanConnector。
            *
            * 不再把vcan0写死在C++代码里。
            */
            auto connector = std::make_unique<CanConnector>(connector_config.interface_name);
            
            /*
            * 保存当前Connector的原始指针。
            *
            * connectors_中的unique_ptr仍然负责对象生命周期，
            * source_connector这里只用于告诉GatewayContext：
            *
            * “这条TelemetryMessage来自哪个Connector”。
            */
            IConnector* source_connector = connector.get();
            
             /*
            * CAN帧在CanConnector内部完成解析后，
            * 已经变成统一TelemetryMessage。
            *
            * 因此这里直接复用GatewayContext已有的
            * handleMessage()入口。
            */
            connector->setMessageCallback(
                [this, source_connector](const TelemetryMessage& message)
                {
                    handleMessage(message,source_connector);
                }
            );

            /*
            * 放入统一Connector容器。
            *
            * 后面的统一start()/stop()
            * 会自动管理CanConnector生命周期。
            */
            connectors_.push_back(std::move(connector));
        }
        else if (connector_config.type == "modbus")
        {
            /*
            * Modbus TCP必须知道：
            *
            * 1. 目标设备IP
            * 2. 目标端口
            * 3. Gateway内部device_id
            *
            * 这些字段缺失时不允许继续启动。
            */
            if (connector_config.host.empty() || connector_config.port <= 0 || connector_config.device_id.empty())
            {
                Logger::error("Invalid Modbus connector configuration: " + connector_config.name);
                stop();
                return false;
            }

            /*
            * 根据gateway.yaml创建ModbusConnector。
            *
            * 当前实际参数：
            *
            * host             = 127.0.0.1
            * port             = 1502
            * device_id        = modbus-plc-01
            * poll_interval_ms = 1000
            */
            auto connector = std::make_unique<ModbusConnector>(connector_config.host,connector_config.port,connector_config.device_id,connector_config.poll_interval_ms);
            
            /*
            * 保存消息来源Connector。
            *
            * 后续Modbus轮询得到TelemetryMessage后，
            * GatewayContext会利用它建立：
            *
            * modbus-plc-01 → ModbusConnector
            *
            * 的下行CommandRouter路由。
            */
            IConnector* source_connector = connector.get();
            
            /*
            * 继续复用整个Gateway已有的统一遥测入口。
            *
            * ModbusConnector不直接操作MQTT或SQLite。
            */
            connector->setMessageCallback(
                [this, source_connector](const TelemetryMessage& message)
                {
                    handleMessage(message,source_connector);
                }
            );
            
            /*
            * 交给GatewayContext统一管理生命周期。
            *
            * 后面的：
            *
            * connector->start()
            *
            * 会自动调用ModbusConnector::start()。
            */
            connectors_.push_back(std::move(connector));
        }

    }

    for (auto& connector : connectors_)
    {
        if (!connector->start())
        {
            Logger::error("Failed to start connector: "+ connector->name());
            
            /*
            * 前面的Connector可能已经成功启动。
            *
            * stop()会统一将已经启动的Connector关闭，
            * 未启动的Connector执行stop()也能够安全处理。
            */
            stop();

            return false;
        }
    }

    worker_thread_ = std::thread(&GatewayContext::workerLoop,this);//创建工作者线程

    /*
    * 启动独立的下行Command Worker。
    *
    * 它负责消费CommandQueue，
    * 与Telemetry Worker相互独立。
    */
    command_worker_thread_ = std::thread(&GatewayContext::commandWorkerLoop,this);

    /*
    * MQTT、Connector、Telemetry Worker、
    * Command Worker都已经启动完成。
    *
    * 最后再开放本地管理接口。
    */
    control_server_ = std::make_unique<GatewayControlServer>("/tmp/gateway.sock",*this);
    if (!control_server_->start())
    {
        Logger::error(
            "Failed to start GatewayControlServer"
        );
        
        /*
        * 此时Connector、Telemetry Worker、
        * Command Worker都已经启动。
        *
        * 必须统一stop()，
        * 否则std::thread仍然处于joinable状态。
        */
        stop();

        return false;
    }

    /*
    * Watchdog最后启动。
    *
    * 原因：
    * 到这里Connector、Telemetry Worker、
    * Command Worker、Control Server都已经启动完成。
    *
    * Watchdog一旦启动，
    * Gateway才正式承担“持续喂狗”的责任。
    */
    if (config.watchdog.enabled)
    {
        watchdog_manager_ =
            std::make_unique<WatchdogManager>(
                config.watchdog.device,
                config.watchdog.timeout_sec,
                config.watchdog.feed_interval_sec,

                /*
                * WatchdogManager不知道Gateway内部结构。
                *
                * 它只通过这个回调询问：
                *
                * “Gateway当前是否健康？”
                */
                [this]()
                {
                    return isCoreHealthyForWatchdog();
                }
            );

        if (!watchdog_manager_->start())
        {
            Logger::error(
                "Failed to start hardware watchdog"
            );

            watchdog_manager_.reset();

            /*
            * 配置明确要求启用Watchdog，
            * 但Watchdog无法工作。
            *
            * 对正式板端部署来说这是启动失败，
            * 不能假装系统具备Watchdog保护。
            */
            stop();

            return false;
        }
    }

    /*
    * 到这里说明：
    *
    * Connector
    * Telemetry Worker
    * Command Worker
    * Control Server
    *
    * 已经全部成功启动。
    *
    * 从这一刻开始计算Gateway运行时间。
    */
    started_at_ = std::chrono::steady_clock::now();
    started_ = true;

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
    * Gateway进入正常关闭流程时，
    * 第一件事先停止Watchdog。
    *
    * 后面的：
    *
    * Connector stop
    * Worker join
    * SQLite close
    *
    * 理论上都可能需要一些时间。
    *
    * 如果Watchdog仍然在运行，
    * 正常shutdown过程中也可能因为没有及时喂狗
    * 被误认为系统卡死而复位。
    */
    if (watchdog_manager_)
    {
        watchdog_manager_->stop();
        watchdog_manager_.reset();
    }    

    /*
    * Control Server必须优先停止。
    *
    * 因为它仍然可能收到status/devices请求，
    * 并访问下面的MQTT和Connector状态。
    */
    if (control_server_)
    {
        control_server_->stop();
        control_server_.reset();
    }
    
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
    * 停止CommandQueue，
    * 唤醒可能正在waitAndPop()等待命令的Command Worker。
    */
    command_queue_.stop();

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
    * 等待Command Worker完成剩余命令并正常退出。
    */
    if (command_worker_thread_.joinable())
    {
        command_worker_thread_.join();
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
    started_ = false;
    Logger::info("Gateway stopped");

}

void GatewayContext::handleMessage(const TelemetryMessage& message,IConnector* source_connector)
{
     /*
     * 设备已经通过该Connector成功产生了一条有效遥测数据，
     * 因此更新它当前所属的下行路由。
     *
     * 例如：
     *
     * sensor-001 -> TcpConnector
     */
    command_router_.registerDevice(message.device_id,source_connector);

    /*
     * 原有上行链路保持不变。
     */
    if (!telemetry_queue_.push(message)) //“生产者”
    {
        Logger::warn("Telemetry queue is full or stopped");
    }
}

/**
 * @brief Gateway接收到MQTT下行Command后的业务入口。
 *
 * 当前职责：
 * 1. 接收MqttPublisher转发来的原始Payload；
 * 2. 使用CommandConverter完成解析和公共字段校验；
 * 3. 得到统一DeviceCommand。
 *
 * 当前阶段暂时只打印解析结果。
 * 下一阶段才交给CommandRouter。
 */
void GatewayContext::handleCommandPayload(const std::string& topic,const std::string& payload)
{
    Logger::info("Gateway received MQTT command, topic=" + topic);

    DeviceCommand command;
     /*
     * 外部MQTT JSON
     *      ↓
     * CommandConverter
     *      ↓
     * DeviceCommand
     */
    if (!CommandConverter::fromJson(payload,command))
    {
        Logger::warn(
            "MQTT command rejected because payload is invalid"
        );
        return;
    }

    /*
     * 到这里说明：
     *
     * request_id
     * device_id
     * action
     *
     * 已经完成基本合法性检查。
     */
    Logger::info("Command parsed successfully: request_id="
        + command.request_id
        + ", device_id="
        + command.device_id
        + ", action="
        + command.action
        + ", parameters="
        + command.parameters.dump()
    );

   /*
    * Command已经完成解析和公共字段校验。
    *
    * 此处不直接执行设备控制，
    * 而是立即放入CommandQueue，
    * 让MQTT回调路径尽快返回。
    */
    if (!command_queue_.push(command))
    {
        Logger::warn(
            "Command queue is full or stopped, request_id="
            + command.request_id
        );

        return;
    }

    Logger::debug(
        "Command queued successfully, request_id="
        + command.request_id
    );
}


/**
 * @brief 补发一批 SQLite 中待发送的离线遥测数据。
 *
 * 当前每轮最多处理 100 条记录。
 *
 * 只有 MQTT 当前已经连接时才执行补发。
 * 每条记录必须满足：
 *
 * SQLite读取
 *     ↓
 * MQTT publish成功
 *     ↓
 * SQLite remove
 *
 * 严禁在 publish 成功之前删除数据库记录，
 * 否则网络发送失败时会造成数据永久丢失。
 */
void GatewayContext::flushOfflineBatch()
{
    /*
     * OfflineStore 或 MQTT Publisher 尚未创建时，
     * 当前没有补发条件。
     */
    if (!offline_store_ || !mqtt_publisher_)
    {
        return;
    }

    /*
     * Broker 当前没有连接时不能执行补发。
     *
     * MQTT 重连由 workerLoop() 统一负责，
     * 本函数只负责“连接已经正常以后”的历史数据补发。
     */
    if (!mqtt_publisher_->isConnected())
    {
        return;
    }

    /*
     * 每次最多读取100条离线记录。
     *
     * loadPending() 内部使用：
     *
     * ORDER BY id ASC
     * LIMIT 100
     *
     * 因此优先取得更早进入 SQLite 的记录。
     */
    const std::vector<OfflineMessage> pending_messages = offline_store_->loadPending(100);
    if (pending_messages.empty())
    {
        return;
    }

    Logger::info("Flushing " + std::to_string(pending_messages.size()) + " offline message(s)");
    
    /*
     * 逐条补发本批离线记录。
     */
    for (const OfflineMessage& offline_message : pending_messages)
    {
        /*
         * Gateway 已开始停止时，
         * 不继续做新的网络补发操作。
         *
         * 数据仍然留在 SQLite，
         * 下次 Gateway 启动后可以再次补发。
         */
        if (stop_requested_.load())
        {
            return;
        }

        /*
         * 直接发送数据库中保存的 JSON Payload。
         *
         * 当初 save() 保存的就是 MQTT 最终要发送的 JSON，
         * 因此这里无需重新构造 TelemetryMessage。
         */
        if (!mqtt_publisher_->publish(offline_message.payload))
        {
            Logger::warn("Failed to flush offline message id=" + std::to_string(offline_message.id));
            
            /*
             * publish 失败通常说明 MQTT 连接已经异常。
             *
             * 主动断开当前 Client 状态，
             * 后续由 workerLoop() 的统一重连逻辑负责恢复。
             */
            mqtt_publisher_->disconnect();

            /*
             * 当前记录没有 remove，
             * 因此仍然安全保留在 SQLite。
             */
            return;
        }

        /*
         * MQTT 已经成功发送当前离线消息。
         *
         * 此时才能删除 SQLite 中对应的记录。
         */
        if (!offline_store_->remove(offline_message.id))
        {
            Logger::error(
                "Offline message published but failed to remove "
                "SQLite record id="
                + std::to_string(offline_message.id)
            );

            /*
             * 删除失败时立即停止本批处理。
             *
             * 数据库记录仍然存在，因此以后可能发生重复发送。
             * 这比“消息已经删除但实际上没发送出去”更安全。
             */
            return;
        }

        Logger::info(
            "Offline message flushed, id="
            + std::to_string(offline_message.id)
            + ", device="
            + offline_message.device_id
        );
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
     * 性能验证代码：
     *
     * 只统计modbus-plc-01实时MQTT成功发布的消息。
     * 不逐条打印性能日志，避免日志I/O本身污染测量结果。
     *
     * serialize：
     *   TelemetryMessage -> JSON
     *
     * publish：
     *   进入MqttPublisher::publish()
     *   到publish()返回
     *
     * total：
     *   JSON转换 + MQTT publish
     */
    constexpr std::size_t kPerfTargetSamples = 600;

    std::vector<double> serialize_latencies_ms;
    std::vector<double> publish_latencies_ms;
    std::vector<double> total_latencies_ms;

    serialize_latencies_ms.reserve(kPerfTargetSamples);
    publish_latencies_ms.reserve(kPerfTargetSamples);
    total_latencies_ms.reserve(kPerfTargetSamples);

    bool perf_reported = false;
    auto next_mqtt_reconnect_time = std::chrono::steady_clock::now();

    /*
     * 下一次允许尝试 MQTT 重连的时间点。
     *
     * 该变量只由 Worker 线程访问，因此不需要 mutex 或 atomic。
     *
     * 初始值设为 now()：
     * 如果 Gateway 启动时 MQTT 连接失败，
     * Worker 启动后可以立即再检查一次连接状态。
     */
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
        * 能从waitAndPopFor()正常返回，
        * 说明Telemetry Worker线程仍然能够推进。
        *
        * 不管本轮：
        *
        * - 收到消息
        * - 只是1秒超时
        *
        * 都属于Worker正常存活。
        */
        telemetry_worker_heartbeat_ms_.store(steadyNowMs());

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
        * MQTT 当前已经连接时，
        * 尝试补发一批 SQLite 历史离线消息。
        *
        * 即使本轮 Queue 没有新的设备数据，
        * waitAndPopFor() 超时醒来后也会运行到这里，
        * 因此 Broker 恢复以后无需等待设备再次发消息，
        * 历史数据就能够自动恢复发送。
        */
        if (mqtt_publisher_->isConnected()&& !stop_requested_.load())
        {
            flushOfflineBatch();

            /*
            * flushOfflineBatch() 中如果历史消息 publish 失败，
            * 会主动 disconnect()。
            *
            * 此时重新安排下一次 MQTT 重连时间，
            * 防止下一轮马上高频执行 connect()。
            */
           if (!mqtt_publisher_->isConnected())
            {
                next_mqtt_reconnect_time =std::chrono::steady_clock::now()+ std::chrono::seconds(5);
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
        /*
        * 从这里开始计时。
        *
        * 注意放在上面的Logger::info()之后，
        * 因此“Worker received...”日志写文件的时间
        * 不会被算进本次性能结果。
        */
        const auto perf_total_start = std::chrono::steady_clock::now();
        const auto perf_serialize_start = perf_total_start;
        const std::string payload = TelemetryConverter::toJson(message);
        const auto perf_serialize_end = std::chrono::steady_clock::now();
                
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
            /*
            * 单独测量MQTT publish调用。
            *
            * publish_start：
            *   即将进入MqttPublisher::publish()
            *
            * publish_end：
            *   publish()已经返回
            */
            const auto perf_publish_start = std::chrono::steady_clock::now();
            const bool publish_ok = mqtt_publisher_->publish(payload);
            const auto perf_publish_end = std::chrono::steady_clock::now();
            if (publish_ok)
            {
                /*
                * 只统计固定Modbus设备，
                * 避免Mock/TCP等不同数据源混入结果。
                */
                if (!perf_reported && message.device_id == "modbus-plc-01")
                {
                    const double serialize_ms =std::chrono::duration<double, std::milli>(perf_serialize_end- perf_serialize_start).count();
                    const double publish_ms = std::chrono::duration<double, std::milli>(perf_publish_end- perf_publish_start).count();
                    const double total_ms = std::chrono::duration<double, std::milli>(perf_publish_end - perf_total_start).count();

                    serialize_latencies_ms.push_back(serialize_ms);
                    publish_latencies_ms.push_back(publish_ms);
                    total_latencies_ms.push_back(total_ms);

                    /*
                    * 收集满600条以后只计算一次。
                    *
                    * 测试期间不逐条输出性能日志，
                    * 尽量减少测量本身带来的干扰。
                    */
                    if (total_latencies_ms.size()>= kPerfTargetSamples)
                    {
                        auto percentile =
                            [](std::vector<double> values,double p) -> double
                            {
                                std::sort(values.begin(),values.end());
                                const std::size_t index = static_cast<std::size_t>(
                                        std::ceil(p* static_cast<double>(values.size()))) - 1;
                                return values[std::min(index,values.size() - 1)];
                            };

                        auto mean =
                            [](const std::vector<double>& values)
                            {
                                return std::accumulate(values.begin(),values.end(),0.0) / static_cast<double>(values.size());
                            };

                        Logger::info(
                            "PERF serialize: samples="+ std::to_string(serialize_latencies_ms.size())
                            + ", mean="
                            + std::to_string(mean(serialize_latencies_ms))
                            + " ms, p50="
                            + std::to_string(percentile(serialize_latencies_ms,0.50))
                            + " ms, p95="
                            + std::to_string(percentile(serialize_latencies_ms,0.95))
                            + " ms, p99="
                            + std::to_string(percentile(serialize_latencies_ms,0.99))
                            + " ms"
                        );

                        Logger::info(
                            "PERF mqtt_publish: samples="
                            + std::to_string(publish_latencies_ms.size())
                            + ", mean="
                            + std::to_string(mean(publish_latencies_ms))
                            + " ms, p50="
                            + std::to_string(percentile(publish_latencies_ms,0.50))
                            + " ms, p95="
                            + std::to_string(percentile(publish_latencies_ms,0.95))
                            + " ms, p99="
                            + std::to_string(percentile(publish_latencies_ms,0.99))
                            + " ms"
                        );

                        Logger::info(
                            "PERF total: samples="
                            + std::to_string(total_latencies_ms.size())
                            + ", mean="
                            + std::to_string(mean(total_latencies_ms))
                            + " ms, p50="
                            + std::to_string(percentile(total_latencies_ms,0.50))
                            + " ms, p95="
                            + std::to_string(percentile(total_latencies_ms,0.95))
                            + " ms, p99="
                            + std::to_string(percentile(total_latencies_ms,0.99))
                            + " ms"
                        );

                        perf_reported = true;
                    }
                }

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
   
/**
 * @brief Gateway下行Command Worker主循环。
 *
 * 当前职责：
 * 1. 阻塞等待CommandQueue中的DeviceCommand；
 * 2. 取出命令；
 * 3. 记录结构化命令信息。
 *
 * 下一阶段将在这里调用CommandRouter，
 * 将命令真正路由到TCP/CAN/Modbus Connector。
 */
void GatewayContext::commandWorkerLoop()
{
    DeviceCommand command;
    /*
     * waitAndPop()：
     *
     * 有命令：
     *   返回true，继续处理。
     *
     * 没命令：
     *   当前线程睡眠，不占用CPU空转。
     *
     * Queue stop且已经没有剩余命令：
     *   返回false，while结束。
     */
    while(command_queue_.waitAndPop(command))
    {
        Logger::info("Command worker received command: request_id="
            + command.request_id
            + ", device_id="
            + command.device_id
            + ", action="
            + command.action
            + ", parameters="
            + command.parameters.dump()
        );

        /*
        * Command已经真正开始执行。
        *
        * Command Worker平时可能长时间没有命令，
        * 那属于正常idle状态。
        *
        * 所以Watchdog不监控“多久没有命令”，
        * 只监控“一条命令执行了多久还没返回”。
        */
        command_started_ms_.store(steadyNowMs());

        command_in_progress_.store(true);

        /*
        * Command已经真正开始执行。
        *
        * Command Worker平时可能长时间没有命令，
        * 那属于正常idle状态。
        *
        * 所以Watchdog不监控“多久没有命令”，
        * 只监控“一条命令执行了多久还没返回”。
        */
        command_started_ms_.store(steadyNowMs());
        command_in_progress_.store(true);
        const bool dispatch_ok = command_router_.dispatch(command);

        /*
        * dispatch()已经正常返回，
        * 当前Command Worker不再处于执行状态。
        */
        command_in_progress_.store(false);

        if (!dispatch_ok)
        {
            Logger::warn(
                "Command dispatch failed, request_id="
                + command.request_id
                + ", device_id="
                + command.device_id
            );
        }

    }
    Logger::info("Command worker stopped");
}

std::vector<std::string> GatewayContext::getOnlineDevices()
{
    /*
     * all_devices：
     * 用来保存整个Gateway汇总后的在线设备列表。
     *
     * 它只是当前查询产生的临时快照，
     * 不是Gateway长期维护的第二份设备状态。
     */
    std::vector<std::string> all_devices;

    /*
     * connectors_里面保存的是：
     *
     * std::unique_ptr<IConnector>
     *
     * 虽然统一表现为IConnector，
     * 但实际对象可能是：
     *
     * TcpConnector
     * MockConnector
     * 后续的CanConnector
     * 后续的ModbusConnector
     */
    for (const auto& connector : connectors_)
    {
        /*
         * 通过IConnector的虚函数调用。
         *
         * 如果实际对象是TcpConnector，
         * 最终执行TcpConnector::getOnlineDevices()。
         *
         * 如果实际对象是MockConnector，
         * 就执行MockConnector::getOnlineDevices()。
         */
        std::vector<std::string> devices = connector->getOnlineDevices();
        /*
         * 将当前Connector返回的设备列表
         * 追加到整个Gateway的all_devices中。
         *
         * insert的位置：
         * all_devices.end()
         *
         * 意思是从当前vector末尾继续追加。
         */
        all_devices.insert(all_devices.end(),devices.begin(),devices.end());
    }
    return all_devices;
}

bool GatewayContext::isMqttConnected() const
{
    /*
     * mqtt_publisher_可能不存在。
     *
     * 例如：
     * MQTT在配置中没有启用，
     * 那么Gateway启动时可能就没有创建MqttPublisher。
     *
     * 所以不能直接：
     *
     * mqtt_publisher_->isConnected()
     *
     * 否则空指针会出问题。
     */
    if (!mqtt_publisher_)
    {
        return false;
    }

    /*
     * 真正的连接状态仍然由MqttPublisher负责。
     *
     * GatewayContext这里只做统一查询入口。
     */
    return mqtt_publisher_->isConnected();
}

std::size_t GatewayContext::getTelemetryQueueSize()
{
    return telemetry_queue_.size();
}

std::size_t GatewayContext::getTelemetryQueueCapacity() const
{
    return telemetry_queue_.capacity();
}

std::size_t GatewayContext::getDroppedTelemetryCount()
{
    return telemetry_queue_.droppedCount();
}

ProcessMetrics GatewayContext::getProcessMetrics() const
{
    /*
     * GatewayContext不保存第二份资源统计。
     *
     * 每次status查询都直接读取当前Linux进程状态。
     */
    return ProcessMetricsReader::read();
}

std::uint64_t GatewayContext::getUptimeSeconds() const
{
    if (!started_)
    {
        return 0;
    }
    const auto now = std::chrono::steady_clock::now();
    const auto uptime = std::chrono::duration_cast<std::chrono::seconds> (now - started_at_);
    return static_cast<std::uint64_t> (uptime.count());
}

bool GatewayContext::isCoreHealthyForWatchdog() const
{
    const std::int64_t now_ms = steadyNowMs();
    const std::int64_t health_timeout_ms = static_cast<std::int64_t>(watchdog_health_timeout_sec_) * 1000;

    /*
     * 第一项：
     * Telemetry Worker必须持续推进。
     *
     * 即使没有设备数据，
     * waitAndPopFor()也会每1秒返回一次Timeout，
     * 所以正常情况下心跳会持续更新。
     */
    const std::int64_t telemetry_heartbeat = telemetry_worker_heartbeat_ms_.load();
    const std::int64_t telemetry_age = now_ms - telemetry_heartbeat;

    if (telemetry_heartbeat <= 0|| telemetry_age > health_timeout_ms)
    {
        Logger::error(
            "Watchdog health check failed: "
            "Telemetry Worker stalled for "
            + std::to_string(telemetry_age)
            + " ms"
        );

        return false;
    }


    /*
     * 第二项：
     * Command Worker空闲时不要求心跳。
     *
     * 只有真正执行DeviceCommand时，
     * 才检查一次dispatch是否卡住。
     */
    if (command_in_progress_.load())
    {
        const std::int64_t command_started = command_started_ms_.load();
        const std::int64_t command_age = now_ms - command_started;

        if (command_started > 0 && command_age > health_timeout_ms)
        {
            Logger::error(
                "Watchdog health check failed: "
                "Command Worker stalled for "
                + std::to_string(command_age)
                + " ms"
            );

            return false;
        }
    }


    /*
     * MQTT断线不检查。
     * PLC断线不检查。
     * TCP客户端离线不检查。
     *
     * 这些都是外部故障，
     * Gateway本身仍然能够正常运行和恢复。
     */
    return true;
}

bool GatewayContext::isWatchdogEnabled() const
{
    return config_.data().watchdog.enabled;
}

bool GatewayContext::isWatchdogFeeding() const
{
    return watchdog_manager_ && watchdog_manager_->isFeeding();
}