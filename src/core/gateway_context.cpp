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
#include <functional>
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
    * 每次Gateway正式启动时重新清除停止标志。
    */
    stop_requested_.store(false);

    /*
    * 保存Watchdog核心健康超时阈值。
    */
    watchdog_health_timeout_sec_ = config.watchdog.health_timeout_sec;

    /*
    * 初始化Telemetry Processing Worker Pool。
    *
    * 配置：
    *
    * runtime.worker_threads
    *
    * 决定真正创建多少个Processing Worker。
    */
    telemetry_queues_.clear();
    telemetry_worker_threads_.clear();
    telemetry_worker_heartbeats_.clear();

    /*
    * 清理 M2、M3、M4 的运行对象。
    */
    rule_engines_.clear();
    anomaly_detectors_.clear();
    ai_anomaly_detectors_.clear();

    /*
    * 每次 Gateway 启动时重置 M3 统计指标。
    */
    anomaly_windows_scored_.store(0,std::memory_order_relaxed);
    anomalies_detected_.store(0,std::memory_order_relaxed);

    /*
    * 原有 M2 统计逻辑保持不变。
    */
    rule_messages_evaluated_.store(0);
    rule_triggers_total_.store(0);
    local_rule_actions_total_.store(0);

    const std::size_t worker_count = static_cast<std::size_t>(config.worker_threads);

    /*
    * V1的TelemetryQueue总容量是1000。
    *
    * Worker Pool之后仍然维持总容量约1000，
    * 而不是每个Worker各1000导致总缓存偷偷扩大。
    */
    const std::size_t base_capacity = kTelemetryQueueCapacity / worker_count;
    const std::size_t remainder = kTelemetryQueueCapacity % worker_count;
    for (std::size_t i = 0;i < worker_count;++i)
    {
        /*
        * 如果1000不能整除worker_count，
        * 把余数分配给前几个Queue。
        */
        const std::size_t queue_capacity = base_capacity + (i < remainder ? 1 : 0);

        telemetry_queues_.push_back(std::make_unique<TelemetryQueue>(queue_capacity));

        auto heartbeat =std::make_unique<std::atomic<std::int64_t>>();
        heartbeat->store(steadyNowMs());

        telemetry_worker_heartbeats_.push_back(std::move(heartbeat));

        /*
        * 每个Processing Worker拥有独立RuleEngine实例。
        *
        * Rules配置很小，
        * 为每个Worker复制一份的成本可以忽略。
        */
        rule_engines_.push_back(std::make_unique<RuleEngine>(config.rules));

        /*
        * M3：每个 Processing Worker 拥有独立的
        * AnomalyDetector。
        *
        * 配置启用：
        *   创建实际检测器并复制 Profile 配置。
        *
        * 配置关闭：
        *   保存 nullptr，不运行异常检测。
        */
        if (config.anomaly_detection.enabled)
        {
            anomaly_detectors_.push_back(std::make_unique<AnomalyDetector>(config.anomaly_detection.profiles));
        }
        else
        {
            anomaly_detectors_.push_back(nullptr);
        }
         /*
         * M4稍后只在负责目标device_id的Worker
         * 创建实际实例。
         *
         * 这里先保证：
         *
         * ai_anomaly_detectors_[i]
         *
         * 和Worker编号完全对应。
         */
        ai_anomaly_detectors_.push_back(nullptr);
    }

    /*
    * 初始化唯一Uplink Worker心跳。
    */
    uplink_worker_heartbeat_ms_.store(steadyNowMs());

    command_in_progress_.store(false);

    command_started_ms_.store(0);


    Logger::init(config.log_level);
    Logger::info("Gateway starting");
    Logger::info("Gateway id=" + config.gateway_id);

        /*
    * ---------------- M4 AI Initialization ----------------
    *
    * AI属于增强能力。
    *
    * 即使RKNN Runtime、模型文件或预处理配置出现问题，
    * 也不能影响：
    *
    * TCP / CAN / Modbus / MQTT / M2 / M3
    *
    * 的基础网关功能。
    */
    if (config.ai_inference.enabled)
    {
        if (!RknnInferenceEngine::runtimeAvailable())
        {
            Logger::warn(
                "M4 AI is enabled in config, but "
                "RKNN Runtime is unavailable in this build; "
                "AI inference is disabled"
            );
        }
        else
        {
            /*
             * 和handleMessage()使用完全相同的
             * device affinity算法。
             *
             * 因此skab-valve-demo后续一定进入
             * 持有这个AI Detector的Worker。
             */
            const std::size_t ai_worker_index = std::hash<std::string>{}(config.ai_inference.device_id) % ai_anomaly_detectors_.size();

            auto detector = std::make_unique<AiAnomalyDetector>(config.ai_inference);

            if (!detector->initialize())
            {
                /*
                 * Fail-open：
                 *
                 * AI坏了，只关闭AI。
                 * Gateway主体继续运行。
                 */
                Logger::error("M4 AI initialization failed; "
                    "AI inference is disabled: "
                    + detector->lastError()
                );
            }
            else
            {
                Logger::info("M4 AI initialized: model=" + detector->modelId()
                    + ", device=" + config.ai_inference.device_id
                    + ", worker=" + std::to_string(ai_worker_index)
                    + ", window=" + std::to_string(detector->windowSize())
                    + ", stride=" + std::to_string(detector->stride())
                    + ", threshold=" + std::to_string(detector->threshold())
                );
                ai_anomaly_detectors_[ai_worker_index] = std::move(detector);
            }
        }
    }

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

    /*
    * 先启动唯一Uplink Worker。
    *
    * 它负责：
    *
    * MQTT reconnect
    * Offline replay
    * MQTT publish
    * SQLite Store-and-Forward
    *
    * Processing Worker不会直接访问这些北向组件。
    */
    uplink_worker_thread_ = std::thread(&GatewayContext::uplinkWorkerLoop,this);

    /*
    * 根据runtime.worker_threads
    * 创建真正的Telemetry Processing Worker Pool。
    */
    telemetry_worker_threads_.reserve(telemetry_queues_.size());

    for (std::size_t worker_index = 0;worker_index < telemetry_queues_.size();++worker_index)
    {
        telemetry_worker_threads_.emplace_back(&GatewayContext::telemetryWorkerLoop,this,worker_index);
    }

    Logger::info("Telemetry processing worker pool started: workers="+ std::to_string(telemetry_worker_threads_.size()));

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
    * Gateway进入停止流程。
    *
    * Uplink Worker看到该状态以后，
    * 不再尝试新的MQTT网络发送。
    */
    stop_requested_.store(true);

    /*
    * 停止所有Telemetry入口Queue。
    *
    * 已经在Queue中的Telemetry仍允许被
    * Processing Worker全部处理完成。
    */
    for (auto& queue : telemetry_queues_)
    {
        queue->stop();
    }

    /*
    * 下行Command Queue维持原来的停止逻辑。
    */
    command_queue_.stop();

    /*
    * 等待所有Processing Worker退出。
    *
    * 此时Uplink Worker仍然保持运行，
    * 所以Processing Worker能够继续把最后的数据
    * 交给uplink_queue_。
    */
    for (auto& worker : telemetry_worker_threads_)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }

    /*
    * 到这里已经不会再产生新的ProcessedTelemetry。
    *
    * 现在才能停止Uplink Queue。
    */
    uplink_queue_.stop();
    if (uplink_worker_thread_.joinable())
    {
        uplink_worker_thread_.join();
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

    if (telemetry_queues_.empty())
    {
        Logger::error(
            "Telemetry worker queues are not initialized"
        );
        return;
    }

     /*
     * Device Affinity。
     *
     * 同一个device_id通过hash
     * 永远映射到同一个Processing Worker。
     *
     * 这样以后：
     *
     * Rule Engine状态
     * Sliding Window
     * AI时序特征
     *
     * 都能够保持单设备消息顺序。
     */
    const std::size_t worker_index = std::hash<std::string>{}(message.device_id) % telemetry_queues_.size();
    if (!telemetry_queues_[worker_index]->push(message))
    {
        Logger::warn(
            "Telemetry queue is full or stopped, device="
            + message.device_id
            + ", worker="
            + std::to_string(worker_index)
        );
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

void GatewayContext::telemetryWorkerLoop(std::size_t worker_index)
{
    TelemetryMessage message;

    TelemetryQueue& queue = *telemetry_queues_[worker_index];

    Logger::info("Telemetry processing worker started: worker="+ std::to_string(worker_index));

    while (true)
    {
        const QueuePopResult pop_result =queue.waitAndPopFor(message,std::chrono::milliseconds(1000));

        /*
         * 当前Worker还能正常从Queue返回，
         * 更新自己的独立Watchdog心跳。
         */
        telemetry_worker_heartbeats_[worker_index]->store(steadyNowMs());

        if (pop_result == QueuePopResult::Stopped)
        {
            break;
        }

        if (pop_result == QueuePopResult::Timeout)
        {
            continue;
        }

        Logger::debug(
            "Telemetry processing worker received message: worker="
            + std::to_string(worker_index)
            + ", device="
            + message.device_id
            + ", sequence="
            + std::to_string(message.sequence)
        );

        /*
        * ---------------- M3 Anomaly Detection ----------------
        *
        * 执行顺序：
        *
        * 原始 TelemetryMessage
        *      ↓
        * AnomalyDetector::evaluate()
        *      ↓
        * 将异常分数加入 message.metrics
        *      ↓
        * 后续 Rule Engine 读取衍生指标
        *
        * 每个 Worker 访问自己的 AnomalyDetector，
        * 不需要跨 Worker 共享滑动窗口状态。
        */
        if (worker_index < anomaly_detectors_.size())
        {
            AnomalyDetector* detector = anomaly_detectors_[worker_index].get();
            /*
            * nullptr 表示 M3 功能未启用。
            */
            if (detector != nullptr)
            {
                /*
                * 根据当前 TelemetryMessage，
                * 更新滑动窗口并获得已经完成预热的评分。
                */
                const std::vector<AnomalyScore> scores = detector->evaluate(message);
                for (const AnomalyScore& result : scores)
                {
                    /*
                    * 每个评分结果对应一个 Profile 的一次评分。
                    */
                    anomaly_windows_scored_.fetch_add(1,std::memory_order_relaxed);
                    /*
                    * 将统计异常分数转为统一指标。
                    *
                    * 例如：
                    * anomaly.plc_temperature.score = 30.0
                    *
                    * 这样既可以被 Rule Engine 读取，
                    * 也可以被 TelemetryConverter 序列化。
                    */
                    MetricValue score_metric;

                    score_metric.name = "anomaly." + result.profile_id + ".score";
                    score_metric.value = result.score;

                    // 异常分数为无量纲数值
                    score_metric.unit = "";
                    message.metrics.push_back(std::move(score_metric));

                    /*
                    * 超过检测阈值时增加统计和记录告警日志。
                    *
                    * M3 不直接生成 DeviceCommand。
                    */
                    if (result.anomalous)
                    {
                        anomalies_detected_.fetch_add(1,std::memory_order_relaxed);
                        Logger::warn(
                            "Anomaly detected: profile="+ result.profile_id
                            + ", device="+ result.device_id
                            + ", metric="+ result.metric
                            + ", value="+ std::to_string(result.features.current)
                            + ", baseline_mean="+ std::to_string(result.features.mean)
                            + ", score="+ std::to_string(result.score)
                        );
                    }
                }
            }
        }

        /*
        * ---------------- M4 NPU AI Detection ----------------
        *
        * 当前顺序：
        *
        * 原始Telemetry
        *      ↓
        * M3统计异常检测
        *      ↓
        * M4多变量AI检测
        *      ↓
        * Rule Engine
        *
        * M4只处理配置指定的device_id。
        */
        if (worker_index <ai_anomaly_detectors_.size())
        {
            AiAnomalyDetector* detector = ai_anomaly_detectors_[worker_index].get();

            if (detector != nullptr)
            {
                AiInferenceResult ai_result;

                const AiEvaluateStatus status = detector->evaluate(message,ai_result);
                if (status == AiEvaluateStatus::Inferred)
                {
                    /*
                     * 将AI异常概率写回统一Metrics。
                     *
                     * 后面的：
                     *
                     * RuleEngine
                     * MQTT
                     *
                     * 都可以直接使用。
                     */
                    MetricValue probability_metric;

                    probability_metric.name = "ai." + ai_result.model_id + ".probability";

                    probability_metric.value = ai_result.probability;

                    probability_metric.unit = "";

                    message.metrics.push_back(std::move(probability_metric));

                    /*
                     * 同时输出0/1异常状态，
                     * 方便MQTT展示和后续业务逻辑使用。
                     */
                    MetricValue anomaly_metric;

                    anomaly_metric.name = "ai." + ai_result.model_id + ".anomalous";

                    anomaly_metric.value = ai_result.anomalous ? 1.0 : 0.0;

                    anomaly_metric.unit = "";

                    message.metrics.push_back(std::move(anomaly_metric));

                    Logger::debug("M4 AI inference: model=" + ai_result.model_id
                        + ", device=" + ai_result.device_id
                        + ", sequence=" + std::to_string(ai_result.sequence)
                        + ", logit=" + std::to_string(ai_result.logit)
                        + ", probability=" + std::to_string(ai_result.probability)
                        + ", threshold=" + std::to_string(ai_result.threshold)
                    );

                    if (ai_result.anomalous)
                    {
                        Logger::warn("M4 AI anomaly detected: model=" + ai_result.model_id
                            + ", device=" + ai_result.device_id
                            + ", probability=" + std::to_string(ai_result.probability)
                            + ", threshold=" + std::to_string(ai_result.threshold)
                        );
                    }
                }
                else if (status ==AiEvaluateStatus::Error)
                {
                    /*
                     * 单次AI失败不丢弃Telemetry。
                     *
                     * 下面的RuleEngine和MQTT继续处理。
                     */
                    Logger::warn("M4 AI processing failed: " + detector->lastError());
                }
            }
        }

        /*
        * ---------------- Rule Engine ----------------
        *
        * Rule Engine运行在Processing Worker阶段。
        *
        * 它只做：
        *
        * Telemetry
        *     ↓
        * Rule evaluation
        *     ↓
        * DeviceCommand
        *
        * 不直接访问任何Connector。
        */
        if (worker_index < rule_engines_.size() && rule_engines_[worker_index])
        {
            rule_messages_evaluated_.fetch_add(1,std::memory_order_relaxed);

            std::vector<RuleTrigger> triggers =rule_engines_[worker_index]->evaluate(message);
            for (const RuleTrigger& trigger :triggers)
            {
                rule_triggers_total_.fetch_add(1,std::memory_order_relaxed);
                Logger::warn("Rule triggered: rule_id="
                    + trigger.rule_id + ", device="
                    + trigger.device_id + ", metric="
                    + trigger.metric + ", value="
                    + std::to_string(trigger.observed_value) + ", threshold="
                    + std::to_string(trigger.threshold)
                );

                /*
                * 所有本地动作继续复用已有CommandQueue。
                *
                * Rule Engine：
                *   Producer
                *
                * Command Worker：
                *   Consumer
                */
                for (const DeviceCommand& command :trigger.commands)
                {
                    /*
                    * Gateway已经进入停止流程时
                    * 不再产生新的本地控制命令。
                    */
                    if (stop_requested_.load())
                    {
                        break;
                    }

                    if (!command_queue_.push(command))
                    {
                        Logger::warn(
                            "Local rule action dropped because "
                            "command queue is full or stopped, "
                            "rule_id=" + trigger.rule_id
                            + ", request_id=" + command.request_id
                        );
                        continue;
                    }

                    local_rule_actions_total_.fetch_add(1,std::memory_order_relaxed);

                    Logger::info(
                        "Local rule action queued: rule_id="+ trigger.rule_id
                        + ", request_id="+ command.request_id+ ", device="
                        + command.device_id + ", action=" + command.action
                    );
                }
            }
        }

        /*
        * Rule处理完成以后，
        * 原Telemetry仍然正常进入MQTT上行。
        */
        std::string payload = TelemetryConverter::toJson(message);
        ProcessedTelemetry processed{std::move(message),std::move(payload)};

        /*
         * 将CPU处理后的结果交给唯一Uplink Worker。
         *
         * Uplink Queue满时push()会等待，
         * 从而形成backpressure。
         */
        if (!uplink_queue_.push(std::move(processed)))
        {
            break;
        }
    }

    Logger::info(
        "Telemetry processing worker stopped: worker="
        + std::to_string(worker_index)
    );
}

void GatewayContext::uplinkWorkerLoop()
{
    /*
     * Uplink Worker每次从uplink_queue_
     * 取出一条Processing Worker已经处理完成的数据。
     */
    ProcessedTelemetry processed;

    /*
     * 下一次允许尝试MQTT重连的时间点。
     */
    auto next_mqtt_reconnect_time = std::chrono::steady_clock::now();
    Logger::info("Telemetry uplink worker started");

    while (true)
    {
        /*
         * Uplink Worker不再直接消费TelemetryMessage。
         *
         * 它消费的是Processing Worker已经处理完成的：
         *
         * ProcessedTelemetry
         */
        const ProcessedQueuePopResult pop_result = uplink_queue_.waitAndPopFor(processed,std::chrono::milliseconds(1000));

        /*
         * Uplink Worker独立心跳。
         */
        uplink_worker_heartbeat_ms_.store(steadyNowMs());

        /*
         * Queue已经停止并且已经排空。
         */
        if (pop_result == ProcessedQueuePopResult::Stopped)
        {
            break;
        }

        /*
         * MQTT没有启用。
         *
         * 保持原项目语义：
         * 不因为主动禁用MQTT就把所有消息写入SQLite。
         */
        if (!mqtt_publisher_)
        {
            if (pop_result == ProcessedQueuePopResult::Message)
            {
                Logger::debug(
                    "MQTT publisher is disabled, "
                    "skip telemetry publish"
                );
            }
            continue;
        }

         /*
         * MQTT断线时按原有5秒节奏尝试重连。
         */
        if (!mqtt_publisher_->isConnected()&& !stop_requested_.load())
        {
            const auto now = std::chrono::steady_clock::now();

            if (now >= next_mqtt_reconnect_time)
            {
                Logger::warn(
                    "MQTT disconnected; "
                    "attempting to reconnect"
                );

                if (mqtt_publisher_->connect())
                {
                    Logger::info("MQTT connection restored");
                }
                else
                {
                    next_mqtt_reconnect_time = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                    Logger::warn(
                        "MQTT reconnect failed; "
                        "next attempt in 5 seconds"
                    );
                }
            }
        }

        /*
        * Broker在线时继续沿用原来的
        * Offline Store replay。
        */
        if (mqtt_publisher_->isConnected() && !stop_requested_.load())
        {
            flushOfflineBatch();
            if (!mqtt_publisher_->isConnected())
            {
                next_mqtt_reconnect_time =std::chrono::steady_clock::now() + std::chrono::seconds(5);
            }
        }

        /*
         * Timeout只是为了让MQTT维护逻辑周期运行。
         * 本轮没有真正的Telemetry结果。
         */
        if (pop_result == ProcessedQueuePopResult::Timeout)
        {
            continue;
        }

        /*
         * 到这里才说明真正收到一条
         * Processing Worker处理完成的数据。
         */
        TelemetryMessage& message = processed.message;
        
        const std::string& payload = processed.payload;
        Logger::debug(
            "Uplink worker received processed telemetry, device="
            + message.device_id
            + ", sequence="
            + std::to_string(message.sequence)
        );

        /*
         * Gateway正在退出：
         *
         * 不再进行新的MQTT网络发送，
         * 将剩余消息保存到SQLite。
         */
        if (stop_requested_.load())
        {
            if (!offline_store_->save(message.device_id,payload,message.timestamp_ms))
            {
                Logger::error("Failed to persist telemetry during shutdown, device="+ message.device_id);
            }
            continue;
        }
        /*
         * MQTT在线：
         * 优先发布实时Telemetry。
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
             * publish过程中网络可能刚好异常。
             *
             * 当前消息后面仍然进入SQLite。
             */
            Logger::warn(
                "MQTT publish failed; "
                "storing telemetry offline"
            );

            mqtt_publisher_->disconnect();
            next_mqtt_reconnect_time = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        }
        else
        {
            Logger::warn(
                "MQTT unavailable; "
                "storing telemetry offline"
            );
        }

        /*
         * MQTT当前不可用，
         * 使用原有SQLite Store-and-Forward。
         */
        if (!offline_store_->save(message.device_id,payload,message.timestamp_ms))
        {
            Logger::error("Failed to persist offline telemetry, device="
                + message.device_id
                + ", sequence="
                + std::to_string(message.sequence)
            );
        }
        else
        {
            Logger::info("Telemetry stored offline, device="
                + message.device_id
                + ", sequence="
                + std::to_string(message.sequence)
            );
        }
    }
    Logger::info("Telemetry uplink worker stopped");
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
    std::size_t total = 0;
    for (const auto& queue : telemetry_queues_)
    {
        total += queue->size();
    }
    return total;
}

std::size_t GatewayContext::getTelemetryQueueCapacity() const
{
    std::size_t total = 0;
    for (const auto& queue : telemetry_queues_)
    {
        total += queue->capacity();
    }
    return total;
}

std::size_t GatewayContext::getDroppedTelemetryCount()
{
    std::size_t total = 0;
    for (const auto& queue : telemetry_queues_)
    {
        total += queue->droppedCount();
    }

    return total;
}

std::size_t GatewayContext::getTelemetryWorkerCount() const
{
    return telemetry_worker_threads_.size();
}

std::size_t GatewayContext::getUplinkQueueSize()
{
    return uplink_queue_.size();
}

std::size_t GatewayContext::getUplinkQueueCapacity() const
{
    return uplink_queue_.capacity();
}

std::size_t GatewayContext::getEnabledRuleCount() const
{
    std::size_t count = 0;

    for (const RuleConfig& rule : config_.data().rules)
    {
        if (rule.enabled)
        {
            ++count;
        }
    }
    return count;
}

std::uint64_t GatewayContext::getRuleMessagesEvaluated() const
{
    return rule_messages_evaluated_.load(std::memory_order_relaxed);
}

std::uint64_t GatewayContext::getRuleTriggerCount() const
{
    return rule_triggers_total_.load(std::memory_order_relaxed);
}

std::uint64_t GatewayContext::getLocalRuleActionCount() const
{
    return local_rule_actions_total_.load(std::memory_order_relaxed);
}

/**
 * @brief 返回真正启用的 M3 Profile 数量。
 */
std::size_t GatewayContext::getEnabledAnomalyProfileCount() const
{
    const AnomalyConfig& config = config_.data().anomaly_detection;
    if (!config.enabled)
    {
        return 0;
    }
    std::size_t count = 0;

    for (const AnomalyProfileConfig& profile : config.profiles)
    {
        if (profile.enabled)
        {
            ++count;
        }
    }
    return count;
}

/**
 * @brief 返回已经完成的异常评分次数。
 */
std::uint64_t GatewayContext::getAnomalyWindowsScored() const
{
    return anomaly_windows_scored_.load(std::memory_order_relaxed);
}

/**
 * @brief 返回满足异常判定条件的评分次数。
 */
std::uint64_t GatewayContext::getAnomaliesDetected() const
{
    return anomalies_detected_.load(std::memory_order_relaxed);
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
    * 检查每一个Telemetry Processing Worker。
    *
    * 不能再用一份公共heartbeat，
    * 否则3个Worker正常、1个Worker卡死时，
    * Watchdog无法发现问题。
    */
    for (std::size_t worker_index = 0;worker_index < telemetry_worker_heartbeats_.size();++worker_index)
    {
        const std::int64_t heartbeat = telemetry_worker_heartbeats_[worker_index]->load();
        const std::int64_t age = now_ms - heartbeat;

        if (heartbeat <= 0 || age > health_timeout_ms)
        {
            Logger::error(
                "Watchdog health check failed: "
                "Telemetry Processing Worker "
                + std::to_string(worker_index)
                + " stalled for "
                + std::to_string(age)
                + " ms"
            );
            return false;
        }
    }

    /*
    * 独立检查Uplink Worker。
    */
    const std::int64_t uplink_heartbeat = uplink_worker_heartbeat_ms_.load();
    const std::int64_t uplink_age = now_ms - uplink_heartbeat;
    if (uplink_heartbeat <= 0|| uplink_age > health_timeout_ms)
    {
        Logger::error(
            "Watchdog health check failed: "
            "Telemetry Uplink Worker stalled for "
            + std::to_string(uplink_age)
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