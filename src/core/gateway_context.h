#pragma once

#include <memory>
#include <thread>
#include <atomic>
#include <vector>
#include <string>
#include <cstddef>
#include <chrono>
#include <cstdint>

#include "core/telemetry_queue.h"
#include "core/command_queue.h"
#include "core/command_router.h"
#include "connectors/iconnector.h"
#include "model/telemetry_message.h"
#include "common/config.h"
#include "common/process_metrics.h"
#include "mqtt/mqtt_publisher.h"
#include "storage/offline_store.h"
#include "control/gateway_control_server.h"
#include "health/watchdog_manager.h"

class GatewayContext
{
public: 
    bool start(const std::string& config_path);
    void stop();

    /**
     * @brief 获取整个Gateway当前在线设备ID列表。
     *
     * 本函数不会维护第二份设备状态。
     * 它会遍历当前所有Connector，
     * 调用各Connector自己的getOnlineDevices()，
     * 再把结果汇总后返回。
     *
     * @return 当前所有Connector在线设备ID的快照。
     */
    std::vector<std::string> getOnlineDevices();

    /**
     * @brief 查询当前MQTT是否处于连接状态。
     *
     * GatewayContext本身不维护第二份MQTT连接状态，
     * 而是直接查询MqttPublisher中的真实连接状态。
     *
     * @return true  当前MQTT已连接。
     * @return false MQTT未启用、Publisher不存在或当前未连接。
     */
    bool isMqttConnected() const;

    /**
     * @brief 获取当前TelemetryQueue中的待处理消息数量。
     */
    std::size_t getTelemetryQueueSize();

    /**
     * @brief 获取TelemetryQueue最大容量。
     */
    std::size_t getTelemetryQueueCapacity() const;

    /**
     * @brief 获取本次运行期间因TelemetryQueue满载而丢弃的消息总数。
     */
    std::size_t getDroppedTelemetryCount();

    /**
     * @brief 获取gatewayd当前Linux进程资源快照。
     *
     * 数据每次直接从/proc读取，
     * GatewayContext不维护第二份资源状态。
     */
    ProcessMetrics getProcessMetrics() const;

    /**
     * @brief 获取本次gatewayd成功启动后的运行时间。
     *
     * @return 运行秒数。
     */
    std::uint64_t getUptimeSeconds() const;

    /**
     * @brief Watchdog是否在配置中启用。
     */
    bool isWatchdogEnabled() const;

    /**
     * @brief Watchdog当前是否仍在正常喂狗。
     */
    bool isWatchdogFeeding() const;


private:
    /**
     * @brief Gateway 统一上行消息入口。
     *
     * 除了将TelemetryMessage放入TelemetryQueue，
     * 同时根据消息来源更新设备到Connector的下行路由。
     *
     * @param message 设备遥测数据。
     * @param source_connector 实际收到该数据的Connector。
     */
    void handleMessage(const TelemetryMessage& message,IConnector* source_connector);//GatewayContext 收到设备数据以后统一进入的入口
    
    /**
     * @brief MQTT 下行 Command 消息入口。
     *
     * 当前阶段只验证 Subscribe + Callback 链路，
     * 因此暂时只记录收到的原始 JSON Payload。
     *
     * 下一阶段将在这里完成：
     * JSON → DeviceCommand → Command Router。
     */
    void handleCommandPayload(const std::string& topic,const std::string& payload);

    void workerLoop();
    /**
     * @brief Command Worker线程入口。
     *
     * 当前阶段只从CommandQueue取出DeviceCommand并记录日志。
     * 下一阶段再将命令交给CommandRouter。
     */
    void commandWorkerLoop();

    /**
     * @brief 补发一批 SQLite 中等待发送的离线遥测消息。
     *
     * 前提：
     * MQTT 当前已经连接 Broker。
     *
     * 本函数每次最多读取并处理一批离线记录，
     * 避免大量历史数据长时间占用 Worker，
     * 影响实时 TelemetryMessage 的正常处理。
     *
     * 对每条记录：
     * 1. 从 SQLite 读取；
     * 2. 通过 MQTT publish；
     * 3. publish 成功后删除 SQLite 记录；
     * 4. publish 失败时立即停止本轮补发。
     *
     * 如果 MQTT publish 失败，
     * 本函数会断开当前 MQTT Client，
     * 后续由 Worker 的重连逻辑重新建立连接。
     */
    void flushOfflineBatch();

    /**
     * @brief Watchdog使用的Gateway核心健康判断。
     *
     * 注意：
     * MQTT、PLC、TCP客户端等外部设备离线
     * 不会直接判定Gateway自身不健康。
     */
    bool isCoreHealthyForWatchdog() const;

    Config config_;

    std::vector<std::unique_ptr<IConnector>> connectors_;

    TelemetryQueue telemetry_queue_{1000};

    /**
     * @brief MQTT下行设备命令队列。
     *
     * Producer：
     *   MQTT Command回调路径。
     *
     * Consumer：
     *   Command Worker线程。
     *
     * 当前设置最多缓存100条待执行Command。
     */
    CommandQueue command_queue_{100};

    /**
     * @brief 下行设备命令路由表。
     *
     * 负责维护：
     * device_id -> Connector
     *
     * Command Worker 从 CommandQueue 取得命令后，
     * 通过该对象找到目标 Connector。
     */
    CommandRouter command_router_;


    std::thread worker_thread_;
    /**
     * @brief 下行Command业务Worker线程。
     *
     * 专门从CommandQueue消费DeviceCommand，
     * 后续负责调用CommandRouter执行设备控制。
     */
    std::thread command_worker_thread_;

    std::unique_ptr<MqttPublisher> mqtt_publisher_;//unique_ptr允许延迟创建对象，初始为 nullptr

    /**
     * @brief SQLite 离线消息存储组件。
     *
     * GatewayContext 独占 OfflineStore。
     *
     * Gateway 启动后创建并打开数据库，
     * Worker 后续通过该对象保存和补发离线消息。
     *
     * nullptr：
     *   OfflineStore 尚未创建或不可用。
     *
     * 非 nullptr：
     *   OfflineStore 对象已经创建。
     */
    std::unique_ptr<OfflineStore> offline_store_;

    /**
     * @brief Gateway 停止请求标志。
     *
     * false：Gateway 正常运行。
     * true ：主线程已经开始执行 stop()，Worker 应停止 MQTT 重连。
     *
     * 该变量会被主线程写入、Worker 线程读取，
     * 因此使用 std::atomic<bool> 避免普通 bool 的多线程数据竞争。
     */
    std::atomic<bool> stop_requested_{false};

    /*
    * gatewayd内部的UDS管理服务。
    *
    * GatewayContext拥有它的生命周期：
    * start()时创建并启动，
    * stop()时停止并销毁。
    */
    std::unique_ptr<GatewayControlServer> control_server_;

    /**
     * @brief gatewayd本次成功启动的时间点。
     *
     * 使用steady_clock而不是system_clock，
     * 因为系统时间即使被NTP校时或人工修改，
     * 运行时长也不应该突然跳变。
     */
    std::chrono::steady_clock::time_point started_at_{};

    /**
     * @brief Gateway是否已经完成正式启动。
     *
     * started_at_只有在启动成功后才具有有效语义。
     */
    bool started_{false};

    /**
     * @brief Telemetry Worker最近一次正常推进的单调时钟时间。
     *
     * 单位：milliseconds。
     */
    std::atomic<std::int64_t> telemetry_worker_heartbeat_ms_{0};

    /**
     * @brief Command Worker当前是否正在执行一条设备命令。
     *
     * Command Worker空闲并不代表异常，
     * 所以只监控“执行中的命令是否卡死”。
     */
    std::atomic<bool> command_in_progress_{false};

    /**
     * @brief 当前Command开始执行的时间。
     *
     * 只有command_in_progress_ == true时才有意义。
     */
    std::atomic<std::int64_t> command_started_ms_{0};

    /**
     * @brief Gateway核心业务允许不推进的最大时间。
     *
     * 来自gateway.yaml watchdog.health_timeout_sec。
     */
    int watchdog_health_timeout_sec_{60};

    /**
     * @brief Linux硬件Watchdog管理器。
     *
     * Ubuntu配置enabled=false时保持nullptr。
     */
    std::unique_ptr<WatchdogManager> watchdog_manager_;

};

