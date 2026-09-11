#pragma once

#include <memory>
#include <thread>
#include <atomic>

#include "core/telemetry_queue.h"
#include "connectors/iconnector.h"
#include "model/telemetry_message.h"
#include "common/config.h"
#include "mqtt/mqtt_publisher.h"
#include "storage/offline_store.h"

class GatewayContext
{
public: 
    bool start(const std::string& config_path);
    void stop();

private:
    void handleMessage(const TelemetryMessage& message);//GatewayContext 收到设备数据以后统一进入的入口
    
    void workerLoop();

    Config config_;

    std::vector<std::unique_ptr<IConnector>> connectors_;

    TelemetryQueue telemetry_queue_{1000};

    std::thread worker_thread_;

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

};

