#pragma once

#include <memory>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <mutex>

#include "core/telemetry_queue.h"
#include "connectors/iconnector.h"
#include "model/telemetry_message.h"
#include "common/config.h"
#include "mqtt/mqtt_publisher.h"

class GatewayContext
{
public: 
    bool start(const std::string& config_path);
    void stop();

private:
    void handleMessage(const TelemetryMessage& message);//GatewayContext 收到设备数据以后统一进入的入口
    
    void workerLoop();

    /**
     * @brief 等待下一次 MQTT 重连时机，并允许 Gateway 停止操作中断等待。
     *
     * 正常情况下最多等待 5 秒，然后允许 Worker 再次尝试连接 Broker。
     * 如果等待期间 GatewayContext::stop() 发出停止请求，
     * 本函数会立即结束等待并返回 false。
     *
     * @return true  等待时间正常结束，可以继续尝试 MQTT 重连。
     * @return false 收到 Gateway 停止请求，不应继续执行 MQTT 重连。
     */
    bool waitForMqttRetry();

    Config config_;

    std::vector<std::unique_ptr<IConnector>> connectors_;

    TelemetryQueue telemetry_queue_{1000};

    std::thread worker_thread_;

    std::unique_ptr<MqttPublisher> mqtt_publisher_;//unique_ptr允许延迟创建对象，初始为 nullptr

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

    /**
     * @brief MQTT 重连等待使用的互斥锁。
     *
     * 主要与 mqtt_retry_condition_ 配合使用，
     * 不用于保护 MQTT Client 本身。
     */
    std::mutex mqtt_retry_mutex_;

    /**
     * @brief MQTT 重连等待条件变量。
     *
     * Worker 可以在这里等待 5 秒。
     * Gateway 停止时通过 notify_all() 主动唤醒 Worker，
     * 避免 Worker 必须等 sleep_for(5s) 完整结束。
     */
    std::condition_variable mqtt_retry_condition_;
};

