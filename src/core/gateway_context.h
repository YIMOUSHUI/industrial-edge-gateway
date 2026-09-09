#pragma once

#include <memory>
#include <thread>

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

    Config config_;

    std::vector<std::unique_ptr<IConnector>> connectors_;

    TelemetryQueue telemetry_queue_{1000};

    std::thread worker_thread_;

    std::unique_ptr<MqttPublisher> mqtt_publisher_;//unique_ptr允许延迟创建对象，初始为 nullptr
};

