#pragma once

#include <memory>

#include "connectors/iconnector.h"
#include "model/telemetry_message.h"
#include "common/config.h"

class GatewayContext
{
public: 
    bool start(const std::string& config_path);
    void stop();

private:
    void handleMessage(const TelemetryMessage& message);//GatewayContext 收到设备数据以后统一进入的入口
    
    Config config_;

    std::vector<std::unique_ptr<IConnector>> connectors_;
};

