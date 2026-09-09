#pragma once

#include <string>
#include <vector>

struct ConnectorConfig
{
    std::string name;
    std::string type;
    bool enabled = true;//成员默认初始化

    int port = 0; //端口号
};

struct MqttConfig
{
    bool enabled = false;

    std::string server_uri = "tcp://127.0.0.1:1883";

    std::string client_id = "gateway-01";

    std::string topic = "gateway/gateway-01/telemetry";

    int qos = 1;
};

struct GatewayConfig
{
    std::string gateway_id;

    int worker_threads = 1;//默认一个工作线程

    std::string log_level = "info";

    std::vector<ConnectorConfig> connectors;//vector方便后续遍历所有配置

    MqttConfig mqtt;
};

class Config
{
public:
    bool load(const std::string& path);

    const GatewayConfig& data() const;

private:
    GatewayConfig data_;
};

