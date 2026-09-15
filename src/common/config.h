#pragma once

#include <string>
#include <vector>

struct ConnectorConfig
{
    std::string name;
    std::string type;
    bool enabled = true;//成员默认初始化


    /**
     * @brief TCP Connector监听端口。
     *
     * 仅 type == "tcp" 时使用。
     */
    int port = 0; //端口号

    /**
     * @brief Linux SocketCAN接口名称。
     *
     * 仅 type == "can" 时使用。
     *
     * Ubuntu虚拟CAN：
     *   vcan0
     *
     * 真实CAN设备：
     *   can0
     */
    std::string interface_name;

};

struct MqttConfig
{
    bool enabled = false;

    std::string server_uri = "tcp://127.0.0.1:1883";

    std::string client_id = "gateway-01";

    /**
     * @brief 遥测数据上行 Topic。
    */
    std::string topic = "gateway/gateway-01/telemetry";

    /**
     * @brief Gateway MQTT 下行命令订阅 Topic。
     *
     * 云端或测试客户端向该 Topic publish 命令后，
     * Broker 会将消息转发给订阅该 Topic 的 gatewayd。
     */
    std::string command_topic = "gateway/gateway-01/command";

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

