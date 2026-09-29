#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <nlohmann/json.hpp>

struct  ConnectorConfig
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

    /**
     * @brief 网络型南向设备的地址。
     *
     * 当前用于Modbus TCP：
     *   127.0.0.1
     *
     * 后续真实PLC可能是：
     *   192.168.10.xxx
     */
    std::string host;

    /**
     * @brief Gateway内部统一设备ID。
     *
     * 当前Modbus测试设备：
     *   modbus-plc-01
     */
    std::string device_id;

    /**
     * @brief Modbus寄存器轮询周期，单位毫秒。
     *
     * 默认每1000ms读取一次。
     */
    std::uint32_t poll_interval_ms = 1000;
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

/**
 * @brief Linux硬件Watchdog配置。
 *
 * Ubuntu开发环境默认关闭。
 * i.MX6ULL部署环境根据实际硬件开启。
 */
struct WatchdogConfig
{
    /**
     * @brief 是否启用硬件Watchdog。
     */
    bool enabled = false;

    /**
     * @brief Linux Watchdog设备节点。
     */
    std::string device = "/dev/watchdog0";

    /**
     * @brief 硬件Watchdog超时时间，单位秒。
     *
     * 超过该时间没有喂狗时，
     * 硬件将触发SoC复位。
     */
    int timeout_sec = 15;

    /**
     * @brief 正常状态下的喂狗周期，单位秒。
     */
    int feed_interval_sec = 5;

    /**
     * @brief Gateway核心业务允许停止推进的最长时间。
     *
     * 注意：
     * 它不是硬件Watchdog timeout。
     *
     * 例如：
     * Telemetry Worker超过60秒没有推进，
     * Gateway才判定自身异常并停止喂狗。
     */
    int health_timeout_sec = 60;
};

/**
 * @brief Rule Engine触发后的动作配置。
 *
 * 当前第一版支持：
 *
 * type = device_command
 *
 * 即通过Gateway已有的CommandQueue / CommandRouter
 * 执行本地设备控制。
 */
struct RuleActionConfig
{
    std::string type = "device_command";

    /**
     * DeviceCommand.action
     *
     * 例如：
     * set_speed
     * set_value
     * set_interval
     */
    std::string action;

    /**
     * DeviceCommand.parameters
     *
     * 例如：
     *
     * {
     *     "value": 800
     * }
     */
    nlohmann::json parameters = nlohmann::json::object();
};


/**
 * @brief 单条边缘规则配置。
 */
struct RuleConfig
{
    bool enabled = true;

    std::string id;

    /**
     * 当前第一版一条规则绑定一个具体设备。
     */
    std::string device_id;

    /**
     * 需要观察的Telemetry指标。
     *
     * 例如：
     * temperature
     * vibration
     * speed
     */
    std::string metric;

    /**
     * 比较运算符：
     *
     * >
     * >=
     * <
     * <=
     * ==
     * !=
     */
    std::string op;

    double threshold = 0.0;

    /**
     * 条件连续成立多久以后才真正触发。
     */
    std::uint32_t duration_ms = 0;

    /**
     * 两次触发之间的最小冷却时间。
     */
    std::uint32_t cooldown_ms = 30000;

    std::vector<RuleActionConfig> actions;
};

struct GatewayConfig
{
    std::string gateway_id;

    int worker_threads = 1;//默认一个工作线程

    std::string log_level = "info";

    std::vector<ConnectorConfig> connectors;//vector方便后续遍历所有配置

    /**
     * @brief 本地边缘规则。
     */
    std::vector<RuleConfig> rules;

    MqttConfig mqtt;

    WatchdogConfig watchdog;
};

class Config
{
public:
    bool load(const std::string& path);

    const GatewayConfig& data() const;

private:
    GatewayConfig data_;
};

