#pragma once

#include <memory>
#include <string>
#include <functional>

#include <mqtt/client.h>

/**
 * @brief Gateway MQTT 客户端封装。
 *
 * 当前类仍沿用 MqttPublisher 名称，
 * 但已经开始同时承担：
 *
 * 1. Telemetry publish；
 * 2. Command subscribe。
 *
 * 后续在 MQTT 双向链路稳定后，
 * 将统一重命名为 MqttClient。
 */
class MqttPublisher : public virtual mqtt::callback
{
public:
    /**
     * @brief MQTT 下行消息回调。
     *
     * 参数：
     * topic   MQTT消息所属Topic。
     * payload MQTT消息正文。
     *
     * MqttPublisher 只负责接收 MQTT 消息，
     * 不在这里执行具体设备控制业务。
     */
    using CommandCallback = std::function<void(const std::string& topic,const std::string& payload)>;
    
    MqttPublisher(
        const std::string& server_uri,
        const std::string& client_id,
        const std::string& topic,
        const std::string& command_topic,
        int qos
    );

    bool connect();

    bool publish(
        const std::string& payload
    );

    void disconnect();

    bool isConnected() const;

    /**
     * @brief 设置 MQTT 下行消息处理回调。
     *
     * GatewayContext 会注册自己的处理函数。
     * 当 Paho 收到 command Topic 消息后，
     * MqttPublisher 会通过该回调把消息交给 GatewayContext。
     */
    void setCommandCallback(CommandCallback callback);

private:
    /**
     * @brief Paho 收到 MQTT 消息后调用。
     *
     * 该函数由 Paho MQTT 内部执行流程触发，
     * 不是 GatewayContext 主动调用。
     */
    void message_arrived(mqtt::const_message_ptr message) override;

     /**
     * @brief MQTT 连接异常丢失时由 Paho 调用。
     *
     * 当前这里只记录日志。
     * 真正的重连仍由 Gateway Worker 统一负责。
     */
    void connection_lost(const std::string& cause) override;

private:
    std::string server_uri_;
    std::string client_id_;

    /**
     * @brief Telemetry 上行 Topic。
    */
    std::string topic_;

    /**
     * @brief Command 下行 Topic。
     */
    std::string command_topic_;

    int qos_;

    std::unique_ptr<mqtt::client> client_;

    /**
     * @brief GatewayContext 注册的下行消息回调。
     *
     * 如果为空，收到命令时不会继续向业务层转发。
     */
    CommandCallback command_callback_;
};