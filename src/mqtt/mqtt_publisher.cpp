#include "mqtt/mqtt_publisher.h"

#include "common/logger.h"

#include <utility>

//MQTT Client主动连接 MQTT Broker 的程序或对象

MqttPublisher::MqttPublisher(const std::string& server_uri,const std::string& client_id,const std::string& topic,const std::string& command_topic,int qos)
    : server_uri_(server_uri),
      client_id_(client_id),
      topic_(topic),
      command_topic_(command_topic),
      qos_(qos)
{
}

bool MqttPublisher::connect()
{
    try
    {
        client_ =
            std::make_unique<mqtt::client>(
                server_uri_,
                client_id_
            );//// 创建客户端对象
        
        /*
        * 把当前 MqttPublisher 对象注册为 Paho Callback。
        *
        * *this：
        * 当前这个 MqttPublisher 对象。
        *
        * 后续该 client_ 收到 MQTT 消息时，
        * Paho 会调用当前对象的 message_arrived()。
        */
        client_->set_callback(*this);

        mqtt::connect_options options;

        options.set_keep_alive_interval(20);//心跳 20 秒
        options.set_clean_session(true);//清洁会话

        /*
        * 与 Broker 建立 TCP + MQTT 连接。
        */
        client_->connect(options);

        /*
        * 订阅 Gateway 下行 Command Topic。
        *
        * Broker 后续收到该 Topic 的消息时，
        * 会将消息转发给当前 MQTT Client。
        */
        client_->subscribe(command_topic_,qos_);

        Logger::info(
            "MQTT connected to "
            + server_uri_
        );

        Logger::info(
            "MQTT subscribed to command topic: "
            + command_topic_
        );

        return true;
    }
    catch (const mqtt::exception& error)
    {
        Logger::error(
            "MQTT connect failed: "
            + std::string(error.what())
        );

        return false;
    }
}

/**
 * @brief 设置下行 MQTT Command 回调。
 *
 * GatewayContext 将自己的处理函数注册到这里。
 */
void MqttPublisher::setCommandCallback(CommandCallback callback)
{
    command_callback_ =std::move(callback);
}

/**
 * @brief Paho 收到 MQTT 消息后的入口。
 *
 * 当前函数只负责：
 * 1. 取得 Topic；
 * 2. 取得 Payload；
 * 3. 转发给 GatewayContext 注册的回调。
 *
 * 不在 MQTT 回调中直接执行设备控制，
 * 避免阻塞 Paho MQTT 内部消息处理流程。
 */
void MqttPublisher::message_arrived(mqtt::const_message_ptr message)
{
    if (!message)
    {
        return;
    }

    const std::string topic = message->get_topic();//消息发到哪个Topic
    const std::string payload =message->to_string();//消息正文是什么
    Logger::info("MQTT message received, topic=" + topic);

    /*
     * 只转发 Gateway Command Topic。
     *
     * 当前 Client 以后可能订阅其他 Topic，
     * 所以这里显式判断 Topic。
     */
    if (topic != command_topic_)
    {
        return;
    }

    if (command_callback_)
    {
        command_callback_(topic,payload);
    }
}

/**
 * @brief Paho 检测到 MQTT 连接异常断开后的回调。
 *
 * 当前阶段不在 Paho Callback 中进行 connect()，
 * 避免出现多个位置同时负责重连。
 *
 * GatewayContext::workerLoop()
 * 仍然是唯一的 MQTT 重连负责人。
 */
void MqttPublisher::connection_lost(const std::string& cause)
{
    Logger::warn("MQTT connection lost" + (cause.empty() ? std::string() : ": " + cause));
}

bool MqttPublisher::isConnected() const
{
    return client_
        && client_->is_connected();  //client存在?，而且连接Broker?
}

bool MqttPublisher::publish(const std::string& payload)
{
    if (!isConnected())
    {
        return false;
    }

    try
    {
        auto message = mqtt::make_message(topic_,payload);

        message->set_qos(qos_);
        message->set_retained(false);

        client_->publish(message);

        return true;
    }
    catch (const mqtt::exception& error)
    {
        Logger::error(
            "MQTT publish failed: "
            + std::string(error.what())
        );

        return false;
    }
}

void MqttPublisher::disconnect()
{
    if(!client_)
    {   
        return;
    }

    try
    {
        if(client_ -> is_connected())
        {
            client_ -> disconnect();

            Logger::info("MQTT disconnected");
        }
    }
    catch (const mqtt::exception& error)
    {
        Logger::error("MQTT disconnect failed: " + std::string(error.what()));
    }
}

