#include "mqtt/mqtt_publisher.h"

#include "common/logger.h"

#include <utility>

//MQTT Client主动连接 MQTT Broker 的程序或对象

MqttPublisher::MqttPublisher(const std::string& server_uri,const std::string& client_id,const std::string& topic,int qos)
    : server_uri_(server_uri),
      client_id_(client_id),
      topic_(topic),
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

        mqtt::connect_options options;

        options.set_keep_alive_interval(20);//心跳 20 秒
        options.set_clean_session(true);//清洁会话

        client_->connect(options);//与 Broker 完成 TCP 连接和 MQTT 握手

        Logger::info(
            "MQTT connected to "
            + server_uri_
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

