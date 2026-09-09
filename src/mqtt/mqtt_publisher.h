#pragma once

#include <memory>
#include <string>

#include <mqtt/client.h>

class MqttPublisher
{
public:
    MqttPublisher(
        const std::string& server_uri,
        const std::string& client_id,
        const std::string& topic,
        int qos
    );

    bool connect();

    bool publish(
        const std::string& payload
    );

    void disconnect();

    bool isConnected() const;

private:
    std::string server_uri_;
    std::string client_id_;
    std::string topic_;
    int qos_;

    std::unique_ptr<mqtt::client> client_;
};