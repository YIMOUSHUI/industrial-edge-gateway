#include "connectors/mock_connector.h"
#include "common/logger.h"

#include <iostream>

bool MockConnector::start()
{
    Logger::info("MockConnector started");
    generateTestMessage();
    return true;
}

void MockConnector::stop()
{
    Logger::info("MockConnector stopped");
}

std::string MockConnector::name() const
{
    return "mock";
}

void MockConnector::setMessageCallback(MessageCallback callback)
{
    message_callback_ = callback;
}

void MockConnector::generateTestMessage()
{
    TelemetryMessage message;
    message.device_id = "mock-device-01";
    message.protocol = "mock";
    message.timestamp_ms = 1000;
    message.sequence = 1;

    MetricValue temperature;
    temperature.name = "temperature";
    temperature.value = 25.6;
    temperature.unit = "C";

    message.metrics.push_back(temperature);
    if (message_callback_)
    {
        message_callback_(message);
    }

}