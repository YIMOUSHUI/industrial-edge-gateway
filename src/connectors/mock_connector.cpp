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

/**
 * @brief Mock设备下行命令处理。
 *
 * 当前仅用于验证CommandRouter和统一IConnector下行接口，
 * 不进行真实网络发送。
 */
bool MockConnector::sendCommand(const DeviceCommand& command)
{
    /*
     * MockConnector 当前只有一台模拟设备：
     * mock-device-01。
     *
     * 如果Router错误地将其他设备的命令交给这里，
     * 明确返回失败。
     */
    if (command.device_id != "mock-device-01")
    {
        Logger::warn(
            "MockConnector cannot handle device: "
            + command.device_id
        );

        return false;
    }

    Logger::info(
        "MockConnector executed command: request_id="
        + command.request_id
        + ", action="
        + command.action
        + ", parameters="
        + command.parameters.dump()
    );
    return true;
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

std::vector<std::string> MockConnector::getOnlineDevices()
{
    /*
     * 当前MockConnector模拟的逻辑设备
     * 就是mock-device-01。
     *
     * Mock本身只是测试Connector，
     * 没有像TcpConnector那样维护真实socket连接表。
     */
    return {
        "mock-device-01"
    };
}