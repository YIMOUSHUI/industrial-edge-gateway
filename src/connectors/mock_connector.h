#pragma once

#include "connectors/iconnector.h"

class MockConnector : public IConnector
{
public:
    bool start() override;
    void stop() override;

    std::string name() const override;

    void setMessageCallback(MessageCallback callback) override;

    /**
     * @brief 模拟处理一条下行设备命令。
     *
     * MockConnector 不连接真实设备，
     * 当前只通过日志模拟命令执行成功。
     */
    bool sendCommand(const DeviceCommand& command) override;

private:
    void generateTestMessage();
    MessageCallback message_callback_;    

};