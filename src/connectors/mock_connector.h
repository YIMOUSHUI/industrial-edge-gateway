#pragma once

#include "connectors/iconnector.h"

class MockConnector : public IConnector
{
public:
    bool start() override;
    void stop() override;

    std::string name() const override;

    void setMessageCallback(MessageCallback callback) override;
private:
    void generateTestMessage();
    MessageCallback message_callback_;    

};