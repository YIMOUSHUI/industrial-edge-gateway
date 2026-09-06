#pragma once

#include <functional>
#include <string>

#include "model/telemetry_message.h"

class IConnector
{
public:
    using MessageCallback = 
        std::function<void(const TelemetryMessage&)>;
        
    virtual ~IConnector() = default;

    virtual bool start() = 0;
    virtual void stop() = 0;

    virtual std::string name() const = 0;

    virtual void setMessageCallback(MessageCallback callback) = 0;
};

