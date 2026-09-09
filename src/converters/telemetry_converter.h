#pragma once

#include <cstdint>
#include <vector>
#include <string>

#include "model/telemetry_message.h"

class TelemetryConverter
{
public:
    static bool fromJson(const std::vector<std::uint8_t>& payload,TelemetryMessage& message);
    static std::string toJson(const TelemetryMessage& message);
};
