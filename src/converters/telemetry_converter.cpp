#include "converters/telemetry_converter.h"

#include "common/logger.h"

#include <nlohmann/json.hpp>
#include <string>

bool TelemetryConverter::fromJson(const std::vector<std::uint8_t>& payload,TelemetryMessage& message)
{
    try
    {
        std::string text(payload.begin(),payload.end());

        nlohmann::json root =nlohmann::json::parse(text);

        message.device_id = root.at("device_id").get<std::string>();
        message.timestamp_ms = root.at("timestamp_ms").get<std::uint64_t>();
        message.sequence = root.at("sequence").get<std::uint64_t>();
        message.metrics.clear();

        for (const auto& metric_json: root.at("metrics"))
        {
            MetricValue metric;
            metric.name = metric_json.at("name").get<std::string>();
            metric.value = metric_json.at("value").get<double>();
            metric.unit = metric_json.at("unit").get<std::string>();
            message.metrics.push_back(metric);
        }

    }
    catch(const nlohmann::json::exception& error)
    {
        Logger::error("Failed to parse telemetry JSON: " + std::string(error.what()));
        return false;
    }
    return true;
}