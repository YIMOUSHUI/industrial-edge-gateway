#pragma once
#include <string>
#include "model/telemetry_message.h"

/**
 * @brief Telemetry Processing Worker 的输出。
 *
 * message：
 *   保留统一遥测模型。
 *   后续 Rule Engine / AI 可以继续使用其中的设备、指标和序号。
 *
 * payload：
 *   已完成 JSON 序列化的 MQTT Payload。
 */
struct ProcessedTelemetry
{
    TelemetryMessage message;
    std::string payload;
};
