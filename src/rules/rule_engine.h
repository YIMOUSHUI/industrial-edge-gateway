#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include "common/config.h"
#include "model/command.h"
#include "model/telemetry_message.h"

/**
 * @brief 一次Rule触发结果。
 */
struct RuleTrigger
{
    std::string rule_id;

    std::string device_id;

    std::string metric;

    double observed_value = 0.0;

    double threshold = 0.0;

    /**
     * 当前Rule触发后需要执行的本地设备命令。
     */
    std::vector<DeviceCommand> commands;
};


/**
 * @brief 单Worker使用的本地规则引擎。
 *
 * 重要线程模型：
 *
 * 每一个Telemetry Processing Worker
 * 拥有自己独立的RuleEngine实例。
 *
 * Device Affinity保证同一个device_id
 * 始终进入同一个Worker。
 *
 * 因此Rule内部的duration/cooldown状态
 * 不需要mutex。
 */
class RuleEngine
{
public:
    explicit RuleEngine(const std::vector<RuleConfig>& rules);

    /**
     * @brief 根据当前Telemetry评估所有匹配规则。
     */
    std::vector<RuleTrigger> evaluate(const TelemetryMessage& message);

private:
    struct RuntimeState
    {
        bool condition_active = false;

        bool triggered_in_episode = false;

        std::chrono::steady_clock::time_point condition_since{};

        bool has_last_trigger = false;

        std::chrono::steady_clock::time_point last_trigger{};
    };

    static std::optional<double> findMetricValue(const TelemetryMessage& message,const std::string& metric_name);
    static bool compare(double value,const std::string& op,double threshold);

private:
    std::vector<RuleConfig> rules_;

    /**
     * states_[i]与rules_[i]一一对应。
     */
    std::vector<RuntimeState> states_;
};