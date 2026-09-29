#include "rules/rule_engine.h"

#include <algorithm>
#include <cmath>
#include <utility>

RuleEngine::RuleEngine(const std::vector<RuleConfig>& rules): rules_(rules),states_(rules.size())
{
}

std::optional<double> RuleEngine::findMetricValue(const TelemetryMessage& message,const std::string& metric_name)
{
    for (const MetricValue& metric : message.metrics)
    {
        if (metric.name == metric_name)
        {
            return metric.value;
        }
    }
    return std::nullopt;
}

bool RuleEngine::compare(double value,const std::string& op,double threshold)
{
    if (op == ">")
    {
        return value > threshold;
    }

    if (op == ">=")
    {
        return value >= threshold;
    }

    if (op == "<")
    {
        return value < threshold;
    }

    if (op == "<=")
    {
        return value <= threshold;
    }

    /*
     * 浮点相等使用一个很小的相对容差，
     * 避免直接double == double。
     */
    const double epsilon = 1e-9 * std::max(1.0,std::abs(threshold));

    if (op == "==")
    {
        return std::abs(value - threshold)<= epsilon;
    }

    if (op == "!=")
    {
        return std::abs(value - threshold) > epsilon;
    }
    return false;
}


std::vector<RuleTrigger> RuleEngine::evaluate(const TelemetryMessage& message)
{
    std::vector<RuleTrigger> triggers;

    const auto now = std::chrono::steady_clock::now();

    for (std::size_t i = 0;i < rules_.size();++i)
    {
        const RuleConfig& rule = rules_[i];
        RuntimeState& state =states_[i];

        if (!rule.enabled)
        {
            continue;
        }

        /*
         * 第一版规则明确绑定device_id。
         */
        if (rule.device_id != message.device_id)
        {
            continue;
        }

        const std::optional<double> value = findMetricValue(message,rule.metric);
        /*
         * 当前Telemetry没有目标指标，
         * 当前Rule无需处理。
         */
        if (!value)
        {
            continue;
        }

        const bool matched = compare(*value,rule.op,rule.threshold);
        /*
         * 条件已经恢复正常。
         *
         * 重新arm该Rule，
         * 下一次异常episode可以再次触发。
         */
        if (!matched)
        {
            state.condition_active = false;
            state.triggered_in_episode = false;
            continue;
        }

        /*
         * 当前刚刚进入异常状态。
         */
        if (!state.condition_active)
        {
            state.condition_active = true;

            state.condition_since = now;

            state.triggered_in_episode = false;
        }

        /*
         * 同一次持续异常只执行一次动作。
         */
        if (state.triggered_in_episode)
        {
            continue;
        }

        /*
         * 判断异常持续时间。
         */
        const auto active_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - state.condition_since).count();
        if (active_ms < static_cast<std::int64_t>(rule.duration_ms))
        {
            continue;
        }

        /*
         * 判断冷却时间。
         */
        if (state.has_last_trigger)
        {
            const auto cooldown_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - state.last_trigger).count();
            if (cooldown_elapsed_ms< static_cast<std::int64_t>(rule.cooldown_ms))
            {
                continue;
            }
        }

        RuleTrigger trigger;
        trigger.rule_id = rule.id;
        trigger.device_id = message.device_id;
        trigger.metric = rule.metric;
        trigger.observed_value = *value;
        trigger.threshold = rule.threshold;

        /*
         * 将Rule Action转换成项目已有的DeviceCommand。
         *
         * RuleEngine自己不访问Connector。
         */
        for (std::size_t action_index = 0;action_index< rule.actions.size();++action_index)
        {
            const RuleActionConfig& action =rule.actions[action_index];
            if (action.type != "device_command")
            {
                continue;
            }

            DeviceCommand command;
            command.request_id = "rule:" + rule.id + ":"+ message.device_id+ ":"+ std::to_string(message.sequence)+ ":"+ std::to_string(action_index);
            command.device_id = message.device_id;
            command.action = action.action;
            command.parameters = action.parameters;
            trigger.commands.push_back(std::move(command));
        }

        triggers.push_back(std::move(trigger));
        state.triggered_in_episode = true;
        state.has_last_trigger = true;
        state.last_trigger = now;
    }
    return triggers;
}