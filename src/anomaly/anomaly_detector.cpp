#include "anomaly/anomaly_detector.h"

#include <algorithm>
#include <cmath>
#include <utility>

/**
 * @brief 初始化检测器。
 *
 * profiles_ 保存检测配置。
 * states_ 为每条配置建立独立的历史窗口。
 */
AnomalyDetector::AnomalyDetector(std::vector<AnomalyProfileConfig> profiles): profiles_(std::move(profiles)),states_(profiles_.size())
{
}

/**
 * @brief 对当前遥测消息执行统计异常检测。
 */
std::vector<AnomalyScore> AnomalyDetector::evaluate(const TelemetryMessage& message)
{
    std::vector<AnomalyScore> scores;

    // 遍历当前检测器中的全部 Profile
    for (std::size_t i = 0; i < profiles_.size(); ++i)
    {
        const AnomalyProfileConfig& profile = profiles_[i];
        // 只处理已启用且设备 ID 匹配的配置
        if (!profile.enabled ||profile.device_id != message.device_id)
        {
            continue;
        }

        /*
         * 一条 TelemetryMessage 可能包含多个 MetricValue。
         * 从 metrics 中查找当前 Profile 对应的指标。
         */

        //在 [begin, end) 范围内查找第一个满足 predicate 的元素。
        const auto it = std::find_if(message.metrics.begin(),message.metrics.end(),
            [&profile](const MetricValue& metric)
            {
                return metric.name == profile.metric;
            }
        );

        // 当前消息没有该指标，直接跳过
        if (it == message.metrics.end())
        {
            continue;
        }

        // 排除 NaN 和正负无穷，避免污染历史窗口
        if (!std::isfinite(it->value))
        {
            continue;
        }

        /*
         * states_[i] 对应 profiles_[i]。
         */
        State& state = states_[i];

        std::deque<Sample>& history = state.history;

        /*
         * 检查采样连续性。
         *
         * 时间戳回退或采样间隔过长，
         * 说明原来的历史基线不再可靠。
         */
        if (!history.empty())
        {
            const std::uint64_t previous_time =history.back().timestamp_ms;
            if (message.timestamp_ms < previous_time || message.timestamp_ms - previous_time > profile.max_gap_ms)
            {
                history.clear();
            }
        }

        /*
         * 只有历史窗口达到指定长度，
         * 才执行异常评分。
         *
         * 这里假定配置解析阶段保证 window_size >= 2。
         */
        if (history.size() == profile.window_size)
        {
            AnomalyScore result;

            result.profile_id = profile.id;
            result.device_id = message.device_id;
            result.metric = profile.metric;
            result.sequence = message.sequence;

            result.features.current = it->value;

            /*
             * 第一步：计算历史均值、最大值、最小值。
             */
            double sum = 0.0;

            result.features.minimum = history.front().value;

            result.features.maximum = history.front().value;

            for (const Sample& sample : history)
            {
                sum += sample.value;

                result.features.minimum = std::min(result.features.minimum,sample.value);
                result.features.maximum =std::max(result.features.maximum,sample.value);
            }

            result.features.mean =sum / static_cast<double>(history.size());
            /*
             * 第二步：计算历史标准差。
             */
            double variance = 0.0;

            for (const Sample& sample : history)
            {
                const double deviation =sample.value - result.features.mean;

                variance += deviation * deviation;
            }
            variance /= static_cast<double>(history.size());
            result.features.stddev = std::sqrt(variance);

            /*
             * 第三步：计算变化量与历史趋势。
             */
            result.features.delta = it->value - history.back().value;
            result.features.trend = (history.back().value -history.front().value) / static_cast<double>(history.size() - 1);

            /*
             * 第四步：计算异常分数。
             *
             * 先得到当前值相对历史平均值的绝对偏差。
             */
            const double absolute_deviation = std::abs(it->value - result.features.mean);

            /*
             * 防止历史标准差为 0。
             */
            const double scale = std::max(result.features.stddev,profile.scale_floor);
            result.score = absolute_deviation / scale;

            /*
             * 同时满足：
             * 1. 标准化异常分数超过阈值
             * 2. 绝对偏差超过最小要求
             */
            result.anomalous = result.score >= profile.score_threshold && absolute_deviation >= profile.min_deviation;
            scores.push_back(std::move(result));
        }

        /*
         * 最后才把当前采样加入历史窗口。
         *
         * 不能在评分之前加入，否则当前数据
         * 会影响用于评价它自己的历史基线。
         */
        history.push_back({it->value,message.timestamp_ms});

        /*
         * 保证窗口大小始终不超过配置上限。
         */
        if (history.size() > profile.window_size)
        {
            history.pop_front();
        }
    }

    return scores;
}