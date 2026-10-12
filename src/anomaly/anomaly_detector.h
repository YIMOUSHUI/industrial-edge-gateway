#pragma once

#include <cstdint>
#include <deque>
#include <string>
#include <vector>

#include "anomaly/anomaly_config.h"
#include "model/telemetry_message.h"

/**
 * @brief 工业时序数据的统计特征。
 *
 * 所有特征均由 AnomalyDetector 提取。
 *
 * M3 使用这些特征进行统计异常评分。
 * M4 可以进一步将其作为 AI 模型的候选输入。
 */
struct AnomalyFeatures
{
    // 当前新采样值
    double current{0.0};

    // 历史窗口的平均值
    double mean{0.0};

    // 历史窗口的标准差
    double stddev{0.0};

    // 历史窗口中的最小值
    double minimum{0.0};

    // 历史窗口中的最大值
    double maximum{0.0};

    // 当前值 - 上一个历史采样值
    double delta{0.0};

    // 历史窗口平均每次采样的变化量
    double trend{0.0};
};

/**
 * @brief 一次异常检测的输出结果。
 *
 * 一个 TelemetryMessage 可能包含多个指标，
 * 因此一次 evaluate() 可以返回多个 AnomalyScore。
 */
struct AnomalyScore
{
    std::string profile_id;
    std::string device_id;
    std::string metric;

    std::uint64_t sequence{0};

    // 当前采样相对于历史窗口的异常分数
    double score{0.0};

    // 是否满足异常判定条件
    bool anomalous{false};

    // 保留本次检测使用的特征
    AnomalyFeatures features;
};

/**
 * @brief 基于滑动窗口的 CPU 统计异常检测器。
 *
 * 当前线程模型：
 * 每个 Telemetry Processing Worker 独占一个实例。
 *
 * 每条 Profile 维护独立的历史窗口，
 * 因此不同设备或指标的数据不会混用。
 */
class AnomalyDetector
{
public:
    explicit AnomalyDetector(
        std::vector<AnomalyProfileConfig> profiles
    );

    /**
     * @brief 检测当前遥测消息。
     *
     * 1. 匹配设备和指标
     * 2. 检查时间间隔
     * 3. 从历史窗口提取特征
     * 4. 计算异常分数
     * 5. 更新历史窗口
     *
     * 窗口未预热完成时，不返回评分结果。
     */
    std::vector<AnomalyScore> evaluate(
        const TelemetryMessage& message
    );

private:
    /**
     * @brief 一次历史采样。
     */
    struct Sample
    {
        double value;
        std::uint64_t timestamp_ms;
    };

    /**
     * @brief 一条 Profile 对应的运行状态。
     *
     * deque 负责维护固定长度的历史样本。
     */
    struct State
    {
        std::deque<Sample> history;
    };

    // 检测配置
    std::vector<AnomalyProfileConfig> profiles_;

    // 每个 Profile 对应一个 State
    std::vector<State> states_;
};