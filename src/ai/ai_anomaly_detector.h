#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <vector>

#include "ai/rknn_inference_engine.h"
#include "common/config.h"
#include "model/telemetry_message.h"

/**
 * @brief 一条Telemetry进入M4后的处理状态。
 */
enum class AiEvaluateStatus
{
    // 不是当前AI模型负责的设备。
    Ignored,

    // 数据有效，但窗口未满或尚未到stride。
    Buffered,

    // 本次真正完成了一次NPU推理。
    Inferred,

    // 输入或RKNN推理发生错误。
    Error
};

/**
 * @brief 一次M4 AI推理结果。
 */
struct AiInferenceResult
{
    std::string model_id;
    std::string device_id;

    std::uint64_t sequence{0};

    float logit{0.0F};

    double probability{0.0};

    double threshold{0.0};

    bool anomalous{false};
};

/**
 * @brief M4 多变量工业时序AI异常检测器。
 *
 * 职责：
 *
 * 原始Telemetry
 *      ↓
 * 按preprocess.json中的features提取8维输入
 *      ↓
 * 标准化
 *      ↓
 * 维护64×8滑动窗口
 *      ↓
 * 每stride条调用一次RKNN
 *      ↓
 * sigmoid + threshold
 *
 * RKNN底层细节由RknnInferenceEngine负责。
 */
class AiAnomalyDetector
{
public:
    explicit AiAnomalyDetector(AiInferenceConfig config);

    /**
     * @brief 加载preprocess.json和RKNN模型。
     */
    bool initialize();

    /**
     * @brief 处理一条Telemetry。
     */
    AiEvaluateStatus evaluate(const TelemetryMessage& message,AiInferenceResult& result);

    const std::string& lastError() const;

    const std::string& modelId() const;

    std::size_t windowSize() const;

    std::size_t stride() const;

    double threshold() const;

private:
    bool loadPreprocess();

    bool buildNormalizedRow(const TelemetryMessage& message,std::vector<float>& row);

    static double sigmoid(float value);

    AiInferenceConfig config_;

    RknnInferenceEngine engine_;

    std::string model_id_;

    std::vector<std::string> features_;

    std::vector<double> mean_;

    std::vector<double> std_;

    std::size_t window_size_{0};

    std::size_t stride_{0};

    double threshold_{0.0};

    /**
     * 每个元素代表一个时间点的全部传感器特征。
     *
     * 当前模型：
     * 64个时间点 × 8维。
     */
    std::deque<std::vector<float>> window_;

    /**
     * 第一次窗口满以后已经执行过推理。
     */
    bool first_inference_done_{false};

    /**
     * 距离上次推理又加入了多少条新采样。
     */
    std::size_t samples_since_last_inference_{0};

    std::string last_error_;
};