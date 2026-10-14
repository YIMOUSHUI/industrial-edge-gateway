#include "ai/ai_anomaly_detector.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <unordered_set>
#include <utility>

#include <nlohmann/json.hpp>

AiAnomalyDetector::AiAnomalyDetector(AiInferenceConfig config): config_(std::move(config))
{
}


bool AiAnomalyDetector::initialize()
{
    last_error_.clear();

    window_.clear();

    first_inference_done_ = false;

    samples_since_last_inference_ = 0;

    /*
     * 第一步：
     * 读取训练阶段固化出来的预处理参数。
     */
    if (!loadPreprocess())
    {
        return false;
    }

    /*
     * 第二步：
     * 加载RKNN模型。
     */
    if (!engine_.load(config_.model_path))
    {
        last_error_ =
            "failed to load RKNN model: "
            + engine_.lastError();

        return false;
    }

    return true;
}


bool AiAnomalyDetector::loadPreprocess()
{
    std::ifstream input(config_.preprocess_path);

    if (!input)
    {
        last_error_ =
            "failed to open preprocess file: "
            + config_.preprocess_path;

        return false;
    }

    try
    {
        nlohmann::json root;

        input >> root;

        model_id_ = root.at("model_id").get<std::string>();

        window_size_ = root.at("window_size").get<std::size_t>();

        stride_ = root.at("stride").get<std::size_t>();

        features_ = root.at("features").get<std::vector<std::string>>();

        mean_ = root.at("mean").get<std::vector<double>>();

        std_ = root.at("std").get<std::vector<double>>();

        threshold_ = root.at("threshold").get<double>();

        const std::string normalization = root.at("normalization").get<std::string>();

        if (normalization != "standard")
        {
            last_error_ = "unsupported normalization: " + normalization;

            return false;
        }

        if (model_id_.empty() ||window_size_ == 0 ||stride_ == 0 ||features_.empty())
        {
            last_error_ = "invalid preprocessing metadata";

            return false;
        }

        if (stride_ > window_size_)
        {
            last_error_ = "stride must not exceed window_size";

            return false;
        }

        if (mean_.size() != features_.size() ||std_.size() != features_.size())
        {
            last_error_ = "features/mean/std size mismatch";

            return false;
        }

        /*
         * 防止同一个Feature在配置中重复出现。
         */
        std::unordered_set<std::string>unique_features;

        for (std::size_t i = 0;i < features_.size();++i)
        {
            if (features_[i].empty())
            {
                last_error_ =
                    "empty feature name";

                return false;
            }

            if (!unique_features.insert(features_[i]).second)
            {
                last_error_ =
                    "duplicate feature: "
                    + features_[i];

                return false;
            }

            /*
             * std接近0会导致标准化除零。
             */
            if (!std::isfinite(std_[i]) ||std::abs(std_[i]) < 1e-12)
            {
                last_error_ =  "invalid std for feature: " + features_[i];

                return false;
            }
        }

        if (threshold_ < 0.0 ||threshold_ > 1.0)
        {
            last_error_ = "threshold must be in [0, 1]";

            return false;
        }

        /*
         * preprocess.json里已经保存模型输入形状。
         * 再检查一次，防止模型配置和实时预处理脱节。
         */
        if (root.contains("input_shape"))
        {
            const auto input_shape = root.at("input_shape").get<std::vector<std::size_t>>();

            if (input_shape.size() != 3 ||input_shape[0] != 1 ||input_shape[1] != window_size_ ||input_shape[2] != features_.size())
            {
                last_error_ =
                    "input_shape does not match "
                    "window/features";

                return false;
            }
        }
    }
    catch (const nlohmann::json::exception& error)
    {
        last_error_ =
            "invalid preprocess JSON: "
            + std::string(error.what());

        return false;
    }

    return true;
}


bool AiAnomalyDetector::buildNormalizedRow(const TelemetryMessage& message,std::vector<float>& row)
{
    row.clear();

    row.reserve(features_.size());

    /*
     * 这里严格按照preprocess.json中的features顺序构造。
     *
     * 即使TelemetryMessage里面metrics顺序不同，
     * 模型输入顺序仍然不会变化。
     */
    for (std::size_t feature_index = 0;feature_index < features_.size();++feature_index)
    {
        const std::string& feature_name =features_[feature_index];

        const auto metric =
            std::find_if(
                message.metrics.begin(),
                message.metrics.end(),
                [&feature_name](const MetricValue& value)
                {
                    return value.name == feature_name;
                }
            );

        if (metric == message.metrics.end())
        {
            last_error_ =
                "missing required metric: "
                + feature_name;

            return false;
        }

        if (!std::isfinite(metric->value))
        {
            last_error_ =
                "non-finite metric value: "
                + feature_name;

            return false;
        }

        /*
         * 和Python训练阶段保持完全一致：
         *
         * normalized =
         *     (raw - mean) / std
         */
        const double normalized = (metric->value -mean_[feature_index])/std_[feature_index];
        if (!std::isfinite(normalized))
        {
            last_error_ =
                "non-finite normalized value: "
                + feature_name;

            return false;
        }

        row.push_back(static_cast<float>(normalized));
    }

    return true;
}


AiEvaluateStatus AiAnomalyDetector::evaluate(const TelemetryMessage& message,AiInferenceResult& result)
{
    last_error_.clear();

    /*
     * 当前模型只处理配置指定设备。
     *
     * 例如：
     * skab-valve-demo
     */
    if (message.device_id != config_.device_id)
    {
        return AiEvaluateStatus::Ignored;
    }

    std::vector<float> row;

    if (!buildNormalizedRow(message, row))
    {
        return AiEvaluateStatus::Error;
    }

    /*
     * 加入一个新的8维时间点。
     */
    window_.push_back(std::move(row));

    /*
     * 始终只保留最近window_size条。
     */
    if (window_.size() > window_size_)
    {
        window_.pop_front();
    }

    /*
     * 还没收满64条时只缓存，不推理。
     */
    if (window_.size() < window_size_)
    {
        return AiEvaluateStatus::Buffered;
    }

    bool should_infer = false;

    /*
     * 第一次：
     * 第64条数据到达立即推理。
     */
    if (!first_inference_done_)
    {
        should_infer = true;
    }
    else
    {
        ++samples_since_last_inference_;

        /*
         * 后续每新增stride条再推理一次。
         *
         * 当前：
         * stride = 8
         */
        if (samples_since_last_inference_ >= stride_)
        {
            should_infer = true;
        }
    }

    if (!should_infer)
    {
        return AiEvaluateStatus::Buffered;
    }

    first_inference_done_ = true;

    samples_since_last_inference_ = 0;

    /*
     * deque中的：
     *
     * 64 × 8
     *
     * 展平成模型要求的连续512个float。
     *
     * 顺序：
     * time0_feature0 ... time0_feature7
     * time1_feature0 ... time1_feature7
     * ...
     */
    std::vector<float> model_input;

    model_input.reserve(window_size_ * features_.size());

    for (const auto& sample : window_)
    {
        model_input.insert(model_input.end(),sample.begin(),sample.end());
    }

    float logit = 0.0F;

    if (!engine_.infer(model_input.data(),model_input.size(),logit))
    {
        last_error_ = "RKNN inference failed: " + engine_.lastError();

        return AiEvaluateStatus::Error;
    }

    result.model_id = model_id_;

    result.device_id = message.device_id;

    result.sequence = message.sequence;

    result.logit = logit;

    result.probability = sigmoid(logit);

    result.threshold = threshold_;

    result.anomalous = result.probability >= threshold_;

    return AiEvaluateStatus::Inferred;
}

double AiAnomalyDetector::sigmoid(float value)
{
    /*
     * 数值稳定版本sigmoid。
     *
     * 避免非常大的正负logit导致exp溢出。
     */
    const double x = static_cast<double>(value);

    if (x >= 0.0)
    {
        return 1.0 / (1.0 + std::exp(-x));
    }

    const double exp_x = std::exp(x);

    return exp_x / (1.0 + exp_x);
}


const std::string&AiAnomalyDetector::lastError() const
{
    return last_error_;
}


const std::string&AiAnomalyDetector::modelId() const
{
    return model_id_;
}


std::size_t AiAnomalyDetector::windowSize() const
{
    return window_size_;
}


std::size_t AiAnomalyDetector::stride() const
{
    return stride_;
}


double AiAnomalyDetector::threshold() const
{
    return threshold_;
}