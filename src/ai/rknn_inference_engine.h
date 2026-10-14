#pragma once

#include <cstddef>
#include <memory>
#include <string>

/**
 * @brief RKNN模型推理引擎。
 *
 * 当前职责非常单一：
 *
 * float输入
 *     ↓
 * RKNN Runtime
 *     ↓
 * 单个float输出logit
 *
 * 它不负责：
 * - 传感器特征提取
 * - 标准化
 * - 滑动窗口
 * - sigmoid
 * - 异常阈值判断
 *
 * 这些属于上层AiAnomalyDetector。
 */
class RknnInferenceEngine
{
public:
    RknnInferenceEngine();

    ~RknnInferenceEngine();

    RknnInferenceEngine(const RknnInferenceEngine&) = delete;

    RknnInferenceEngine& operator=(const RknnInferenceEngine&) = delete;

    /**
     * @brief 加载.rknn模型。
     */
    bool load(const std::string& model_path);

    /**
     * @brief 执行一次模型推理。
     *
     * 当前SKAB模型要求：
     *
     * input_count = 64 * 8 = 512
     *
     * @param input       FP32输入数组。
     * @param input_count 输入元素数量。
     * @param logit       模型输出。
     *
     * @return true 推理成功。
     */
    bool infer(const float* input,std::size_t input_count,float& logit);

    /**
     * @brief 模型是否已经成功加载。
     */
    bool isLoaded() const;

    /**
     * @brief 返回最近一次失败原因。
     */
    const std::string& lastError() const;

    /**
     * @brief 当前gatewayd是否编译了RKNN Runtime支持。
     *
     * Ubuntu开发机通常返回false。
     * RK3576板端应返回true。
     */
    static bool runtimeAvailable();

private:
    class Impl;
    
    std::unique_ptr<Impl> impl_;
};