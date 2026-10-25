#include "ai/rknn_inference_engine.h"

#include <cstdint>
#include <string>
#include <utility>

#ifdef GATEWAY_HAS_RKNN
#include <rknn_api.h>
#endif

class RknnInferenceEngine::Impl
{
public:
#ifdef GATEWAY_HAS_RKNN
    rknn_context context = 0;
#endif

    bool loaded = false;

    std::string last_error;

    std::size_t input_elements = 0;

    ~Impl()
    {
#ifdef GATEWAY_HAS_RKNN
        if (context != 0)
        {
            rknn_destroy(context);
            context = 0;
        }
#endif
    }
};


RknnInferenceEngine::RknnInferenceEngine(): impl_(std::make_unique<Impl>())
{
}


RknnInferenceEngine::~RknnInferenceEngine() = default;

bool RknnInferenceEngine::runtimeAvailable()
{
#ifdef GATEWAY_HAS_RKNN
    return true;
#else
    return false;
#endif
}

bool RknnInferenceEngine::load(const std::string& model_path)
{
    impl_->loaded = false;
    impl_->input_elements = 0;
    impl_->last_error.clear();

#ifndef GATEWAY_HAS_RKNN

    (void)model_path;

    impl_->last_error = "RKNN Runtime support is not available in this build";
    return false;

#else

    /*
     * 如果对象之前已经加载过模型，
     * 先释放旧Context。
     */
    if (impl_->context != 0)
    {
        rknn_destroy(impl_->context);
        impl_->context = 0;
    }

    const int init_ret = rknn_init(&impl_->context,const_cast<char*>(model_path.c_str()),0,0,nullptr);

    if (init_ret != RKNN_SUCC)
    {
        impl_->last_error = "rknn_init failed, code=" + std::to_string(init_ret);

        impl_->context = 0;
        return false;
    }

    /*
     * 确认模型输入输出数量。
     *
     * 当前skab_hybrid.rknn固定：
     *
     * 1个输入
     * 1个输出
     */
    rknn_input_output_num io_num{};

    const int io_ret = rknn_query(impl_->context,RKNN_QUERY_IN_OUT_NUM,&io_num,sizeof(io_num));
    if (io_ret != RKNN_SUCC)
    {
        impl_->last_error = "RKNN_QUERY_IN_OUT_NUM failed, code=" + std::to_string(io_ret);

        rknn_destroy(impl_->context);
        impl_->context = 0;

        return false;
    }

    if (io_num.n_input != 1 ||io_num.n_output != 1)
    {
        impl_->last_error ="RKNN model must have exactly one input "
            "and one output";

        rknn_destroy(impl_->context);
        impl_->context = 0;

        return false;
    }

    /*
     * 查询输入Tensor。
     *
     * 后续infer()会检查元素数量，
     * 防止窗口维度错误却仍然送入NPU。
     */
    rknn_tensor_attr input_attr{};

    input_attr.index = 0;

    const int attr_ret = rknn_query(impl_->context,RKNN_QUERY_INPUT_ATTR,&input_attr,sizeof(input_attr));

    if (attr_ret != RKNN_SUCC)
    {
        impl_->last_error =
            "RKNN_QUERY_INPUT_ATTR failed, code="
            + std::to_string(attr_ret);

        rknn_destroy(impl_->context);
        impl_->context = 0;

        return false;
    }

    impl_->input_elements = static_cast<std::size_t>(input_attr.n_elems);

    impl_->loaded = true;

    return true;

#endif
}


bool RknnInferenceEngine::infer(const float* input,std::size_t input_count,float& logit)
{
#ifndef GATEWAY_HAS_RKNN

    (void)input;
    (void)input_count;
    (void)logit;

    impl_->last_error = "RKNN Runtime support is not available in this build";
    return false;
#else

    if (!impl_->loaded || impl_->context == 0)
    {
        impl_->last_error = "RKNN model is not loaded";
        return false;
    }

    if (input == nullptr)
    {
        impl_->last_error =
            "RKNN input pointer is null";

        return false;
    }

    if (input_count != impl_->input_elements)
    {
        impl_->last_error =
            "RKNN input element count mismatch: expected="
            + std::to_string(impl_->input_elements)
            + ", actual="
            + std::to_string(input_count);

        return false;
    }

    /*
     * 与之前已经在RK3576验证成功的
     * rknn_model_smoke_test保持完全相同：
     *
     * 输入给Runtime的是FP32，
     * Runtime负责转换成模型内部的FP16。
     */
    rknn_input rknn_input_data{};

    rknn_input_data.index = 0;

    rknn_input_data.buf = const_cast<float*>(input);

    rknn_input_data.size = static_cast<std::uint32_t>(input_count * sizeof(float));

    rknn_input_data.pass_through = 0;

    rknn_input_data.type = RKNN_TENSOR_FLOAT32;

    /*
     * 当前时序模型查询结果为：
     *
     * format=UNDEFINED
     *
     * 不能像图像模型一样强行设置NHWC。
     */
    rknn_input_data.fmt = RKNN_TENSOR_UNDEFINED;

    int ret = rknn_inputs_set(impl_->context,1,&rknn_input_data);

    if (ret != RKNN_SUCC)
    {
        impl_->last_error =
            "rknn_inputs_set failed, code="
            + std::to_string(ret);

        return false;
    }

    ret =
        rknn_run(
            impl_->context,
            nullptr
        );

    if (ret != RKNN_SUCC)
    {
        impl_->last_error =
            "rknn_run failed, code="
            + std::to_string(ret);

        return false;
    }

    rknn_output output{};

    /*
     * Runtime返回FP32，
     * 便于gatewayd后面直接进行sigmoid。
     */
    output.want_float = 1;

    ret =
        rknn_outputs_get(
            impl_->context,
            1,
            &output,
            nullptr
        );

    if (ret != RKNN_SUCC)
    {
        impl_->last_error =
            "rknn_outputs_get failed, code="
            + std::to_string(ret);

        return false;
    }

    bool success = false;

    if (output.buf != nullptr)
    {
        logit =
            static_cast<float*>(
                output.buf
            )[0];

        success = true;
    }
    else
    {
        impl_->last_error =
            "RKNN output buffer is null";
    }

    const int release_ret =
        rknn_outputs_release(
            impl_->context,
            1,
            &output
        );

    if (release_ret != RKNN_SUCC)
    {
        impl_->last_error =
            "rknn_outputs_release failed, code="
            + std::to_string(release_ret);

        return false;
    }

    return success;

#endif
}


bool RknnInferenceEngine::isLoaded() const
{
    return impl_->loaded;
}


const std::string&RknnInferenceEngine::lastError() const
{
    return impl_->last_error;
}