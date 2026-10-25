#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include "rknn_api.h"

// 独立 RKNN 推理测试程序。
// 不依赖 gatewayd，也不修改现有网关状态。

struct RknnContext {
    rknn_context ctx = 0;

    ~RknnContext() {
        if (ctx != 0) {
            rknn_destroy(ctx);
        }
    }
};

void check(int ret, const char* operation) {
    if (ret != 0) {
        throw std::runtime_error(
            std::string(operation) +
            " failed, code=" + std::to_string(ret)
        );
    }
}

void printTensor(const char* title,const rknn_tensor_attr& attr) {
    std::cout << title
              << " name=" << attr.name
              << " shape=[";

    for (uint32_t i = 0; i < attr.n_dims; ++i) {
        if (i != 0) {
            std::cout << ", ";
        }
        std::cout << attr.dims[i];
    }

    std::cout << "] elements=" << attr.n_elems
              << " type=" << get_type_string(attr.type)
              << " format=" << get_format_string(attr.fmt)
              << '\n';
}

/**
 * @brief 从已经排序的延迟样本中计算分位数。
 *
 * 使用线性插值：
 *
 * P50：一半推理低于该延迟
 * P95：95%的推理低于该延迟
 * P99：99%的推理低于该延迟
 */
double percentile(
    const std::vector<double>& sorted_values,
    double ratio
)
{
    if (sorted_values.empty()) {
        throw std::runtime_error(
            "Cannot calculate percentile from empty samples"
        );
    }

    const double position =
        ratio *
        static_cast<double>(
            sorted_values.size() - 1
        );

    const std::size_t lower =
        static_cast<std::size_t>(
            std::floor(position)
        );

    const std::size_t upper =
        static_cast<std::size_t>(
            std::ceil(position)
        );

    if (lower == upper) {
        return sorted_values[lower];
    }

    const double weight =
        position -
        static_cast<double>(lower);

    return
        sorted_values[lower] *
            (1.0 - weight)
        +
        sorted_values[upper] *
            weight;
}


/**
 * @brief 打印一组延迟统计。
 */
void printLatencyStats(
    const std::string& title,
    std::vector<double> values
)
{
    if (values.empty()) {
        throw std::runtime_error(
            "Latency sample list is empty"
        );
    }

    std::sort(
        values.begin(),
        values.end()
    );

    const double mean =
        std::accumulate(
            values.begin(),
            values.end(),
            0.0
        )
        /
        static_cast<double>(
            values.size()
        );

    std::cout
        << title << '\n'
        << "  Mean : "
        << mean << " ms\n"
        << "  P50  : "
        << percentile(values, 0.50)
        << " ms\n"
        << "  P95  : "
        << percentile(values, 0.95)
        << " ms\n"
        << "  P99  : "
        << percentile(values, 0.99)
        << " ms\n"
        << "  Min  : "
        << values.front()
        << " ms\n"
        << "  Max  : "
        << values.back()
        << " ms\n";
}


int main(int argc, char* argv[]) {
    try {
        if (argc < 2 || argc > 5) {
            std::cerr
                << "Usage: rknn_model_smoke_test "
                << "model.rknn "
                << "[input_f32.bin] "
                << "[warmup] "
                << "[repeats]\n";

            return 1;
        }

        int warmup = 20;
        int repeats = 200;

        if (argc >= 4) {
            warmup = std::stoi(argv[3]);
        }

        if (argc >= 5) {
            repeats = std::stoi(argv[4]);
        }

        if (warmup < 0) {
            throw std::runtime_error(
                "warmup must be >= 0"
            );
        }

        if (repeats <= 0) {
            throw std::runtime_error(
                "repeats must be > 0"
            );
        }

        // 1. 加载 RKNN 模型
        RknnContext runtime;

        check(
            rknn_init(
                &runtime.ctx,
                static_cast<void*>(argv[1]),
                0,
                0,
                nullptr
            ),
            "rknn_init"
        );

        std::cout << "Model loaded successfully\n";

        // 2. 查看 Runtime / 驱动版本
        rknn_sdk_version version{};

        check(
            rknn_query(
                runtime.ctx,
                RKNN_QUERY_SDK_VERSION,
                &version,
                sizeof(version)
            ),
            "query SDK version"
        );

        std::cout << "Runtime: " << version.api_version
                  << "\nDriver : " << version.drv_version
                  << '\n';

        // 3. 查询模型真实输入输出
        rknn_input_output_num io{};

        check(
            rknn_query(
                runtime.ctx,
                RKNN_QUERY_IN_OUT_NUM,
                &io,
                sizeof(io)
            ),
            "query IO count"
        );

        if (io.n_input != 1 || io.n_output != 1) {
            throw std::runtime_error(
                "Expected one input and one output"
            );
        }

        rknn_tensor_attr input_attr{};
        rknn_tensor_attr output_attr{};

        input_attr.index = 0;
        output_attr.index = 0;

        check(
            rknn_query(
                runtime.ctx,
                RKNN_QUERY_INPUT_ATTR,
                &input_attr,
                sizeof(input_attr)
            ),
            "query input"
        );

        check(
            rknn_query(
                runtime.ctx,
                RKNN_QUERY_OUTPUT_ATTR,
                &output_attr,
                sizeof(output_attr)
            ),
            "query output"
        );

        printTensor("Input :", input_attr);
        printTensor("Output:", output_attr);

        // 当前模型应为 64 × 8 = 512 个输入值。
        if (input_attr.n_elems != 512 ||
            output_attr.n_elems != 1) {
            throw std::runtime_error(
                "Unexpected model input/output size"
            );
        }

        // 4. 构造零值输入，仅用于推理链路验证。
        std::vector<float> sensor_window(512, 0.0f);

        if (argc >= 3) {
            std::ifstream file(
                argv[2],
                std::ios::binary | std::ios::ate
            );

            if (!file) {
                throw std::runtime_error(
                    "Failed to open input file"
                );
            }

            const auto file_size = file.tellg();

            const auto expected_size =
                static_cast<std::streamsize>(
                    sensor_window.size() * sizeof(float)
                );

            if (file_size != expected_size) {
                throw std::runtime_error(
                    "Unexpected input file size"
                );
            }

            file.seekg(0, std::ios::beg);

            if (!file.read(
                    reinterpret_cast<char*>(
                        sensor_window.data()
                    ),
                    expected_size)) {

                throw std::runtime_error(
                    "Failed to read input file"
                );
            }

            std::cout
                << "Loaded input: "
                << argv[2]
                << '\n';
        }

        rknn_input input{};
        input.index = 0;
        input.buf = sensor_window.data();
        input.size = static_cast<uint32_t>(
            sensor_window.size() * sizeof(float)
        );
        input.pass_through = 0;
        input.type = RKNN_TENSOR_FLOAT32;

        input.fmt = input_attr.fmt;

        /*
        * ----------------------------------------------------------
        * RKNN latency benchmark
        *
        * 生产环境中的 RknnInferenceEngine::infer() 每一次都会执行：
        *
        * rknn_inputs_set()
        *        ↓
        * rknn_run()
        *        ↓
        * rknn_outputs_get()
        *        ↓
        * rknn_outputs_release()
        *
        * 因此 Total RKNN API latency 按同样路径统计。
        *
        * 同时单独记录 rknn_run()，
        * 用于观察 Runtime 执行阶段本身的耗时。
        * ----------------------------------------------------------
        */

        std::vector<double> total_latencies_ms;
        std::vector<double> run_latencies_ms;

        total_latencies_ms.reserve(
            static_cast<std::size_t>(repeats)
        );

        run_latencies_ms.reserve(
            static_cast<std::size_t>(repeats)
        );

        double last_logit = 0.0;

        for (
            int i = 0;
            i < warmup + repeats;
            ++i
        ) {
            /*
            * Total latency从输入提交开始计算。
            */
            const auto total_begin =
                std::chrono::steady_clock::now();

            check(
                rknn_inputs_set(
                    runtime.ctx,
                    1,
                    &input
                ),
                "rknn_inputs_set"
            );

            /*
            * 单独统计rknn_run。
            */
            const auto run_begin =
                std::chrono::steady_clock::now();

            check(
                rknn_run(
                    runtime.ctx,
                    nullptr
                ),
                "rknn_run"
            );

            const auto run_end =
                std::chrono::steady_clock::now();

            rknn_output output{};

            /*
            * 和gatewayd生产路径保持一致：
            * 要求Runtime输出FP32。
            */
            output.want_float = 1;

            check(
                rknn_outputs_get(
                    runtime.ctx,
                    1,
                    &output,
                    nullptr
                ),
                "rknn_outputs_get"
            );

            if (output.buf == nullptr) {
                throw std::runtime_error(
                    "RKNN output buffer is null"
                );
            }

            last_logit =
                static_cast<float*>(
                    output.buf
                )[0];

            check(
                rknn_outputs_release(
                    runtime.ctx,
                    1,
                    &output
                ),
                "rknn_outputs_release"
            );

            const auto total_end =
                std::chrono::steady_clock::now();

            /*
            * 预热阶段的数据不计入统计。
            */
            if (i >= warmup) {
                const double total_ms =
                    std::chrono::duration<
                        double,
                        std::milli
                    >(
                        total_end -
                        total_begin
                    ).count();

                const double run_ms =
                    std::chrono::duration<
                        double,
                        std::milli
                    >(
                        run_end -
                        run_begin
                    ).count();

                total_latencies_ms.push_back(
                    total_ms
                );

                run_latencies_ms.push_back(
                    run_ms
                );
            }
        }

        std::cout
            << std::fixed
            << std::setprecision(6);

        std::cout
            << "Warmup iterations : "
            << warmup << '\n';

        std::cout
            << "Measured iterations: "
            << repeats << '\n';

        std::cout
            << "Last anomaly logit: "
            << last_logit << '\n';

        const double probability =
            1.0 /
            (
                1.0 +
                std::exp(-last_logit)
            );

        std::cout
            << "Anomaly probability: "
            << probability << '\n';

        std::cout << '\n';

        printLatencyStats(
            "Total RKNN API latency "
            "(inputs_set + run + outputs_get/release):",
            total_latencies_ms
        );

        std::cout << '\n';

        printLatencyStats(
            "rknn_run latency:",
            run_latencies_ms
        );

        /*
        * 串行推理的理论吞吐能力。
        *
        * 这里只根据单次API平均耗时换算，
        * 不代表整个Gateway的Telemetry吞吐量。
        */
        const double mean_total_ms =
            std::accumulate(
                total_latencies_ms.begin(),
                total_latencies_ms.end(),
                0.0
            )
            /
            static_cast<double>(
                total_latencies_ms.size()
            );

        std::cout
            << '\n'
            << "Sequential RKNN throughput: "
            << 1000.0 / mean_total_ms
            << " inference/s\n";

        std::cout
            << "RKNN benchmark succeeded\n";

        std::cout << "RKNN inference succeeded\n";

        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << '\n';
        return 1;
    }
}
