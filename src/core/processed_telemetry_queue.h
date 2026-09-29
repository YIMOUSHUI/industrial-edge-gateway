#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>

#include "model/processed_telemetry.h"

enum class ProcessedQueuePopResult
{
    Message,
    Timeout,
    Stopped
};

/**
 * @brief Processing Worker 与 Uplink Worker 之间的有界队列。
 *
 * 与入口 TelemetryQueue 不同：
 *
 * push() 在队列满时采用等待策略，
 * 而不是直接丢弃。
 *
 * 原因：
 * 上游真正的过载保护仍由 TelemetryQueue 完成。
 * 这里通过阻塞 Processing Worker 将背压向入口传递，
 * 避免系统出现第二个无意义的丢包点。
 */
class ProcessedTelemetryQueue
{
public:
    explicit ProcessedTelemetryQueue(std::size_t capacity);

    bool push(ProcessedTelemetry telemetry);

    ProcessedQueuePopResult waitAndPopFor(
        ProcessedTelemetry& telemetry,
        std::chrono::milliseconds timeout
    );

    std::size_t size();
    std::size_t capacity() const;

    void stop();

private:
    std::queue<ProcessedTelemetry> queue_;
    const std::size_t capacity_;

    std::mutex mutex_;

    std::condition_variable not_empty_;
    std::condition_variable not_full_;

    bool stopped_{false};
};

