#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>

#include "model/telemetry_message.h"

class TelemetryQueue
{
public:
    explicit TelemetryQueue(std::size_t capacity);//最多1000条TelemetryMessage

    bool push(const TelemetryMessage& message);//Producer 调用
    bool waitAndPop(TelemetryMessage& message);//Consumer 调用

    void stop();

private:
    std::queue<TelemetryMessage> queue_;
    std::size_t capacity_;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool stopped_ = false;
};
