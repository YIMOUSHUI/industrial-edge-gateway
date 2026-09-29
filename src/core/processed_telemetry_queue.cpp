#include "core/processed_telemetry_queue.h"

#include <utility>

ProcessedTelemetryQueue::ProcessedTelemetryQueue(std::size_t capacity): capacity_(capacity)
{
}

bool ProcessedTelemetryQueue::push(ProcessedTelemetry telemetry)
{
    std::unique_lock<std::mutex> lock(mutex_);

    /*
     * Uplink 慢时不要继续无限搬运数据。
     *
     * Worker 在这里等待，
     * 背压最终会传递到入口 TelemetryQueue。
     */
    not_full_.wait(
        lock,
        [this]()
        {
            return stopped_ || queue_.size() < capacity_;
        }
    );

    if (stopped_)
    {
        return false;
    }

    queue_.push(std::move(telemetry));

    lock.unlock();

    not_empty_.notify_one();

    return true;
}

ProcessedQueuePopResult ProcessedTelemetryQueue::waitAndPopFor(ProcessedTelemetry& telemetry,std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(mutex_);

    const bool ready = not_empty_.wait_for(
        lock,
        timeout,
        [this]()
        {
            return stopped_ || !queue_.empty();
        }
    );

    if (!ready)
    {
        return ProcessedQueuePopResult::Timeout;
    }

    if (stopped_ && queue_.empty())
    {
        return ProcessedQueuePopResult::Stopped;
    }

    telemetry = std::move(queue_.front());
    queue_.pop();

    lock.unlock();

    /*
     * 队列已经空出一个位置，
     * 唤醒可能因为背压而等待的 Processing Worker。
     */
    not_full_.notify_one();

    return ProcessedQueuePopResult::Message;
}

std::size_t ProcessedTelemetryQueue::size()
{
    std::lock_guard<std::mutex> lock(mutex_);

    return queue_.size();
}

std::size_t ProcessedTelemetryQueue::capacity() const
{
    return capacity_;
}

void ProcessedTelemetryQueue::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopped_ = true;
    }

    not_empty_.notify_all();
    not_full_.notify_all();
}

