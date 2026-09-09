#include "core/telemetry_queue.h"

#include <utility>

TelemetryQueue::TelemetryQueue(std::size_t capacity):capacity_(capacity)
{

}

bool TelemetryQueue::push(const TelemetryMessage& message)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if(stopped_)
        {
            return false;
        }

        if(queue_.size() >= capacity_)
        {
            return false;
        }

        queue_.push(message);
    }//离开 lock对象析构 自动Unlock 控制锁的生命周期
    condition_.notify_one(); //通知一个正在等待这个 condition_variable 的线程

    return true;
}

bool TelemetryQueue::waitAndPop(TelemetryMessage& message)//【Worker线程】 等待和处理工作队列
{
    std::unique_lock<std::mutex> lock(mutex_);

    condition_.wait(
        lock,
        [this]()
        {
            return stopped_ || !queue_.empty();
        }
    );

    if(queue_.empty())
    {
        return false;
    }

    message = std::move(queue_.front());

    queue_.pop();

    return true;
}

void TelemetryQueue::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        stopped_ = true;
    }

    condition_.notify_all();
}