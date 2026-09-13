#include "core/command_queue.h"

#include <utility>

CommandQueue::CommandQueue(std::size_t capacity)
    : capacity_(capacity)
{
}

/**
 * @brief MQTT接收路径向CommandQueue生产一条命令。
 */
bool CommandQueue::push(const DeviceCommand& command)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        /*
         * Gateway已经停止时不再接受新的下行命令。
         */
        if (stopped_)
        {
            return false;
        }

        /*
         * 使用有界Queue，避免异常情况下命令无限堆积，
         * 最终耗尽进程内存。
         */
        if (queue_.size() >= capacity_)
        {
            return false;
        }

        queue_.push(command);
    }

    /*
     * 队列中已经有新命令，
     * 唤醒一个等待中的Command Worker。
     */
    condition_.notify_one();
    return true;
}

/**
 * @brief Command Worker阻塞等待下一条设备命令。
 */
bool CommandQueue::waitAndPop(DeviceCommand& command)
{
    std::unique_lock<std::mutex> lock(mutex_);
    /*
     * Queue为空时线程睡眠。
     *
     * push()或stop()调用notify以后重新醒来检查：
     *
     * 1. Queue是否已有命令；
     * 2. Queue是否已经停止。
     */
    condition_.wait(
        lock,
        [this]()
        {
            return stopped_ || !queue_.empty();
        }
    );
    
    /*
     * 能醒来但Queue仍为空，
     * 说明当前是stop()唤醒。
     *
     * Command Worker可以退出。
     */
    if (queue_.empty())
    {
        return false;
    }

    /*
     * 将队首命令移动到Consumer提供的command对象中，
     * 然后从Queue删除。
     */
    command = std::move(queue_.front());
    queue_.pop();

    return true;
}

/**
 * @brief 停止CommandQueue。
 */
void CommandQueue::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopped_ = true;
    }

    /*
     * 唤醒可能正在waitAndPop()中的Command Worker。
     */
    condition_.notify_all();
}