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
            /*
            * Queue已经达到容量上限。
            *
            * 当前策略不是让生产者无限阻塞，
            * 而是拒绝新的TelemetryMessage，
            * 防止内存无限增长最终拖死gatewayd。
            *
            * 这里记录一次真实的“过载丢弃”。
            */
            ++dropped_count_;

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

/**
 * @brief 在指定时间内等待并取出一条 TelemetryMessage。
 *
 * 该接口用于需要周期性执行维护任务的 Worker。
 * 即使当前没有新消息，Worker 也可以在 timeout 到期后醒来，
 * 继续执行 MQTT 重连、离线消息补发等逻辑。
 */
QueuePopResult TelemetryQueue::waitAndPopFor(TelemetryMessage& message,std::chrono::milliseconds timeout)
{
    /*
     * condition_variable::wait_for() 需要 unique_lock。
     *
     * 等待期间：
     *   wait_for() 会自动释放 mutex，
     *   允许生产者线程调用 push()。
     *
     * 被唤醒后：
     *   wait_for() 会重新获得 mutex，
     *   然后继续检查 Queue 状态。
     */
    std::unique_lock<std::mutex> lock(mutex_);

    /*
     * 最多等待 timeout。
     *
     * predicate 为 true 的条件：
     *
     * 1. stopped_ == true
     *    Queue 已经收到停止请求；
     *
     * 2. !queue_.empty()
     *    Queue 中已经有消息可以处理。
     *
     * wait_for() 返回：
     *
     * true：
     *   predicate 在等待结束时成立。
     *
     * false：
     *   等待时间到达，但 predicate 仍然不成立。
     */
    const bool ready = condition_.wait_for(
        lock,
        timeout,
        [this]()
        {
            return stopped_ || !queue_.empty();
        }
    );

    /*
     * ready == false：
     *
     * 说明等待时间已经到达，
     * 但 Queue 既没有停止，也没有新消息。
     *
     * 这是正常超时，不是错误。
     */
    if (!ready)
    {
        return QueuePopResult::Timeout;
    }

    /*
     * Queue 已经停止，并且没有剩余消息。
     *
     * 此时不会再产生新的 TelemetryMessage，
     * 调用线程可以安全结束。
     */
    if (stopped_ && queue_.empty())
    {
        return QueuePopResult::Stopped;
    }

    /*
     * 能运行到这里说明 Queue 中存在消息。
     *
     * 即使 stopped_ 已经为 true，
     * 只要 Queue 中仍有剩余消息，
     * 当前实现仍然允许把已有消息取出。
     *
     * 这样保持和原 waitAndPop() 类似的“先处理剩余数据”语义。
     */
    message = std::move(queue_.front());
    queue_.pop();

    return QueuePopResult::Message;
}

std::size_t TelemetryQueue::size()
{
    /*
     * queue_同时可能被：
     *
     * Connector生产者线程 push()
     * Telemetry Worker pop()
     *
     * 所以读取size也必须加同一把mutex_，
     * 避免与push/pop同时访问std::queue产生数据竞争。
     */
    std::lock_guard<std::mutex> lock(mutex_);

    return queue_.size();
}

std::size_t TelemetryQueue::capacity() const
{
    /*
     * capacity_在构造完成后不会再修改，
     * 因此这里可以直接读取。
     */
    return capacity_;
}

std::size_t TelemetryQueue::droppedCount()
{
    /*
     * dropped_count_在push()中会被生产者线程修改，
     * 所以查询时也使用同一把mutex_。
     */
    std::lock_guard<std::mutex> lock(mutex_);

    return dropped_count_;
}



void TelemetryQueue::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopped_ = true;
    }

    condition_.notify_all();
}