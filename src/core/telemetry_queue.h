#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <chrono>

#include "model/telemetry_message.h"


/**
 * @brief TelemetryQueue 等待取消息的结果。
 *
 * Message：
 *   成功从 Queue 中取得一条 TelemetryMessage。
 *
 * Timeout：
 *   在指定时间内没有消息到达，
 *   但 Queue 仍然正常运行。
 *
 * Stopped：
 *   Queue 已经停止并且没有剩余消息，
 *   调用线程应结束等待。
 */
enum class QueuePopResult
{
    Message,
    Timeout,
    Stopped
};

class TelemetryQueue
{
public:
    explicit TelemetryQueue(std::size_t capacity);//最多1000条TelemetryMessage

    bool push(const TelemetryMessage& message);//Producer 调用

    /**
     * @brief 获取当前Queue中等待处理的TelemetryMessage数量。
     *
     * @return 当前队列长度。
     */
    std::size_t size();

    /**
     * @brief 获取TelemetryQueue最大容量。
     *
     * 当前GatewayContext配置为1000。
     */
    std::size_t capacity() const;

    /**
     * @brief 获取因为Queue已满而被拒绝的遥测总数。
     *
     * 该计数从gatewayd本次启动开始累计。
     * Queue stop期间拒绝的消息不计入这里，
     * 因为那属于正常退出流程，不属于系统过载丢包。
     */
    std::size_t droppedCount();


    bool waitAndPop(TelemetryMessage& message);//Consumer 调用


    /**
     * @brief 等待一条遥测消息，但最多等待指定时间。
     *
     * 与 waitAndPop() 的区别：
     *
     * waitAndPop()：
     *   Queue 为空时会一直等待，直到有消息或 Queue stop。
     *
     * waitAndPopFor()：
     *   Queue 为空时最多等待 timeout，
     *   即使没有消息也会在超时后返回。
     *
     * @param message 成功取出消息时，用于接收消息内容。
     * @param timeout 本次最多等待多长时间。
     *
     * @return QueuePopResult::Message
     *         成功取得一条消息。
     *
     * @return QueuePopResult::Timeout
     *         等待超时，但 Queue 仍正常运行。
     *
     * @return QueuePopResult::Stopped
     *         Queue 已停止并且没有剩余消息。
     */
    QueuePopResult waitAndPopFor(TelemetryMessage& message,std::chrono::milliseconds timeout);


    void stop();

private:
    std::queue<TelemetryMessage> queue_;
    std::size_t capacity_;

    /*
    * 从本次gatewayd启动开始，
    * 因TelemetryQueue容量已满而被拒绝的消息总数。
    *
    * 它和queue_使用同一把mutex_保护，
    * 因此不再额外使用atomic。
    */
    std::size_t dropped_count_{0};

    std::mutex mutex_;
    std::condition_variable condition_;
    bool stopped_ = false;
};
