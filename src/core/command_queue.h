#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>

#include "model/command.h"

/**
 * @brief Gateway 下行设备命令队列。
 *
 * Producer：
 *   MQTT Command 回调路径。
 *
 * Consumer：
 *   Command Worker 线程。
 *
 * 作用：
 *   将 MQTT 命令接收与真正的设备控制逻辑解耦，
 *   避免 TCP / CAN / Modbus 等设备操作阻塞 MQTT 回调。
 */
class CommandQueue
{
public:
    /**
     * @brief 创建有界命令队列。
     *
     * @param capacity 队列允许保存的最大命令数量。
     */
    explicit CommandQueue(std::size_t capacity);

    /**
     * @brief 向队列中加入一条命令。
     *
     * @return true  入队成功。
     * @return false 队列已停止或已经满。
     */
    bool push(const DeviceCommand& command);

    /**
     * @brief 阻塞等待并取出一条命令。
     *
     * Queue为空：
     *   Command Worker 睡眠等待。
     *
     * 有命令：
     *   取出一条命令并返回 true。
     *
     * Queue已经stop且没有剩余命令：
     *   返回 false，让Command Worker退出。
     */
    bool waitAndPop(DeviceCommand& command);

    /**
     * @brief 停止队列并唤醒等待中的 Command Worker。
     */
    void stop();

private:
    std::queue<DeviceCommand> queue_;

    std::size_t capacity_;

    std::mutex mutex_;

    std::condition_variable condition_;

    bool stopped_{false};
};






