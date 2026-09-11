#include "core/telemetry_queue.h"

#include <chrono>
#include <iostream>

/**
 * @brief TelemetryQueue 定时等待最小测试。
 *
 * 验证：
 * 1. 空 Queue 等待到期后返回 Timeout；
 * 2. Queue 中存在消息时返回 Message；
 * 3. Queue stop 且无剩余消息时返回 Stopped。
 */
int main()
{
    TelemetryQueue queue(10);

    TelemetryMessage message;

    /*
     * 测试1：
     * Queue 当前为空，也没有 stop。
     *
     * 最多等待500ms后应返回 Timeout。
     */
    QueuePopResult result =
        queue.waitAndPopFor(
            message,
            std::chrono::milliseconds(500)
        );

    if (result != QueuePopResult::Timeout)
    {
        std::cerr << "Expected Timeout\n";
        return 1;
    }

    std::cout << "Timeout test passed\n";

    /*
     * 测试2：
     * 先主动放入一条消息。
     */
    TelemetryMessage input;
    input.device_id = "sensor-001";
    input.protocol = "tcp";
    input.sequence = 1;

    if (!queue.push(input))
    {
        std::cerr << "Queue push failed\n";
        return 1;
    }

    result =
        queue.waitAndPopFor(
            message,
            std::chrono::milliseconds(500)
        );

    if (result != QueuePopResult::Message)
    {
        std::cerr << "Expected Message\n";
        return 1;
    }

    std::cout
        << "Message test passed, device="
        << message.device_id
        << '\n';

    /*
     * 测试3：
     * 当前 Queue 已为空，然后调用 stop()。
     */
    queue.stop();

    result =
        queue.waitAndPopFor(
            message,
            std::chrono::milliseconds(500)
        );

    if (result != QueuePopResult::Stopped)
    {
        std::cerr << "Expected Stopped\n";
        return 1;
    }

    std::cout << "Stopped test passed\n";

    return 0;
}