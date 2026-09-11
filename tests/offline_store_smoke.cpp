#include "storage/offline_store.h"

#include <iostream>

/**
 * @brief OfflineStore 最小完整闭环测试。
 *
 * 验证：
 * 1. 打开数据库；
 * 2. 保存离线消息；
 * 3. 查询待补发消息；
 * 4. 删除已成功处理的消息；
 * 5. 再次查询确认记录已被删除。
 */
int main()
{
    OfflineStore store("offline_store_test.db");

    if (!store.open())
    {
        std::cerr << "OfflineStore open failed\n";
        return 1;
    }

    /*
     * 写入一条模拟离线消息。
     */
    if (!store.save(
            "sensor-001",
            R"({"temperature":25.6})",
            1757422800000))
    {
        std::cerr << "OfflineStore save failed\n";
        return 1;
    }

    /*
     * 最多读取10条待补发消息。
     */
    auto messages = store.loadPending(10);

    std::cout
        << "Loaded "
        << messages.size()
        << " offline message(s)\n";

    /*
     * 打印本次查询到的记录。
     */
    for (const auto& message : messages)
    {
        std::cout
            << "id=" << message.id
            << ", device_id=" << message.device_id
            << ", payload=" << message.payload
            << ", created_at=" << message.created_at
            << '\n';
    }

    /*
     * 这里只是组件测试：
     * 模拟“第一条消息已经成功补发”，
     * 因此删除第一条记录。
     */
    if (!messages.empty())
    {
        const std::int64_t id =
            messages.front().id;

        if (!store.remove(id))
        {
            std::cerr
                << "OfflineStore remove failed\n";

            return 1;
        }

        std::cout
            << "Removed offline message id="
            << id
            << '\n';
    }

    /*
     * 再查询一次，
     * 验证刚才删除的记录已经不存在。
     */
    const auto remaining =
        store.loadPending(10);

    std::cout
        << "Remaining offline messages: "
        << remaining.size()
        << '\n';

    return 0;
}