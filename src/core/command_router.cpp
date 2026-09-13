#include "core/command_router.h"

#include "common/logger.h"

/**
 * @brief 注册设备当前所属 Connector。
 */
void CommandRouter::registerDevice(const std::string& device_id,IConnector* connector)
{
    if (device_id.empty() || connector == nullptr)
    {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);

     /*
     * 使用 []：
     *
     * device_id 不存在：
     *   新增路由。
     *
     * 已经存在：
     *   更新路由。
     *
     * 因此设备重新连接或路由变化时也能覆盖旧记录。
     */
    routes_[device_id] = connector;
}

/**
 * @brief 将命令交给目标设备所属 Connector。
 */
bool CommandRouter::dispatch(const DeviceCommand& command)
{
    IConnector* connector = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        const auto it =routes_.find(command.device_id);
        if (it == routes_.end())
        {
            Logger::warn(
                "No command route for device: "
                + command.device_id
            );
            return false;
        }

        connector = it->second;
    }
    /*
     * 注意：
     * 到这里已经释放 Router 的 mutex。
     *
     * sendCommand() 后续可能进行网络 I/O，
     * 不应该在慢操作期间一直占着路由表锁。
     */
    return connector->sendCommand(command);
}