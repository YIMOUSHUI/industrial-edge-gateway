#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "connectors/iconnector.h"
#include "model/command.h"

/**
 * @brief Gateway 下行设备命令路由器。
 *
 * CommandRouter 负责维护：
 *
 * device_id -> IConnector*
 *
 * 的映射关系。
 *
 * 它只决定“命令应该交给哪个 Connector”，
 * 不负责 TCP/CAN/Modbus 的具体发送方式。
 */
class CommandRouter
{
public:
    /**
     * @brief 注册或更新设备所属的 Connector。
     *
     * 当前策略：
     * 当 Gateway 从某个 Connector 收到设备遥测数据时，
     * 就认为该 device_id 当前由这个 Connector 管理。
     *
     * @param device_id 设备唯一标识。
     * @param connector 实际收到该设备数据的 Connector。
     */
    void registerDevice(const std::string& device_id,IConnector* connector);

    /**
     * @brief 将 DeviceCommand 路由到目标设备所属 Connector。
     *
     * @return true  找到路由且 Connector 成功处理命令。
     * @return false 找不到设备路由或 Connector 处理失败。
     */
    bool dispatch(const DeviceCommand& command);

private:
    /**
     * @brief device_id 到 Connector 的动态路由表。
     *
     * 这里保存的是非拥有型指针：
     * Connector 对象真正由 GatewayContext 的 unique_ptr 管理。“我知道这个对象在哪里，但我不负责销毁它。”
     */
    std::unordered_map<std::string, IConnector*> routes_;

    /**
     * @brief 保护 routes_。
     *
     * 注册发生在 Connector I/O 线程，
     * dispatch 发生在 Command Worker，
     * 因此存在跨线程访问。
     */
    std::mutex mutex_;
};