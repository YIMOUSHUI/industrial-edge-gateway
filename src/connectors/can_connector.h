#pragma once

#include "connectors/iconnector.h"

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

class CanConnector : public IConnector
{
public:
    /**
     * @brief 创建CAN Connector。
     *
     * @param interface_name
     *        Linux SocketCAN接口名称。
     *
     * Ubuntu虚拟测试：
     *   vcan0
     *
     * 以后真实板端：
     *   can0
     */
    explicit CanConnector(std::string interface_name);

    ~CanConnector() override;
    /**
     * @brief 创建并绑定SocketCAN socket，
     *        启动CAN接收线程。
     */
    bool start() override;

    /**
     * @brief 停止接收线程并释放CAN socket。
     */
    void stop() override;

    /**
     * @brief 返回Connector名称。
     */
    std::string name() const override;

    /**
     * @brief 设置统一Telemetry回调。
     *
     * CanConnector把CAN帧转换成TelemetryMessage后，
     * 通过这个回调交给GatewayContext。
     */
    void setMessageCallback(MessageCallback callback) override;

    /**
     * @brief 将DeviceCommand转换成CAN Frame并发送。
     *
     * 第一版后续只实现一个简单的测试协议。
     */
    bool sendCommand(const DeviceCommand& command) override;

    /**
     * @brief 返回当前已经观察到的CAN设备。
     */
    std::vector<std::string> getOnlineDevices() override;

private:
    /**
     * @brief CAN接收线程入口。
     */
    void receiveLoop();

private:
    /**
     * SocketCAN接口名称。
     *
     * 当前：
     *   vcan0
     *
     * 后续真实硬件：
     *   can0
     */
    std::string interface_name_;

    /**
     * SocketCAN socket文件描述符。
     *
     * -1表示当前没有有效socket。
     */
    int can_fd_{-1};

    /**
     * Connector是否应该继续运行。
     */
    std::atomic<bool> running_{false};

    /**
     * CAN接收线程。
     *
     * 后续专门负责：
     *
     * read CAN frame
     *      ↓
     * 转TelemetryMessage
     *      ↓
     * 调用message_callback_
     */
    std::thread receive_thread_;

    /**
     * GatewayContext设置进来的Telemetry回调。
     */
    MessageCallback message_callback_;

    /**
     * CAN和TCP不一样：
     *
     * TCP有明确的connect/disconnect，
     * CAN总线没有“某设备建立TCP连接”这种概念。
     *
     * 第一版我们把：
     *
     *   已经收到过帧的CAN ID
     *
     * 视为当前已观察到的设备。
     *
     * 例如收到CAN ID 0x101：
     *
     *   "can-101"
     *
     * 会加入这里。
     */
    std::unordered_set<std::string> seen_devices_;

    /**
     * receive_thread_会写seen_devices_，
     * gatewayctl管理线程可能同时读取，
     * 因此需要mutex保护。
     */
    std::mutex devices_mutex_;

    /**
     * @brief CAN遥测消息的递增序列号。
     *
     * 每成功生成一条TelemetryMessage就递增一次。
     * atomic保证即使以后存在多个处理线程，也不会产生数据竞争。
     */
    std::atomic<std::uint64_t> sequence_{0};
};