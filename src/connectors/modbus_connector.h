#pragma once

#include "connectors/iconnector.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>
#include <mutex>

/*
 * libmodbus中的上下文类型。
 *
 * 这里只保存指针，不需要在头文件中了解它的内部结构。
 * 真正的libmodbus头文件会在modbus_connector.cpp中包含。
 */
struct _modbus;
using modbus_t = struct _modbus;

/**
 * @brief Modbus TCP南向协议连接器。
 *
 * 当前第一版负责：
 *
 * 1. 主动连接一个Modbus TCP设备；
 * 2. 周期性读取Holding Register；
 * 3. 将寄存器数据转换为TelemetryMessage；
 * 4. 通过IConnector统一回调交给GatewayContext；
 * 5. 后续支持DeviceCommand → 写寄存器。
 *
 * 当前开发环境：
 *
 * gatewayd
 *     ↓ Modbus TCP
 * 127.0.0.1:1502
 *     ↓
 * modbus_device_simulator
 */

class ModbusConnector : public IConnector
{
public:
    /**
     * @param host              Modbus设备IP地址。
     * @param port              Modbus TCP端口。
     * @param device_id         Gateway内部统一设备ID。
     * @param poll_interval_ms  轮询寄存器间隔，单位毫秒。
     */
    ModbusConnector(std::string host,int port,std::string device_id,std::uint32_t poll_interval_ms);
    ~ModbusConnector() override;

    /**
     * @brief 创建libmodbus上下文并启动轮询线程。
     *
     * start()只负责初始化Connector自身。
     *
     * 远端PLC是否在线不决定start()是否成功；
     * PLC连接、断线检测和自动重连由pollingLoop()负责。
     */
    bool start() override;

    /**
     * @brief 停止轮询线程并关闭Modbus连接。
     */
    void stop() override;

    /**
     * @brief 返回Connector类型名称。
     */
    std::string name() const override;

    /**
     * @brief 设置统一Telemetry消息回调。
     */
    void setMessageCallback(MessageCallback callback) override;

    /**
     * @brief 将统一DeviceCommand转换为Modbus写寄存器操作。
     *
     * 第一版随后实现：
     *
     * set_speed
     *      ↓
     * 写Holding Register 2
     */
    bool sendCommand(const DeviceCommand& command) override;

    /**
     * @brief 返回当前Modbus设备列表。
     *
     * 第一版一个ModbusConnector对应一个设备。
     */
    std::vector<std::string> getOnlineDevices() override;

private:
    /**
     * @brief 周期读取Modbus Holding Register的线程入口。
     *
     * 后续流程：
     *
     * read HR0~HR2
     *      ↓
     * 转换为TelemetryMessage
     *      ↓
     * message_callback_
     */
    void pollingLoop();

private:
    /**
     * Modbus TCP服务器地址。
     *
     * 当前：
     *   127.0.0.1
     */
    std::string host_;

    /**
     * Modbus TCP端口。
     *
     * 当前开发环境：
     *   1502
     */
    int port_{0};

    /**
     * Gateway内部统一设备ID。
     *
     * 当前测试：
     *   modbus-plc-01
     */
    std::string device_id_;

    /**
     * 两次读取寄存器之间的时间间隔。
     *
     * 当前计划：
     *   1000 ms
     */
    std::uint32_t poll_interval_ms_{1000};

    /**
     * libmodbus通信上下文。
     *
     * nullptr：
     *   尚未创建或已经释放。
     *
     * 非nullptr：
     *   当前已经创建Modbus TCP上下文。
     */
    modbus_t* context_{nullptr};

    /**
     * @brief 串行保护libmodbus网络I/O。
     *
     * polling_thread_会通过context_执行：
     * - modbus_connect()
     * - modbus_read_registers()
     *
     * Command Worker会通过sendCommand()执行：
     * - modbus_write_register()
     *
     * 它们共享同一个Modbus TCP连接，
     * 因此一次只允许一个线程执行完整的Modbus事务。
     */
    std::mutex io_mutex_;

    /**
     * Connector运行状态。
     *
     * 主线程负责修改，
     * polling_thread_负责读取，
     * 因此使用atomic避免数据竞争。
     */
    std::atomic<bool> running_{false};

    /**
     * 是否已经成功连接Modbus设备。
     *
     * gatewayctl devices后续会根据这个状态
     * 判断是否返回device_id_。
     */
    std::atomic<bool> connected_{false};

    /**
     * 周期读取寄存器的独立线程。
     */
    std::thread polling_thread_;

    /**
     * GatewayContext设置进来的统一Telemetry回调。
     */
    MessageCallback message_callback_;

    /**
     * Modbus遥测消息递增序号。
     */
    std::atomic<std::uint64_t> sequence_{0};
};