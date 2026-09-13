#pragma once

#include <functional>
#include <string>
#include <vector>

#include "model/telemetry_message.h"
#include "model/command.h"

class IConnector
{
public:
    using MessageCallback = 
        std::function<void(const TelemetryMessage&)>;
        
    virtual ~IConnector() = default;

    virtual bool start() = 0;
    virtual void stop() = 0;

    virtual std::string name() const = 0;

    virtual void setMessageCallback(MessageCallback callback) = 0;

    /**
     * @brief 向该 Connector 管理的目标设备下发一条命令。
     *
     * CommandRouter 会先确定目标命令应该交给哪个 Connector，
     * 然后通过统一的 IConnector 接口调用本函数。
     *
     * 不同协议由各自 Connector 决定具体发送方式：
     *
     * TcpConnector：
     *   device_id → socket fd → TCP frame
     *
     * CanConnector：
     *   device_id → CAN ID → CAN frame
     *
     * ModbusConnector：
     *   device_id → slave/register → Modbus request
     *
     * @param command 已完成解析和公共字段校验的内部 DeviceCommand。
     *
     * @return true  命令成功交给该 Connector 处理/发送。
     * @return false 命令无法发送或处理失败。
     */
    virtual bool sendCommand(const DeviceCommand& command) = 0;//所有 Connector 必须具有“下发命令”这个能力

    /**
     * @brief 获取当前由该Connector管理的在线设备ID列表。
     *
     * 不同协议Connector自己决定如何判断设备在线：
     *
     * TcpConnector
     *   -> 根据当前TCP连接表查询
     *
     * CanConnector
     *   -> 后续根据CAN设备状态查询
     *
     * ModbusConnector
     *   -> 后续根据Modbus设备状态查询
     *
     * GatewayContext只使用这个统一接口，
     * 不需要了解各协议内部如何保存设备状态。
     *
     * @return 当前在线设备ID列表的快照。
     */
    virtual std::vector<std::string> getOnlineDevices() = 0;

};

