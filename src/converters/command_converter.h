#pragma once

#include <string>

#include "model/command.h"

/**
 * @brief MQTT Command JSON 与 Gateway 内部 DeviceCommand
 *        之间的转换组件。
 *
 * 当前职责：
 * 1. 解析 JSON 文本；
 * 2. 校验公共必需字段；
 * 3. 构造 DeviceCommand。
 *
 * 该类不负责：
 * - MQTT 收发；
 * - Command 路由；
 * - TCP/CAN/Modbus 设备控制。
 */
class CommandConverter
{
public:
    /**
     * @brief 将 MQTT Command JSON 转换为 DeviceCommand。
     *
     * @param payload MQTT 消息正文。
     * @param command 成功解析后写入的内部命令对象。
     *
     * @return true  JSON合法且公共字段符合要求。
     * @return false JSON解析失败或字段不合法。
     */
    static bool fromJson(const std::string& payload,DeviceCommand& command);

    /**
     * @brief 将Gateway内部DeviceCommand转换为JSON字符串。
     *
     * 主要用于南向设备下行：
     *
     * DeviceCommand
     *      ↓
     * JSON Payload
     *      ↓
     * TCP/CAN/其他协议继续处理
     *
     * @param command 已完成解析和路由的内部设备命令。
     *
     * @return 可以发送给设备的JSON字符串。
     */
    static std::string toJson(const DeviceCommand& command);

};
