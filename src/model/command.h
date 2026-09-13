#pragma once

#include <string>

#include <nlohmann/json.hpp>

/**
 * @brief Gateway 内部统一的设备下行命令模型。
 *
 * DeviceCommand 不负责 MQTT 收发，
 * 也不负责 TCP / CAN / Modbus 的具体协议实现。
 *
 * 它只描述：
 *
 * “Gateway 想让哪个设备执行什么操作，以及操作参数是什么。”
 *
 * 外部 MQTT JSON 会先经过 CommandConverter 的解析和校验，
 * 成功后才转换为 DeviceCommand，
 * 再交给后续 CommandRouter 处理。
 */
struct DeviceCommand
{
    /**
     * @brief 本次命令请求的唯一标识。
     *
     * 后续 Gateway 返回执行结果时，
     * 可通过 request_id 将响应与原始请求对应起来。
     */
    std::string request_id;

    /**
     * @brief 目标设备 ID。
     *
     * CommandRouter 后续主要根据该字段找到
     * 负责该设备的 Connector。
     */
    std::string device_id;

    /**
     * @brief 要执行的操作名称。
     *
     * 例如：
     * set_interval
     * start
     * stop
     * write_register
     */
    std::string action;

    /**
     * @brief 当前命令自己的参数。
     *
     * 不同 action 需要的参数不同，因此第一版
     * 使用 JSON 对象保存可变参数，
     * 避免现在过早为所有未来命令设计固定结构。
     *
     * 例如：
     *
     * {
     *     "interval_ms": 1000
     * }
     */
    nlohmann::json parameters;
};