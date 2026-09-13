#include "converters/command_converter.h"

#include "common/logger.h"

#include <nlohmann/json.hpp>

/**
 * @brief 将 MQTT Command JSON 解析为统一 DeviceCommand。
 */
bool CommandConverter::fromJson(const std::string& payload,DeviceCommand& command)
{
    try
    {
        /*
         * 将 MQTT Payload 字符串解析成 JSON 对象。
         *
         * 如果 payload 根本不是合法 JSON，
         * parse() 会抛出 nlohmann::json::exception，
         * 最后由 catch 统一处理。
         */
        const nlohmann::json root =
            nlohmann::json::parse(payload);

        /*
         * Command 的最外层必须是一个 JSON Object。
         *
         * 合法：
         * {
         *     "request_id": "...",
         *     ...
         * }
         *
         * 非法：
         * [1, 2, 3]
         *
         * 或：
         * "hello"
         */
        if (!root.is_object())
        {
            Logger::warn(
                "Invalid MQTT command: root must be a JSON object"
            );

            return false;
        }

        /*
         * 三个公共字段必须存在。
         *
         * contains() 只检查“有没有这个字段”，
         * 字段类型稍后再检查。
         */
        if (!root.contains("request_id")
            || !root.contains("device_id")
            || !root.contains("action"))
        {
            Logger::warn(
                "Invalid MQTT command: "
                "request_id, device_id and action are required"
            );

            return false;
        }

        /*
         * 三个公共字段都必须是字符串。
         *
         * 例如下面这种就是非法：
         *
         * "device_id": 123
         */
        if (!root["request_id"].is_string()
            || !root["device_id"].is_string()
            || !root["action"].is_string())
        {
            Logger::warn(
                "Invalid MQTT command: "
                "request_id, device_id and action must be strings"
            );

            return false;
        }

        /*
         * 到这里公共字段的存在性和类型已经确认，
         * 可以安全读取。
         */
        command.request_id =
            root["request_id"].get<std::string>();

        command.device_id =
            root["device_id"].get<std::string>();

        command.action =
            root["action"].get<std::string>();

        /*
         * 不接受空的公共字段。
         *
         * JSON语法虽然合法：
         *
         * "device_id": ""
         *
         * 但对Gateway业务没有意义。
         */
        if (command.request_id.empty()
            || command.device_id.empty()
            || command.action.empty())
        {
            Logger::warn(
                "Invalid MQTT command: "
                "request_id, device_id and action must not be empty"
            );

            return false;
        }

        /*
         * parameters 是可选字段。
         *
         * 没提供：
         *   使用空对象 {}。
         *
         * 提供了：
         *   必须是一个 JSON Object。
         */
        if (root.contains("parameters"))
        {
            if (!root["parameters"].is_object())
            {
                Logger::warn(
                    "Invalid MQTT command: "
                    "parameters must be a JSON object"
                );

                return false;
            }

            command.parameters =
                root["parameters"];
        }
        else
        {
            command.parameters =
                nlohmann::json::object();
        }
    }
    catch (const nlohmann::json::exception& error)
    {
        /*
         * 包括：
         * - JSON语法错误；
         * - JSON类型转换异常；
         * - 其他nlohmann/json解析异常。
         */
        Logger::warn(
            "Failed to parse MQTT command JSON: "
            + std::string(error.what())
        );

        return false;
    }

    return true;
}

/**
 * @brief 将统一DeviceCommand序列化为JSON字符串。
 *
 * 本函数不负责TCP发送，
 * 只负责“内部命令对象 → JSON文本”的数据格式转换。
 */
std::string CommandConverter::toJson(const DeviceCommand& command)
{
    /*
     * 创建一个JSON Object。
     *
     * root只是当前函数里的临时对象，
     * 用来组织最终要发送的Command字段。
     */
    nlohmann::json root;

     /*
     * 将DeviceCommand中的字段逐个写入JSON。
     *
     * DeviceCommand:
     *
     * request_id
     * device_id
     * action
     * parameters
     *
     * ↓
     *
     * JSON:
     *
     * {
     *   "request_id": "...",
     *   "device_id": "...",
     *   "action": "...",
     *   "parameters": {...}
     * }
     */
    root["request_id"] = command.request_id;
    root["device_id"] = command.device_id;
    root["action"] = command.action;
    root["parameters"] = command.parameters;

    /*
     * dump()：
     * 将nlohmann::json对象转换成std::string。
     *
     * 例如：
     *
     * {"request_id":"cmd-001", ...}
     */
    return root.dump();
}






