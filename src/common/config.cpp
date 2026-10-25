#include "common/config.h"

#include <algorithm>
#include <cmath>
#include <iostream>
#include <unordered_set>
#include <utility>

#include <yaml-cpp/yaml.h>

bool Config::load(const std::string& path)
{
    try
    {   
        YAML::Node root = YAML::LoadFile(path);

        data_.gateway_id = root["gateway"]["id"].as<std::string>();//.as<T>() 转换为相应类型
        data_.worker_threads =root["runtime"]["worker_threads"].as<int>();
        if (data_.worker_threads <= 0|| data_.worker_threads > 32)
        {
            std::cerr
                << "[Config] runtime.worker_threads must be in [1, 32]"
                << std::endl;

            return false;
        }


        data_.log_level =root["logging"]["level"].as<std::string>();
        
        data_.connectors.clear();
        
        for(const YAML::Node& node : root["connectors"])
        {
            ConnectorConfig connector;
            connector.name =node["name"].as<std::string>();
            connector.type =node["type"].as<std::string>();
            connector.enabled =node["enabled"].as<bool>();
            
            if(node["port"])
            {
                connector.port = node["port"].as<int>();
            }

            /*
            * SocketCAN接口名称。
            *
            * 只有CAN Connector需要这个字段。
            *
            * 例如gateway.yaml：
            *
            *   type: can
            *   interface: vcan0
            */
            if (node["interface"])
            {
                connector.interface_name =node["interface"].as<std::string>();
            }

            /*
            * Modbus TCP目标设备地址。
            *
            * 例如：
            *   host: 127.0.0.1
            */
            if (node["host"])
            {
                connector.host =node["host"].as<std::string>();
            }

            /*
            * Gateway内部统一设备ID。
            *
            * 例如：
            *   device_id: modbus-plc-01
            */
            if (node["device_id"])
            {
                connector.device_id =node["device_id"].as<std::string>();
            }

            /*
            * Modbus寄存器轮询周期。
            *
            * 例如：
            *   poll_interval_ms: 1000
            */
            if (node["poll_interval_ms"])
            {
                connector.poll_interval_ms = node["poll_interval_ms"].as<std::uint32_t>();
            }

            data_.connectors.push_back(connector);
        }

        /*
        * 加载本地Rule Engine配置。
        *
        * rules不存在：
        *   表示当前Gateway没有配置任何本地规则，
        *   属于合法配置。
        */
        data_.rules.clear();
        if (root["rules"])
        {
            if (!root["rules"].IsSequence())
            {
                std::cerr<< "[Config] rules must be a YAML sequence"<< std::endl;
                return false;
            }

            std::unordered_set<std::string> rule_ids;
            for (const YAML::Node& node : root["rules"])
            {
                RuleConfig rule;

                if (node["enabled"])
                {
                    rule.enabled =node["enabled"].as<bool>();
                }
                /*
                * 第一版Rule必须明确：
                *
                * id
                * device_id
                * metric
                * condition
                */
                if (!node["id"]|| !node["device_id"]|| !node["metric"]|| !node["condition"])
                {
                    std::cerr<< "[Config] rule requires "<< "id, device_id, metric and condition"<< std::endl;
                    return false;
                }
                rule.id = node["id"].as<std::string>();
                rule.device_id = node["device_id"].as<std::string>();
                rule.metric = node["metric"].as<std::string>();

                if (rule.id.empty()|| rule.device_id.empty()|| rule.metric.empty())
                {
                    std::cerr<< "[Config] rule id, device_id and metric "<< "must not be empty"<< std::endl;
                    return false;
                }

                /*
                * Rule ID必须唯一。
                */
                if (!rule_ids.insert(rule.id).second)
                {
                    std::cerr<< "[Config] duplicate rule id: "<< rule.id<< std::endl;
                    return false;
                }

                const YAML::Node condition = node["condition"];
                if (!condition["operator"]|| !condition["threshold"])
                {
                    std::cerr<< "[Config] rule condition requires "<< "operator and threshold: "<< rule.id<< std::endl;
                    return false;
                }

                rule.op = condition["operator"].as<std::string>();
                rule.threshold = condition["threshold"].as<double>();
                if (rule.op != ">"&& rule.op != ">="&& rule.op != "<"&& rule.op != "<="&& rule.op != "=="&& rule.op != "!=")
                {
                    std::cerr<< "[Config] unsupported rule operator: "<< rule.op<< ", rule="<< rule.id<< std::endl;
                    return false;
                }

                if (condition["duration_ms"])
                {
                    const std::int64_t duration = condition["duration_ms"].as<std::int64_t>();
                    if (duration < 0)
                    {
                        std::cerr<< "[Config] rule duration_ms " << "must not be negative: "<< rule.id<< std::endl;
                        return false;
                    }

                    rule.duration_ms = static_cast<std::uint32_t>(duration);
                }

                if (node["cooldown_ms"])
                {
                    const std::int64_t cooldown = node["cooldown_ms"].as<std::int64_t>();
                    if (cooldown < 0)
                    {
                        std::cerr<< "[Config] rule cooldown_ms "<< "must not be negative: "<< rule.id<< std::endl;
                        return false;
                    }
                    rule.cooldown_ms = static_cast<std::uint32_t>(cooldown);
                }

                /*
                * 当前第一版action只支持：
                *
                * type: device_command
                *
                * parameters当前只解析简单标量参数。
                */
                if (node["actions"])
                {
                    if (!node["actions"].IsSequence())
                    {
                        std::cerr<< "[Config] rule actions "<< "must be a sequence: "<< rule.id<< std::endl;
                        return false;
                    }

                    for (const YAML::Node& action_node :node["actions"])
                    {
                        RuleActionConfig action;
                        if (action_node["type"])
                        {
                            action.type = action_node["type"].as<std::string>();
                        }
                        if (action.type != "device_command")
                        {
                            std::cerr<< "[Config] unsupported rule action type: "<< action.type<< std::endl;
                            return false;
                        }
                        if (!action_node["action"])
                        {
                            std::cerr<< "[Config] device_command action "<< "requires action field: "<< rule.id<< std::endl;
                            return false;
                        }
                        action.action = action_node["action"].as<std::string>();
                        if (action_node["parameters"])
                        {
                            const YAML::Node parameters = action_node["parameters"];
                            if (!parameters.IsMap())
                            {
                                std::cerr << "[Config] rule action parameters "<< "must be an object: "<< rule.id<< std::endl;
                                return false;
                            }

                            for (const auto& entry :parameters)
                            {
                                const std::string key = entry.first.as<std::string>();
                                const YAML::Node value = entry.second;
                                /*
                                * 当前已有Connector命令主要使用：
                                *
                                * value
                                * interval_ms
                                *
                                * 等简单标量参数。
                                */
                                try
                                {
                                    action.parameters[key] =value.as<std::int64_t>();
                                    continue;
                                }
                                catch (...)
                                {
                                }

                                try
                                {
                                    action.parameters[key] = value.as<double>();
                                    continue;
                                }
                                catch (...)
                                {
                                }

                                try
                                {
                                    action.parameters[key] = value.as<bool>();
                                    continue;
                                }
                                catch (...)
                                {
                                }

                                action.parameters[key] = value.as<std::string>();
                            }
                        }

                        rule.actions.push_back(std::move(action));
                    }
                }
                data_.rules.push_back(std::move(rule));
            }
        }

        /*
        * M3：加载工业时序异常检测配置。
        *
        * 如果 YAML 中没有 anomaly_detection，
        * 默认关闭，不影响原有 M2 功能。
        *
        * 使用 {} 重置，避免重复调用 load() 时
        * 保留上一次加载的 Profile。
        */
        data_.anomaly_detection = AnomalyConfig{};
        if (root["anomaly_detection"])
        {
            const YAML::Node anomaly_node = root["anomaly_detection"];
            /*
            * anomaly_detection 必须是 YAML 映射。
            * 不能直接写成字符串或数组。
            */
            if (!anomaly_node.IsMap())
            {
                std::cerr<< "[Config] anomaly_detection must be a map\n";
                return false;
            }

            /*
            * 读取总开关。
            * 未配置时保留默认值 false。
            */
            if (anomaly_node["enabled"])
            {
                data_.anomaly_detection.enabled = anomaly_node["enabled"].as<bool>();
            }

            /*
            * profiles 必须是 YAML 数组。
            */
            if (anomaly_node["profiles"])
            {
                const YAML::Node profiles_node = anomaly_node["profiles"];
                if (!profiles_node.IsSequence())
                {
                    std::cerr
                        << "[Config] anomaly_detection.profiles "
                        << "must be a sequence\n";

                    return false;
                }

                /*
                * 用于检查 Profile ID 是否重复。
                */
                std::unordered_set<std::string> profile_ids;
                for (const YAML::Node& item : profiles_node)
                {
                    /*
                    * 每条 Profile 至少需要：
                    * id、device_id、metric。
                    */
                    if (!item.IsMap() ||!item["id"] ||!item["device_id"] ||!item["metric"])
                    {
                        std::cerr
                            << "[Config] anomaly profile requires "
                            << "id, device_id and metric\n";
                        return false;
                    }

                    AnomalyProfileConfig profile;
                    profile.id = item["id"].as<std::string>();
                    profile.device_id = item["device_id"].as<std::string>();
                    profile.metric = item["metric"].as<std::string>();

                    /*
                    * 读取可选字段。
                    * 没写的参数使用结构体中的默认值。
                    */
                    if (item["enabled"])
                    {
                        profile.enabled = item["enabled"].as<bool>();
                    }

                    if (item["window_size"])
                    {
                        /*
                        * 先读取成有符号整数。
                        * 避免负数直接转换成 size_t。
                        */
                        const std::int64_t value = item["window_size"].as<std::int64_t>();
                        if (value < 2 || value > 256)
                        {
                            std::cerr
                                << "[Config] window_size must be "
                                << "in [2, 256]: "
                                << profile.id << '\n';

                            return false;
                        }
                        profile.window_size = static_cast<std::size_t>(value);
                    }

                    if (item["score_threshold"])
                    {
                        profile.score_threshold = item["score_threshold"].as<double>();
                    }

                    if (item["scale_floor"])
                    {
                        profile.scale_floor = item["scale_floor"].as<double>();
                    }

                    if (item["min_deviation"])
                    {
                        profile.min_deviation = item["min_deviation"].as<double>();
                    }

                    if (item["max_gap_ms"])
                    {
                        const std::int64_t value = item["max_gap_ms"].as<std::int64_t>();
                        if (value <= 0)
                        {
                            std::cerr << "[Config] max_gap_ms must be "<< "positive: "<< profile.id << '\n';
                            return false;
                        }
                        profile.max_gap_ms = static_cast<std::uint64_t>(value);
                    }

                    /*
                    * 检查 ID 是否只包含字母、数字、
                    * 下划线和连字符。
                    *
                    * 后续会用 Profile ID 构造
                    * anomaly.<profile_id>.score 指标名称。
                    */
                    const bool valid_id = std::all_of(profile.id.begin(),profile.id.end(),
                        [](unsigned char c)
                        {
                            return
                                (c >= 'a' && c <= 'z') ||
                                (c >= 'A' && c <= 'Z') ||
                                (c >= '0' && c <= '9') ||
                                c == '_' || c == '-';
                        }
                    );

                    /*
                    * 集中校验配置合法性。
                    */
                    if (profile.id.empty() ||!valid_id ||profile.device_id.empty() ||profile.metric.empty() ||profile.window_size < 2 ||
                        profile.window_size > 256 ||profile.max_gap_ms == 0 ||!std::isfinite(profile.score_threshold) ||profile.score_threshold <= 0.0 ||
                        !std::isfinite(profile.scale_floor) ||profile.scale_floor <= 0.0 ||!std::isfinite(profile.min_deviation) ||profile.min_deviation < 0.0)
                    {
                        std::cerr
                            << "[Config] invalid anomaly profile: "
                            << profile.id << '\n';

                        return false;
                    }

                    /*
                    * 不允许出现重复 ID。
                    */
                    if (!profile_ids.insert(profile.id).second)
                    {
                        std::cerr
                            << "[Config] duplicate anomaly profile id: "
                            << profile.id << '\n';

                        return false;
                    }

                    /*
                    * 校验通过后，保存当前 Profile。
                    */
                    data_.anomaly_detection.profiles.push_back(std::move(profile));
                }
            }

            /*
            * 如果启用异常检测，
            * 至少必须有一条真正启用的 Profile。
            */
            if (data_.anomaly_detection.enabled)
            {
                const auto& profiles = data_.anomaly_detection.profiles;
                const bool has_enabled_profile = std::any_of(profiles.begin(),profiles.end(),
                    [](const AnomalyProfileConfig& profile)
                    {
                        return profile.enabled;
                    }
                );

                if (!has_enabled_profile)
                {
                    std::cerr
                        << "[Config] anomaly detection enabled "
                        << "but no active profiles configured\n";

                    return false;
                }
            }
        }

        /*
        * M4：RK3576 NPU多变量时序AI推理配置。
        *
        * ai_inference不存在：
        *   默认关闭，保持Ubuntu和旧配置兼容。
        */
        data_.ai_inference = AiInferenceConfig{};
        if (root["ai_inference"])
        {
            const YAML::Node ai_node = root["ai_inference"];

            if (!ai_node.IsMap())
            {
                std::cerr
                    << "[Config] ai_inference must be a map"
                    << std::endl;

                return false;
            }

            if (ai_node["enabled"])
            {
                data_.ai_inference.enabled =
                    ai_node["enabled"].as<bool>();
            }

            if (ai_node["device_id"])
            {
                data_.ai_inference.device_id =
                    ai_node["device_id"].as<std::string>();
            }

            if (ai_node["model_path"])
            {
                data_.ai_inference.model_path =
                    ai_node["model_path"].as<std::string>();
            }

            if (ai_node["preprocess_path"])
            {
                data_.ai_inference.preprocess_path =
                    ai_node["preprocess_path"].as<std::string>();
            }

            /*
            * AI功能启用以后，
            * 三个核心字段必须全部明确配置。
            */
            if (data_.ai_inference.enabled)
            {
                if (data_.ai_inference.device_id.empty())
                {
                    std::cerr
                        << "[Config] ai_inference.device_id "
                        << "must not be empty"
                        << std::endl;

                    return false;
                }

                if (data_.ai_inference.model_path.empty())
                {
                    std::cerr
                        << "[Config] ai_inference.model_path "
                        << "must not be empty"
                        << std::endl;

                    return false;
                }

                if (data_.ai_inference.preprocess_path.empty())
                {
                    std::cerr
                        << "[Config] ai_inference.preprocess_path "
                        << "must not be empty"
                        << std::endl;

                    return false;
                }
            }
        }

        if (root["mqtt"])
        {
            const YAML::Node mqtt_node = root["mqtt"];

            if (mqtt_node["enabled"])
            {
                data_.mqtt.enabled = mqtt_node["enabled"].as<bool>();
            }
            if (mqtt_node["server_uri"])
            {
                data_.mqtt.server_uri = mqtt_node["server_uri"].as<std::string>();
            }
            if (mqtt_node["client_id"])
            {
                data_.mqtt.client_id = mqtt_node["client_id"].as<std::string>();
            }
            if (mqtt_node["topic"])
            {
                data_.mqtt.topic = mqtt_node["topic"].as<std::string>();
            }
            /*
            * MQTT 下行 Command Topic。
            *
            * 配置不存在时保留 MqttConfig 中定义的默认值。
            */
            if (mqtt_node["command_topic"])
            {
                data_.mqtt.command_topic =
                    mqtt_node["command_topic"].as<std::string>();
            }
            if (mqtt_node["qos"])
            {
                data_.mqtt.qos = mqtt_node["qos"].as<int>();
            }
        }

        /*
        * Hardware Watchdog配置。
        *
        * 配置块不存在时保持WatchdogConfig中的默认值：
        * enabled = false
        *
        * 因此Ubuntu开发环境不会因为缺少/dev/watchdog0而启动失败。
        */
        if (root["watchdog"])
        {
            const YAML::Node watchdog_node = root["watchdog"];

            if (watchdog_node["enabled"])
            {
                data_.watchdog.enabled =watchdog_node["enabled"].as<bool>();
            }
            if (watchdog_node["device"])
            {
                data_.watchdog.device = watchdog_node["device"].as<std::string>();
            }
            if (watchdog_node["timeout_sec"])
            {
                data_.watchdog.timeout_sec = watchdog_node["timeout_sec"].as<int>();
            }
            if (watchdog_node["feed_interval_sec"])
            {
                data_.watchdog.feed_interval_sec =watchdog_node["feed_interval_sec"].as<int>();
            }
            if (watchdog_node["health_timeout_sec"])
            {
                data_.watchdog.health_timeout_sec =watchdog_node["health_timeout_sec"].as<int>();
            }
        }

        /*
        * 只有真正启用Watchdog时才要求参数合法。
        */
        if (data_.watchdog.enabled)
        {
            if (data_.watchdog.timeout_sec <= 0|| data_.watchdog.feed_interval_sec <= 0|| data_.watchdog.health_timeout_sec <= 0)
            {
                std::cerr
                    << "[Config] invalid watchdog timeout configuration"
                    << std::endl;

                return false;
            }

            /*
            * 喂狗周期必须小于硬件超时。
            *
            * 否则正常运行时都可能来不及喂狗。
            */
            if (data_.watchdog.feed_interval_sec>= data_.watchdog.timeout_sec)
            {
                std::cerr<< "[Config] watchdog feed_interval_sec must be "
                "smaller than timeout_sec"
                << std::endl;

                return false;
            }
        }

    }
    catch(const YAML::Exception& error)
    {
        std::cerr << "[Config] failed to load "
                  << path
                  << ": "
                  << error.what()
                  << std::endl;

        return false;
    }
    return true;
}

const GatewayConfig& Config::data() const
{
    return data_; //返回 data_ 的引用
}