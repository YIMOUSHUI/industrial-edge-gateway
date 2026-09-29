#include "common/config.h"

#include <iostream>

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