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