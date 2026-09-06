#include "common/config.h"

#include <iostream>

#include <yaml-cpp/yaml.h>

bool Config::load(const std::string& path)
{
    try
    {   
        YAML::Node root = YAML::LoadFile(path);

        data_.gateway_id = root["gateway"]["id"].as<std::string>();
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

            data_.connectors.push_back(connector);
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
    return data_;
}