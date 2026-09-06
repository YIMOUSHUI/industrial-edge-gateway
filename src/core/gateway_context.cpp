#include "core/gateway_context.h"
#include "connectors/mock_connector.h"
#include "connectors/tcp_connector.h"
#include "common/logger.h"

#include <utility>
#include <memory>

bool GatewayContext::start(const std::string& config_path)
{
    if (!config_.load(config_path))
    {
        return false;
    }
    const GatewayConfig& config =config_.data();

    Logger::init(config.log_level);
    Logger::info("Gateway starting");
    Logger::info("Gateway id=" + config.gateway_id);

    for (const ConnectorConfig& connector_config :config.connectors)          
    {
        if (!connector_config.enabled)
        {
            continue;
        }

        if (connector_config.type == "mock")
        {
            auto connector = std::make_unique<MockConnector>();
            connector->setMessageCallback(
                [this](const TelemetryMessage& message)
                {
                    handleMessage(message);
                }
             );
            connectors_.push_back(std::move(connector));
        }
        else if(connector_config.type == "tcp")
        {
            auto connector = std::make_unique<TcpConnector>(connector_config.port);
            connector->setMessageCallback(
                [this](const TelemetryMessage& message)
                {
                    handleMessage(message);
                }
            );
             connectors_.push_back(std::move(connector));
        }
    }

    for (auto& connector : connectors_)
    {
        if (!connector->start())
        {
            Logger::error("Failed to start connector: "+ connector->name());
            return false;
        }
    }
    Logger::info("Gateway started");
    return true;
}

void GatewayContext::stop()
{
    for (auto& connector : connectors_)
    {
        connector->stop();
    }
    Logger::info("Gateway stopped");
}

void GatewayContext::handleMessage(const TelemetryMessage& message)
{
    Logger::info("Received message from " +message.device_id);
    
    for(const MetricValue& metric : message.metrics)
    {
        Logger::debug("metric: " +metric.name +"=" +std::to_string(metric.value) +" " +metric.unit);
    }

}