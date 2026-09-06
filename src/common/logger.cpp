#include "common/logger.h"

#include <spdlog/spdlog.h>

void Logger::init(const std::string& level)
{
    if(level == "debug")
    {
        spdlog::set_level(spdlog::level::debug);
    }
    else if (level == "warn")
    {
        spdlog::set_level(spdlog::level::warn);
    }
    else if (level == "error")
    {
        spdlog::set_level(spdlog::level::err);
    }
    else
    {
        spdlog::set_level(spdlog::level::info);
    }

    spdlog::set_pattern(
        "[%Y-%m-%d %H:%M:%S.%e] [%l] %v"
    );
}

void Logger::debug(const std::string& message)
{
    spdlog::debug(message);
}

void Logger::info(const std::string& message)
{
    spdlog::info(message);
}

void Logger::warn(const std::string& message)
{
    spdlog::warn(message);
}

void Logger::error(const std::string& message)
{
    spdlog::error(message);
}