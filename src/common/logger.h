#pragma once

#include <string>

class Logger
{
public:
    static void init(const std::string& level);

    static void debug(const std::string& message);
    static void info(const std::string& message);
    static void warn(const std::string& message);
    static void error(const std::string& message);
};
