#include <iostream>
#include <csignal>
#include <unistd.h>

#include "core/gateway_context.h"

volatile std::sig_atomic_t g_running = 1;

void signalHandler(int signal)
{
    g_running = 0;
}

int main(int argc, char* argv[])
{
    std::cout << "Industrial Edge Gateway starting..." << std::endl;

    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    /*
     * 默认仍然使用PC开发环境配置。
     *
     * 如果命令行提供第一个参数，
     * 则使用用户指定的配置文件。2.
     *
     * 示例：
     *   ./gatewayd config/gateway.rk3576.yaml
     */
    const std::string config_path =
        (argc > 1)
            ? argv[1]
            : "config/gateway.yaml";

    GatewayContext gateway;

    if (!gateway.start(config_path))
    {
        std::cerr << "Gateway start failed" << std::endl;
        return 1;
    }

    while (g_running)
    {
        sleep(1);
    }

    gateway.stop();
    return 0;
}