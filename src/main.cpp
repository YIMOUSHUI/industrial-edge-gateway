#include <iostream>
#include <csignal>
#include <unistd.h>

#include "core/gateway_context.h"

volatile std::sig_atomic_t g_running = 1;

void signalHandler(int signal)
{
    g_running = 0;
}

int main()
{
    std::cout << "Industrial Edge Gateway starting..."<< std::endl;

    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    GatewayContext gateway;
    
    if(!gateway.start("config/gateway.yaml"))
    {   
        std::cerr<< "Gateway start failed"<< std::endl;
        return 1;
    }

    while (g_running)
    {
        sleep(1);
    }

    gateway.stop();
    return 0;
}

