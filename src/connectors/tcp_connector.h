#pragma once

#include <atomic>
#include <thread>
#include <unordered_set>

#include "connectors/iconnector.h"

class TcpConnector : public IConnector
{
public:
    explicit TcpConnector(int port);

    bool start() override;
    void stop() override;

    std::string name() const override;

    void setMessageCallback(MessageCallback callback) override;

private:
    void ioLoop();//长期运行的网络事件循环  负责：事件分发

    void handleClientReadable(int client_fd);
    void removeClient(int client_fd);
private:
    int port_;

    int listen_fd_ = -1; //初始值用 -1 表示没有有效socket
    int epoll_fd_ = -1;

    std::atomic<bool> running_{false};
    std::thread io_thread_;

    std::unordered_set<int> client_fds_;

    MessageCallback message_callback_;
};