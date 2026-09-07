#pragma once

#include <atomic>
#include <thread>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "connectors/iconnector.h"

struct ClientState
{
    std::vector<std::uint8_t> recv_buffer;
};

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

    void handleClientReadable(int client_fd);//负责recv字节
    void removeClient(int client_fd);
    bool processBuffer(int client_fd);//负责从recv_buffer里拆完整Frame
private:
    int port_;

    int listen_fd_ = -1; //初始值用 -1 表示没有有效socket
    int epoll_fd_ = -1;

    std::atomic<bool> running_{false};
    std::thread io_thread_;

    std::unordered_map<int, ClientState> clients_;

    MessageCallback message_callback_;
};