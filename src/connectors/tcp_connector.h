#pragma once

#include <atomic>
#include <thread>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <string>
#include <mutex>
#include <cstddef>
#include <queue>

#include "connectors/iconnector.h"

struct ClientState
{
    /**
     * @brief 当前TCP连接尚未完成解析的接收字节。
     */
    std::vector<std::uint8_t> recv_buffer;

    /**
     * @brief 当前TCP连接等待发送的字节流。
     *
     * Command Worker不会直接操作socket，
     * 下行Command最终会由TcpConnector I/O线程
     * 转换为待发送字节并追加到这里。
     *
     * 如果一次send()没有全部发送完成，
     * 剩余数据继续保留，等待下一次EPOLLOUT。
     */
    std::vector<std::uint8_t> send_buffer;

    /**
     * @brief send_buffer中已经成功发送的字节位置。
     *
     * 例如：
     *
     * send_buffer总共100字节，
     * 第一次send()只发送30字节，
     *
     * 此时：
     * send_offset = 30
     *
     * 下一次从send_buffer[30]继续发送。
     */
    std::size_t send_offset{0};

    /**
     * @brief 当前TCP连接绑定的逻辑设备ID。
     *
     * 第一条合法Telemetry到来前为空。
     *
     * 当前设计约定：
     * 一个TCP连接对应一个逻辑设备。
     */
    std::string device_id;
};

class TcpConnector : public IConnector
{
public:
    explicit TcpConnector(int port);

    bool start() override;
    void stop() override;

    std::string name() const override;

    void setMessageCallback(MessageCallback callback) override;

    /**
     * @brief 向TCP设备下发DeviceCommand。
     *
     * 当前阶段接口已建立，
     * 下一阶段完成device_id → socket fd映射后
     * 再实现真实TCP发送。
     */
    bool sendCommand(const DeviceCommand& command) override;

private:
    void ioLoop();//长期运行的网络事件循环  负责：事件分发

    void handleClientReadable(int client_fd);//负责recv字节

    /**
     * @brief 处理某个TCP客户端的可写事件。
     *
     * 当epoll返回EPOLLOUT时调用。
     *
     * 本函数会从该客户端的send_offset位置开始，
     * 尽可能继续发送send_buffer中的待发送字节。
     *
     * 可能结果：
     * 1. 全部发送完成：
     *    清空send_buffer，重置send_offset，
     *    并关闭EPOLLOUT监听。
     *
     * 2. 只发送一部分：
     *    更新send_offset，继续尝试发送。
     *
     * 3. 返回EAGAIN/EWOULDBLOCK：
     *    当前暂时不能继续发送，
     *    保留send_buffer和send_offset，
     *    等待下一次EPOLLOUT。
     *
     * 4. 发生真正socket错误：
     *    删除当前客户端连接。
     */
    void handleClientWritable(int client_fd);

    /**
     * @brief 处理wake_fd_产生的内部唤醒事件。
     *
     * 当Command Worker向wake_fd_写入通知后，
     * epoll_wait()会返回wake_fd_的EPOLLIN事件，
     * ioLoop()随后调用本函数。
     *
     * 当前这一阶段只负责：
     *   读取并清除eventfd中的通知计数。
     *
     * 下一阶段再从pending_commands_中
     * 真正取出待发送Command。
     */
    void handleWakeup();

    /**
     * @brief 将跨线程待发送队列中的Command
     *        转移到对应TCP客户端的发送缓冲区。
     *
     * 调用线程：
     *   TcpConnector I/O线程。
     *
     * 数据流：
     *
     * pending_commands_
     *      ↓
     * 根据device_id查询当前client_fd
     *      ↓
     * clients_[client_fd].send_buffer
     *
     * 本函数当前只负责“分配到具体连接”，
     * 暂不执行真正的socket send()。
     */
    void drainPendingCommands();


    void removeClient(int client_fd);
    bool processBuffer(int client_fd);//负责从recv_buffer里拆完整Frame

    /**
     * @brief 将Command JSON封装成当前TCP协议使用的完整Frame。
     *
     * Frame格式：
     *
     * [4字节Payload长度][JSON Payload]
     *
     * 长度字段使用网络字节序，
     * 与设备上行Telemetry的Frame格式保持一致。
     *
     * @param payload Command对应的JSON字符串。
     *
     * @return 可以直接进入TCP发送流程的完整字节帧。
     */
    std::vector<std::uint8_t> buildFrame(const std::string& payload) const;

    /**
     * @brief 将TCP连接绑定到逻辑设备ID。
     *
     * 在收到该连接第一条合法Telemetry后调用。
     *
     * 同一个TCP连接后续只能继续使用第一次绑定的device_id。
     *
     * @return true  当前device_id合法，连接身份有效。
     * @return false device_id为空、连接不存在或发生身份变更。
     */
    bool registerDeviceConnection(int client_fd,const std::string& device_id);

    /**
     * @brief 删除TCP连接对应的设备映射。
     *
     * 在客户端断线或连接异常被移除时调用。
     */
    void unregisterDeviceConnection(int client_fd);

    /**
     * @brief 修改某个TCP客户端当前由epoll监听的事件。
     *
     * 基础事件始终保留：
     *   EPOLLIN    - 客户端有上行数据可读
     *   EPOLLRDHUP - 对端关闭连接
     *
     * want_write为true时额外监听：
     *   EPOLLOUT   - socket当前可以继续写数据
     *
     * @param client_fd 要修改的客户端socket文件描述符。
     * @param want_write true表示需要监听EPOLLOUT，
     *                   false表示取消监听EPOLLOUT。
     *
     * @return 修改成功返回true，失败返回false。
     */
    bool updateClientEvents(int client_fd,bool want_write);

    /**
     * @brief 获取当前通过TCP连接在线的设备ID列表。
     *
     * 数据来源仍然是device_fds_，
     * 本函数不会维护第二份在线设备状态。
     *
     * @return 当前在线设备ID的拷贝。
     */
    std::vector<std::string> getOnlineDevices() override;

    /**
     * @brief 获取当前TCP在线设备数量。
     *
     * @return 当前device_fds_中的设备数量。
     */
    std::size_t onlineDeviceCount();

private:
    int port_;

    int listen_fd_ = -1; //初始值用 -1 表示没有有效socket
    int epoll_fd_ = -1;

    /**
     * @brief 用于跨线程唤醒TcpConnector I/O线程的eventfd。
     *
     * eventfd是Linux提供的一种“事件通知文件描述符”。
     *
     * 当前用途：
     *
     * Command Worker
     *      ↓
     * 将Command加入pending_commands_
     *      ↓
     * write(wake_fd_)
     *      ↓
     * epoll_wait()检测到wake_fd_可读
     *      ↓
     * TcpConnector I/O线程被立即唤醒
     *
     * wake_fd_只负责“通知”：
     *   队列中有新的任务需要处理。
     *
     * 真正的Command数据仍然保存在：
     *   pending_commands_
     *
     * -1表示当前尚未创建有效eventfd。
     */
    int wake_fd_ = -1;

    std::atomic<bool> running_{false};
    std::thread io_thread_;

    std::unordered_map<int, ClientState> clients_;

    /**
     * @brief 逻辑设备ID到TCP连接fd的映射。
     *
     * 例如：
     *
     * sensor-001 -> 8
     * sensor-002 -> 10
     *
     * Command下行时通过device_id找到目标socket。
     */
    std::unordered_map<std::string, int> device_fds_;

    /**
     * @brief 保护device_fds_的并发访问。
     *
     * TcpConnector I/O线程会注册/删除设备映射，
     * Command Worker后续会读取该映射发送下行命令。
     */
    std::mutex device_map_mutex_;

    MessageCallback message_callback_;

    /**
     * @brief Command Worker提交给TCP I/O线程的下行任务。
     *
     * 注意这里保存device_id，而不是长期保存client_fd。
     *
     * 原因：
     * 设备可能在任务排队期间发生重连，
     * fd可能已经失效甚至被系统复用。
     *
     * I/O线程真正处理任务时，
     * 会重新根据device_id查找当前有效fd。
     */
    struct PendingCommandFrame
    {
        std::string device_id;
        std::vector<std::uint8_t> frame;
    };

    /**
     * @brief Command Worker -> TcpConnector I/O线程
     *        的跨线程待发送队列。
     */
    std::queue<PendingCommandFrame> pending_commands_;

    /**
     * @brief 保护pending_commands_。
     *
     * Producer：
     *   Command Worker / sendCommand()
     *
     * Consumer：
     *   TcpConnector I/O线程
     */
    std::mutex pending_commands_mutex_;
};