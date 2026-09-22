#include "connectors/tcp_connector.h"
#include "converters/telemetry_converter.h"
#include "converters/command_converter.h"
#include "common/logger.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <utility>

TcpConnector::TcpConnector(int port):port_(port)
{

}

bool TcpConnector::start()
{
    Logger::info("TcpConnector starting");
    listen_fd_ = socket(AF_INET,SOCK_STREAM,0);
    if(listen_fd_ < 0)
    {
        Logger::error("TcpConnector socket() failed");
        return false;
    }

    int flags = fcntl(listen_fd_,F_GETFL,0);//取得当前 fd 状态
    fcntl(listen_fd_,F_SETFL,flags | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY); //监听本机所有网卡地址
    addr.sin_port = htons(port_);//h -> short网络字节序

    if(bind(listen_fd_,reinterpret_cast<sockaddr*>(&addr),sizeof(addr)) < 0)
    {
        Logger::error("TcpConnector bind() failed");

        close(listen_fd_);
        listen_fd_ = -1;

        return false;
    }

    if(listen(listen_fd_,128) < 0)
    {   
        Logger::error("TcpConnector listen() failed");

        close(listen_fd_);
        listen_fd_ = -1;

        return false;
    }
    
    epoll_fd_ = epoll_create1(0);
    if(epoll_fd_ < 0)
    {
        Logger::error("epoll_create1() failed");

        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    /*
    * 创建用于跨线程唤醒I/O线程的eventfd。
    *
    * 第一个参数0：
    *   eventfd内部的64位计数器初始值为0。
    *
    * EFD_NONBLOCK：
    *   将eventfd设置为非阻塞模式。
    *   如果当前没有通知可读，read()不会永久阻塞。
    *
    * EFD_CLOEXEC：
    *   如果未来当前进程执行exec系列函数启动其他程序，
    *   这个内部eventfd不会被新程序继承。
    *
    * 当前项目最重要的是EFD_NONBLOCK；
    * EFD_CLOEXEC属于良好的资源管理习惯。
    */
    wake_fd_ = eventfd(0,EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0)
    {
        Logger::error(
            "TcpConnector eventfd() failed"
        );

        close(epoll_fd_);
        epoll_fd_ = -1;

        close(listen_fd_);
        listen_fd_ = -1;

        return false;
    }

    epoll_event event{};
    event.events = EPOLLIN; //可读事件
    event.data.fd = listen_fd_; //只关心监听描述符
    if(epoll_ctl(epoll_fd_,EPOLL_CTL_ADD,listen_fd_,&event) < 0)
    {
        Logger::error("epoll_ctl() failed");
        close(epoll_fd_);
        epoll_fd_ = -1;
        close(listen_fd_);
        listen_fd_ = -1;

        return false;
    }

    /*
    * 将wake_fd_加入当前epoll实例。
    *
    * EPOLLIN：
    *   表示我们关注wake_fd_的“可读事件”。
    *
    * 当其他线程write(wake_fd_)以后，
    * eventfd内部计数器变为非0，
    * wake_fd_就会变成可读状态，
    * epoll_wait()因此立即返回。
    */
    epoll_event wake_event{};

    wake_event.events = EPOLLIN;
    wake_event.data.fd = wake_fd_;
    if (epoll_ctl(epoll_fd_,EPOLL_CTL_ADD,wake_fd_,&wake_event) < 0)
    {
        Logger::error(
            "Failed to add wake_fd to epoll"
        );
        
        close(wake_fd_);
        wake_fd_ = -1;

        close(epoll_fd_);
        epoll_fd_ = -1;

        close(listen_fd_);
        listen_fd_ = -1;

        return false;
    }

    running_ = true;
    Logger::info("TcpConnector listening on port: " + std::to_string(port_));
    
    io_thread_ = std::thread(&TcpConnector::ioLoop,this);//创建线程 执行当前 TcpConnector 对象的 ioLoop()
    return true;
}

void TcpConnector::stop()
{   
    /*
    * 先告诉I/O线程：
    * TcpConnector已经进入停止流程。
    */
    running_ = false;
    
    /*
    * I/O线程可能正在epoll_wait()中睡眠。
    *
    * 主线程通过wake_fd_主动唤醒它，
    * 避免stop()必须等待epoll_wait的1秒超时。
    */
    if (wake_fd_ >= 0)
    {
        const std::uint64_t wake_signal = 1;
        ssize_t wake_result = -1;

        do
        {
            wake_result = write(wake_fd_,&wake_signal,sizeof(wake_signal));
        }
        while (wake_result < 0 && errno == EINTR);
        /*
        * EAGAIN / EWOULDBLOCK：
        *
        * eventfd计数器当前已经不可继续增加。
        * 此时wake_fd_本身已经处于可读状态，
        * I/O线程仍然能够被epoll唤醒，
        * 所以不把它视为stop失败。
        */
        if (wake_result < 0&& errno != EAGAIN&& errno != EWOULDBLOCK)
        {
            Logger::warn(
                "Failed to wake TcpConnector during stop: "
                + std::string(std::strerror(errno))
            );
        }
    }

    if(io_thread_.joinable())
    {
        io_thread_.join();
    }

    for (const auto& [client_fd, state] : clients_)
    {
        close(client_fd);
    }

    clients_.clear();

    {
        std::lock_guard<std::mutex> lock(device_map_mutex_);

        device_fds_.clear();
    }
    
    if(listen_fd_ >= 0)  //只有socket已经成功创建才能 close。
    {
        close(listen_fd_);
        listen_fd_ = -1;
    }

    /*
    * I/O线程已经join完成，
    * 后续不会再read/write wake_fd_，
    * 现在可以安全释放eventfd。
    */
    if (wake_fd_ >= 0)
    {
        close(wake_fd_);
        wake_fd_ = -1;
    }

    if(epoll_fd_ >= 0)
    {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }

    Logger::info("TcpConnector stopped");
}

void TcpConnector::ioLoop()
{
    constexpr int MAX_EVENTS = 16;//一次 epoll_wait() 最多从内核取回16个“已经就绪的事件”

    epoll_event events[MAX_EVENTS];
    while (running_)
    {
        int count = epoll_wait(epoll_fd_,events,MAX_EVENTS,1000);//会进行阻塞睡眠 但时间为1000ms 超时count=0,节省CPU;如果有accept事件则被唤醒
        
        if (!running_)
        {
            break;
        }

        if(count < 0)
        {
            /*
            * EINTR：
            *
            * epoll_wait() 在等待期间被信号中断。
            *
            * 这不表示 epoll 实例或网络连接发生故障，
            * 重新进入下一轮 epoll_wait() 即可。
            */
            if (errno == EINTR)
            {
                continue;
            }
            
             /*
            * 其他错误说明 epoll_wait() 本身已经无法正常工作。
            *
            * 例如：
            *   EBADF  - epoll_fd_ 已经不是有效文件描述符；
            *   EINVAL - epoll fd 或调用参数非法。
            *
            * 如果这里仍然 continue，
            * 下一轮 epoll_wait() 很可能继续立即失败，
            * 导致 I/O 线程进入错误空转。
            *
            * 因此记录明确错误并结束当前 I/O 线程。
            */
            Logger::error(
                "TcpConnector epoll_wait() failed: "
                + std::string(std::strerror(errno))
            );

            running_ = false;
            break;
        }

        for(int i=0;i<count;++i)
        {
            int fd = events[i].data.fd;

            /*
            * wake_fd_不是TCP客户端socket。
            *
            * 它表示其他线程向TcpConnector提交了内部任务。
            */
            if (fd == wake_fd_)
            {
                handleWakeup();
                continue;
            }
            if(fd == listen_fd_)
            {
                /*
                * listen_fd_ 是非阻塞 socket。
                *
                * 一次 EPOLLIN 不代表只有一个客户端等待建立连接。
                * 在连接突发到达时，内核 accept queue 中可能已经积压多个连接。
                *
                * 因此这里持续 accept，
                * 直到 accept() 返回 EAGAIN / EWOULDBLOCK，
                * 表示当前所有已经就绪的连接都已经取完。
                */
                while(running_)
                {   
                    sockaddr_in client_addr{};
                    socklen_t client_len = sizeof(client_addr);

                    const int client_fd =accept(listen_fd_,reinterpret_cast<sockaddr*>(&client_addr),&client_len);
                    
                    if(client_fd < 0)
                    {
                        /*
                        * accept() 被信号中断。
                        *
                        * EINTR 不表示 listen socket 出错，
                        * 直接重新执行 accept() 即可。
                        */
                        if (errno == EINTR)
                        {
                            continue;
                        }

                        /*
                        * 非阻塞 listen socket 当前已经没有
                        * 其他等待接收的连接。
                        *
                        * 这是本轮 accept queue 已经排空的正常结束条件。
                        */
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            break;
                        }
                        /*
                        * 其他 errno 才属于真正的 accept 异常。
                        *
                        * 当前只结束本轮 accept 处理，
                        * 不直接终止整个 TcpConnector，
                        * 后续 epoll 仍然可以继续工作。
                        */
                        Logger::error(
                            "TCP accept() failed: "
                            + std::string(std::strerror(errno))
                        );

                        break;
                    }

                    /*
                    * 新建立的客户端 socket 同样使用非阻塞模式，
                    * 后续读写全部由 epoll 驱动。
                    */
                    const int client_flags = fcntl(client_fd, F_GETFL, 0);
                    if (client_flags < 0 || fcntl(client_fd,F_SETFL,client_flags | O_NONBLOCK) < 0)
                    {
                        Logger::error(
                            "Failed to set TCP client socket non-blocking, fd="
                            + std::to_string(client_fd)
                        );

                        close(client_fd);
                        continue;
                    }

                     /*
                    * 初始只关注：
                    *
                    * EPOLLIN：
                    *   客户端上行数据。
                    *
                    * EPOLLRDHUP：
                    *   对端关闭连接。
                    *
                    * EPOLLOUT 仍然由真正存在下行数据时动态开启。
                    */
                    epoll_event client_event{};
                    client_event.events = EPOLLIN | EPOLLRDHUP;
                    client_event.data.fd = client_fd;

                    if (epoll_ctl(epoll_fd_,EPOLL_CTL_ADD,client_fd,&client_event) < 0)
                    {
                        Logger::error(
                            "Failed to add TCP client to epoll, fd="
                            + std::to_string(client_fd)
                        );

                        close(client_fd);
                        continue;
                    }

                     /*
                    * socket 已经成功进入 epoll，
                    * 此时才正式纳入 TcpConnector 的连接状态表。
                    */
                    clients_.emplace(client_fd,ClientState{});

                    Logger::info(
                        "TCP client connected, fd: "
                        + std::to_string(client_fd)
                    );
                }

            }
            else
            {
                /*
                * socket发生真正错误或连接已经失效。
                */
                if(events[i].events & (EPOLLERR | EPOLLHUP))
                {
                    removeClient(fd);
                    continue;
                }

                /*
                * 处理设备上行数据。
                *
                * 注意：
                * handleClientReadable()内部可能因为：
                *
                * recv() == 0
                *
                * 或真正的recv错误
                *
                * 而调用removeClient(fd)。
                */
                if(events[i].events & (EPOLLIN | EPOLLRDHUP))//有数据可读 以及对端关闭相关事件
                {
                    handleClientReadable(fd);
                }

                /*
                * 上面的handleClientReadable()可能已经
                * 删除了这个客户端。
                *
                * 所以在继续处理同一个fd的EPOLLOUT之前，
                * 先确认它仍然存在。
                */
                if (clients_.find(fd) == clients_.end())
                {
                    continue;
                }

                /*
                * EPOLLOUT：
                *
                * Linux告诉我们：
                * 当前socket现在可以尝试继续写数据。
                *
                * 于是处理该连接自己的send_buffer。
                */
                if (events[i].events & EPOLLOUT)
                {
                    handleClientWritable(fd);
                }

            }
        }
    }
}

void TcpConnector::handleClientReadable(int client_fd)
{
    std::uint8_t buffer[4096];
    
    while(true)
    {
        ssize_t n = recv(client_fd,buffer,sizeof(buffer),0);
        if(n > 0)
        {
            Logger::debug("TCP received " + std::to_string(n) + " bytes from fd= " + std::to_string(client_fd));
            auto it = clients_.find(client_fd);
            if (it == clients_.end()) //fd 已经不在客户端管理表里，就不处理
            {
                return;
            }
            auto& recv_buffer =it->second.recv_buffer;
            recv_buffer.insert(recv_buffer.end(),buffer,buffer + n);

            if (!processBuffer(client_fd))
            {
                return;
            }

            continue;
        }
        if(n == 0)//对端关闭
        {
            Logger::info("TCP client disconnected, fd= "+ std::to_string(client_fd));
            removeClient(client_fd);

            return;
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno == EAGAIN ||errno == EWOULDBLOCK)//非阻塞 socket 暂时没数据了
        {
            return;
        }
        Logger::error("TCP recv() failed, fd= " + std::to_string(client_fd));

        removeClient(client_fd);
        return;
    }
}

void TcpConnector::handleClientWritable(int client_fd)
{
    /*
     * 根据fd找到这个TCP连接自己的ClientState。
     *
     * ClientState里面包含：
     *
     * send_buffer
     *   当前还需要发送的数据。
     *
     * send_offset
     *   已经成功发送到哪个位置。
     */
    auto client_it =clients_.find(client_fd);
    if (client_it == clients_.end())
    {
        return;
    }
    ClientState& state = client_it->second;

    /*
     * 只要还有没有发送完成的数据，
     * 就继续尝试send()。
     */
    while (state.send_offset < state.send_buffer.size())
    {
        /*
         * 计算当前还有多少字节没有发送。
         *
         * 例如：
         *
         * send_buffer.size() = 100
         * send_offset        = 30
         *
         * remaining = 70
         */
        const std::size_t remaining = state.send_buffer.size() - state.send_offset;

        /*
         * 从send_offset指向的位置开始发送。
         *
         * state.send_buffer.data()
         *     -> 第0个字节地址
         *
         * + state.send_offset
         *     -> 第一个尚未发送字节的地址
         *
         * MSG_NOSIGNAL：
         * 如果对端已经关闭连接，
         * 不让send()产生SIGPIPE把整个gatewayd进程终止。
         *
         * 错误通过返回值和errno正常处理。
         */
        const ssize_t n = send(client_fd,state.send_buffer.data() + state.send_offset,remaining,MSG_NOSIGNAL);

        /*
         * n > 0：
         * 本次成功向Linux内核交了n个字节。
         */
        if (n > 0)
        {
            state.send_offset +=static_cast<std::size_t>(n);

            /*
             * 继续while。
             *
             * 如果socket现在仍然可以继续接收数据，
             * 我们就继续发送。
             *
             * 如果后面写满了，
             * send()自然会返回EAGAIN。
             */
            continue;
        }

        /*
         * EINTR：
         *
         * 当前send()被信号中断。
         *
         * 不代表socket坏了，
         * 重新尝试本次发送即可。
         */
        if (n < 0 && errno == EINTR)
        {
            continue;
        }

        /*
         * EAGAIN / EWOULDBLOCK：
         *
         * 非阻塞socket当前暂时写不进去。
         *
         * 这里不能：
         *
         * clear send_buffer
         * reset send_offset
         * close EPOLLOUT
         *
         * 因为数据还没有发送完成。
         *
         * 直接return，
         * 等Linux下一次通过EPOLLOUT通知：
         *
         * “现在又可以继续写了。”
         */
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            return;
        }

        /*
         * 能运行到这里，
         * 说明不是暂时性的EAGAIN，
         * 也不是可以重试的EINTR。
         *
         * 认为当前TCP连接已经发生真正错误。
         */
        Logger::warn(
            "TCP send failed, fd="
            + std::to_string(client_fd)
        );

        removeClient(client_fd);

        return;
    }

    /*
     * 能走出while，说明：
     *
     * send_offset
     * ==
     * send_buffer.size()
     *
     * 也就是当前积压的所有字节
     * 都已经成功交给Linux内核。
     */


    /*
     * 清空已经发送完成的数据。
     */
    state.send_buffer.clear();

    /*
     * send_buffer已经重新为空，
     * 所以下一次新的发送任务从第0字节开始。
     */
    state.send_offset = 0;

    /*
     * 当前已经没有数据等待发送。
     *
     * 如果继续监听EPOLLOUT，
     * socket通常仍然处于“可写”状态，
     * epoll会不断无意义唤醒。
     *
     * 所以现在关闭EPOLLOUT。
     */
    if (!updateClientEvents(client_fd,false))
    {
        Logger::warn(
            "Failed to disable EPOLLOUT, fd="
            + std::to_string(client_fd)
        );

        /*
         * epoll事件配置已经无法正常修改，
         * 当前连接状态不可靠，
         * 直接清理该客户端。
         */
        removeClient(client_fd);
    }
}


/**
 * @brief 处理eventfd唤醒通知。
 *
 * wake_fd_只承担“线程通知”的职责，
 * 不保存真正的DeviceCommand。
 *
 * 当前阶段：
 * 只读取eventfd中的通知计数，
 * 确认TcpConnector I/O线程能够被Command Worker正常唤醒。
 *
 * 下一阶段：
 * 在这里进一步处理pending_commands_中的待发送任务。
 */
void TcpConnector::handleWakeup()
{
    /*
     * eventfd内部维护的是64位无符号计数器，
     * 所以读取时同样必须准备uint64_t变量。
     */
    std::uint64_t wake_count = 0;
    ssize_t n = -1;
    do
    {
        n = read(wake_fd_,&wake_count,sizeof(wake_count));
    }
    while (n < 0 && errno == EINTR);

    if (n < 0)
    {
        /*
         * 非阻塞eventfd在当前没有可读通知时，
         * 会返回EAGAIN/EWOULDBLOCK。
         *
         * 这种情况不属于真正错误。
         */
        if (errno == EAGAIN|| errno == EWOULDBLOCK)
        {
            return;
        }

        Logger::error("Failed to read TcpConnector wake_fd: "+ std::string(std::strerror(errno)));
        return;
    }

    /*
    * 普通eventfd的读取单位固定为uint64_t，也就是8字节。
    *
    * 正常情况下read()成功就应该返回sizeof(uint64_t)。
    */
    if (n != static_cast<ssize_t>(sizeof(wake_count)))
    {
        Logger::error(
            "Unexpected TcpConnector wake_fd read size: "
            + std::to_string(n)
        );
        return;
    }

     /*
     * 正常情况下n应该等于8：
     * sizeof(uint64_t)。
     *
     * wake_count表示自上次读取以来
     * eventfd累计收到了多少通知值。
     *
     * 例如连续write三次1：
     *
     * wake_count == 3
     */
    Logger::debug(
        "TcpConnector I/O thread woken, wake_count="
        + std::to_string(wake_count)
    );

    /*
    * eventfd通知已经消费完成。
    *
    * wake_fd_只负责告诉I/O线程：
    * “有新的内部任务。”
    *
    * 真正的Command数据保存在pending_commands_中，
    * 因此现在开始处理这些待发送任务。
    */
    drainPendingCommands();
}

/**
 * @brief 将待发送Command转移到目标TCP客户端的发送缓冲区。
 *
 * 本函数只由TcpConnector I/O线程调用。
 *
 * 处理流程：
 *
 * pending_commands_
 *      ↓
 * 取出PendingCommandFrame
 *      ↓
 * 根据device_id查询当前client_fd
 *      ↓
 * 找到clients_[client_fd]
 *      ↓
 * 将frame追加到该连接的send_buffer
 *
 * 当前阶段只完成“进入发送缓冲区”，
 * 尚未真正调用send()。
 */
void TcpConnector::drainPendingCommands()
{
    /*
     * local_commands：
     * I/O线程自己的局部队列。
     *
     * 后面会把共享的pending_commands_
     * 快速转移到这个局部队列中。
     *
     * 这样真正处理命令时，
     * 不需要一直占着pending_commands_mutex_。
     */
    std::queue<PendingCommandFrame> local_commands;
    {
        /*
         * pending_commands_同时会被：
         *
         * Command Worker：
         *   push()
         *
         * TcpConnector I/O线程：
         *   取走
         *
         * 所以访问共享队列时必须加锁。
         */
        std::lock_guard<std::mutex> lock(
            pending_commands_mutex_
        );

        /*
         * swap()：
         * 交换两个queue内部保存的数据。
         *
         * 调用前：
         *
         * pending_commands_ = [A, B, C]
         * local_commands    = []
         *
         * 调用后：
         *
         * pending_commands_ = []
         * local_commands    = [A, B, C]
         *
         * 这样锁很快就可以释放，
         * 后面慢慢处理A/B/C时，
         * Command Worker仍然可以继续push新的任务。
         */
        pending_commands_.swap(
            local_commands
        );
    }

    /*
     * 从这里开始：
     *
     * pending_commands_mutex_已经释放。
     *
     * local_commands是当前I/O线程自己的局部变量，
     * 其他线程看不到它，
     * 所以后续处理不需要这个mutex。
     */
    while (!local_commands.empty())
    {
        /*
         * 取得队头任务。
         *
         * std::move()表示：
         * 当前任务马上要从local_commands中移除，
         * 因此直接接管其中frame的资源，
         * 避免再复制整个vector。
         */
        PendingCommandFrame command = std::move(local_commands.front());
        local_commands.pop();
        /*
         * client_fd初始设为-1：
         *
         * -1表示目前还没有找到
         * 该device_id对应的有效TCP连接。
         */
        int client_fd = -1;
        {
            /*
             * device_fds_是跨线程共享表：
             *
             * device_id -> client_fd
             *
             * I/O线程可能注册/注销设备，
             * Command Worker也可能读取它，
             * 因此查询时使用device_map_mutex_保护。
             */
            std::lock_guard<std::mutex> lock(
                device_map_mutex_
            );

            const auto device_it =device_fds_.find(command.device_id);
            if (device_it !=device_fds_.end())
            {
                client_fd =device_it->second;
            }
        }


        /*
         * 没找到设备连接。
         *
         * 可能发生的情况：
         *
         * sendCommand()把任务放进队列时
         * 设备还是在线；
         *
         * 但I/O线程真正处理任务之前
         * 设备已经断开。
         *
         * 这种情况下不能使用旧fd，
         * 当前任务只能发送失败。
         */
        if (client_fd < 0)
        {
            Logger::warn(
                "TCP command dropped because device is offline, device_id="
                + command.device_id
            );

            continue;
        }


        /*
         * 根据fd找到真正的ClientState。
         *
         * 当前函数运行在TcpConnector I/O线程，
         * clients_也主要由该线程维护，
         * 所以这里不需要再给clients_增加一个mutex。
         */
        auto client_it = clients_.find(client_fd);

        if (client_it == clients_.end())
        {
            Logger::warn(
                "TCP command target client no longer exists, device_id="
                + command.device_id
                + ", fd="
                + std::to_string(client_fd)
            );

            continue;
        }


        /*
         * 取得目标客户端自己的连接状态。
         *
         * state不是一份复制，
         * 而是clients_中ClientState对象的引用。
         *
         * 所以后面对state.send_buffer的修改，
         * 就是在真正修改clients_[client_fd].send_buffer。
         */
        ClientState& state = client_it->second;


        /*
         * 将当前Command Frame追加到该连接的send_buffer。
         *
         * 注意使用insert，而不是直接：
         *
         * state.send_buffer = command.frame;
         *
         * 因为send_buffer中可能已经有上一条
         * 尚未发送完成的Command。
         *
         * 例如原来：
         *
         * [Frame A]
         *
         * 现在又来Frame B：
         *
         * 最终：
         *
         * [Frame A][Frame B]
         *
         * 每个Frame自己都有4字节长度头，
         * 所以设备端以后仍然可以逐帧拆包。
         */
        state.send_buffer.insert(state.send_buffer.end(),command.frame.begin(),command.frame.end());

        Logger::debug(
            "TCP command moved to client send buffer, device_id="
            + command.device_id
            + ", fd="
            + std::to_string(client_fd)
            + ", pending_bytes="
            + std::to_string(
                state.send_buffer.size()
                - state.send_offset
            )
        );

        if (!updateClientEvents(client_fd,true))
        {
            Logger::warn(
                "Failed to enable EPOLLOUT for device_id="
                + command.device_id
            );
        }

    }
}

bool TcpConnector::processBuffer(int client_fd)
{
    auto it = clients_.find(client_fd);

    if(it == clients_.end())
    {
        return false;
    }

    auto& buffer = it->second.recv_buffer;
    constexpr std::size_t HEADER_SIZE = sizeof(std::uint32_t);

    while(true) //还能拆出完整Frame，就一直拆
    {
        if (buffer.size() < HEADER_SIZE)
        {
            return true;
        }
        std::uint32_t network_length = 0;
        //buffer.data() 指向容器内部第一个元素的指针
        std::memcpy(&network_length,buffer.data(),HEADER_SIZE);

        std::uint32_t payload_length =ntohl(network_length);

        constexpr std::uint32_t MAX_PAYLOAD_SIZE =1024 * 1024;
        if (payload_length > MAX_PAYLOAD_SIZE)
        {
            Logger::error("TCP frame too large, fd= " + std::to_string(client_fd));   
            removeClient(client_fd);

            return false;
        }
        
        //判断完整帧
        std::size_t frame_size =HEADER_SIZE + payload_length;
        if (buffer.size() < frame_size)
        {
            return true;
        }   

        std::vector<std::uint8_t> payload(buffer.begin() + HEADER_SIZE,buffer.begin() + frame_size);
        Logger::info("TCP complete frame received, fd= " + std::to_string(client_fd) + ", payload= " + std::to_string(payload.size()) + " bytes");

        TelemetryMessage message;
        if (TelemetryConverter::fromJson(payload,message)) //把：JSON字节转换成：TelemetryMessage
        {
            message.protocol = "tcp";

            /*
            * 一个TCP连接在首次合法Telemetry之后
            * 就固定绑定一个device_id。
            *
            * 如果后续Telemetry试图改变设备身份，
            * 不能继续把该Telemetry交给Gateway业务层。
            */
            if (!registerDeviceConnection(client_fd,message.device_id))
            {
                Logger::warn(
                    "TCP telemetry rejected because device identity is invalid, fd="
                    + std::to_string(client_fd)
                    + ", device_id="
                    + message.device_id
                );
                /*
                * 当前连接已经违反：
                *
                * 一个TCP连接 == 一个逻辑设备
                *
                * 的协议约束。
                *
                * 直接关闭连接，避免后续继续产生身份不确定的数据。
                */
                removeClient(client_fd);
                /*
                * removeClient()已经删除clients_[client_fd]。
                *
                * processBuffer()中的recv_buffer引用此时已经失效，
                * 所以必须立即返回，
                * 不能继续处理当前buffer。
                */
                return false;
            }

            if (message_callback_)
            {
                message_callback_(message);
            }
        }
        buffer.erase(buffer.begin(),buffer.begin() + frame_size);
    }
}

/**
 * @brief 将JSON Payload封装为TCP长度头协议Frame。
 *
 * 当前协议：
 *
 * 4字节网络字节序Payload长度
 * +
 * JSON Payload
 */
std::vector<std::uint8_t>TcpConnector::buildFrame(const std::string& payload) const
{
    /*
     * payload.size()类型是size_t，
     * TCP长度头固定使用uint32_t，
     * 因此这里显式转换为32位无符号整数。
     *
     * 当前项目单帧最大1MiB，
     * 所以不会超过uint32_t表示范围。
     */
    const std::uint32_t payload_length = static_cast<std::uint32_t>(payload.size());

    /*
     * htonl：
     * host to network long
     *
     * 中文：
     * 主机字节序 → 网络字节序。
     *
     * TCP传输中的长度头统一采用网络字节序。
     */
    const std::uint32_t network_length = htonl(payload_length);

    /*
     * 最终Frame总长度：
     *
     * 4字节长度头+JSON正文长度。
     */
    std::vector<std::uint8_t> frame(sizeof(network_length) + payload.size());

    /*
     * 前4字节写入长度头。
     */
    std::memcpy(frame.data(),&network_length,sizeof(network_length));

    /*
     * 4字节长度头之后写入JSON正文。
     */
    std::memcpy(frame.data() + sizeof(network_length),payload.data(),payload.size());

    return frame;
}

void TcpConnector::removeClient(int client_fd)
{
    /*
     * clients_中还保存着device_id时先清理设备映射。
     */
    unregisterDeviceConnection(client_fd);

    epoll_ctl(epoll_fd_,EPOLL_CTL_DEL,client_fd,nullptr);

    close(client_fd);
    clients_.erase(client_fd);
}

std::string TcpConnector::name() const  //这个成员函数承诺不修改当前 TcpConnector 对象的普通成员状态。
{
    return "tcp";
}

void TcpConnector::setMessageCallback(MessageCallback callback)
{
    message_callback_ = callback;
}

/**
 * @brief 接收CommandRouter路由来的TCP设备命令。
 *
 * 本函数运行在线程：
 *
 * Command Worker
 *
 * 而不是TcpConnector I/O线程。
 *
 * 因此当前函数不直接操作clients_，
 * 也不直接调用send()。
 *
 * 当前职责只有：
 *
 * 1. 判断目标设备现在是否有TCP连接；
 * 2. DeviceCommand → JSON；
 * 3. JSON → TCP Frame；
 * 4. 将Frame放入pending_commands_；
 *
 * 后续再由TcpConnector I/O线程真正处理socket发送。
 *
 * @param command 已完成MQTT解析和CommandRouter路由的设备命令。
 *
 * @return true  命令已经成功进入TcpConnector待发送队列。
 * @return false Connector未运行或目标设备当前不在线。
 */
bool TcpConnector::sendCommand(const DeviceCommand& command)
{
     /*
     * Gateway停止过程中不再接受新的TCP下行任务。
     */
    if (!running_)
    {
        Logger::warn(
            "TCP command rejected because connector is stopped, device_id="
            + command.device_id
        );
        return false;
    }

    /*
     * 先确认目标设备当前存在TCP在线连接。
     *
     * device_fds_由：
     *
     * device_id -> client_fd
     *
     * 组成。
     *
     * 例如：
     *
     * sensor-001 -> 8
     *
     * 该表同时会被TcpConnector I/O线程修改，
     * 所以Command Worker读取时必须加device_map_mutex_。
     */
    {
        std::lock_guard<std::mutex> lock(device_map_mutex_);

        const auto device_it = device_fds_.find(command.device_id);
        if (device_it == device_fds_.end())
        {
            Logger::warn(
                "TCP command rejected because device is offline, device_id="
                + command.device_id
            );

            return false;
        }
    }

    /*
     * 第一步：
     *
     * DeviceCommand
     *      ↓
     * JSON字符串
     *
     * 例如：
     *
     * {
     *   "request_id":"cmd-001",
     *   "device_id":"sensor-001",
     *   "action":"set_interval",
     *   "parameters":{"interval_ms":1000}
     * }
     */
    const std::string payload = CommandConverter::toJson(command);

    /*
     * 第二步：
     *
     * JSON字符串
     *      ↓
     * 当前TCP协议Frame
     *
     * 最终格式：
     *
     * [4字节长度头][JSON Payload]
     */
    std::vector<std::uint8_t> frame = buildFrame(payload);

    /*
    * 第三步：
    *
    * “通知I/O线程”与“发布Command任务”
    * 必须作为一个一致的生产者操作。
    *
    * pending_commands_mutex_在整个过程中保持持有：
    *
    *   1. 先确保I/O线程能够被唤醒；
    *   2. 再把Command放入pending_commands_；
    *   3. 最后释放mutex。
    *
    * 即使I/O线程在write(eventfd)之后立即醒来，
    * 它进入drainPendingCommands()时也必须取得同一个mutex，
    * 因此只有等当前Command完成入队后才能真正读取队列。
    */
    {
        std::lock_guard<std::mutex> lock(pending_commands_mutex_);

        const std::uint64_t wake_signal = 1;
        ssize_t wake_result = -1;
        do
        {
            wake_result =write(wake_fd_,&wake_signal,sizeof(wake_signal));
        }
        while (wake_result < 0 && errno == EINTR);

        if (wake_result < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
        {
            Logger::error(
                "Failed to wake TcpConnector I/O thread, request_id="
                + command.request_id
                + ", error="
                + std::string(std::strerror(errno))
            );
            /*
            * 注意：
            *
            * 此时Command还没有放入pending_commands_。
            *
            * 所以返回false以后，
            * 不存在“上层认为失败，但Command以后又被发送”
            * 的状态不一致问题。
            */
            return false;
        }
        /*
        * 唤醒条件已经建立。
        *
        * 现在才正式发布Command任务。
        */
        pending_commands_.push(PendingCommandFrame{command.device_id,std::move(frame)});
        //std::move把 frame 内部那块动态数组资源交给 PendingCommandFrame 使用，而不是再完整复制一份。
    }

    /*
    * pending_commands_中的任务已经准备完成。
    *
    * 现在通过eventfd通知TcpConnector I/O线程：
    *
    * “待发送队列中有新的Command，请从epoll_wait()醒来。”
    *
    * eventfd要求每次write()写入一个uint64_t，
    * 即8字节无符号整数。
    */
    
    /*
    * pending_commands_已经准备完成。
    *
    * 通过eventfd唤醒I/O线程。
    *
    * EINTR只表示本次系统调用被信号打断，
    * 所以重新执行write()。
    */
    

    Logger::info("TCP command queued: request_id=" + command.request_id + ", device_id=" + command.device_id);

    /*
     * 注意当前true的含义：
     *
     * 不是：
     * “设备已经收到并执行成功”
     *
     * 而是：
     * “TcpConnector已经接受该命令，
     *  并把它放进待发送任务队列。”
     */
    return true;
}

/**
 * @brief 建立device_id到当前TCP连接fd的映射。
 *
 * 当前策略：
 * 一个TCP连接在首次上报有效Telemetry后绑定一个device_id。
 *
 * 如果同一device_id通过新的TCP连接重新上线，
 * 则更新device_id -> fd映射，使后续Command指向新连接。
 */
bool TcpConnector::registerDeviceConnection(int client_fd,const std::string& device_id)
{
    /*
     * TCP设备必须具有明确的逻辑设备ID。
     *
     * 空device_id不能建立有效的：
     *
     * device_id -> client_fd
     *
     * 路由关系。
     */
    if (device_id.empty())
    {
        Logger::warn("TCP telemetry rejected because device_id is empty, fd="+ std::to_string(client_fd));
        return false;
    }

    /*
     * client_fd必须仍然属于当前TcpConnector管理的连接。
    */
    auto client_it = clients_.find(client_fd);
    if (client_it == clients_.end())
    {
        Logger::warn("TCP device registration failed because client no longer exists, fd=" + std::to_string(client_fd));
        return false;
    }

    ClientState& state = client_it->second;

    /*
     * 当前连接已经绑定过device_id。
     *
     * 一个TCP连接在整个生命周期内只允许代表一个逻辑设备。
     */
    if (!state.device_id.empty())
    {
        /*
         * 后续Telemetry仍然来自原设备：
         *
         * 这是正常情况。
         *
         * processBuffer()每收到一条Telemetry都会执行到这里，
         * 所以必须允许相同device_id重复出现。
         */
        if (state.device_id == device_id)
        {
            return true;
        }

         /*
         * 同一个TCP连接突然声明成另外一个设备。
         *
         * 这可能是：
         *
         * - 设备协议实现错误；
         * - 客户端状态异常；
         * - 身份冒用；
         * - 上层数据污染。
         *
         * 当前设计不允许一个连接动态切换身份。
         */
        Logger::warn(
            "TCP client attempted to change device identity, fd="
            + std::to_string(client_fd)
            + ", old_device="
            + state.device_id
            + ", new_device="
            + device_id
        );
        
        return false;
    }

    /*
     * 第一次收到合法Telemetry：
     *
     * 正式把当前TCP连接绑定到device_id。
     */
    state.device_id = device_id;

    {
        /*
         * device_fds_还会被Command Worker访问，
         * 因此修改映射时使用mutex保护。
         */
        std::lock_guard<std::mutex> lock(
            device_map_mutex_
        );

        const auto existing = device_fds_.find(device_id);

        /*
         * 如果同一个device_id已经通过另外一个fd存在，
         * 当前暂时沿用V1策略：
         *
         * 新连接更新device_id -> fd映射。
         *
         * “新旧两个连接同时存活”的生命周期问题
         * 单独留到下一步处理，
         * 本轮不混进来。
         */
        if (existing != device_fds_.end()&& existing->second != client_fd)
        {
            Logger::warn(
                "TCP device reconnected, updating connection: device_id="
                + device_id
                + ", old_fd="
                + std::to_string(existing->second)
                + ", new_fd="
                + std::to_string(client_fd)
            );
        }

        device_fds_[device_id] =client_fd;
    }

    Logger::info(
        "TCP device registered: device_id="
        + device_id
        + ", fd="
        + std::to_string(client_fd)
    );
    return true;
}

/**
 * @brief 删除某个TCP客户端对应的设备身份映射。
 */
void TcpConnector::unregisterDeviceConnection(int client_fd)
{
    const auto client_it = clients_.find(client_fd);
    
    if (client_it == clients_.end())
    {
        return;
    }

    const std::string device_id =client_it->second.device_id;
    if (device_id.empty())
    {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(
            device_map_mutex_
        );

        const auto device_it = device_fds_.find(device_id);

        /*
         * 只有当前映射仍然指向这个fd时才删除。
         *
         * 这一判断对“设备重新连接”非常重要。
         */
        if (device_it != device_fds_.end() && device_it->second == client_fd)
        {
            device_fds_.erase(device_it);
        }
    }

    Logger::info(
        "TCP device unregistered: device_id="
        + device_id
        + ", fd="
        + std::to_string(client_fd)
    );
}

bool TcpConnector::updateClientEvents(int client_fd,bool want_write)
{
    /*
     * 先设置这个客户端一直都要关注的基础事件。
     *
     * EPOLLIN：
     *   设备有数据发给Gateway时，需要继续能够接收。
     *
     * EPOLLRDHUP：
     *   用来感知对端关闭连接。
     */
    std::uint32_t events = EPOLLIN | EPOLLRDHUP;
    /*
     * 只有当前连接确实存在待发送数据时，
     * 才额外关注EPOLLOUT。
     */
    if (want_write)
    {
        events |= EPOLLOUT;
    }
    /*
     * 构造新的epoll事件配置。
     */
    epoll_event event{};
    event.events = events;
    event.data.fd = client_fd;

    /*
     * client_fd在accept()以后已经通过
     * EPOLL_CTL_ADD加入过epoll。
     *
     * 所以这里不是再次ADD，
     * 而是使用EPOLL_CTL_MOD：
     *
     * MOD = modify = 修改
     *
     * 修改这个fd当前关注哪些事件。
     */
    if (epoll_ctl(epoll_fd_,EPOLL_CTL_MOD,client_fd,&event) < 0)
    {
        Logger::error(
            "Failed to update epoll events for client fd="
            + std::to_string(client_fd)
        );

        return false;
    }
    return true;
}

std::vector<std::string>TcpConnector::getOnlineDevices()
{
    /*
     * 返回值使用一个局部vector。
     *
     * 我们只把当前device_id复制出来，
     * 不把内部device_fds_直接暴露给外部。
     */
    std::vector<std::string> devices;

     /*
     * device_fds_会被TcpConnector I/O线程修改，
     * 以后又会被管理线程查询。
     *
     * 所以读取时仍然使用已有的device_map_mutex_保护。
     */
    {
        std::lock_guard<std::mutex> lock(
            device_map_mutex_
        );

        /*
         * 预留足够空间。
         *
         * reserve()不是功能必须，
         * 只是因为我们已经知道最多要放多少个device_id，
         * 可以减少vector扩容次数。
         */
        devices.reserve(device_fds_.size());

        /*
         * device_fds_结构类似：
         *
         * sensor-001 -> 8
         * sensor-002 -> 10
         *
         * gatewayctl只需要device_id，
         * 所以这里只取map中的key。
         */
        for (const auto& [device_id, client_fd]: device_fds_)
        {
            /*
             * 当前client_fd在这个查询里不需要使用。
             *
             * 这里只把device_id复制到结果中。
             */
            (void)client_fd;
            devices.push_back(device_id);
        }
    }

    /*
     * 到这里mutex已经释放。
     *
     * 返回的是一份“查询瞬间”的设备列表快照。
     *
     * 后续即使真实device_fds_发生变化，
     * 也不会影响这份已经返回的vector。
     */
    return devices;
}

std::size_t TcpConnector::onlineDeviceCount()
{
    /*
     * 即使这里只读取size()，
     * device_fds_也可能正在被I/O线程修改，
     * 所以同样需要加锁。
     */
    std::lock_guard<std::mutex> lock(
        device_map_mutex_
    );

    return device_fds_.size();
}