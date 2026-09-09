#include "connectors/tcp_connector.h"
#include "converters/telemetry_converter.h"
#include "common/logger.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>

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

    running_ = true;
    Logger::info("TcpConnector listening on port: " + std::to_string(port_));
    
    io_thread_ = std::thread(&TcpConnector::ioLoop,this);//创建线程
    return true;
}

void TcpConnector::stop()
{   
    running_ = false;
    
    if(io_thread_.joinable())
    {
        io_thread_.join();
    }

    for (const auto& [client_fd, state] : clients_)
    {
        close(client_fd);
    }

    clients_.clear();
    
    if(listen_fd_ >= 0)  //只有socket已经成功创建才能 close。
    {
        close(listen_fd_);
        listen_fd_ = -1;
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

        if(count < 0)
        {
            continue;
        }

        for(int i=0;i<count;++i)
        {
            int fd = events[i].data.fd;
            if(fd == listen_fd_)
            {
                sockaddr_in client_addr{};
                socklen_t client_len =sizeof(client_addr);

                int client_fd = accept(listen_fd_,reinterpret_cast<sockaddr*>(&client_addr),&client_len);
                if(client_fd < 0)
                {
                    if (errno == EAGAIN ||errno == EWOULDBLOCK)//暂时没有新连接
                    {
                        continue;
                    }

                    Logger::error("accept() failed");
                    continue;
                }
                
                int client_flags =fcntl(client_fd, F_GETFL, 0);
                fcntl(client_fd,F_SETFL,client_flags | O_NONBLOCK);

                epoll_event client_event{};
                client_event.events = EPOLLIN | EPOLLRDHUP;
                client_event.data.fd = client_fd;

                if(epoll_ctl(epoll_fd_,EPOLL_CTL_ADD,client_fd,&client_event) < 0)
                {
                    Logger::error("Failed to add client to epoll");
                    close(client_fd);
                    continue;
                }

                clients_.emplace(client_fd,ClientState{});
                Logger::info("TCP client connected, fd: "+ std::to_string(client_fd));

            }
            else
            {
                if(events[i].events & (EPOLLERR | EPOLLHUP))
                {
                    removeClient(fd);
                    continue;
                }

                if(events[i].events & (EPOLLIN | EPOLLRDHUP))//有数据可读 以及对端关闭相关事件
                {
                    handleClientReadable(fd);
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
        if (errno == EAGAIN ||errno == EWOULDBLOCK)
        {
            return;
        }
        Logger::error("TCP recv() failed, fd= " + std::to_string(client_fd));

        removeClient(client_fd);
        return;
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
        if (TelemetryConverter::fromJson(payload,message))
        {
            message.protocol = "tcp";
            if (message_callback_)
            {
                message_callback_(message);
            }
        }
        buffer.erase(buffer.begin(),buffer.begin() + frame_size);
    }
}

void TcpConnector::removeClient(int client_fd)
{
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