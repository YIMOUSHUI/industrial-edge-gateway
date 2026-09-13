#include <iostream>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

bool sendAll(int sock_fd,const std::string& data)
{
    /*
     * offset表示：
     *
     * data前面已经有多少字节
     * 成功交给内核发送。
     */
    std::size_t offset = 0;

    while (offset < data.size())
    {
        /*
         * 从还没有发送的位置继续发送。
         */
        const ssize_t n =send(sock_fd,data.data() + offset,data.size() - offset,MSG_NOSIGNAL);
        /*
         * 成功发送了一部分。
         */
        if (n > 0)
        {
            offset +=static_cast<std::size_t>(n);
            continue;
        }
        /*
         * send()被信号临时打断，
         * socket本身没有坏，
         * 重新发送即可。
         */
        if (n < 0 && errno == EINTR)
        {
            continue;
        }

        /*
         * 其他情况认为发送失败。
         */
        return false;
    }
    return true;
}


int main(int argc, char* argv[])
{
    /*
     * gatewayctl第一版要求：
     *
     * gatewayctl status
     * gatewayctl devices
     *
     * 所以正常情况下必须有两个参数：
     *
     * argv[0] = "gatewayctl"
     * argv[1] = "status" 或 "devices"
     */
    if (argc != 2)
    {
        std::cerr<< "Usage: gatewayctl <status|devices>\n";
        return 1;
    }

    /*
     * argv[1]是用户真正输入的管理命令。
     *
     * 例如：
     *
     * gatewayctl status
     *
     * command = "status"
     */
    const std::string command = argv[1];

    /*
     * 第一版只支持两个命令：
     *
     * status
     * devices
     *
     * 如果用户输入其他内容，
     * 直接在客户端这里提示，不需要再发给gatewayd。
     */
    if (command != "status"&& command != "devices")
    {
        std::cerr<< "Unknown command: "<< command<< '\n';
        return 1;
    }

    /*
     * 前面我们约定：
     *
     * gatewayctl -> gatewayd
     *
     * 每一条请求都必须以 '\n' 结尾。
     *
     * 所以：
     *
     * command = "status"
     *
     * 最终：
     *
     * request = "status\n"
     */
    const std::string request =command + '\n';

    /*
    * GatewayControlServer当前监听的
    * Unix Domain Socket路径。
    *
    * gatewayctl和gatewayd必须使用同一个路径，
    * 否则客户端找不到服务端。
    */
    const std::string socket_path ="/tmp/gateway.sock";

    /*
    * 创建Unix Domain Socket客户端。
    *
    * AF_UNIX：
    *   本机进程间通信。
    *
    * SOCK_STREAM：
    *   使用可靠、有序的字节流连接。
    *
    * 成功：
    *   返回一个非负文件描述符。
    *
    * 失败：
    *   返回-1。
    */
    const int sock_fd =socket(AF_UNIX,SOCK_STREAM,0);
    if (sock_fd < 0)
    {
        std::cerr
            << "Failed to create Unix domain socket: "
            << std::strerror(errno)
            << '\n';
        return 1;
    }
    /*
    * 准备Unix Domain Socket地址。
    *
    * 这里的address描述：
    *
    * “我要连接哪个UDS服务端？”
    */
    sockaddr_un address{};
    address.sun_family = AF_UNIX;

    /*
    * sun_path是固定长度字符数组。
    *
    * 如果socket_path太长，
    * 就不能安全放进去。
    */
    if (socket_path.size()>= sizeof(address.sun_path))
    {
        std::cerr<< "Socket path is too long\n";

        close(sock_fd);
        return 1;
    }
    std::strncpy(address.sun_path,socket_path.c_str(),sizeof(address.sun_path) - 1);

    /*
    * 连接gatewayd中的GatewayControlServer。
    *
    * 服务端必须已经：
    *
    * socket()
    * bind()
    * listen()
    *
    * 并且监听同一个：
    *
    * /tmp/gateway.sock
    */
    if (connect(sock_fd,reinterpret_cast<sockaddr*>(&address),sizeof(address)) < 0)
    {
        std::cerr
            << "Failed to connect to gatewayd: "
            << std::strerror(errno)
            << '\n';

        close(sock_fd);

        return 1;
    }

    /*
    * 把完整请求发送给gatewayd。
    *
    * 例如：
    *
    * request = "status\n"
    */
    if (!sendAll(sock_fd,request))
    {
        std::cerr<< "Failed to send request to gatewayd\n";
        close(sock_fd);

        return 1;
    }

    /*
    * 接收gatewayd返回的响应。
    *
    * 服务端发送完整响应以后会关闭client_fd。
    *
    * 所以客户端一直recv，
    * 直到recv()返回0，
    * 就表示整条响应已经结束。
    */
    char buffer[1024];
    while (true)
    {
        const ssize_t n =recv(sock_fd,buffer,sizeof(buffer),0);

        /*
        * n > 0：
        * 本次成功收到n个字节。
        */
        if (n > 0)
        {
            /*
            * 注意：
            * buffer不一定以'\0'结尾，
            * 所以不能直接：
            *
            * std::cout << buffer;
            *
            * 必须明确告诉cout：
            * 这次只输出前n个字节。
            */
            std::cout.write(buffer,n);
            continue;
        }

        /*
        * n == 0：
        *
        * 服务端已经关闭当前连接。
        *
        * 按照我们的协议，
        * 这意味着整个响应已经接收完成。
        */
        if (n == 0)
        {
            break;
        }

        /*
        * recv()被信号临时中断，
        * socket本身没有坏，
        * 继续接收即可。
        */
        if (errno == EINTR)
        {
            continue;
        }

        /*
        * 其他recv错误。
        */
        std::cerr<< "Failed to receive response from gatewayd: "<< std::strerror(errno)<< '\n';

        close(sock_fd);

        return 1;
    }
    close(sock_fd);
    return 0;
}