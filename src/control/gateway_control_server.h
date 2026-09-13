#pragma once

#include <atomic>
#include <string>
#include <thread>

class GatewayContext;//前向声明 后面存在一个叫 GatewayContext 的类

/**
 * @brief Gateway本地管理服务。
 *
 * GatewayControlServer运行在gatewayd进程内部，
 * 通过Unix Domain Socket为本机gatewayctl提供管理接口。
 *
 * 第一版计划支持：
 *
 *   gatewayctl status
 *   gatewayctl devices
 *
 * 本类只负责：
 *
 *   1. 创建并监听UDS；
 *   2. 接收gatewayctl请求；
 *   3. 向GatewayContext查询实际状态；
 *   4. 将查询结果返回gatewayctl。
 *
 * 它本身不维护MQTT状态或在线设备状态。
 */
class GatewayControlServer
{
public:
    /**
     * @brief 创建本地管理服务。
     *
     * @param socket_path
     *        Unix Domain Socket使用的文件路径，
     *        例如：
     *
     *        /tmp/gateway.sock
     *
     * @param gateway
     *        当前GatewayContext对象。
     *
     *        Control Server收到管理请求以后，
     *        会通过它查询：
     *
     *        getOnlineDevices()
     *        isMqttConnected()
     */
    GatewayControlServer(std::string socket_path,GatewayContext& gateway);

    /**
     * @brief 析构时确保服务已经停止。
     */
    ~GatewayControlServer();

    /**
     * @brief 启动UDS管理服务。
     *
     * 后续实现中会完成：
     *
     * socket()
     * bind()
     * listen()
     * 启动control_thread_
     *
     * @return true  启动成功。
     * @return false 创建或绑定UDS失败。
     */
    bool start();

    /**
     * @brief 停止UDS管理服务。
     *
     * 后续负责：
     *
     * 通知control_thread_退出，
     * 等待线程结束，
     * 关闭listen_fd_，
     * 删除UDS路径。
     */
    void stop();

private:
    /**
     * @brief Control Server线程入口。
     *
     * 这条线程后续会主要阻塞在accept()，
     * 等待gatewayctl连接。
     */
    void controlLoop();

    /**
     * @brief 从一个gatewayctl连接中接收一整行请求。
     *
     * 第一版管理协议规定：
     *
     *   每条请求以 '\n' 结尾。
     *
     * 例如：
     *
     *   status\n
     *   devices\n
     *
     * @param client_fd 当前gatewayctl连接的socket fd。
     * @param line      输出参数，成功后保存去掉'\n'后的命令文本。
     *
     * @return true  成功收到完整一行。
     * @return false 对端关闭连接或recv发生错误。
     */
    bool receiveLine(int client_fd,std::string& line);

    /**
     * @brief 根据gatewayctl请求生成响应文本。
     *
     * 当前支持：
     *
     *   status
     *   devices
     *
     * 本函数只负责：
     *   request -> response
     *
     * 不负责recv()、send()和close()。
     *
     * @param request 已经完整接收到的管理命令。
     *
     * @return 返回给gatewayctl的文本响应。
     */
    std::string handleRequest(const std::string& request);

    /**
     * @brief 将一段响应文本完整发送给gatewayctl。
     *
     * SOCK_STREAM的一次send()不保证发送全部数据，
     * 所以这里循环发送，直到全部发送完成或发生错误。
     *
     * @param client_fd 当前gatewayctl连接fd。
     * @param data      要发送的完整响应文本。
     *
     * @return true  全部发送成功。
     * @return false 发送过程中发生错误。
     */
    bool sendAll(int client_fd,const std::string& data);


private:
    /**
     * @brief Unix Domain Socket的文件系统路径。
     *
     * 例如：
     *
     * /tmp/gateway.sock
     */
    std::string socket_path_;

    /**
     * @brief UDS监听socket的文件描述符。
     *
     * -1表示当前还没有有效监听socket。
     */
    int listen_fd_{-1};

    /**
     * @brief Control Server是否应该继续运行。
     *
     * 主线程：
     *   start()/stop()修改它。
     *
     * control_thread_：
     *   controlLoop()读取它。
     *
     * 因为两个线程都会访问，
     * 所以使用atomic<bool>。
     */
    std::atomic<bool> running_{false};


    /**
     * @brief 专门负责UDS管理服务的线程。
     *
     * 后续会运行controlLoop()。
     */
    std::thread control_thread_;

    /**
     * @brief 当前GatewayContext的引用。
     *
     * GatewayControlServer不自己保存业务状态，
     * 收到status/devices请求后，
     * 通过这个引用向GatewayContext查询。
     *
     * 注意：
     * 这里不是拥有GatewayContext，
     * 只是引用已经存在的GatewayContext对象。
     */
    GatewayContext& gateway_;
};