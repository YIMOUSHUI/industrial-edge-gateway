#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

/**
 * @brief Linux硬件Watchdog管理器。
 *
 * 职责：
 *
 * 1. 打开/dev/watchdog设备；
 * 2. 设置硬件超时时间；
 * 3. 周期执行Gateway健康检查；
 * 4. Gateway健康时执行KEEPALIVE；
 * 5. Gateway异常时停止喂狗；
 * 6. 正常退出时执行Magic Close。
 *
 * WatchdogManager不负责定义：
 *
 * “什么叫Gateway健康”
 *
 * 健康判断由GatewayContext通过HealthCheck回调提供。
 */
class WatchdogManager
{
public:
    using HealthCheck =std::function<bool()>;
    WatchdogManager(std::string device,int timeout_sec,int feed_interval_sec,HealthCheck health_check);

    ~WatchdogManager();

    /**
     * @brief 打开并启动硬件Watchdog。
     *
     * @return true 启动成功。
     * @return false Watchdog设备无法正常启用。
     */
    bool start();

    /**
     * @brief 正常关闭Watchdog。
     *
     * Gateway主动退出时调用。
     */
    void stop();

    /**
     * @brief Watchdog设备当前是否已启动。
     */
    bool isRunning() const;

    /**
     * @brief 当前是否仍在周期喂狗。
     *
     * running=true但feeding=false：
     * 已经检测到Gateway异常，
     * 正在等待硬件Watchdog超时复位。
     */
    bool isFeeding() const;

private:
    void watchdogLoop();

    bool feed();

    /**
     * @brief 正常释放Watchdog设备。
     *
     * 写入Magic Close字符'V'后close，
     * 避免Gateway正常退出却被Watchdog复位。
     */
    void closeDeviceGracefully();

    std::string device_;

    int requested_timeout_sec_{0};

    int actual_timeout_sec_{0};

    int feed_interval_sec_{0};

    HealthCheck health_check_;

    int watchdog_fd_{-1};

    std::atomic<bool> running_{false};

    std::atomic<bool> feeding_{false};

    std::thread watchdog_thread_;

    /*
     * 用condition_variable代替单纯sleep()。
     *
     * 这样Gateway stop()时，
     * Watchdog线程能够立即被唤醒退出，
     * 不必等待完整feed_interval。
     */
    std::mutex wait_mutex_;

    std::condition_variable wait_condition_;
};