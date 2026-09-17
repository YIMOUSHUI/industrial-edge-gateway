#include "health/watchdog_manager.h"

#include "common/logger.h"

#include <fcntl.h>
#include <linux/watchdog.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <utility>


WatchdogManager::WatchdogManager(std::string device,int timeout_sec,int feed_interval_sec,HealthCheck health_check)
    : device_(std::move(device)),
      requested_timeout_sec_(timeout_sec),
      feed_interval_sec_(feed_interval_sec),
      health_check_(std::move(health_check))
{
}

WatchdogManager::~WatchdogManager()
{
    stop();
}

bool WatchdogManager::start()
{
    if (running_)
    {
        return true;
    }

    /*
     * 打开Linux Watchdog字符设备。
     *
     * 打开以后，当前进程开始承担周期喂狗责任。
     */
    watchdog_fd_ = open(device_.c_str(),O_WRONLY | O_CLOEXEC);
    if (watchdog_fd_ < 0)
    {
        Logger::error(
            "Failed to open watchdog device: "
            + device_
            + ", error="
            + std::strerror(errno)
        );

        return false;
    }

    /*
     * 查询Watchdog驱动能力。
     *
     * 当前i.MX6ULL实测：
     *
     * identity = imx2+ watchdog
     */
    watchdog_info info{};
    if (ioctl(watchdog_fd_,WDIOC_GETSUPPORT,&info) == 0)
    {
        Logger::info("Watchdog detected: " + std::string(reinterpret_cast<char*>(info.identity) ));
    }

    /*
     * 设置硬件Watchdog超时时间。
     *
     * ioctl返回以后timeout中的值，
     * 是驱动最终真正接受的超时时间。
     */
    int timeout = requested_timeout_sec_;

    if (ioctl(watchdog_fd_,WDIOC_SETTIMEOUT,&timeout) != 0)
    {
        Logger::error(
            "Failed to set watchdog timeout: "
            + std::string(
                std::strerror(errno)
            )
        );

        closeDeviceGracefully();

        return false;
    }

    actual_timeout_sec_ = timeout;

    /*
     * 驱动实际接受的timeout可能和请求值略有不同。
     *
     * 再检查一次：
     * feed interval必须严格小于真实timeout。
     */
    if (feed_interval_sec_>= actual_timeout_sec_)
    {
        Logger::error(
            "Watchdog feed interval is not smaller "
            "than actual timeout"
        );

        closeDeviceGracefully();
        return false;
    }

    /*
     * 启动前立即喂一次。
     *
     * 从这里开始重新计算Watchdog倒计时。
     */
    if (!feed())
    {
        closeDeviceGracefully();
        return false;
    }

    running_ = true;
    feeding_ = true;

    /*
     * Watchdog使用独立线程。
     *
     * 该线程只负责：
     *
     * health check
     * +
     * KEEPALIVE
     */
    watchdog_thread_ = std::thread(&WatchdogManager::watchdogLoop,this);

    Logger::info(
        "Watchdog started: device="
        + device_
        + ", timeout="
        + std::to_string(
            actual_timeout_sec_
        )
        + "s, feed_interval="
        + std::to_string(
            feed_interval_sec_
        )
        + "s"
    );

    return true;
}


void WatchdogManager::watchdogLoop()
{
    while (running_)
    {
        /*
         * 正常情况下等待一个喂狗周期。
         *
         * stop()会notify，
         * 因此正常退出不用傻等5秒。
         */
        std::unique_lock<std::mutex> lock(wait_mutex_);

        const bool stopping =wait_condition_.wait_for(lock,
            std::chrono::seconds(feed_interval_sec_),
                [this]()
                {
                    return !running_.load();
                }
            );

        if (stopping)
        {
            break;
        }

        lock.unlock();


        /*
         * GatewayContext负责判断核心业务是否正常推进。
         *
         * 如果健康检查失败：
         *
         * 绝对不能close Watchdog。
         * 绝对不能继续KEEPALIVE。
         *
         * 必须保持设备打开并停止喂狗，
         * 等待硬件最终复位板卡。
         */
        if (health_check_&& !health_check_())
        {
            feeding_ = false;

            Logger::error(
                "Gateway health check failed; "
                "watchdog feeding has been stopped"
            );

            /*
             * 当前线程继续活着，
             * 但以后不再发送KEEPALIVE。
             *
             * 如果Gateway没有恢复/正常stop，
             * 硬件Watchdog最终会触发SoC reset。
             */
            while (running_)
            {
                std::unique_lock<std::mutex>unhealthy_lock(wait_mutex_);

                wait_condition_.wait_for(unhealthy_lock,
                    std::chrono::seconds(1),
                    [this]()
                    {
                        return !running_.load();
                    }
                );
            }

            break;
        }


        /*
         * Gateway健康：
         * 正常喂狗。
         */
        if (!feed())
        {
            feeding_ = false;

            Logger::error(
                "Watchdog KEEPALIVE failed; "
                "stop feeding and wait for hardware reset"
            );

            while (running_)
            {
                std::unique_lock<std::mutex>
                    failed_lock(wait_mutex_);

                wait_condition_.wait_for(
                    failed_lock,
                    std::chrono::seconds(1),
                    [this]()
                    {
                        return !running_.load();
                    }
                );
            }
            break;
        }
    }
}


bool WatchdogManager::feed()
{
    if (watchdog_fd_ < 0)
    {
        return false;
    }

    if (ioctl(watchdog_fd_,WDIOC_KEEPALIVE,nullptr) != 0)
    {
        Logger::error(
            "Failed to feed watchdog: "
            + std::string(
                std::strerror(errno)
            )
        );

        return false;
    }

    return true;
}


void WatchdogManager::stop()
{
    /*
     * 即使Watchdog线程因为health failure
     * 已经停止喂狗，
     * Gateway收到SIGTERM/SIGINT进行正常关闭时，
     * 仍然允许走正常Magic Close。
     */
    running_ = false;

    wait_condition_.notify_all();

    if (watchdog_thread_.joinable())
    {
        watchdog_thread_.join();
    }

    feeding_ = false;

    closeDeviceGracefully();
}


void WatchdogManager::closeDeviceGracefully()
{
    if (watchdog_fd_ < 0)
    {
        return;
    }

    /*
     * Linux Watchdog Magic Close：
     *
     * 正常退出前写入字符'V'，
     * 告诉驱动：
     *
     * “这是Gateway主动正常关闭，
     *  不是进程异常死亡。”
     *
     * 当前i.MX6ULL imx2-wdt实测支持该能力。
     */
    const char magic_close = 'V';
    const ssize_t result =write(watchdog_fd_,&magic_close,1);

    if (result != 1)
    {
        Logger::warn(
            "Failed to write watchdog magic close"
        );
    }

    close(watchdog_fd_);

    watchdog_fd_ = -1;
}


bool WatchdogManager::isRunning() const
{
    return running_.load();
}


bool WatchdogManager::isFeeding() const
{
    return feeding_.load();
}