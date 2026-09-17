#pragma once

#include <cstddef>

/**
 * @brief gatewayd当前Linux进程的资源快照。
 *
 * 这些数据不是长期缓存状态，
 * 每次gatewayctl status查询时都会重新从/proc读取。
 */
struct ProcessMetrics
{
    /**
     * @brief 当前进程实际驻留在物理内存中的大小，单位KB。
     *
     * 来源：
     * /proc/self/status -> VmRSS
     */
    std::size_t rss_kb{0};

    /**
     * @brief 当前gatewayd打开的文件描述符数量。
     *
     * 包括：
     * socket、SQLite文件、CAN socket、UDS等。
     */
    std::size_t open_fd_count{0};

    /**
     * @brief 当前gatewayd线程总数。
     *
     * 来源：
     * /proc/self/status -> Threads
     */
    std::size_t thread_count{0};

    /**
     * @brief 是否成功取得完整资源信息。
     */
    bool valid{false};
};

/**
 * @brief Linux进程资源读取工具。
 *
 * 本类本身不保存状态，
 * read()每次都读取gatewayd当前的真实/proc信息。
 */
class ProcessMetricsReader
{
public:
    /**
     * @brief 获取当前gatewayd进程资源快照。
     */
    static ProcessMetrics read();
};