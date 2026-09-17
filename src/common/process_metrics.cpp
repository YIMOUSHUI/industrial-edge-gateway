#include "common/process_metrics.h"

#include <dirent.h>

#include <fstream>
#include <sstream>
#include <string>

ProcessMetrics ProcessMetricsReader::read()
{
    ProcessMetrics metrics;

    /*
     * 第一部分：
     * 从/proc/self/status读取：
     *
     * VmRSS
     * Threads
     */
    std::ifstream status_file("/proc/self/status");
    bool rss_found = false;
    bool threads_found = false;

    if (status_file.is_open())
    {
        std::string line;
        while (std::getline(status_file, line))
        {
            std::istringstream stream(line);

            std::string key;
            stream >> key;

            if (key == "VmRSS:")
            {
                /*
                 * 典型格式：
                 *
                 * VmRSS:    10880 kB
                 *
                 * 这里只取数值10880。
                 */
                stream >> metrics.rss_kb;
                rss_found = true;
            }
            else if (key == "Threads:")
            {
                /*
                 * 典型格式：
                 *
                 * Threads:  8
                 */
                stream >> metrics.thread_count;
                threads_found = true;
            }
        }
    }


    /*
     * 第二部分：
     * 统计/proc/self/fd中的文件描述符数量。
     */
    DIR* fd_directory =opendir("/proc/self/fd");
    bool fd_count_ok = false;
    if (fd_directory != nullptr)
    {
        std::size_t count = 0;

        /*
         * opendir()本身也会临时打开一个fd。
         *
         * 如果直接统计目录内容，
         * 会把“用于查看fd目录的这个fd”自己也算进去。
         *
         * 所以这里记住它，
         * 遍历时主动跳过。
         */
        const int directory_fd = dirfd(fd_directory);
        const std::string directory_fd_name = std::to_string(directory_fd);
        dirent* entry = nullptr;
        while ((entry = readdir(fd_directory))!= nullptr)
        {
            const std::string name =entry->d_name;

            /*
             * 跳过：
             *
             * .
             * ..
             */
            if (name == "."|| name == "..")
            {
                continue;
            }

            /*
             * 跳过opendir()为本次统计临时创建的fd。
             *
             * 否则gatewayctl显示的数量会比：
             *
             * ls /proc/$PID/fd | wc -l
             *
             * 多1。
             */
            if (name == directory_fd_name)
            {
                continue;
            }

            ++count;
        }

        closedir(fd_directory);

        metrics.open_fd_count = count;
        fd_count_ok = true;
    }


    /*
     * 三类数据都成功获得，
     * 才认为本次ProcessMetrics完整有效。
     */
    metrics.valid =rss_found&& threads_found&& fd_count_ok;
    return metrics;
}