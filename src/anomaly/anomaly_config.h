#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

/**
 * @brief 单个工业设备指标的异常检测配置。
 *
 * 一个 Profile 对应：
 * 一个设备 + 一个指标 + 一套检测参数。
 *
 * 例如：
 * modbus-plc-01 的 temperature 指标。
 *
 * 本结构体只存储配置，不执行异常检测。
 */
struct AnomalyProfileConfig
{
    // 是否启用当前检测配置
    bool enabled = true;

    // 配置唯一标识，用于日志和异常结果
    std::string id;

    // 要检测的设备
    std::string device_id;

    // 要检测的指标
    std::string metric;

    // 历史窗口最多保存多少个采样值
    std::size_t window_size = 8;

    // 异常分数达到该值时才可能判定异常
    double score_threshold = 4.0;

    // 标准差下限，防止稳定数据造成除零或极端评分
    double scale_floor = 0.5;

    // 当前值与历史均值至少相差多少，才可能判定异常
    double min_deviation = 3.0;

    // 两次采样间允许的最大时间间隔，单位毫秒
    std::uint64_t max_gap_ms = 10000;
};


/**
 * @brief 网关全局异常检测配置。
 *
 * 一个网关可以同时配置多个检测 Profile。
 */
struct AnomalyConfig
{
    // 总开关，默认关闭，避免影响旧版网关行为
    bool enabled = false;

    // 所有设备指标的异常检测配置
    std::vector<AnomalyProfileConfig> profiles;
};