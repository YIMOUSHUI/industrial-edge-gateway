#pragma once  

#include <cstdint>
#include <string>
#include <vector>

struct MetricValue
{
    std::string name;//指标
    double value;//值
    std::string unit;//单位
};

struct TelemetryMessage
{
    std::string device_id;//现实设备 ID 不一定是纯数字
    std::string protocol;

    std::uint64_t timestamp_ms;//时间戳
    std::uint64_t sequence; //序号

    std::vector<MetricValue> metrics;
};
