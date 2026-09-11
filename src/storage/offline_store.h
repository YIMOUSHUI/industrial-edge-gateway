#pragma once

#include <cstdint>
#include <string>
#include <string>
#include <vector>
#include <cstddef>
/*
 * SQLite 自己定义的数据库类型。
 *
 * 这里只提前声明 sqlite3，
 * 这样头文件不必把整个 sqlite3.h 暴露给其他模块。
 *
 * 真正使用 SQLite API 的 offline_store.cpp
 * 再 #include <sqlite3.h>。
 */
struct sqlite3;


/**
 * @brief 一条已经持久化到 SQLite 的离线遥测记录。
 *
 * id：
 *   SQLite 为该记录分配的唯一 ID，
 *   MQTT 补发成功后用于精确删除该记录。
 *
 * device_id：
 *   原始设备标识。
 *
 * payload：
 *   等待重新发送的 MQTT JSON Payload。
 *
 * created_at：
 *   消息产生时间，单位为毫秒。
 */
struct OfflineMessage
{
    std::int64_t id;
    std::string device_id;
    std::string payload;
    std::uint64_t created_at;
};

/**
 * @brief SQLite 离线遥测存储组件。
 *
 * OfflineStore 负责将暂时无法通过 MQTT 上报的遥测数据
 * 持久化到本地 SQLite 数据库。
 *
 * 当前阶段仅实现：
 * 1. 打开/创建数据库；
 * 2. 创建离线消息表；
 * 3. 保存一条离线消息；
 * 4. 关闭数据库。
 *
 * 后续将在此基础上增加：
 * - 查询待补发消息；
 * - MQTT 补发成功后删除记录；
 * - 批量处理与事务等能力。
 */
class OfflineStore
{
public:
    /**
     * @brief 创建 OfflineStore 对象。
     *
     * 构造函数这里只保存数据库路径，
     * 不直接打开数据库。
     *
     * @param db_path SQLite 数据库文件路径。
     */
    explicit OfflineStore(const std::string& db_path);
    
    /**
     * @brief 析构 OfflineStore。
     *
     * 对象销毁时负责关闭已经打开的 SQLite 数据库，
     * 避免数据库句柄泄漏。
     */
    ~OfflineStore();

    /*
     * OfflineStore 独占一个 sqlite3* 数据库句柄。
     * 当前不允许复制，避免两个对象错误地管理同一个数据库句柄。
     */    
    OfflineStore(const OfflineStore&) = delete;
    OfflineStore& operator=(const OfflineStore&) = delete;    
    
    /**
     * @brief 打开或创建 SQLite 数据库。
     *
     * 数据库成功打开后，会自动确保
     * offline_messages 表已经存在。
     *
     * @return true  数据库及表初始化成功。
     * @return false 数据库打开或建表失败。
     */
    bool open();

    /**
     * @brief 保存一条离线遥测消息。
     *
     * @param device_id 产生该消息的设备 ID。
     * @param payload   准备发送给 MQTT 的 JSON Payload。
     * @param created_at 消息产生时间，单位为毫秒。
     *
     * @return true  数据成功写入 SQLite。
     * @return false 数据写入失败。
     */    
    bool save(const std::string& device_id,const std::string& payload,std::uint64_t created_at);

    /**
     * @brief 按进入数据库的顺序读取待补发消息。
     *
     * 记录按照 id 从小到大读取，
     * 即优先处理更早进入离线缓存的消息。
     *
     * @param limit 本次最多读取多少条记录。
     *
     * @return 待补发消息列表。
     *         如果数据库未打开或查询失败，则返回空 vector。
     */
    std::vector<OfflineMessage> loadPending(std::size_t limit);

    /**
     * @brief 删除一条已经成功补发的离线消息。
     *
     * MQTT publish 成功后，
     * 通过数据库记录 ID 删除对应离线记录。
     *
     * @param id SQLite 记录唯一 ID。
     *
     * @return true  删除操作执行成功。
     * @return false 删除操作失败。
     */
    bool remove(std::int64_t id);

    /**
     * @brief 关闭数据库。
     *
     * 如果数据库尚未打开，则不会执行额外操作。
     */
    void close();

private:
    /**
     * @brief 创建离线消息表。
     *
     * open() 成功打开数据库后调用。
     *
     * @return true  表已经存在或创建成功。
     * @return false 创建失败。
     */
    bool createTable();

private:
    /**
     * @brief SQLite 数据库文件路径。
     */
    std::string db_path_;

    /**
     * @brief SQLite 数据库句柄。
     *
     * nullptr：
     *   当前没有打开数据库。
     *
     * 非 nullptr：
     *   指向 SQLite 打开的数据库对象。
     */
    sqlite3* db_{nullptr};
};
