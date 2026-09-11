#include "storage/offline_store.h"

#include <sqlite3.h>

#include <iostream>

/**
 * @brief 构造 OfflineStore。
 *
 * 当前构造阶段只记录数据库文件路径，
 * 真正的数据库打开操作由 open() 完成。
 */
OfflineStore::OfflineStore(const std::string& db_path): db_path_(db_path)
{
}

/**
 * @brief 析构 OfflineStore。
 *
 * 确保 OfflineStore 生命周期结束时，
 * 已打开的 SQLite 数据库被正常关闭。
 */
OfflineStore::~OfflineStore()
{
    close();
}

/**
 * @brief 打开数据库并初始化离线消息表。
 */
bool OfflineStore::open()
{
    /*
     * 防止同一个 OfflineStore 重复打开数据库。
     */
    if (db_ != nullptr)
    {
        return true;
    }

    /*
     * sqlite3_open()：
     *
     * 第一个参数：
     *   数据库文件路径。
     *
     * 第二个参数：
     *   sqlite3**。
     *   SQLite 成功打开数据库后，
     *   会把数据库句柄写入 db_。
     */
    const int rc = sqlite3_open(db_path_.c_str(),&db_);

    if (rc != SQLITE_OK)
    {
        std::cerr
            << "Failed to open SQLite database: "
            << sqlite3_errmsg(db_)
            << '\n';

        close();
        return false;
    }

    /*
     * 数据库已经打开，
     * 接下来确保 offline_messages 表存在。
     */
    if (!createTable())
    {
        close();
        return false;
    }

    return true;
}

/**
 * @brief 创建离线消息表。
 */
bool OfflineStore::createTable()
{
    /*
     * IF NOT EXISTS 使该操作可以重复执行。
     *
     * 第一次运行：
     *   创建 offline_messages。
     *
     * 后续程序再次启动：
     *   表已经存在，不重复创建，也不会因此报错。
     */
    const char* sql =
        "CREATE TABLE IF NOT EXISTS offline_messages ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "device_id TEXT NOT NULL,"
        "payload TEXT NOT NULL,"
        "created_at INTEGER NOT NULL"
        ");";

    const int rc = sqlite3_exec(db_,sql,nullptr,nullptr,nullptr);
    if (rc != SQLITE_OK)
    {
        std::cerr
            << "Failed to create offline_messages table: "
            << sqlite3_errmsg(db_)
            << '\n';

        return false;
    }
    return true;
}

/**
 * @brief 将一条遥测消息持久化到 SQLite。
 */
bool OfflineStore::save(const std::string& device_id,const std::string& payload,std::uint64_t created_at)
{
    /*
     * OfflineStore 必须先 open()。
     *
     * 如果 db_ == nullptr，
     * 当前没有可用的 SQLite 数据库句柄。
     */
    if (db_ == nullptr)
    {
        std::cerr
            << "Cannot save offline message: database is not open\n";

        return false;
    }

    /*
     * 使用 ? 作为参数占位符。
     *
     * 1 -> device_id
     * 2 -> payload
     * 3 -> created_at
     */
    const char* sql =
        "INSERT INTO offline_messages "
        "(device_id, payload, created_at) "
        "VALUES (?, ?, ?);";

    sqlite3_stmt* stmt = nullptr;

    /*
     * Prepare：
     * 将 SQL 模板交给 SQLite 解析，
     * 得到可执行的 Statement。
     */
    int rc = sqlite3_prepare_v2(db_,sql,-1,&stmt,nullptr);
    if (rc != SQLITE_OK)
    {
        std::cerr
            << "Failed to prepare offline INSERT: "
            << sqlite3_errmsg(db_)
            << '\n';

        return false;
    }

    /*
     * Bind 第1个参数：device_id。
     *
     * SQLITE_TRANSIENT：
     * SQLite 自己复制字符串内容，
     * 不依赖 std::string 原始内存的生命周期。
     */
    rc = sqlite3_bind_text(stmt,1,device_id.c_str(),-1,SQLITE_TRANSIENT);
    if (rc != SQLITE_OK)
    {
        std::cerr
            << "Failed to bind device_id: "
            << sqlite3_errmsg(db_)
            << '\n';

        sqlite3_finalize(stmt);
        return false;
    }

    /*
     * Bind 第2个参数：JSON Payload。
     */
    rc = sqlite3_bind_text( stmt,2,payload.c_str(),-1,SQLITE_TRANSIENT);
    if (rc != SQLITE_OK)
    {
        std::cerr
            << "Failed to bind payload: "
            << sqlite3_errmsg(db_)
            << '\n';

        sqlite3_finalize(stmt);
        return false;
    }

    /*
     * Bind 第3个参数：毫秒时间戳。
     *
     * SQLite INTEGER 使用有符号 64 位整数。
     * 当前正常 Unix 毫秒时间戳远小于 int64_t 上限，
     * 因此转换为 sqlite3_int64 保存。
     */
    rc = sqlite3_bind_int64(stmt,3,static_cast<sqlite3_int64>(created_at));
    if (rc != SQLITE_OK)
    {
        std::cerr
            << "Failed to bind created_at: "
            << sqlite3_errmsg(db_)
            << '\n';

        sqlite3_finalize(stmt);
        return false;
    }

    /*
     * Step：
     * 真正执行 INSERT。
     *
     * INSERT 成功执行完成时，
     * sqlite3_step() 应返回 SQLITE_DONE。
     */
    rc = sqlite3_step(stmt);

    if (rc != SQLITE_DONE)
    {
        std::cerr
            << "Failed to insert offline message: "
            << sqlite3_errmsg(db_)
            << '\n';

        sqlite3_finalize(stmt);
        return false;
    }

    /*
     * Statement 已完成使命，
     * 释放 SQLite 为该 Statement 分配的资源。
     */
    sqlite3_finalize(stmt);
    return true;
}

/**
 * @brief 读取一批待补发的离线遥测记录。
 *
 * 数据按照 SQLite 自增 ID 从小到大排序，
 * 这样可以优先补发更早进入离线缓存的消息。
 *
 * @param limit 本次最多读取的记录数量。
 *
 * @return 查询得到的 OfflineMessage 列表。
 */
std::vector<OfflineMessage> OfflineStore::loadPending(std::size_t limit)
{
     /*
     * 保存本次查询结果。
     *
     * 每读取一行 SQLite 数据，
     * 就构造一个 OfflineMessage 并 push_back 到这里。
     */
    std::vector<OfflineMessage> messages;

    /*
     * 数据库尚未打开时无法执行查询。
     */
    if (db_ == nullptr)
    {
        std::cerr<< "Cannot load offline messages: database is not open\n";
        return messages;
    }
    /*
     * LIMIT ?：
     * 本次最多读取多少条记录。
     *
     * ORDER BY id ASC：
     * 按进入数据库的先后顺序读取。
     */
    const char* sql =
        "SELECT id, device_id, payload, created_at "
        "FROM offline_messages "
        "ORDER BY id ASC "
        "LIMIT ?;";
    
    sqlite3_stmt* stmt = nullptr;

     /*
     * Prepare：
     * 将 SELECT SQL 解析成 SQLite Statement。
     */
    int rc = sqlite3_prepare_v2(db_,sql,-1,&stmt,nullptr);
    if (rc != SQLITE_OK)
    {
        std::cerr<< "Failed to prepare offline SELECT: "<< sqlite3_errmsg(db_)<< '\n';
        return messages;
    }

    /*
     * LIMIT 是 SQL 中第1个 ?，
     * 因此参数编号为1。
     *
     * size_t 是无符号整数类型，
     * SQLite 使用 sqlite3_int64，
     * 这里显式转换。
     */    
    rc = sqlite3_bind_int64(stmt,1,static_cast<sqlite3_int64>(limit));
    if (rc != SQLITE_OK)
    {
        std::cerr
            << "Failed to bind offline SELECT limit: "
            << sqlite3_errmsg(db_)
            << '\n';

        sqlite3_finalize(stmt);
        return messages;
    }

    /*
     * sqlite3_step() 每成功读取一行，
     * 都会返回 SQLITE_ROW。
     *
     * 每一轮 while：
     *   读取当前数据库行；
     *   转换为 OfflineMessage；
     *   放入 vector。
     */
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        OfflineMessage message;
        /*
         * SELECT 的列编号从0开始：
         *
         * 0 -> id
         * 1 -> device_id
         * 2 -> payload
         * 3 -> created_at
         */
        message.id =sqlite3_column_int64(stmt, 0);
        const unsigned char* device_id = sqlite3_column_text(stmt, 1);
        const unsigned char* payload = sqlite3_column_text(stmt, 2);

        /*
         * 数据库表已经定义为 NOT NULL，
         * 正常情况下 device_id 和 payload 不会为空。
         *
         * 这里仍做空指针保护，避免异常数据导致字符串构造错误。
         */
        if (device_id != nullptr)
        {
            message.device_id = reinterpret_cast<const char*>(device_id);
        }

        if (payload != nullptr)
        {
            message.payload = reinterpret_cast<const char*>(payload);
        }

        message.created_at = static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 3));
        /*
         * 当前数据库行转换完成，
         * 保存到本次查询结果中。
         */
        messages.push_back(std::move(message));
        //move允许 vector 尽量接管 message 内部已有的资源，而不是把字符串重新完整复制一遍。
    }

    /*
     * SELECT 正常读取完所有结果以后，
     * sqlite3_step() 应返回 SQLITE_DONE。
     */
    if (rc != SQLITE_DONE)
    {
        std::cerr
            << "Failed while loading offline messages: "
            << sqlite3_errmsg(db_)
            << '\n';

        /*
         * 查询发生异常时，
         * 当前已经读取出的部分结果不再返回给业务层，
         * 避免调用方误以为本次查询完整成功。
         */
        messages.clear();
    }
    /*
     * Statement 使用结束，
     * 释放 SQLite 内部资源。
     */
    sqlite3_finalize(stmt);
    return messages;
}

/**
 * @brief 删除一条已经成功补发的离线消息。
 *
 * 只有 MQTT publish 成功以后，
 * 才应该调用本函数删除对应记录。
 *
 * @param id SQLite 记录唯一 ID。
 *
 * @return true  DELETE 执行成功。
 * @return false DELETE 执行失败。
 */
bool OfflineStore::remove(std::int64_t id)
{
    /*
     * 数据库必须已经打开。
     */
    if (db_ == nullptr)
    {
        std::cerr<< "Cannot remove offline message: database is not open\n";

        return false;
    }

    /*
     * 只根据主键 id 删除一条记录。
     *
     * 使用参数绑定而不是字符串拼接，
     * 保持 SQL 结构和业务数据分离。
     */
    const char* sql =
        "DELETE FROM offline_messages "
        "WHERE id = ?;";

    sqlite3_stmt* stmt = nullptr;
    
    /*
     * Prepare DELETE。
     */
    int rc = sqlite3_prepare_v2(db_,sql,-1,&stmt,nullptr);
    if (rc != SQLITE_OK)
    {
        std::cerr<< "Failed to prepare offline DELETE: "<< sqlite3_errmsg(db_)<< '\n';
        return false;
    }

    /*
     * SQL 中只有一个 ?，
     * 因此把记录 ID 绑定到第1个参数。
     */
    rc = sqlite3_bind_int64(stmt,1,static_cast<sqlite3_int64>(id));
    if (rc != SQLITE_OK)
    {
        std::cerr<< "Failed to bind offline DELETE id: "<< sqlite3_errmsg(db_)<< '\n';

        sqlite3_finalize(stmt);
        return false;
    }

    /*
     * 真正执行 DELETE。
     *
     * DELETE 和 INSERT 一样，
     * 正常执行完成时返回 SQLITE_DONE。
     */
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE)
    {
        std::cerr<< "Failed to delete offline message: "<< sqlite3_errmsg(db_)<< '\n';
        sqlite3_finalize(stmt);
        return false;
    }

    /*
     * DELETE Statement 已使用完成。
     */
    sqlite3_finalize(stmt);
    return true;
}



/**
 * @brief 关闭 SQLite 数据库。
 */
void OfflineStore::close()
{
    /*
     * nullptr 说明数据库没有打开，
     * 无需执行 sqlite3_close()。
     */
    if (db_ == nullptr)
    {
        return;
    }
    sqlite3_close(db_);
    /*
     * 关闭完成后重新置空。
     *
     * 这样后续代码可以通过 db_ == nullptr
     * 明确判断数据库已经关闭。
     */
    db_ = nullptr;
}

