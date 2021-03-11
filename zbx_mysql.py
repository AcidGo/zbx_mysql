# -*- coding: utf8 -*-
"""
Author: AcidGo
Usage: mysql monitor for zabbix, it can depend on multi item mode to monitor mysql. Only for Py3.
"""

import logging
import json
import pymysql

from collections import Iterable
from logging.handlers import RotatingFileHandler


def init_logger(level, logfile=None):
    """日志功能初始化。
    如果使用日志文件记录，那么则默认使用 RotatinFileHandler 的大小轮询方式，
    默认每个最大 10 MB，最多保留 5 个。
    Args:
        level: 设定的最低日志级别。
        logfile: 设置日志文件路径，如果不设置则表示将日志输出于标准输出。
    """
    import os
    import sys
    if not logfile:
        logging.basicConfig(
            level = getattr(logging, level.upper()),
            format = "%(asctime)s [%(levelname)s] %(message)s",
            datefmt = "%Y-%m-%d %H:%M:%S"
        )
    else:
        logger = logging.getLogger()
        logger.setLevel(getattr(logging, level.upper()))
        if logfile.lower() == "local":
            logfile = os.path.join(sys.path[0], os.path.basename(os.path.splitext(__file__)[0]) + ".log")
        handler = RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logging.info("Logger init finished.")


class MySQLInst(object):
    """
    """
    def __init__(self, host, port, user, passwd, database=None):
        self.host = host
        self.port = int(port)
        self.user = user
        self.passwd = passwd
        self.database = database

        self.conn = None

    def __del__(self):
        self.close()

    def connect(self, autocommit=True):
        self.autocommit = autocommit
        conn = pymysql.connect(
            host = self.host,
            port = self.port,
            user = self.user,
            password = self.passwd,
            database = self.database,
            autocommit = autocommit,
            cursorclass=pymysql.cursors.DictCursor,
        )
        self.conn = conn

    def close(self):
        if self.conn:
            self.conn.close()
        logging.debug("close the mysql conn")

    def execute(self, sql):
        # TODO: maybe add support for limit/offset
        res = {}
        with self.conn.cursor() as cursor:
            logging.debug("execute sql: {!s}".format(sql))
            cursor.execute(sql)
            res = cursor.fetchall()
        if not self.autocommit:
            self.conn.commit()
        return res

    def format_result(self, res_execute, selector={}):
        res = {"data": []}
        if not selector:
            res["data"] = res_execute
            return res
        for row in res_execute:
            for k, v in row.items():
                if k in selector:
                    if isinstance(selector[k], Iterable):
                        if v in selector[k]:
                            res["data"].append(row)
                            break
                    else:
                        if v == selector[k]:
                            res["data"].append(row)
                            break
        return res

    def get_all_server_status(self, selector):
        res = {}
        sql = "show global status;"
        res_execute = self.execute(sql)
        for row in res_execute:
            k = row["Variable_name"]
            v = row["Value"]
            if k in selector:
                res[k] = v
        return res

    def get_all_replication_status(self):
        sql = "show slave status;"
        res_execute = self.execute(sql)
        return self.format_result(res_execute)


class ZabbixMySQL(MySQLInst):
    support_funcs = {
        "multi_server_status",
        "multi_server_conf",
        "multi_innodb_status",
        "multi_innodb_conf",
        "multi_replication_conf",
        "multi_replication_status",
        "isalive",
        "hostname",
        "discovery_repl",
        "ps_wait_event",
        "multi_ps_setup",
        "discovery_ps",
    }

    def call(self, func, *args):
        func = func.replace(".", "_") if "." in func else func
        if func not in self.support_funcs:
            msg = "the func called {!s} is not in support funcs".format(func)
            raise ValueError(msg)
        f = getattr(self, func)
        return f(*args)

    def multi_server_status(self):
        res = {}
        selector = {
            # 拒绝的连接数
            "Aborted_connects",
            # 当前客户端连接的数量
            "Threads_connected",
            # 激活状态线程的数量
            "Threads_running",
            # 接收的数据量，单位为字节
            "Bytes_received",
            # 发送的数据量，单位为字节
            "Bytes_sent",
            # 执行全表扫描的次数
            "Select_scan",
            # 慢查询、没有使用索引的查询次数
            "Slow_queries",
            # 没有主键联合的执行次数
            "Select_full_join",
            # 每秒的begin次数
            "Com_begin",
            # 每秒的commit次数
            "Com_commit",
            # 每秒的delete次数
            "Com_delete",
            # 每秒的insert次数
            "Com_insert",
            # 实例的QPS
            "Questions",
            # 每秒的rollback次数
            "Com_rollback",
            # 每秒的select次数
            "Com_select",
            # 每秒的update次数
            "Com_update",
            # 用于查询缓存的内存数量
            "Qcache_free_memory",
            # 查询缓存被访问的次数
            "Qcache_hits",
            # 数据库监听接收数据时的错误次数
            "Connection_errors_accept",
            # 数据库内部原因导致连接发生错误的次数
            "Connection_errors_internal",
            # 立即能够获取表锁的次数
            "Table_locks_immediate",
            # 等待表锁的次数
            "Table_locks_waited",
        }
        res = self.get_all_server_status(selector)
        return res

    def multi_server_conf(self):
        res = {}
        selector = {
            # 配置的binlog日志格式
            "binlog_format",
            "character_set_database",
            "character_set_server",
            "connect_timeout",
            "gtid_mode",
            "have_ssl",
            "sync_binlog",
            "log_bin",
            "max_connections",
            "version",
            "wait_timeout",
            "read_only",
            "hostname",
            "server_id",
            "lower_case_table_names",
        }
        sql = "show global variables where Variable_name in ('{!s}');".format("', '".join(selector))
        res_execute = self.execute(sql)
        for row in res_execute:
            k = row["Variable_name"]
            v = row["Value"]
            res[k] = v
        return res

    def multi_innodb_conf(self):
        res = {}
        selector = {
            "innodb_buffer_pool_size",
            "innodb_buffer_pool_instances",
            "innodb_io_capacity",
            "innodb_log_files_in_group",
            "innodb_write_io_threads",
            "innodb_doublewrite",
            "innodb_fast_shutdown",
            "innodb_file_per_table",
            "innodb_flush_log_at_trx_commit",
        }
        sql = "show global variables where Variable_name in ('{!s}');".format("', '".join(selector))
        res_execute = self.execute(sql)
        for row in res_execute:
            k = row["Variable_name"]
            v = row["Value"]
            res[k] = v
        return res

    def multi_innodb_status(self):
        res = {}
        selector = {
            # 当前的脏页数
            "Innodb_buffer_pool_pages_dirty",
            # 缓冲池中含有数据的页数
            "Innodb_buffer_pool_pages_data",
            # 缓冲池读的请求次数
            "Innodb_buffer_pool_read_requests",
            # 转为物理读的请求次数
            "Innodb_buffer_pool_reads",
            # 空闲页数
            "Innodb_buffer_pool_pages_free",
            # 缓冲池总页数
            "Innodb_buffer_pool_pages_total",
            # 行锁定花费的总时间，单位为毫秒
            "Innodb_row_lock_time",
            # 执行 fsync 的累计次数
            "Innodb_data_fsyncs",
            # redo 日志组写请求次数
            "Innodb_log_writes",
            # redo 日志组执行 fsync 的累计次数
            "Innodb_os_log_fsyncs",
            # 执行行记录删除的累计次数
            "Innodb_rows_deleted",
            # 等待行锁的累计次数
            "Innodb_row_lock_waits",
        }
        res = self.get_all_server_status(selector)
        return res

    def multi_replication_conf(self, status="enable"):
        res = {}
        selector = {
            "Master_Host",
            "Master_User",
            "Master_Port",
            "Master_Server_Id",
        }
        sql = "show slave status;"
        res_execute = self.execute(sql)
        for k, v in res_execute[0].items():
            if k in selector:
                res[k] = v
        return res

    def multi_replication_status(self, status="enable"):
        res = {}
        selector = {
            "Slave_IO_Running",
            "Slave_SQL_Running",
            "Last_Errno",
            "Seconds_Behind_Master",
            "Last_IO_Errno",
            "Last_SQL_Errno",
        }
        sql = "show slave status;"
        res_execute = self.execute(sql)
        for k, v in res_execute[0].items():
            if k in selector:
                res[k] = v
        return res

    def multi_ps_setup(self, status="enable"):
        res = {}
        selector = {
            # 当前等待事件采集
            "events_waits_current",
        }
        sql = "select NAME, ENABLED from setup_consumers limit 1000"
        res_execute = self.execute(sql)
        for i in res_execute:
            if i["NAME"] in selector:
                res[i["NAME"]] = i["ENABLED"]
        return res

    def ps_wait_event(self, status="enable"):
        res = ""
        sql = """
            select concat(EVENT_NAME, '-[', count(EVENT_NAME), ']') as res from performance_schema.events_waits_current 
            where END_EVENT_ID is null and EVENT_NAME <> 'idle' group by EVENT_NAME order by count(EVENT_NAME) desc limit 1
        """
        res_execute = self.execute(sql)
        if len(res_execute) == 0:
            res = ""
        else:
            res = res_execute[0]["res"]
        return res

    def isalive(self):
        sql = "select 1+1 as res from dual;"
        tmp = -1
        try:
            tmp = self.execute(sql)[0]["res"]
        except Exception as e:
            logging.error("get an error when check the db isalive: {!s}".format(e))
            tmp = -1
        return 1 if tmp == 2 else 0

    def hostname(self):
        sql = "show global variables where Variable_name = 'hostname';"
        tmp = ""
        try:
            tmp = self.execute(sql)[0]["Value"]
        except Exception as e:
            logging.error("get an error when get hostname: {!s}".format(e))
            tmp = ""
        return tmp
    
    def discovery_repl(self):
        sql = "show slave status;"
        res = {"data": []}
        tmp = []
        try:
            tmp = self.execute(sql)
        except Exception as e:
            logging.error("get an error when execute sql: {!s}".format(e))
            tmp = []
        if len(tmp) != 0:
            res["data"].append({"{#REPL}": "ok"})
        return res

    def discovery_ps(self):
        sql = "show variables like 'performance_schema'"
        res = {"data": []}
        tmp = []

        try:
            tmp = self.execute(sql)
        except Exception as e:
            logging.error("get an error when execute sql: {!s}".format(e))
            tmp = []
        if len(tmp) != 1:
            logging.error("expect 1 row result, but it is %d row(s)", len(tmp))
            return res

        res["data"] = [{"{#PS}": "ok"}]
        sql = "select NAME, ENABLED from setup_consumers limit 1000"
        tmp = []
        tmp = self.execute(sql)
        for i in tmp:
            if i["ENABLED"].upper() == "YES":
                res["data"][0]["{#%s}" %(i["NAME"].upper())] = i["ENABLED"].upper()

        return res


def discovery_inst(type_):
    import zbx_mysql_config
    res = {"data": []}
    for db, value in zbx_mysql_config.db_config.items():
        for k,v  in value.get("flag", {}).items():
            if k == type_ and v:
                res["data"].append({
                    "{#INST}": db,
                    "{#INFO}": value.get("info", "UNKNOW"),
                    "{#IP}": value["connect"]["host"],
                    "{#PORT}": value["connect"]["port"],
                })
    return res


def main():
    import sys
    """sys.argv struct:
    <db_config key>
    <ZabbixMySQL func string>
    <**args>
    """
    if len(sys.argv) <= 1:
        logging.error("the input args is empty")
        raise ValueError("the input args is empty")
    else:
        logging.debug("the input is {!s}".format(sys.argv))

    res = None
    if sys.argv[1] in globals():
        func = sys.argv[1]
        args = sys.argv[2:]
        res = globals().get(func)(*args)

    else:
        # config for connect
        import zbx_mysql_config
        db = sys.argv[1]
        if db not in zbx_mysql_config.db_config:
            msg = "the db is not found in the db_config"
            logging.error(msg)
            raise ValueError(msg)
        db_config_connect = zbx_mysql_config.db_config[db]["connect"]
        # EOF config

        func = sys.argv[2]
        args = sys.argv[3:]
        zbx_mysql = ZabbixMySQL(**db_config_connect)
        zbx_mysql.connect()
        res = zbx_mysql.call(func, *args)

    if isinstance(res, dict):
        res = json.dumps(res, ensure_ascii=False)
        logging.debug("the res is dict, finish json dumps convert")
    logging.debug("the result of item key call is:\n{!s}".format(res))
    print(res)


if __name__ == "__main__":
    init_logger("debug", "local")
    try:
        main()
    except Exception as e:
        logging.exception(e)
        raise e
