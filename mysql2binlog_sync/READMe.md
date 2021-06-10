https://blog.csdn.net/weixin_30384031/article/details/95367432

#!/usr/bin/env python3
# _*_ coding:utf8 _*_
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent,)
MYSQL_SETTINGS = { "host": "127.0.0.1", "port": 3306, "user": "root", "passwd": "123456"}

def main():
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,server_id=5, blocking=True,
                                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])
    try:
        for binlogevent in stream:
            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    info = dict(row["values"].items())
                    ##如果有主键
                    print("DELETE FROM `%s`.`%s` WHERE %s = %s ;" %(binlogevent.schema ,binlogevent.table,binlogevent.primary_key,info[binlogevent.primary_key]) )
                elif isinstance(binlogevent, UpdateRowsEvent):
                    info_before = dict(row["before_values"].items())
                    info_after = dict(row["after_values"].items())
                    info_set = str(info_after).replace(":","=").replace("{","").replace("}","")
                    print("UPDATE `%s`.`%s` SET %s WHERE %s = %s ;"%(binlogevent.schema,binlogevent.table,info_set,binlogevent.primary_key,info_before[binlogevent.primary_key]   )  )
                elif isinstance(binlogevent, WriteRowsEvent):
                    info = dict(row["values"].items())
                    print("INSERT INTO %s.%s(%s)VALUES%s ;"%(binlogevent.schema,binlogevent.table , ','.join(info.keys()) ,str(tuple(info.values()))  )  )
    except Exception, e:
        print(e)
    finally:
        stream.close()
if __name__ == "__main__":
    main()

'''
BinLogStreamReader()参数
ctl_connection_settings：集群保存模式信息的连接设置
resume_stream：从位置或binlog的最新事件或旧的可用事件开始
log_file：设置复制开始日志文件
log_pos：设置复制开始日志pos（resume_stream应该为true）
auto_position：使用master_auto_position gtid设置位置
blocking：在流上读取被阻止

only_events：允许的事件数组
ignored_events：被忽略的事件数组

only_tables：包含要观看的表的数组（仅适用于binlog_format ROW）
ignored_tables：包含要跳过的表的数组

only_schemas：包含要观看的模式的数组
ignored_schemas：包含要跳过的模式的数组

freeze_schema：如果为true，则不支持ALTER TABLE。速度更快。
skip_to_timestamp：在达到指定的时间戳之前忽略所有事件。
report_slave：在SHOW SLAVE HOSTS中报告奴隶。
slave_uuid：在SHOW SLAVE HOSTS中报告slave_uuid。
fail_on_table_metadata_unavailable：如果我们无法获取有关row_events的表信息，应该引发异常
slave_heartbeat：（秒）主站应主动发送心跳连接。这也减少了复制恢复时GTID复制的流量（在许多事件在binlog中跳过的情况下）。请参阅mysql文档中的MASTER_HEARTBEAT_PERIOD以了解语义
'''