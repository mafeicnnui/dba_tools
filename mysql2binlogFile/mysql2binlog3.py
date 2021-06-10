#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 参考代码：http://blog.csdn.net/selectdb/article/details/16861063
# 解析出来的每个事务对应的DB名、表名、事务开始时间、结束时间、开始log pos、结束log pos保存在参数指定的MySQL实例的test.t_binlog_event中

import pymysql
import re
import os
import sys
import datetime

usage = """\nflashback_mysql 1.0 for python 2.6+
Usage:
    ./flashback_mysql  mysql-binlog-path  -S='2016-05-16T18:00:00' -E='2016-05-16T19:00:00'  -h=192.168.88.11  -P=3306  -u=root  -p=xxx
    Argv1 is mandatory, and must be set as mysql-binlog-path,
    The other argvs are optional, '-S' indicates '--start-datetime', '-E' indicates '--stop-datetime'
    tag:
        1. need MySQLdb module
        2. need your mysql server desc table privileges
        3. results will be stored in test.t_binlog_event on the MySQL instance you provided.
"""


class ClassFlashback:
    def __init_db(self):
        self.mysql_db = pymysql.connect(host=self.host, user=self.user, passwd=self.password, port=self.port,
                                        charset='utf8')
        self.mysql_db.autocommit(True)
        self.mysql_cur = self.mysql_db.cursor()

    def __init__(self):
        self.host = ''
        self.user = ''
        self.password = ''
        self.port = ''
        self.db = ''
        self.start_datetime = ''
        self.stop_datetime = ''
        self.tmp_binlog_file = 'mysqlbin000125.txt'
        self.field = []
        self.db_name = ''
        self.tb_name = ''
        self.patt = re.compile("/* .* */")
        self._get_argv()
        self.__init_db()
        self.begin_time = ''
        self.end_time = ''
        self.start_pos = ''
        self.end_pos = ''
        self.dml_sql = ''
        self.undo_sql = ''

    def _get_argv(self):
        if len(sys.argv) == 1:
            print
            usage
            sys.exit(1)
        elif sys.argv[1] == '--help' or sys.argv[1] == '-h':
            print
            usage
            sys.exit()
        elif len(sys.argv) > 2:
            for i in sys.argv[2:]:
                _argv = i.split('=')
                if _argv[0] == '-S':
                    self.start_datetime = _argv[1].replace('T', ' ')
                elif _argv[0] == '-E':
                    self.stop_datetime = _argv[1].replace('T', ' ')
                # elif _argv[0] == '-d':  self.db = _argv[1]
                elif _argv[0] == '-h':
                    self.host = '%s' % _argv[1]
                elif _argv[0] == '-P':
                    self.port = int('%s' % _argv[1])
                elif _argv[0] == '-u':
                    self.user = '%s' % _argv[1]
                elif _argv[0] == '-p':
                    if len(_argv) == 2:
                        self.password = '%s' % _argv[1]
                    elif len(_argv) == 1:
                        self.password = input('Please enter your mysql passwd: ')
                else:
                    print
                usage;
                sys.exit(1)
        self.input_binlog_file = sys.argv[1]
        if self.port == '':
            self.port = 3306
        elif self.password == '':
            self.password = input('Please enter your mysql passwd: ')
        elif self.start_datetime != '' and self.stop_datetime != '':
            self.start_datetime = "--start-datetime='" + self.start_datetime + "'"
            self.stop_datetime = "--stop-datetime='" + self.stop_datetime + "'"
        elif self.start_datetime != '':
            self.start_datetime = "--start-datetime='" + self.start_datetime + "'"
        elif self.stop_datetime != '':
            self.stop_datetime = "--stop-datetime='" + self.stop_datetime + "'"

    def _create_tab(self):
        create_tb_sql = """
        CREATE TABLE IF NOT EXISTS test.t_binlog_event (
            auto_id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
            binlog_name VARCHAR(100) NOT NULL ,
            dml_start_time DATETIME NOT NULL,
            dml_end_time DATETIME NOT NULL,
            start_log_pos BIGINT NOT NULL,
            end_log_pos BIGINT NOT NULL,
            db_name VARCHAR(100) NOT NULL ,
            table_name VARCHAR(200) NOT NULL ,
            dml_sql LONGTEXT NULL ,
            undo_sql LONGTEXT NULL ,
            PRIMARY KEY (auto_id),
            INDEX dml_start_time (dml_start_time),
            INDEX dml_end_time (dml_end_time),
            INDEX start_log_pos (start_log_pos),
            INDEX end_log_pos (end_log_pos),
            INDEX db_name (db_name),
            INDEX table_name (table_name)
        )
        COLLATE='utf8_general_ci' ENGINE=InnoDB;
        """
        self.mysql_cur.execute(create_tb_sql)

    def _release_db(self):
        self.mysql_cur.close()
        self.mysql_db.close()

    def _get_table_name(self, line):
        try:
            if line.find('Table_map:') != -1:
                l = line.index('server')
                m = line.index('end_log_pos')
                n = line.index('Table_map')
                begin_time = line[:l:].rstrip(' ').replace('#', '20')
                self.begin_time = begin_time[0:4] + '-' + begin_time[4:6] + '-' + begin_time[6:]
                self.start_pos = int(line[m::].split(' ')[1])
                self.db_name = line[n::].split(' ')[1].replace('`', '').split('.')[0]
                self.tb_name = line[n::].split(' ')[1].replace('`', '').split('.')[1]
        except Exception as ex:
            print(ex)

    def _get_end_time(self, line):
        try:
            if line.find('Xid =') != -1:
                l = line.index('server')
                m = line.index('end_log_pos')
                end_time = line[:l:].rstrip(' ').replace('#', '20')
                self.end_time = end_time[0:4] + '-' + end_time[4:6] + '-' + end_time[6:]
                self.end_pos = int(line[m::].split(' ')[1])

                self.dml_sql = self.dml_sql.replace("'", "''''") + ';'
                if self.dml_sql.find('INSERT INTO ') != -1:
                    self.undo_sql = self.dml_sql.replace('INSERT INTO', 'DELETE FROM').replace('SET', 'WHERE')
                elif self.dml_sql.find('UPDATE ') != -1:
                    self.undo_sql = self.dml_sql.replace('WHERE', 'WHERETOxxx').replace('SET', 'WHERE').replace(
                        'WHERETOxxx', 'SET')
                elif self.dml_sql.find('DELETE ') != -1:
                    self.undo_sql = self.dml_sql.replace('DELETE FROM', 'INSERT INTO').replace('WHERE', 'SET')
                # print self.begin_time, self.end_time, self.start_pos, self.end_pos, self.db_name, self.tb_name, self.dml_sql
                # print self.undo_sql + '\n\n\n'
                insert_sql = "insert into test.t_binlog_event values (NULL, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (
                self.input_binlog_file, self.begin_time, self.end_time, self.start_pos, self.end_pos, self.db_name,
                self.tb_name, self.dml_sql, self.undo_sql)
                # print insert_sql
                self.mysql_cur.execute(insert_sql)
                self.dml_sql = ''
                self.undo_sql = ''
                # 此处一个事务结束, 将self.dml_sql 和self.undo_sql置空, 用于存放下一条解析出来的DML语句
        except Exception as  ex:
            print(ex)


    def _get_table_structure(self, db_name, tb_name):
        desc_sql = 'desc %s.%s' % (db_name, tb_name)
        self.field = []
        self.mysql_cur.execute(desc_sql)
        res = self.mysql_cur.fetchall()
        for j in res:
            self.field.append(j[0])

    def _do(self):
        '''先把mysql二进制的binlog解析成可识别文件，再从里面提取需要的数据'''

        starttime = datetime.datetime.now()
        print("\nConverting binlog to text file...")
        # os.popen('mysqlbinlog --no-defaults -v -v --base64-output=DECODE-ROWS %s %s %s > %s' %(self.start_datetime, self.stop_datetime, self.input_binlog_file,self.tmp_binlog_file))
        print
        "mysqlbinlog --no-defaults -v -v --base64-output=DECODE-ROWS %s %s %s > %s" % (
        self.start_datetime, self.stop_datetime, self.input_binlog_file, self.tmp_binlog_file)
        print("File converting complete.")
        endtime = datetime.datetime.now()
        timeinterval = endtime - starttime
        print("Converting elapsed :" + str(timeinterval.seconds) + '.' + str(timeinterval.microseconds) + " seconds")

        self._create_tab()
        print("\nParsing file...")
        starttime = datetime.datetime.now()
        with open(self.tmp_binlog_file, "r") as infile:
            for line in infile.readlines():
                if line.find('Table_map:') != -1:  # 匹配到表名，解析开始时间、开始log pos、DB名、表名，并获取表结构
                    self._get_table_name(line)
                    self._get_table_structure(self.db_name, self.tb_name)
                    line = ''  # 将这一行内容清空，否则line的内容将会传入到dml_sql中去
                elif line.find('###   @') != -1:  # 匹配到字段内容, 将字段位置代码替换成字段名，并去除字段值后面的字段类型等内容
                    i = line.replace('###   @', '').split('=')[0]
                    #line = unicode(line, "utf-8")
                    line = line.replace('###   @' + str(i), self.field[int(i) - 1])
                    # if(int(i) == len(self.field)):
                    #     line = self.patt.sub(' ', line)
                    # else:
                    line = self.patt.sub(',', line)
                elif line.find('###') != -1:  # 匹配到关键字，如UPDATE, WHERE, SET等等
                    line = line.replace('###', '')
                elif line.find('Xid =') != -1:  # 匹配到提交时间，到这里整个事务解析完毕
                    self._get_end_time(line)
                    line = ''  # 将这一行内容清空，否则line的内容将会传入到dml_sql中去
                else:
                    line = ''  # 丢弃其他信息

                if line.rstrip('\n') != '':
                    # 到此处只会是SQL语句
                    self.dml_sql = self.dml_sql + line + ' '

        print("\nParsing completed.")
        endtime = datetime.datetime.now()
        timeinterval = endtime - starttime
        print("Parsing elapsed :" + str(timeinterval.seconds) + '.' + str(timeinterval.microseconds) + " seconds")


def main():
    p = ClassFlashback()
    p._do()
    p._release_db()


if __name__ == "__main__":
    main()
