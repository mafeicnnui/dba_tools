工具介绍：

    MySQL二进制日志(binlog)解析工具,支持mysql5.6/5.7
    binlog2parser.json文件配置日志文件对应的数据库连接信息，用于解析列名

支持版本：

    mysql:mysql5.6/5.7
    python:3.6
   
主要功能：

    1.通过binlog文件生成DML语句
    2.通过binlog文件生成回滚语句
    3.支持日期、POS点、库、表过滤,SQL类型进行过滤

配置文件：

    binlog2parser.json

帮助示例：

    yum intall python3

    pip3 install -r requirement.txt

    python3 binlog2parser.py  --help
    
    python3 binlog2parser.py  --binlogfile mysql_172.17.219.137_3306_mysql-bin.003752 --schema hopsonone_point --type=insert --rollback=Y --debug DEBUG