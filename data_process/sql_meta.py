#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/4/12 17:25
# @Author : ma.fei
# @File : sql_metadata.py
# @Software: PyCharm

from sql_metadata.compat import get_query_columns, get_query_tables

# from sql_metadata import get_query_columns, get_query_tables, get_query_table_aliases


sql = '   select x1,x2 from liepin.a as atable left         join b on atable.id = b.id right join c on c.id = atable.id'
sql = ' '.join(sql.split())


print(get_query_tables("select * from user, user2 left join c on user.id = c.id right join d on d.id = e.id"))
print(get_query_tables(sql))
print(get_query_tables("select x1, x2 from (select x1, x2 from (select x1, x2 from apple.a)) left join orange.b as ob on a.id=ob.id   where b.id in (select id from f)"))
print(get_query_tables("select * from user as u where u.id = 99"))

print(get_query_columns("select * from user, user2 left join c on user.id = c.id right join d on d.id = e.id"))
print(get_query_columns(sql))
print(get_query_columns("select x1, x2 from (select x1, x2 from (select x1, x2 from apple.a)) left join orange.b as ob on a.id=ob.id   where b.id in (select id from f)"))
print(get_query_columns("select * from user as u where u.id = 99"))

print(get_query_tables("select * from user, user2 left join c on user.id = c.id right join d on d.id = e.id"))
