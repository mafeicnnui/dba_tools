#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/4/12 13:48
# @Author : ma.fei
# @File : sql_parse.py
# @Software: PyCharm

'''
select * from t_user_query_grants
SELECT `uid`,`dbid`,`schema`,`table`,`columns` FROM `puppet`.`t_user_query_grants`

SELECT `uid`,`dbid`,`schema`,`table`,`columns`
FROM `puppet`.`t_user_query_grants`
where schema='hopsonone_cms'


'''

import sqlparse
with open('sql_parse.sql','r',encoding='utf8') as sql_file:
     file_parse = sqlparse.parse(sql_file.read().strip())

for token in file_parse[0].tokens:
    print(type(token),'|',token.ttype,'|',token.value)


for token in file_parse[0].tokens:
    if isinstance(token,sqlparse.sql.IdentifierList):
        #print(token)
        for identifier in token.get_identifiers():
            print(type(identifier), identifier.value, identifier.get_real_name(),identifier.get_name())



# print(str(file_parse[0]))

# print('--------------------------------------------------------------------')
#
# identifier_list = file_parse[0].tokens[3]
# f = identifier_list.get_identifiers()
# print(f)
#
# for identifier in identifier_list.get_identifiers():
#     print(type(identifier),identifier.ttype,identifier.value,identifier.get_real_name())
#
