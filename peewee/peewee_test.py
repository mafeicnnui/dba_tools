# coding=utf-8
import warnings
import datetime
from peewee import *

db = MySQLDatabase('test',host ='10.2.39.18',port=3306,user='puppet',passwd='Puppet@123')

class BaseModel(Model):
    class Meta:
        database = db

class T_xtqx(BaseModel):
    name               = CharField(max_length=20)
    parent_id          = CharField(max_length=10)
    url                = CharField(max_length=100)
    url_front          = CharField(max_length=100)
    status             = CharField(max_length=1)
    icon               = CharField(max_length=50)
    create_date        = DateTimeField(default=datetime.datetime.now)
    creator            = CharField(max_length=20)
    last_update_date   = DateTimeField(default=datetime.datetime.now)
    updator            = CharField(max_length=20)

class T_user(BaseModel):
    name               = CharField(max_length=20)
    wkno               = CharField(max_length=20)
    gender             = CharField(max_length=2)
    email              = CharField(max_length=40)
    phone              = CharField(max_length=20)
    project_group      = CharField(max_length=1)
    dept               = CharField(max_length=20)
    expire_date        = DateField(default=datetime.datetime.now)
    password           = CharField(max_length=200)
    status             = CharField(max_length=1)
    creation_date      = DateTimeField(default=datetime.datetime.now)
    creator            = CharField(max_length=20)
    last_update_date   = DateTimeField(default=datetime.datetime.now)
    updator            = CharField(max_length=20)
    login_name         = CharField(max_length=20)
    file_path          = CharField(max_length=200)
    file_name          = CharField(max_length=200)

def insert_data(P_user):
    r1 = P_user.create(name='mafei',
                  wkno='190343',
                  gender='1',
                  email='190343@lifeat.cn',
                  phone='12343434343',
                  project_group='1',
                  dept='2',
                  expire_date='2020-12-31',
                  password='Abcd@1234',
                  status='1',
                  creator='dba',
                  updator='dba',
                  login_name='mafei')

    r2 = P_user.create(name='zhangfei',
                  wkno='190342',
                  gender='1',
                  email='190342@lifeat.cn',
                  phone='12343434343',
                  project_group='1',
                  dept='2',
                  # expire_date='2020-12-31',
                  password='Abcd@1234',
                  status='1',
                  creator='dev',
                  updator='dev',
                  login_name='zhangfei')

    r3 = P_user.create(name='wangwu',
                  wkno='190341',
                  gender='1',
                  email='190341@lifeat.cn',
                  phone='12343434343',
                  project_group='1',
                  dept='2',
                  # expire_date='2020-12-31',
                  password='root123HOPSON',
                  status='1',
                  creator='uat',
                  updator='uat',
                  login_name='wangwu')
    return r1,r2,r3

def update_data(P_r1,P_r2,P_r3):
    P_r1.gender='2'
    P_r1.phone='12343434343'
    P_r1.project_group='3'
    P_r1.dept='3'
    P_r1.save()

def main():
    warnings.filterwarnings("ignore")
    print('建立连接...')
    db.connect()

    print('删除表...')
    db.drop_tables([T_xtqx,T_user])

    print('创建表...')
    db.create_tables([T_xtqx,T_user])

    print('插入测试数据...')
    r1,r2,r3 = insert_data(T_user)

    print('查询单行数据...')
    r = T_user.get(id=1)
    print('r=',r.id,r.name,r.gender,r.email,r.phone)
    r = T_user.get(id=4)
    print('r=',r.id,r.name,r.gender,r.email,r.phone)

    print('查询多行数据...')
    rs = T_user.filter(phone='12343434343')
    for r in rs:
       print('r=',r.id,r.name,r.gender,r.email,r.phone)

    print('更新查询结果...')
    rs = T_user.filter(phone='12343434343')
    for r in rs:
        r.email = r.email + '.com'
        r.save()

    print('更新单行数据...')
    update_data(r1,r2,r3)

    print('通过where查询单行数据...')
    rs = T_user.select().where(T_user.status=='1').get()
    print('rs=',rs,rs.name)

    print('通过where查询多行数据1...')
    rs = T_user.select()
    for r in rs:
       print(r.name)

    print('通过where查询多行数据2...')
    rs = T_user.select().where(T_user.status=='1')
    for r in rs:
        print(r.name,r.email,r.expire_date)


    print('表达式查询...')
    expression = fn.Lower(fn.Substr(T_user.name, 1, 1)) == 'z'
    for user in T_user.select().where(expression):
        print(user.name)

    print('模糊查询 like ...1')
    for user in T_user.select().where(T_user.name.contains('a')):
        print(user.name)

    print('模糊查询 like ...2')
    for user in T_user.select().where(T_user.name** '%m%'):
        print(user.name)

    print('in 查询 ...1')
    for user in T_user.select().where(T_user.id <<[1,7]):
        print(user.name)

    print('in 查询 ...2')
    for user in T_user.select().where(T_user.id.in_([1, 7])):
        print(user.name)

    print('删除数据...')
    r3.delete_instance()

    print('关闭连接...')
    db.close()

if __name__=='__main__':
    main()