#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2024/1/2 13:43
# @Author : ma.fei
# @File : month_problem_member.py
# @Software: PyCharm


import os
import re
import pymysql
import pymongo
import pymysql as py
import pandas as pd
import datetime as dt
import openpyxl
import dateutil.parser
import zipfile
import pathlib
import smtplib
from openpyxl.styles import Side,Border,PatternFill,Font
from datetime import datetime,timedelta
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

class SendMail(object):
    '''
    https://cloud.tencent.com/developer/article/1598257
    '''
    def __init__(self,sender,Cc,title,content):
        self.sender = sender.split(',')  #发送地址
        self.Cc = Cc.split(',')  # 抄送地址
        self.title = title  # 标题
        self.content = content  # 发送内容
        self.sys_sender = '190343@lifeat.cn'  # 系统账户
        self.sys_pwd = 'R86hyfjobMBYR76h'  # 系统账户密码

    def send(self,file_list):
        """
        发送邮件
        :param file_list: 附件文件列表
        :return: bool
        """
        try:
            # 创建一个带附件的实例
            msg = MIMEMultipart()
            # 发件人格式
            msg['From'] = formataddr(["技术服务部", self.sys_sender])
            # 收件人格式
            print('a=',[formataddr(["", u]) for u in self.sender])
            msg['To'] = sender

            # 抄送人
            msg['Cc'] = Cc

            # 邮件主题
            msg['Subject'] = self.title

            # 邮件正文内容
            msg.attach(MIMEText(self.content, 'plain', 'utf-8'))

            # 多个附件
            for file_name in file_list:
                print("file_name",file_name)
                # 构造附件
                xlsxpart = MIMEApplication(open(file_name, 'rb').read())
                # filename表示邮件中显示的附件名
                xlsxpart.add_header('Content-Disposition','attachment',filename = '%s'%file_name)
                msg.attach(xlsxpart)

            # SMTP服务器
            server = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465,timeout=10)
            # 登录账户
            server.login(self.sys_sender, self.sys_pwd)
            # 发送邮件
            #server.sendmail(self.sys_sender, [self.sender, ], msg.as_string())
            server.sendmail(self.sys_sender,
                            [formataddr(["", u]) for u in self.sender]+
                            [formataddr(["", u]) for u in self.Cc],
                            msg.as_string())
            # 退出账户
            server.quit()
            return True
        except Exception as e:
            print(e)
            return False

def file_to_zip(path_original, path_zip):
    '''
     作用：压缩文件到指定压缩包里
     参数一：压缩文件的位置
     参数二：压缩后的压缩包
    '''
    # 提前读取，避免把压缩包自己加上去
    # 这里用list()做一个克隆提前执行下，不然会在后面循环时才执行这一引用，如果压缩包在这个路径下，会将它读取进来。
    f_list = list(pathlib.Path(path_original).glob("**/*"))
    z = zipfile.ZipFile(path_zip, 'w')
    #print('len(path_original)=',len(path_original))
    for f in f_list:
        #print('f=',f,str(f),str(f)[len(path_original):])
        z.write(f, str(f)[len(path_original):])
    z.close()

def get_last_month_first_last_day():
    now = datetime.now()
    this_month_start = datetime(now.year, now.month, 1)
    last_month_end = this_month_start - timedelta(days=1)
    last_month_start = datetime(last_month_end.year, last_month_end.month, 1)
    v_last_month_start = datetime.strftime(last_month_start, "%Y-%m-%d")
    v_last_month_end = datetime.strftime(last_month_end, "%Y-%m-%d")
    return v_last_month_start, v_last_month_end

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip,
                           port=int(port),
                           user=user,
                           passwd=password,
                           db=service,
                           charset='utf8',
                           cursorclass = pymysql.cursors.DictCursor,
                           autocommit=False)
    return conn

def get_start_stop_unixtime(start_rq,end_rq):
    db_mysql = get_ds_mysql('rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com', '3306', 'hopsonone_do', 'hopsonone_do','8Loxk2IJxaenJkE3')
    cr_mysql = db_mysql.cursor()
    cr_mysql.execute("select unix_timestamp(concat('{}', ' 00:00:00'))*1000 as start_rq,unix_timestamp(concat('{}', ' 23:59:59'))*1000 as end_rq".format(start_rq,end_rq))
    rs_mysql = cr_mysql.fetchone()
    return rs_mysql['start_rq'],rs_mysql['end_rq']

def get_ds_mongo(mongodb_str):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    user          = mongodb_str.split(':')[3]
    password      = mongodb_str.split(':')[4]
    conn          = pymongo.MongoClient(host=ip, port=int(port))
    db            = conn[service]
    db.authenticate(user, password)
    return db

def query_point_worn_from_mongo(p_start_unixtime,p_stop_unixtime):
    db_mongo = get_ds_mongo('dds-2ze39d52c51a85f42163-pub.mongodb.rds.aliyuncs.com:3717:hopsonone_rule:hopsonone_ro:lvOD4ljLBkREapZd')
    rs_mongo = db_mongo['member_list'].aggregate([
                {'$match':{'list_type':{'$gt':0},'update_time':{'$gt':1698768000000,'$lt':1701359999000}}},
                {'$project':{'_id':0,'m_id':1,'market_id':1,'operation_name':1,
                'range':{'$switch':{'branches':[
                  {'case':{'$eq':["$range",1]},'then':'单店铺'},
                  {'case':{'$eq':["$range",2]},'then':'全场'}
                ],'default':'-'}},
                'list_type':{'$switch':{'branches':[
                {'case':{'$eq':["$list_type",1]},'then':'预警'},
                {'case':{'$eq':["$list_type",2]},'then':'异常'}
                ],'default':'黑名单'}},
                'into_channel':{'$switch':{'branches':[
                {'case':{'$eq':["$into_channel",0]},'then':'预警转入'},
                {'case':{'$eq':["$into_channel",1]},'then':'导入'},
                {'case':{'$eq':["$into_channel",2]},'then':'拉取'}
                ],'default':'触发规则'}},
                'business_id':1,'rule_content':1,'remark':1,'update_time':1,
                'update_time':{'$dateToString': {'format': "%Y-%m-%d", 'date':{"$add":[dateutil.parser.parse("1970-01-01T00:00:00Z"),"$update_time"]}}}}},
                {'$project':{'项目ID':'$market_id','处理人':'$operation_name',
                             '会员ID':'$m_id','积分类型':'$range',
                             '进入渠道':'$into_channel','商家编码':'$business_id',
                             '规则内容':'$rule_content','备注':'$remark',
                             '异常时间':'$update_time','类型':'$list_type'}}
                ])
    return rs_mongo

def ins_point_worn_to_mysql(p_start_unixtime,p_stop_unixtime):
    db_mysql = get_ds_mysql('rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com', '3306', 'hopsonone_do', 'hopsonone_do', '8Loxk2IJxaenJkE3')
    cr_mysql = db_mysql.cursor()
    cr_mysql.execute('truncate table point_worn')
    print('Table `point_worn` truncate!')
    ins_data = query_point_worn_from_mongo(p_start_unixtime,p_stop_unixtime)
    for r in ins_data:
        st = '''insert into point_worn(`项目ID`,`处理人`,`会员ID`,`积分类型`,`进入渠道`,`商家编码`,`规则内容`,`备注`,`异常时间`,`类型`) 
                      values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
                  '''.format(r.get('项目ID',''),
                             r.get('处理人',''),
                             r.get('会员ID',''),
                             r.get('积分类型',''),
                             r.get('进入渠道',''),
                             r.get('商家编码',''),
                             r.get('规则内容',''),
                             r.get('备注',''),
                             r.get('异常时间',''),
                             r.get('类型',''))
        cr_mysql.execute(st)
    print('Table `point_worn` write complete!')
    db_mysql.commit()

def create_temp_table():
    db_mysql = get_ds_mysql('rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com', '3306', 'hopsonone_do', 'hopsonone_do','8Loxk2IJxaenJkE3')
    cr_mysql = db_mysql.cursor()
    cr_mysql.execute('drop table if exists point_worn_sc')
    cr_mysql.execute('''create table point_worn_sc as
select a.market_name as '项目名称',
w.`处理人`,
w.`会员ID`,
m.m_mobile as '会员手机号',
w.`积分类型`,	
w.`进入渠道`,	
w.`商家编码`,	
w.`规则内容`,	
w.`备注`,	
w.`异常时间`,	
w.`类型`
from hopsonone_do.point_worn w
left join 
hopsonone_members.members m
on w.`会员ID`=m.m_id
left join merchant_entity.market a
on w.`项目ID`=a.id ''')

def write_problem_members(p_rq_start,p_rq_end,p_week_rq):
    开始日期, 结束日期, 周报日期 = p_rq_start, p_rq_end, p_week_rq

    # 连接数据库，执行SQL语句
    连接对象 = py.connect(host='rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com', user='hopsonone_do', passwd='8Loxk2IJxaenJkE3', database='hopsonone_do')

    项目文件夹 = output_dir+'积分问题会员月报-' + 周报日期
    os.mkdir(项目文件夹)
    # 问题名单SQL语句
    问题名单sql = """ select * from hopsonone_do.point_worn_sc"""
    问题名单 = pd.read_sql(问题名单sql, con=连接对象)
    # 积分明细数据SQL语句
    积分明细数据sql = """
    select a.market_name as '项目名称',
    m.m_mobile as '会员手机号',
    case p.state
    when 0 then '产生积分'
    when 1 then '消耗积分'
    end as '积分方向',
    p.behavior_name as '积分行为',
    p.point as '积分',
    p.create_time as '积分时间',
    p.business_id as '商家编码',
    s.store_name as '商户名称',
    p.consume_amount/100 as '消费金额（元）',
    p.consume_time as '消费时间'
    from hopsonone_do.point_worn w
    left join
    hopsonone_point_real_time.members_points_detail p
    on w.`会员ID`=p.m_id
    left join 
    hopsonone_members.members m
    on w.`会员ID`=m.m_id
    left join merchant_entity.market a
    on w.`项目ID`=a.id
    left join merchant_entity.entity_store s
    on p.business_id=s.store_id
    where p.create_time >=%(date_op)s
    and p.create_time <=concat(%(date_ed)s,' 23:59:59')
    """
    积分明细 = pd.read_sql(积分明细数据sql, con=连接对象, params={'date_op': 开始日期, 'date_ed': 结束日期})

    数据源拆分列 = list(问题名单['项目名称'].drop_duplicates())
    for i in 数据源拆分列:
        问题名单新 = 问题名单[问题名单['项目名称'] == i]
        积分明细新 = 积分明细[积分明细['项目名称'] == i]
        with pd.ExcelWriter(项目文件夹 + '/' + i + '-积分问题会员月报' + 周报日期 + '.xlsx') as 表:
            问题名单新.to_excel(表, sheet_name='问题名单', index=False)
            积分明细新.to_excel(表, sheet_name='积分明细', index=False)


if __name__ == '__main__':
    output_dir = './out/积分问题会员/'
    os.system('rm -rf ./out/积分问题会员/*')
    start_rq, end_rq = get_last_month_first_last_day()
    week_rq = start_rq[5:7] + start_rq[8:] + '~' + end_rq[5:7] + end_rq[8:]
    start_rq_unix,end_rq_unix = get_start_stop_unixtime(start_rq, end_rq)

    print('合生通项目组-积分问题会员(月报):')
    print('-----------------------------------------')
    print('开始日期：', start_rq,start_rq_unix)
    print('结束日期：', end_rq,end_rq_unix)
    print('周报日期：', week_rq)
    print('-----------------------------------------')

    # write mysql:point_worn table
    ins_point_worn_to_mysql(start_rq_unix,end_rq_unix)

    # create temp table
    create_temp_table()

    #  write_problem_members to excel
    write_problem_members(start_rq, end_rq,week_rq)

    # 文件打包
    path_original = 'out/积分问题会员'
    path_zip = './out/积分问题会员/积分问题会员月报-{}.zip'.format(week_rq)
    zip_name = '积分问题会员月报-{}.zip'.format(week_rq)

    file_to_zip(path_original, path_zip)
    print('文件打包完成!')

    # 发送邮件及附件
    sender = '190634@lifeat.cn,850646@cre-hopson.com,820618@cre-hopson.com,546564@hopson.com.cn'
    Cc = '190343@lifeat.cn,820987@cre-hopson.com'
    title = '合生通积分问题会员(月报)-{}'.format(week_rq)
    content = '''各位领导：
             {}
             详见附件,请查收。
             '''.format(title)
    # 附件列表
    os.chdir('./out/积分问题会员')
    file_list = [zip_name]
    ret = SendMail(sender, Cc, title, content).send(file_list)
    print(ret, type(ret))
    print('邮件邮件完成')