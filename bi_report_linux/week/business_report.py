#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2024/1/3 14:47
# @Author : ma.fei
# @File : business_report.py.py
# @Software: PyCharm

import pymysql as py
import pandas as pd
import datetime as dt
import os
import openpyxl
import zipfile
import pathlib
import smtplib
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

def get_last_week5_to_this_week4(today, weekly=0):
    """
    :function: 获取指定日期的周一和周日的日期
    :param today: '2021-11-16'; 当前日期：today = datetime.now().strftime('%Y-%m-%d')
    :param weekly: 获取指定日期的上几周或者下几周，weekly=0当前周，weekly=-1上一周，weekly=1下一周
    :return: 返回指定日期的周一和周日日期
    :return_type: tuple
    """
    last = weekly * 7
    today = datetime.strptime(str(today), "%Y-%m-%d")
    monday = datetime.strftime(today - timedelta(today.weekday() - last), "%Y-%m-%d")
    monday_ = datetime.strptime(monday, "%Y-%m-%d")
    last_week5 = datetime.strftime(monday_ + timedelta(monday_.weekday() - 3), "%Y-%m-%d")
    this_week4 = datetime.strftime(monday_ + timedelta(monday_.weekday() + 3), "%Y-%m-%d")
    return last_week5, this_week4

def write_business_report(p_rq_start,p_rq_end,p_week_rq):
    开始日期, 结束日期, 周报日期 = p_rq_start, p_rq_end, p_week_rq
    # 连接数据库，执行SQL语句
    连接对象 = py.connect(host='rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com', user='hopsonone_do', passwd='8Loxk2IJxaenJkE3',database='hopsonone_do')

    # 科技赋能GMV量SQL语句
    科技赋能GMVsql = """
    select round(sum(x.`全年累计完成`),2) as '全年累计完成',
        round(sum(x.`本月完成`),2) as '本月完成',round(sum(x.`本周完成`),2) as '本周完成'
    from (
    # 7.POS GMV
    select sum(o.total_amount) / 100 / 10000 as '全年累计完成',
        sum(case when p.create_time>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01')
        and p.create_time<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_amount END )/100/10000 as '本月完成',
        sum(case when p.create_time>=%(date_op)s
        and p.create_time<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_amount END )/100/10000 as '本周完成'
    from hopson_hft_real_time.intel_order_payment p,
    hopson_hft_real_time.intel_order o
    where p.trade_no = o.trade_no
    and p.sub_code = 1 
    and p.trans_way != 3
    and p.create_time >= concat(date_format(%(date_op)s,'%%Y'),'-01-01')  
    and p.create_time <= CONCAT(%(date_ed)s,' 23:59:59') 
    and o.market_id!='90015'
    union all
    # 8.账单GMV
    select sum(o.total_fee) / 100 / 10000 as '全年累计完成',
        sum(case when o.complete_date>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01')
        and o.complete_date<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '本月完成',
        sum(case when o.complete_date>=%(date_op)s
        and o.complete_date<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '本周完成'
    from hft_business_pay.bill_order o
    where o.state = 3 
    and o.complete_date >= concat(date_format(%(date_op)s,'%%Y'),'-01-01') 
    and o.complete_date <= CONCAT(%(date_ed)s,' 23:59:59')
    ) x
    """
    # 计时
    科技赋能GMV运行开始时间 = dt.datetime.now()
    科技赋能GMV = pd.read_sql(科技赋能GMVsql, con=连接对象, params={'date_op': 开始日期, 'date_ed': 结束日期})
    科技赋能GMV运行结束时间 = dt.datetime.now()
    科技赋能GMV运行sql用时 = (科技赋能GMV运行结束时间 - 科技赋能GMV运行开始时间).seconds
    print(f'科技赋能GMV运行SQL时间为{科技赋能GMV运行sql用时}秒')

    # 本周会员销售额SQL语句
    本周会员销售额sql = """
    select round((ifnull(sum(a.point_amount),0)-ifnull(sum(c.point_amount),0))/10000,2) as '本周会员销售额（万元）'
    from
    (select sum(d.consume_amount/100) as 'point_amount'
    from hopsonone_point_real_time.members_points_detail d
    where d.state = 0
    and d.behavior in (8,20,24,26,27,28)
    and d.consume_time>= %(date_op)s
    and d.consume_time <= CONCAT(%(date_ed)s,' 23:59:59') ) a ,
    (select sum(d.consume_amount/100) as 'point_amount'
    from hopsonone_point_real_time.members_points_detail d
    where d.state = 1
    and d.behavior in (8,20,24,26,27,28)
    and d.consume_time>= %(date_op)s
    and d.consume_time <= CONCAT(%(date_ed)s,' 23:59:59') ) c 
    """
    # 计时
    本周会员销售额运行开始时间 = dt.datetime.now()
    本周会员销售额 = pd.read_sql(本周会员销售额sql, con=连接对象, params={'date_op': 开始日期, 'date_ed': 结束日期})
    本周会员销售额运行结束时间 = dt.datetime.now()
    本周会员销售额运行sql用时 = (本周会员销售额运行结束时间 - 本周会员销售额运行开始时间).seconds
    print(f'本周会员销售额运行SQL时间为{本周会员销售额运行sql用时}秒')

    # 本年累计会员销售额SQL语句
    本年累计会员销售额sql = """
    select round((ifnull(sum(a.point_amount),0)-ifnull(sum(c.point_amount),0))/10000,2) as '本年累计会员销售额（万元）'
    from
    (select sum(d.consume_amount/100) as 'point_amount'
    from hopsonone_point_real_time.members_points_detail d
    where d.state = 0
    and d.behavior in (8,20,24,26,27,28)
    and d.consume_time>=concat(date_format(%(date_op)s,'%%Y'),'-01-01')
    and d.consume_time <= CONCAT(%(date_ed)s,' 23:59:59') ) a ,
    (select sum(d.consume_amount/100) as 'point_amount'
    from hopsonone_point_real_time.members_points_detail d
    where d.state = 1
    and d.behavior in (8,20,24,26,27,28)
    and d.consume_time>=concat(date_format(%(date_op)s,'%%Y'),'-01-01') 
    and d.consume_time <= CONCAT(%(date_ed)s,' 23:59:59') ) c 
    """
    # 计时
    本年累计会员销售额运行开始时间 = dt.datetime.now()
    本年累计会员销售额 = pd.read_sql(本年累计会员销售额sql, con=连接对象, params={'date_op': 开始日期, 'date_ed': 结束日期})
    本年累计会员销售额运行结束时间 = dt.datetime.now()
    本年累计会员销售额运行sql用时 = (本年累计会员销售额运行结束时间 - 本年累计会员销售额运行开始时间).seconds
    print(f'本年累计会员销售额运行SQL时间为{本年累计会员销售额运行sql用时}秒')

    # 会员注册量SQL语句
    会员注册sql = """
    SELECT
    count(case when m.create_time>=concat(date_format(%(date_op)s,'%%Y'),'-01-01')
    and m.create_time<= CONCAT(%(date_ed)s,' 23:59:59') then m.m_id END ) as '全年累计完成',
    count(case when m.create_time>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01')
    and m.create_time<= CONCAT(%(date_ed)s,' 23:59:59') then m.m_id END ) as '本月完成',
    count(case when m.create_time>=%(date_op)s
    and m.create_time<= CONCAT(%(date_ed)s,' 23:59:59') then m.m_id END ) as '本周完成'
    FROM hopsonone_members.members m
    """
    # 计时
    会员注册运行开始时间 = dt.datetime.now()
    会员注册 = pd.read_sql(会员注册sql, con=连接对象, params={'date_op': 开始日期, 'date_ed': 结束日期})
    会员注册运行结束时间 = dt.datetime.now()
    会员注册运行sql用时 = (会员注册运行结束时间 - 会员注册运行开始时间).seconds
    print(f'会员注册运行SQL时间为{会员注册运行sql用时}秒')

    # 付费会员新增SQL语句
    付费会员新增sql = """
    select 
    count((case when o.payment_dt>=concat(date_format(%(date_op)s,'%%Y'),'-01-01')
    and o.payment_dt<=concat(%(date_ed)s,' 23:59:59') then o.buyer_id end)) as '全年累计完成',
    count((case when o.payment_dt>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01')
    and o.payment_dt<=concat(%(date_ed)s,' 23:59:59') then o.buyer_id end)) as '本月完成',
    count((case when o.payment_dt>=%(date_op)s
    and o.payment_dt<=concat(%(date_ed)s,' 23:59:59') then o.buyer_id end)) as '本周完成'
    from hopsonone_order_center.orders_detail_plusvip a,
    hopsonone_order_center.orders o
    where a.trade_no = o.trade_no
    and o.state not in (1,2)
    and o.market_id in (108,110,132,164,213,218,237,234,278,287,306)
    and o.payment_dt >= concat(date_format(%(date_op)s,'%%Y'),'-01-01')  
    and o.payment_dt <= CONCAT(%(date_ed)s,' 23:59:59') 
    """
    # 计时
    付费会员新增运行开始时间 = dt.datetime.now()
    付费会员新增 = pd.read_sql(付费会员新增sql, con=连接对象, params={'date_op': 开始日期, 'date_ed': 结束日期})
    付费会员新增运行结束时间 = dt.datetime.now()
    付费会员新增运行sql用时 = (付费会员新增运行结束时间 - 付费会员新增运行开始时间).seconds
    print(f'付费会员新增运行SQL时间为{付费会员新增运行sql用时}秒')

    # 会员营销量SQL语句
    会员营销sql = """
    SELECT
    ifnull(a.`全年累计完成`,0) + ifnull(b.`全年累计完成`,0) + ifnull(c.`全年累计完成`,0)+ ifnull(d.`全年累计完成`,0)-ifnull(e.`全年累计完成`,0)-ifnull(f.`全年累计完成`,0) as '全年累计完成',
    ifnull(a.`本月完成`,0) + ifnull(b.`本月完成`,0) + ifnull(c.`本月完成`,0)+ ifnull(d.`本月完成`,0)-ifnull(e.`本月完成`,0)-ifnull(f.`本月完成`,0) as '本月完成',
    ifnull(a.`本周完成`,0) + ifnull(b.`本周完成`,0) + ifnull(c.`本周完成`,0)+ ifnull(d.`本周完成`,0)-ifnull(e.`本周完成`,0)-ifnull(f.`本周完成`,0) as '本周完成'
    FROM
    (SELECT
    sum(case when o.pay_dt>=concat(date_format(%(date_op)s,'%%Y'),'-01-01')
    and o.pay_dt<= CONCAT(%(date_ed)s,' 23:59:59') then o.coupons_value END )/100/10000 as '全年累计完成',
    sum(case when o.pay_dt>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01')
    and o.pay_dt<= CONCAT(%(date_ed)s,' 23:59:59') then o.coupons_value END )/100/10000 as '本月完成',
    sum(case when o.pay_dt>=%(date_op)s
    and o.pay_dt<= CONCAT(%(date_ed)s,' 23:59:59') then o.coupons_value END )/100/10000 as '本周完成'
    FROM hopsonone_coupons_v2.coupons_sub_order o
    WHERE o.get_way = 3
    and o.order_state not in (1,2)) a,
    (SELECT
    sum(case when d.create_time>=concat(date_format(%(date_op)s,'%%Y'),'-01-01')
    and d.create_time<= CONCAT(%(date_ed)s,' 23:59:59') then d.card_value END )/100/10000 as '全年累计完成',
    sum(case when d.create_time>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01')
    and d.create_time<= CONCAT(%(date_ed)s,' 23:59:59') then d.card_value END )/100/10000 as '本月完成',
    sum(case when d.create_time>=%(date_op)s
    and d.create_time<= CONCAT(%(date_ed)s,' 23:59:59') then d.card_value END )/100/10000 as '本周完成'
    FROM hopsonone_card.order_card_detail d
    WHERE d.channel != 2) b,
    (SELECT
    sum(case when o.payment_dt>=concat(date_format(%(date_op)s,'%%Y'),'-01-01')
    and o.payment_dt<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '全年累计完成',
    sum(case when o.payment_dt>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01')
    and o.payment_dt<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '本月完成',
    sum(case when o.payment_dt>=%(date_op)s
    and o.payment_dt<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '本周完成'
    FROM hopsonone_order_center.orders o
    WHERE o.order_type in (4,8)
    and o.state not in (1,2)) c,
    (select sum(c.`全年累计完成`) as '全年累计完成',sum(c.`本月完成`) as '本月完成',sum(c.`本周完成`) as '本周完成'
    from (select c.account_no,
    ifnull(sum(case c.direction when 2 then c.confirm_amount when 1 then -c.confirm_amount end)/100/10000,0) as '全年累计完成',
    0 as'本月完成',
    0 as '本周完成'
    from hft_wallet.customer_balance_details c
    where c.status=1 #成功
    and c.interface_branch in ('6129-0','6130-0','6139-0','909101-1')
    and c.create_time>=concat(date_format(%(date_op)s,'%%Y'),'-01-01') 
    and c.create_time<= CONCAT(%(date_ed)s,' 23:59:59')
    group by c.account_no
    union all
    select c.account_no,
    0 as'全年累计完成',
    ifnull(sum(case c.direction when 2 then c.confirm_amount when 1 then -c.confirm_amount end)/100/10000,0) as '本月完成',0 as '本周完成'
    from hft_wallet.customer_balance_details c
    where c.status=1 #成功
    and c.interface_branch in ('6129-0','6130-0','6139-0','909101-1')
    and c.create_time>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01') 
    and c.create_time<= CONCAT(%(date_ed)s,' 23:59:59')
    group by c.account_no
    union all
    select c.account_no,
    0 as'全年累计完成',
    0 as '本月完成',
    ifnull(sum(case c.direction when 2 then c.confirm_amount when 1 then -c.confirm_amount end)/100/10000,0) as '本周完成'
    from hft_wallet.customer_balance_details c
    where c.status=1 #成功
    and c.interface_branch in ('6129-0','6130-0','6139-0','909101-1')
    and c.create_time>=%(date_op)s
    and c.create_time<= concat(%(date_ed)s,' 23:59:59')
    group by c.account_no) c,
    hft_wallet.wallet_account_info f
    where f.market_id in (108,110,132,164,213,218,237,234,278,287,306)
    and c.account_no=f.account_no collate utf8mb4_unicode_ci) d,
    (select 
    sum(case when c.receive_time>=concat(date_format(%(date_op)s,'%%Y'),'-01-01')
    and c.receive_time<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '全年累计完成',
    sum(case when c.receive_time>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01')
    and c.receive_time<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '本月完成',
    sum(case when c.receive_time>=%(date_op)s
    and c.receive_time<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '本周完成'
    from hopsonone_coupons_v2.coupons_sub_order c,
    hopsonone_order_center.orders o
    where o.trade_no=c.order_trade_no
    and c.order_state not in (1,2) #排除未支付
    and c.get_channel_type=10 #秒杀
    and c.get_channel=23 #秒杀
    and c.receive_time>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01') 
    and c.receive_time<= CONCAT(%(date_ed)s,' 23:59:59')) e,
    (select 
    sum(case when c.receive_time>=concat(date_format(%(date_op)s,'%%Y'),'-01-01')
    and c.receive_time<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '全年累计完成',
    sum(case when c.receive_time>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01')
    and c.receive_time<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '本月完成',
    sum(case when c.receive_time>=%(date_op)s
    and c.receive_time<= CONCAT(%(date_ed)s,' 23:59:59') then o.total_fee END )/100/10000 as '本周完成'
    from hopsonone_coupons_v2.coupons_sub_order c,
    hopsonone_coupons_v2.coupons v,
    hopsonone_order_center.orders o,
    hopsonone_coupons_v2.coupons_label_relations r
    where c.order_state not in (1,2) #排除未支付
    and c.get_channel_type in (1,6) #领券中心+导购屏
    and c.get_channel in (3,12) #领券中心+导购屏
    # 适用店铺只有一家的卡券，取卡券编码
    and c.coupons_id IN (SELECT s.coupons_id
    FROM hopsonone_coupons_v2.coupons_use_scope s 
    WHERE s.use_scope = 3 
    GROUP BY s.coupons_id
    having count(DISTINCT s.use_scope_id)=1)
    and r.label_id in (602,588,609,581,595,574,567,956) #美食标签
    and r.coupons_id=c.coupons_id
    and v.send_type=1
    and v.get_way=3
    and v.id=c.coupons_id
    and o.trade_no=c.order_trade_no
    and c.receive_time>=concat(date_format(%(date_op)s,'%%Y-%%m'),'-01') 
    and c.receive_time<= CONCAT(%(date_ed)s,' 23:59:59')) f
    """
    # 计时
    会员营销运行开始时间 = dt.datetime.now()
    会员营销 = pd.read_sql(会员营销sql, con=连接对象, params={'date_op': 开始日期, 'date_ed': 结束日期})
    会员营销运行结束时间 = dt.datetime.now()
    会员营销运行sql用时 = (会员营销运行结束时间 - 会员营销运行开始时间).seconds
    print(f'会员营销运行SQL时间为{会员营销运行sql用时}秒')

    # 取读模板
    经营周报 = openpyxl.load_workbook('business_report.xlsx')

    周报 = 经营周报['结算']

    if 科技赋能GMV.shape[0] > 0:
        周报['J5'].value = 科技赋能GMV.loc[0]['全年累计完成']
        周报['I5'].value = 科技赋能GMV.loc[0]['本月完成']
        周报['H5'].value = 科技赋能GMV.loc[0]['本周完成']

    if 本周会员销售额.shape[0] > 0:
        周报['I2'].value = 本周会员销售额.loc[0]['本周会员销售额（万元）']

    if 本年累计会员销售额.shape[0] > 0:
        周报['J2'].value = 本年累计会员销售额.loc[0]['本年累计会员销售额（万元）']

    if 会员注册.shape[0] > 0:
        周报['D2'].value = 会员注册.loc[0]['全年累计完成']
        周报['C2'].value = 会员注册.loc[0]['本月完成']
        周报['B2'].value = 会员注册.loc[0]['本周完成']

    if 付费会员新增.shape[0] > 0:
        周报['D3'].value = 付费会员新增.loc[0]['全年累计完成']
        周报['C3'].value = 付费会员新增.loc[0]['本月完成']
        周报['B3'].value = 付费会员新增.loc[0]['本周完成']

    if 会员营销.shape[0] > 0:
        周报['D4'].value = 会员营销.loc[0]['全年累计完成']
        周报['C4'].value = 会员营销.loc[0]['本月完成']
        周报['B4'].value = 会员营销.loc[0]['本周完成']

    新路径 = output_dir + '经营周报-' + 周报日期 + '.xlsx'
    经营周报.save(新路径)
    print('完成')

if __name__ == '__main__':
    output_dir = './out/经营汇报PPT数据周报/'
    os.system('rm -rf ./out/经营汇报PPT数据周报/*')
    start_rq, end_rq = get_last_week5_to_this_week4(datetime.now().strftime('%Y-%m-%d'), 0)
    week_rq = start_rq[5:7] + start_rq[8:] + '~' + end_rq[5:7] + end_rq[8:]

    print('经营汇报PPT数据周报:')
    print('-----------------------------------------')
    print('开始日期：', start_rq)
    print('结束日期：', end_rq)
    print('周报日期：', week_rq)
    print('-----------------------------------------')

    # write excel
    write_business_report(start_rq,end_rq,week_rq)

    # compressed files
    path_original = 'out/经营汇报PPT数据周报'
    path_zip = './out/经营汇报PPT数据周报/经营汇报PPT数据周报-{}.zip'.format(week_rq)
    zip_name = '经营汇报PPT数据周报-{}.zip'.format(week_rq)
    file_to_zip(path_original, path_zip)
    print('文件打包完成!')

    # send mail
    sender = '190634@lifeat.cn,850646@cre-hopson.com'
    Cc = '820987@cre-hopson.com'
    title = '合生通经营汇报PPT数据周报-{}'.format(week_rq)
    content = '''各位领导：
             {}
             详见附件,请查收。
             '''.format(title)
    os.chdir('./out/经营汇报PPT数据周报')
    file_list = [zip_name]
    ret = SendMail(sender, Cc, title, content).send(file_list)
    print(ret, type(ret))
    print('邮件邮件完成')