/*
SQLyog Ultimate v11.24 (64 bit)
MySQL - 5.6.16-log : Database - hopsonone_do
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
/* Procedure structure for procedure `proc_month_key_index` */

/*!50003 DROP PROCEDURE IF EXISTS  `proc_month_key_index` */;

DELIMITER $$

/*!50003 CREATE DEFINER=`hopsononebi_2019`@`%` PROCEDURE `proc_month_key_index`(p_begin_rq DATE,p_end_rq DATE)
BEGIN
#导入本月会员数据
SET @data_op = p_begin_rq;
SET @data_ed = p_end_rq;
#二、其他活跃会员分别建表
#停车临停(150000)
drop table if exists active_m_id本月;
create table active_m_id本月 as
select i.m_id,i.market_id
from hopsonone_park.park_order i
where i.market_id in (108,110,218)
and i.in_time>=@data_op
and i.in_time<=concat(@data_ed,' 23:59:59')
group by i.m_id;
#登陆APP(6000)
insert into active_m_id本月
select a.m_id,a.market_id
from hopsonone_personal.members_behavior a
where a.market_id in (108,110,218)
and a.create_time>=@data_op
and a.create_time<=concat(@data_ed,' 23:59:59')
group by a.m_id;
#有积分变动(10000)
insert into active_m_id本月
select c.m_id,c.market_id
from hopsonone_point_real_time.members_points_detail c
where c.market_id in (108,110,218)
and c.create_time>=@data_op
and c.create_time<=concat(@data_ed,' 23:59:59')
group by c.m_id;
#有下线消费(40)
insert into active_m_id本月
select d.member_id,d.market_id
from hopsonone_bill.bill d
where d.market_id in (108,110,218)
and d.member_id is not null
and d.member_id !=0
and d.order_time>=@data_op
and d.order_time<=concat(@data_ed,' 23:59:59')
group by d.member_id;
#获取粮票(60)
insert into active_m_id本月
select e.mid,e.market_id
from hopsonone_card.order e
where e.market_id in (108,110,218)
and e.create_time>=@data_op
and e.create_time<=concat(@data_ed,' 23:59:59')
group by e.mid;
#卡券总订单(4000)
insert into active_m_id本月
select l.m_id,l.market_id
from hopsonone_coupons_v2.coupons_order l
where l.market_id in (108,110,218)
and l.create_time>=@data_op
and l.create_time<=concat(@data_ed,' 23:59:59')
group by l.m_id;
#开通超级会员(150)
insert into active_m_id本月
select f.m_id,f.opt_market
from hopsonone_personal.members_plusvip_log f
where f.opt_market in (108,110,218)
and f.create_time>=@data_op
and f.create_time<=concat(@data_ed,' 23:59:59')
group by f.m_id;
#套餐订单
insert into active_m_id本月
select n.mid,n.market_id
from hopsonone_catering.combo_order n
where n.market_id in (108,110,218)
and n.order_dt>=@data_op
and n.order_dt<=concat(@data_ed,' 23:59:59')
group by n.mid;
#商品订单(700)
insert into active_m_id本月
select o.m_id,o.market_id
from mall_goods_v2.goods_order o
where o.market_id in (108,110,218)
and o.create_time>=@data_op
and o.create_time<=concat(@data_ed,' 23:59:59')
group by o.m_id;
END */$$
DELIMITER ;

/* Procedure structure for procedure `proc_month_member_active` */

/*!50003 DROP PROCEDURE IF EXISTS  `proc_month_member_active` */;

DELIMITER $$

/*!50003 CREATE DEFINER=`hopsononebi_2019`@`%` PROCEDURE `proc_month_member_active`(p_begin_rq VARCHAR(20),p_end_rq VARCHAR(20))
BEGIN
#导入本月会员数据
SET @data_op = p_begin_rq;
SET @data_ed = p_end_rq;
set @market_id=108;
#停车临停(150000)
drop table if exists active_m_id本月;
create table active_m_id本月 as
select i.m_id,i.market_id
from hopsonone_park.park_order i
where i.market_id=@market_id
and i.in_time>=@data_op
and i.in_time<=concat(@data_ed,' 23:59:59')
group by i.m_id;
#登陆APP(6000)
insert into active_m_id本月
select a.m_id,a.market_id
from hopsonone_personal.members_behavior a
where a.market_id=@market_id
and a.create_time>=@data_op
and a.create_time<=concat(@data_ed,' 23:59:59')
group by a.m_id;
#有积分变动(10000)
insert into active_m_id本月
select c.m_id,c.market_id
from hopsonone_point_real_time.members_points_detail c
where c.market_id=@market_id
and c.create_time>=@data_op
and c.create_time<=concat(@data_ed,' 23:59:59')
group by c.m_id;
#有下线消费(40)
insert into active_m_id本月
select d.member_id,d.market_id
from hopsonone_bill.bill d
where d.market_id=@market_id
and d.member_id is not null
and d.member_id !=0
and d.order_time>=@data_op
and d.order_time<=concat(@data_ed,' 23:59:59')
group by d.member_id;
#获取粮票(60)
insert into active_m_id本月
select e.mid,e.market_id
from hopsonone_card.order e
where e.market_id=@market_id
and e.create_time>=@data_op
and e.create_time<=concat(@data_ed,' 23:59:59')
group by e.mid;
#卡券总订单(4000)
insert into active_m_id本月
select l.m_id,l.market_id
from hopsonone_coupons_v2.coupons_order l
where l.market_id=@market_id
and l.create_time>=@data_op
and l.create_time<=concat(@data_ed,' 23:59:59')
group by l.m_id;
#开通超级会员(150)
insert into active_m_id本月
select f.m_id,f.opt_market
from hopsonone_personal.members_plusvip_log f
where f.opt_market=@market_id
and f.create_time>=@data_op
and f.create_time<=concat(@data_ed,' 23:59:59')
group by f.m_id;
#套餐订单
insert into active_m_id本月
select n.mid,n.market_id
from hopsonone_catering.combo_order n
where n.market_id=@market_id
and n.order_dt>=@data_op
and n.order_dt<=concat(@data_ed,' 23:59:59')
group by n.mid;
#商品订单(700)
insert into active_m_id本月
select o.m_id,o.market_id
from mall_goods_v2.goods_order o
where o.market_id=@market_id
and o.create_time>=@data_op
and o.create_time<=concat(@data_ed,' 23:59:59')
group by o.m_id;
create index idx_d_n1 ON active_m_id本月(m_id);
END */$$
DELIMITER ;

/* Procedure structure for procedure `proc_month_vip_member_active` */

/*!50003 DROP PROCEDURE IF EXISTS  `proc_month_vip_member_active` */;

DELIMITER $$

/*!50003 CREATE DEFINER=`hopsononebi_2019`@`%` PROCEDURE `proc_month_vip_member_active`(p_begin_rq varchar(20),p_end_rq VARCHAR(20))
BEGIN
#一、临停会员建表
SET @data_op = p_begin_rq;
SET @data_ed = p_end_rq;
SET @market_id=108;
#停车临停(150000)
drop table if exists active_m_idVPI本月;
CREATE TABLE active_m_idVPI本月 AS
select i.m_id as 'mid',i.market_id
from hopsonone_park.park_order i,
hopsonone_personal.members_plusvip_log k
where i.market_id=@market_id
and i.in_time>=@data_op
and i.in_time<=concat(@data_ed,' 23:59:59')
and k.due_time>=@data_op
and i.m_id=k.m_id
group by i.m_id;
#登陆APP(6000)
insert into active_m_idVPI本月
select a.m_id,a.market_id
from hopsonone_personal.members_behavior a,
hopsonone_personal.members_plusvip_log k
where a.market_id=@market_id
and a.create_time>=@data_op
and a.create_time<=concat(@data_ed,' 23:59:59')
and k.due_time>=@data_op
and a.m_id=k.m_id
group by a.m_id;
#有积分变动(10000)
insert into active_m_idVPI本月
select c.m_id,c.market_id
from hopsonone_point_real_time.members_points_detail c,
hopsonone_personal.members_plusvip_log k
where c.market_id=@market_id
and c.create_time>=@data_op
and c.create_time<=concat(@data_ed,' 23:59:59')
and k.due_time>=@data_op
and c.m_id=k.m_id
group by c.m_id;
#有下线消费(40)
insert into active_m_idVPI本月
select d.member_id,d.market_id
from hopsonone_bill.bill d,
hopsonone_personal.members_plusvip_log k
where d.market_id=@market_id
and d.member_id is not null
and d.member_id !=0
and d.order_time>=@data_op
and d.order_time<=concat(@data_ed,' 23:59:59')
and k.due_time>=@data_op
and d.member_id=k.m_id
group by d.member_id;
#获取粮票(60)
insert into active_m_idVPI本月
select e.mid,e.market_id
from hopsonone_card.order e,
hopsonone_personal.members_plusvip_log k
where e.market_id=@market_id
and e.create_time>=@data_op
and e.create_time<=concat(@data_ed,' 23:59:59')
and k.due_time>=@data_op
and e.mid=k.m_id
group by e.mid;
#卡券总订单(4000)
insert into active_m_idVPI本月
select l.m_id,l.market_id
from hopsonone_coupons_v2.coupons_order l,
hopsonone_personal.members_plusvip_log k
where l.market_id=@market_id
and l.create_time>=@data_op
and l.create_time<=concat(@data_ed,' 23:59:59')
and k.due_time>=@data_op
and l.m_id=k.m_id
group by l.m_id;
#套餐订单
insert into active_m_idVPI本月
select n.mid,n.market_id
from hopsonone_catering.combo_order n,
hopsonone_personal.members_plusvip_log k
where n.market_id=@market_id
and n.order_dt>=@data_op
and n.order_dt<=concat(@data_ed,' 23:59:59')
and k.due_time>=@data_op
and n.mid=k.m_id
group by n.mid;
#商品订单(700)
insert into active_m_idVPI本月
select o.m_id,o.market_id
from mall_goods_v2.goods_order o,
hopsonone_personal.members_plusvip_log k
where o.market_id=@market_id
and o.create_time>=@data_op
and o.create_time<=concat(@data_ed,' 23:59:59')
and k.due_time>=@data_op
and o.m_id=k.m_id
group by o.m_id;
CREATE INDEX idx_d_n1 ON active_m_idVPI本月(mid);
END */$$
DELIMITER ;

/* Procedure structure for procedure `proc_week_pos_hz` */

/*!50003 DROP PROCEDURE IF EXISTS  `proc_week_pos_hz` */;

DELIMITER $$

/*!50003 CREATE DEFINER=`hopsononebi_2019`@`%` PROCEDURE `proc_week_pos_hz`()
BEGIN
#汇总
drop table if exists pos_汇总1;
create table pos_汇总1 as
select s.*
from (select p.`商场名称`,p.`楼层`,
round(sum(p.`商户业绩上报营业额`),2) as '商户业绩上报营业额',
round(sum(if(p.`商户是否已开通POS`='是' ,p.`商户业绩上报营业额`,2)),2) as '开通POS商户业绩上报营业额',
round(sum(p.`POS总净收入(元)`),2) as 'POS总收入(元)',
round(sum(p.`非现金收入(元)`),2) as 'POS非现金收入(元)',
concat(round(sum(p.`非现金收入(元)`)/sum(p.`商户业绩上报营业额`)*100,2),'%') as'POS非现金收入(元)/项目全场营业额',	
round(sum(p.`非现金净收入(元)`),2) as 'POS非现金净收入(元)',
concat(round(sum(p.`非现金净收入(元)`)/sum(p.`商户业绩上报营业额`)*100,2),'%') as 'POS非现金净收入(元)/项目全场营业额'
from hopsonone_do.`pos_数据源` p
group by p.`商场名称`,p.`楼层`
union all
select p1.`商场名称`,'业绩未上报商户POS流水' as '楼层',
null as '商户业绩上报营业额',
null as '开通POS商户业绩上报营业额',
round(sum(p1.`POS总净收入(元)`),2) as 'POS总收入(元)',
round(sum(p1.`非现金收入(元)`),2) as 'POS非现金收入(元)',
null as'POS非现金收入(元)/项目全场营业额',	
round(sum(p1.`非现金净收入(元)`),2) as 'POS非现金净收入(元)',
null as 'POS非现金净收入(元)/项目全场营业额'
from hopsonone_do.`pos_未上报业绩商户pos流水明细` p1
group by p1.`商场名称`) s;
drop table if exists pos_汇总2;
create table pos_汇总2 as
select p2.`商场名称`,'总计' as '楼层',
round(sum(p2.`商户业绩上报营业额`),2) as '商户业绩上报营业额',
round(sum(p2.`开通POS商户业绩上报营业额`),2) as '开通POS商户业绩上报营业额',
round(sum(p2.`POS总收入(元)`),2) as 'POS总收入(元)',
round(sum(p2.`POS非现金收入(元)`),2) as 'POS非现金收入(元)',
concat(round(sum(p2.`POS非现金收入(元)`)/sum(p2.`商户业绩上报营业额`)*100,2),'%') as'POS非现金收入(元)/项目全场营业额',	
round(sum(p2.`POS非现金净收入(元)`),2) as 'POS非现金净收入(元)',
concat(round(sum(p2.`POS非现金净收入(元)`)/sum(p2.`商户业绩上报营业额`)*100,2),'%') as 'POS非现金净收入(元)/项目全场营业额'
from hopsonone_do.pos_汇总1 p2
group by p2.`商场名称`;
drop table if exists pos_汇总;
create table pos_汇总 as
select s.*
from (select * from hopsonone_do.pos_汇总1
union all select * from hopsonone_do.pos_汇总2) s
order by s.`商场名称`,s.`楼层`;
END */$$
DELIMITER ;

/* Procedure structure for procedure `proc_week_pos_source` */

/*!50003 DROP PROCEDURE IF EXISTS  `proc_week_pos_source` */;

DELIMITER $$

/*!50003 CREATE DEFINER=`hopsononebi_2019`@`%` PROCEDURE `proc_week_pos_source`(p_begin_rq date,p_end_rq date)
BEGIN
set @date_op = p_begin_rq;
set @date_ed = p_end_rq;
#pos交易流水表
drop table if exists pos_jyls;
create table pos_jyls as
SELECT
o.market_id as '项目ID',
o.hst_business_id as '合生通商家编码',
o.business_id as 'POS商家编码',
round(ifnull(sum(case when p.sub_code = 1 and p.trans_way != 3 and p.trans_type = 1 then o.total_amount END)/100,0),2) as '非现金收入(元)',
round(ifnull(sum(case when p.sub_code = 1 and p.trans_way != 3 and p.trans_type in (2,3) then o.total_amount END)/100,0),2) as '非现金退款(元)',
round(ifnull(sum(case when p.sub_code = 1 and p.trans_way = 3 and p.trans_type = 1 then o.total_amount END)/100,0),2) as '现金收入(元)',
round(ifnull(sum(case when p.sub_code = 1 and p.trans_way = 3 and p.trans_type in (2,3) then o.total_amount END)/100,0),2) as '现金退款(元)',
count(*) as '交易笔数'
FROM hopson_hft_real_time.intel_order o,
hopson_hft_real_time.intel_order_payment p
where o.trade_no = p.trade_no 
and o.market_id in (108,110,132,164,213,218,237,234,278,159,287,306) 
and o.create_time >=@date_op
and o.create_time <= concat(@date_ed,' 23:59:59')
GROUP BY o.hst_business_id;
create index idx_d_n1 on pos_jyls(`合生通商家编码`);
#pos开通明细表
drop table if exists pos_ktmx;
create table pos_ktmx as
select 
b.identification as '合生通商家编码'
from hopson_hft.business b,
hopson_hft.business_terminal t
where b.pos_type in (1,2) 
and b.status = 1 
and b.business_id = t.business_id
and b.market_id in (108,110,132,164,213,218,237,234,278,159,287,306)
group by b.identification;
create index idx_d_n1 on pos_ktmx(`合生通商家编码`);
#pos商户信息表
drop table if exists pos_shxx;
create table pos_shxx as
select
a.`商家编码`,
a.`项目ID`,
a.`商场名称`,
a.`商家名称`,
a.`租金架构`,
a.`收银方式（合同）`,
a.`收银方式（实际）`,
d.dic_desc as '业态',
b.dic_desc as '楼层',
a.`铺位号`
from (SELECT
k.store_id as '商家编码',
b.market_id as '项目ID',
m.market_name as '商场名称',
k.store_name as '商家名称',
k.store_berth as '铺位号',
case rentName
when 'RT102' then '二者取高'
when 'RT100' then '固定租金'
when 'RT101' then '提成租金'
end as '租金架构',
case 
when locate(cashName,0) then ''
when locate(cashName,1) then 'POS机'
when locate(cashName,2) then '小黑盒'
when locate(cashName,3) then '手工报表'
when locate(cashName,4) then '手工报表'
when locate(cashName,5) then '手工报表'
when cashName='3,4' then '手工报表,手工报表'
else cashName
end as '收银方式（合同）',
case 
when locate(pos_access,0) then '小黑盒'
when locate(pos_access,1) then ''
when locate(pos_access,2) then ''
when locate(pos_access,3) then '传统POS'
when locate(pos_access,4) then '智能POS'
when locate(pos_access,5) then '手工报表'
when pos_access='1,3' then '传统POS'
when pos_access='1,4' then '智能POS'
when pos_access='1,5' then '手工报表'
when pos_access='2,0' then '小黑盒'
when pos_access='2,4' then '智能POS'
when pos_access='2,5' then '手工报表'
when pos_access='0,4' then '小黑盒,智能POS'
when pos_access='0,5' then '小黑盒,手工报表'
when pos_access='3,4' then '传统POS,智能POS'
when pos_access='4,0' then '智能POS,小黑盒'
when pos_access='4,3' then '智能POS,传统POS'
when pos_access='4,5' then '智能POS,手工报表'
when pos_access='5,4' then '手工报表,智能POS'
else pos_access
end as '收银方式（实际）',
k.format_type as 'business_type',
k.floor_type as 'business_fool',
b.market_id
FROM merchant_entity.market m,
merchant_entity.entity_store_market b,
(select store_id,floor_type,format_type,store_name,store_berth,m.cashName,m.rentName
from merchant_entity.entity_store s
left join hopsonone_do.pos_mongo m
on m.businessId=s.store_id) k
WHERE b.market_id in (108,110,132,164,213,218,237,234,278,159,287,306)
and b.market_id = m.id
and k.store_id=b.store_id) a
LEFT JOIN
(select s.market_id,s.dic_desc,s.dic_value
from hopsonone_cms.sys_dic s,
merchant_entity.market m
where s.type_name = 'operationtype' 
and s.market_id in (108,110,132,164,213,218,237,234,278,159,287,306)
and s.market_id=m.id) d
on a.market_id=d.market_id and d.dic_value=a.business_type
LEFT JOIN
(select s.market_id,s.dic_desc,s.dic_value
from hopsonone_cms.sys_dic s,
merchant_entity.market m
where s.type_name = 'floortype'
and s.market_id in (108,110,132,164,213,218,237,234,278,159,287,306)
and s.market_id=m.id) b
on a.market_id=b.market_id and b.dic_value=a.business_fool
group by a.`商家编码`;
create index idx_d_n1 on pos_shxx(`商家编码`);
#pos商户业绩上报表
drop table if exists pos_yjsb;
create table pos_yjsb as
SELECT
d.business_id as '商家编码',
round(sum(d.adjust_tax_sales_amount_month) / 100,2) as '商户业绩上报营业额'
from shop_side_operation_real_time.sales_report_details d,
merchant_entity.entity_store b
where d.trade_date >=@date_op
and d.trade_date <= concat(@date_ed,' 23:59:59')
and b.market_id in (108,110,132,164,213,218,237,234,278,159,287,306)
and d.business_id = b.store_id
group by d.business_id;
create index idx_d_n1 on pos_yjsb(`商家编码`);
#pos数据源
drop table if exists pos_数据源;
create table pos_数据源 as
select s.`项目ID`,s.`商场名称`,y.`商家编码`,s.`商家名称`,s.`租金架构`,
s.`收银方式（合同）`,s.`收银方式（实际）`,s.`业态`,s.`楼层`,s.`铺位号`,
if(k.`合生通商家编码` is null ,'否','是') as '商户是否已开通POS',
if(h.business_id is not null,'是',' ') as '是否是海信POS',
y.`商户业绩上报营业额`,
j.`交易笔数`,j.`非现金收入(元)`,j.`非现金退款(元)`,
j.`非现金收入(元)`-j.`非现金退款(元)` as '非现金净收入(元)',
j.`现金收入(元)`,j.`现金退款(元)`,
j.`现金收入(元)`-j.`现金退款(元)` as '现金净收入(元)',
j.`非现金收入(元)`+j.`现金收入(元)` as 'POS总收入(元)',
j.`非现金收入(元)`+j.`现金收入(元)`-j.`非现金退款(元)`-j.`现金退款(元)` as 'POS总净收入(元)',
concat(round(j.`非现金收入(元)`/y.`商户业绩上报营业额`*100,2),'%') as '非现金收入(元)/商户业绩上报营业额(%)',
concat(round((j.`非现金收入(元)`-j.`非现金退款(元)`)/y.`商户业绩上报营业额`*100,2),'%') as '非现金净收入/商户业绩上报营业额(%)'
from hopsonone_do.pos_yjsb y
left join hopsonone_do.pos_shxx s on y.`商家编码`=s.`商家编码`
left join hopsonone_do.pos_jyls j on y.`商家编码`=j.`合生通商家编码`
left join hopsonone_do.pos_ktmx k on y.`商家编码`=k.`合生通商家编码`
left join hopsonone_do.pos_218hx h on y.`商家编码`=h.business_id
order by s.`项目ID`;
create index idx_d_n1 on pos_数据源(`商家编码`);
#pos_未上报业绩商户pos流水明细
drop table if exists pos_未上报业绩商户pos流水明细;
create table pos_未上报业绩商户pos流水明细 as
select j.`项目ID`,
case j.`项目ID`
when '132' then '广州海珠合生广场(南)'
when '237' then '广州增城合生汇'
when '234' then '上海青浦合生新天地'
when '108' then '成都温江合生汇'
when '110' then '上海五角场合生汇'
when '164' then '北京木樨园合生广场'
when '213' then '北京合生麒麟新天地'
when '218' then '北京朝阳合生汇'
when '278' then '广州海珠合生新天地'
when '287' then '西安南门合生汇'
when '306' then '上海MOHO'
else j.`项目ID`
end as '商场名称',
j.`合生通商家编码`,s.`商家名称`,s.`租金架构`,s.`收银方式（合同）`,s.`收银方式（实际）`,s.`业态`,s.`楼层`,s.`铺位号`,
'是' as '商户是否已开通POS',
if(h.business_id is not null,'是',' ') as '是否是海信POS',
j.`交易笔数`,j.`非现金收入(元)`,j.`非现金退款(元)`,
j.`非现金收入(元)`-j.`非现金退款(元)` as '非现金净收入(元)',
j.`现金收入(元)`,j.`现金退款(元)`,
j.`现金收入(元)`-j.`现金退款(元)` as '现金净收入(元)',
j.`非现金收入(元)`+j.`现金收入(元)` as 'POS总收入(元)',
j.`非现金收入(元)`+j.`现金收入(元)`-j.`非现金退款(元)`-j.`现金退款(元)` as 'POS总净收入(元)'
from 
(select j.*
from hopsonone_do.pos_jyls j
left join hopsonone_do.pos_yjsb y on j.`合生通商家编码`=y.`商家编码`
where y.`商家编码` is null) j
left join hopsonone_do.pos_shxx s on j.`合生通商家编码`=s.`商家编码`
left join hopsonone_do.pos_218hx h on j.`合生通商家编码`=h.business_id
order by j.`项目ID`;
END */$$
DELIMITER ;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
