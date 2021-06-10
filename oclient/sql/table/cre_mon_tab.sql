create user oclient identified by oclient;
grant connect,dba,resource to oclient;
grant select any dictionary to oclient; 

create sequence mon_log_seq
minvalue 1
maxvalue 999999999999
start with 1
increment by 1
cache 200
order;

create sequence mon_log_detail_seq
minvalue 1
maxvalue 999999999999
start with 1
increment by 1
cache 200
order;

drop table mon_log;
create table MON_LOG
(
  mon_id      INTEGER not null,
  item_name   VARCHAR2(50) not null,
  item_value  VARCHAR2(100) not null,
  mail_header VARCHAR2(100),
  mail_body   VARCHAR2(4000),
  fail_times  INTEGER,
  if_handle   CHAR(1),
  created     DATE default sysdate,
  updated     DATE default sysdate,
  first_send  INTEGER default 0,
  second_send INTEGER default 0,
  third_send  INTEGER default 0
);

alter table MON_LOG
  add primary key (MON_ID)
  
drop table mon_log_detail;
create table MON_LOG_DETAIL
(
  mon_detail_id INTEGER not null,
  item_name     VARCHAR2(50) not null,
  item_value    VARCHAR2(100) not null,
  mail_header   VARCHAR2(100),
  mail_body     VARCHAR2(4000),
  if_sendmail   CHAR(1),
  if_handle     CHAR(1),
  mon_id        INTEGER
);


drop table mon_script;
create table MON_SCRIPT
(
  id             INTEGER not null,
  item           VARCHAR2(50) not null,
  warn_script    VARCHAR2(1000) not null,
  check_script   VARCHAR2(1000) not null,
  warn_message   VARCHAR2(1000) not null,
  cancel_message VARCHAR2(1000)
);

create table MON_THRESHOLD
(
  mon_item unique VARCHAR2(100),
  mon_value VARCHAR2(100)
);


  
