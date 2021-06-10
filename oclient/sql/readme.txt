#目标用户
sqlplus / as sysdba
create user test identified by test default tablespace apps;
grant connect,resource,select any dictionary to test;


#模拟正在运行的SQL
select sid from v$mystat where rownum=1;
declare
 n int;
begin
  for i in  1..1000 loop            
    execute immediate 'select count(0) from dba_objects where object_type in(''TABLE'',''VIEW'') and object_name like ''%CON%''' into n;
  end loop;
end;


select sid from v$mystat where rownum=1;
declare
 n int;
begin
  for i in  1..1000000 loop            
    execute immediate 'select  owner,object_type,object_name from dba_objects where owner=''SYS'' and object_type in(''TABLE'',''VIEW'') and object_name like ''%CON%'' order by owner,object_type,object_name' ;
  end loop;
end;


select a.tablespace_name,
       round(sum(a.bytes)/1024/1024,2) as "TOTAL_SIZE(MB)",
       round(sum(b.bytes)/1024/1024,2) as "FREE_SIZE(MB)",  
       round(1-sum(b.bytes)/sum(a.bytes),4)*100||'%' as "USAGE_RATE",
       case when 1-round(sum(b.bytes)/sum(a.bytes),4)>dp_mon_build.get_mon_threshold('TABLESPACE_USAGE_RATE') then 1 else 0 end as "IF_WARN"
from dba_data_files a,dba_free_space b
where a.TABLESPACE_NAME=b.TABLESPACE_NAME
 group by a.tablespace_name

truncate table mon_log;
truncate table mon_log_detail;
select * from MON_THRESHOLD for update
select * from mon_script for update  

select * from mon_log;
select * from mon_log_detail;

truncate table secure;
insert into secure   select * from dba_objects;
insert into secure   select * from secure;
insert into secure   select * from secure;
insert into secure   select * from secure;
insert into secure   select * from secure;
commit;

select count(0) from syn_log where if_syn='N' and tname='LX2' order by syn_id;

update syn_log set if_syn='N' where tname='LX2';

select sum(decode(otype,'I',1,0)) as ins_rec, 
                       sum(decode(otype,'U',1,0)) as upd_rec, 
                       sum(decode(otype,'D',1,0)) as del_rec,
                       sum(decode(otype,'DDL',1,0)) as ddl_rec
                from SYN_LOG where if_syn='N' and tname='LX2'
                
                
select count(0) from syn_log 
               where if_syn='N'  
			 and tname='LX2' 
			   and ddl_stat like '%CREATE%' and ddl_stat like '%TABLE%'
			   
select * from syn_log   where if_syn='N' and tname='LX2' order by syn_id;
 and syn_id between 154 and 1000 
alter session set db_file_multiblock_read_count=256;         
select dp_syn_build.gen_syn_sql(syn_id) as exec_sql,syn_id,if_syn,
                         decode(otype,'D','DML','I','DML','U','DML',otype) as otype
                  from SYN_LOG
                  where if_syn='N' and tname='LX2'
                 order by syn_id         