prompt Importing table mon_script...
set feedback off
set define off
insert into mon_script (ID, ITEM, WARN_SCRIPT, CHECK_SCRIPT, WARN_MESSAGE, CANCEL_MESSAGE)
values (1, 'TABLESPACE_USAGE_RATE', 'select a.tablespace_name,
       round(sum(a.bytes)/1024/1024,2) as "TOTAL_SIZE(MB)",
       round(sum(b.bytes)/1024/1024,2) as "FREE_SIZE(MB)",  
       round(1-sum(b.bytes)/sum(a.bytes),4)*100||''%'' as "USAGE_RATE",
       case when 1-round(sum(b.bytes)/sum(a.bytes),4)>dp_mon_build.get_mon_threshold(''TABLESPACE_USAGE_RATE'') then 1 else 0 end as "IF_WARN"
from dba_data_files a,dba_free_space b
where a.TABLESPACE_NAME=b.TABLESPACE_NAME
 group by a.tablespace_name', 'select  case when 1-round(sum(b.bytes)/sum(a.bytes),4)<dp_mon_build.get_mon_threshold(''TABLESPACE_USAGE_RATE'') then 1
        else 0 end
from dba_data_files a,dba_free_space b
where a.TABLESPACE_NAME=b.TABLESPACE_NAME
  and a.TABLESPACE_NAME=:1', '表空间使用率预警', '表空间使用率预警解除');

prompt Done.
