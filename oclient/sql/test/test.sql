select dp_syn_build.gen_syn_log('TEST','U') from dual;
select dp_syn_build.gen_syn_sql(4) from dual;
select * from SYN_LOG for update;
select * from test for update;
insert into test(empno,ename,job,mgr,hiredate,sal,comm,deptno) 
  values(7780,'MAFEI','CLERK',7902,to_date('20020320','yyyymmdd'),12000,null,20);

update test set sal=sal+1  where empno=7369

delete from test where empno=7780;
insert into test(empno,ename,job,mgr,hiredate,sal,comm,deptno)  values(7780,'MAFEI','CLERK',7902,to_date('20020320','yyyymmdd'),12000,null,20);
update test set ename='ZHDN',JOB='ANALYST',MGR=7782,HIREDATE=to_date('19791005','yyyy-mm-dd'),sal=20000,deptno=10 where empno=7780;


--------------------------------------xs1----------------------------------------------------------
create table xs(xh int primary key,xm varchar2(20),age int,csrq date);
exec dp_syn_build.init_tab('XS');

insert into xs(xh,xm,age,csrq) values(1,'ZHANG.FEI',20,to_date('2002-10-10','yyyy-mm-dd'));
insert into xs(xh,xm,age,csrq) values(2,'CAO.CAO',70,to_date('1900-10-10','yyyy-mm-dd'));
insert into xs(xh,xm,age,csrq) values(3,'WANG.WU',43,to_date('188-7-10','yyyy-mm-dd'));

update xs set xm='CAO.JUN' where xh=2;
update xs set csrq=to_date('2005-5-5','yyyy-mm-dd') where xh=3;
update xs set age=67 where xh=1;

--------------------------------------xs2----------------------------------------------------------
create table xs2(xh int ,xm varchar2(20),age int,csrq date);
exec dp_syn_build.init_tab('XS2');

insert into xs2(xh,xm,age,csrq) values(1,'ZHANG.FEI',20,to_date('2002-10-10','yyyy-mm-dd'));
insert into xs2(xh,xm,age,csrq) values(2,'CAO.CAO',70,to_date('1900-10-10','yyyy-mm-dd'));
insert into xs2(xh,xm,age,csrq) values(3,'WANG.WU',43,to_date('1988-7-10','yyyy-mm-dd'));
insert into xs2(xh,xm,age,csrq) values(4,'WANG.LIU',33,to_date('1968-7-10','yyyy-mm-dd'));

update xs2 set xm='CAO.JUN' where xh=2;
update xs2 set csrq=to_date('2005-5-5','yyyy-mm-dd') where xh=3;
update xs2 set age=67 where xh=1;

-------------------------------------oclient------------------------------------------------------
oclient>call DP_SYN_BUILD.INIT_TAB(xs);
procedure executed!
oclient>call dp_syn_build.init_syn;
procedure executed!



select dp_syn_build.gen_syn_sql(syn_id) as exec_sql,syn_id,if_syn
  from SYN_LOG
  where if_syn='N' and tname='XS5'
 order by syn_time



select sum(decode(otype,'I',1,0)) as "ins_rec", 
       sum(decode(otype,'U',1,0)) as "upd_rec", 
       sum(decode(otype,'D',1,0)) as "del_rec"
from SYN_LOG where if_syn<>'Y' and tname='XS5'    

select client_info from v$session;

------------------------------------------------test----------------------------------------------------------
exec dp_syn_build.init_syn;
exec dp_syn_build.init_tab('TEST');

truncate table test;
truncate table syn_log;
insert into test(id,znxm,sfzh,zcsybm,tjhcbm,tjrq,csyy,syzhm,cszhm,syzh,syrq,rhrq,grzbh,lgrzrq,znswyybm,znswrq,xjtfqsfzh,xjtfqxm,xjtmqsfzh) 
select 
id,znxm,sfzh,zcsybm,tjhcbm,tjrq,csyy,syzhm,cszhm,syzh,syrq,rhrq,grzbh,lgrzrq,znswyybm,znswrq,xjtfqsfzh,xjtfqxm,xjtmqsfzh 
from FAMILYINFO where rownum<10000;
commit;



exec dp_syn_build.init_syn;

create or replace package dp_xs is
  function get_xm(p_xh int) return varchar2;
end;
/
create or replace package body dp_xs is
  function get_xm(p_xh int) return varchar2 is
    v_xm varchar2(20);
  begin
     select xm into v_xm  from xs where xh=p_xh;
     return v_xm;
  exception
    when others then
    return '';
  end;
end;
/

select substr(dp_xs.get_xm(1),1,20) from dual;

create or replace function get_val(i int) return int is
begin
 return i*i;
end;

create or replace procedure upd_sal is 
begin
 update scott.emp set sal=sal+100;
 commit;
end;


create table sys_log(id int,msg varchar2(100));

declare
  n_xh int;
begin
 select max(xh) into n_xh from xs;
 insert into sys_log(id,msg) values(1,n_xh);
 commit;
end;

select substr(id,1,3),substr(msg,1,10) from sys_log;



select otype,dp_syn_build.gen_syn_sql(syn_id) as exec_sql, syn_id, if_syn,syn_time
  from SYN_LOG
 where if_syn = 'N'
   and tname = 'LX10'
order by syn_time;


select * from syn_log 
               where if_syn='N'  
         and tname='LX8' 
         and upper(dp_syn_build.gen_syn_sql(syn_id)) like '%CREATE%' 
         and upper(dp_syn_build.gen_syn_sql(syn_id)) like '%TABLE%'
				 and instr(upper(dp_syn_build.gen_syn_sql(syn_id)),'CREATE')<instr(upper(dp_syn_build.gen_syn_sql(syn_id)),'TABLE') 