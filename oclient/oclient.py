#!/usr/bin/env python3
import sys
import cx_Oracle
import traceback
import os
import configparser
import time
import threading
import datetime
import xlwt
import smtplib
from email.mime.text import MIMEText

class ProgressBar:
    def __init__(self, count = 0, total = 0, width = 50):
        self.count = count
        self.total = total
        self.width = width
    def move(self):
        self.count += 1
    def log(self, s):
        sys.stdout.write(' ' * (self.width + 9) + '\r')
        sys.stdout.flush()
        #print(s)
        progress = self.width * self.count / self.total
        sys.stdout.write('{0:3}/{1:3}: '.format(self.count, self.total))
        sys.stdout.write('#' * int(progress) + '-' * (self.width - int(progress)) + '\r')
        #if progress == self.width:
        #  sys.stdout.write('\n')
        sys.stdout.flush()

def func(bar,val,total):
        global timer
        bar.move()
        bar.log("")
        time.sleep(1)
        val=val+1
        if val==total+1:
          sys.exit(0)
        timer=threading.Timer(2,func(bar,val,total))
        timer.start()

def get_db(conn_string,conn_user):
    v_ip         = conn_string.split(':')[0]
    n_port       = int(conn_string.split(':')[1])
    v_instance   = conn_string.split(':')[2]
    v_user       = conn_user.split(':')[0]
    v_pass       = conn_user.split(':')[1]
    #print(v_ip,n_port,v_instance,v_user,v_pass)
    tns          = cx_Oracle.makedsn(v_ip,n_port,v_instance)
    db           = cx_Oracle.connect(v_user,v_pass,tns)
    return db

def get_sess(db):
   sql="""select 
           substr(to_char(s.sid),1,4) sid,
           substr(to_char(p.spid),1,8) spid,
           substr(s.machine,1,20) machine,
           substr(s.program,1,20) program,
           substr(s.client_info,1,16) client_ip,  --must create database trigger
           substr(to_char(s.logon_time,'yyyy.mm.dd hh24:mi:ss'),1,19) logon_time,
           substr(s.status,1,8) status
          from v$session s,v$process p
          where s.schemaname<>'SYS'
            and s.paddr=p.addr
          order by s.logon_time """
   sqlSelect(db,sql)    

def get_sess_sid(db,param):
   sql="""select
           substr(to_char(s.sid),1,4) sid,
           substr(to_char(s.serial#),1,4) serial,
           substr(to_char(p.spid),1,8) spid,
           substr(s.machine,1,20) machine,
           s.action,
           s.module, 
           s.osuser,
           s.program, 
           rawtohex(s.saddr) saddr,
           rawtohex(s.paddr) paddr,
           to_char(s.SQL_HASH_VALUE) sql_hash_value,
           to_char(s.PREV_HASH_VALUE) prev_hash_value,
           p.spid,
           s.event,
           s.logon_time,
           substr(s.client_info,1,16) client_ip,  --must create database trigger
           substr(to_char(s.logon_time,'yyyy.mm.dd hh24:mi:ss'),1,19) logon_time,
           substr(s.status,1,8) status,
           to_char(s.sql_id)
          from v$session s,v$process p
          where s.schemaname<>'SYS'
            and s.paddr=p.addr
            and s.sid='{0}'
          order by s.logon_time """.format(param)
   print("Current SID :{0} for details:".format(param))
   sqlSelectPortrait(db,sql)
   print("\nCurrent Executing SQL:\n"+'-'.ljust(50,'-'))
   sql="""select 
             dp_ops_build.get_run_sql(s.sid) as "exesql"
          from v$session s
          where s.sid='{0}'""".format(param)
   print(get_sql_val(db,sql))
 
def get_wait(db):
   sql="""select 
              substr(to_char(sid),1,4) sid,
              nvl(substr(to_char((select spid 
                                  from v$process p ,v$session s
                                  where p.addr=s.paddr and s.sid=w.sid)),1,6),' ') SPID,
              substr(event,1,50) event,
              substr(state,1,10) state,
              nvl(substr(to_char((select sid 
                              from v$session 
                              where taddr=(select addr from v$transaction
                                           where xidsqn=w.p3))),1,4),' ') block 
          from v$session_wait w
          where event not like '%message%'
          order by sid"""
   sqlSelect(db,sql)

 
def debug_info(bz,source_sql,exec_sql):
    if bz.upper()=="Y" :
      print("source sql:")
      print('-'.ljust(60,'-'))
      print(source_sql)
      print("exec sql:")
      print('-'.ljust(60,'-'))
      print(exec_sql)

def exception_info():
    e_str=traceback.format_exc()
    while True:
      if e_str[-1]=='\n' or e_str[-1]=='\r' :
        e_str=e_str[0:-1]
        continue
      else:
        break
    print(e_str[e_str.find("ORA"):])

def exception_logon_info():
    e_str=traceback.format_exc()
    while True:
      if e_str[-1]=='\n' or e_str[-1]=='\r' :
        e_str=e_str[0:-1]
        continue
      else:
        break
    if e_str[e_str.find("ORA"):]=="\'":
      print('Please: oclient -h for help!')
    else:
      print(e_str[e_str.find("ORA"):])

def exception_info2():
    e_str=traceback.format_exc()
    print(e_str)

def exception_info3(v_str):
    e_str=traceback.format_exc()
    while True:
      if e_str[-1]=='\n' or e_str[-1]=='\r' :
        e_str=e_str[0:-1]
        continue
      else:
        break
    print(v_str+e_str[e_str.find("ORA"):])

def get_sid(db):
    cr=db.cursor()
    sql="select sid from v$mystat where rownum=1"
    cr.execute(sql)
    rs=cr.fetchall()
    #print(rs[0],type(rs[0]),rs[0][0])
    cr.close()
    return str(rs[0][0])

def sqlSelectParam(db,param):
    cr=db.cursor()
    sql="select name,value from v$parameter where name like '%{0}%'".format(param.lower())
    cr.execute(sql)
    rs=cr.fetchall()
    cr.close()
    print(out_char.ljust(80,out_char))
    for i in rs:
        s=''
        for j in range(len(i)):
           s=s+str(i[j]).ljust(40,' ')+'  '
        print(s)

def sqlSelectProp(db,prop):
    cr=db.cursor()
    sql="select property_name,property_value from database_properties where property_name like '%{0}%'".format(prop.upper())
    cr.execute(sql)
    rs=cr.fetchall()
    cr.close()
    print(out_char.ljust(80,out_char))
    for i in rs:
      s=''
      for j in range(len(i)):
         s=s+str(i[j]).ljust(40,' ')+'  '
      print(s)

def sqlSelectTableSpace(db):
    sql="""select substr(a.tablespace_name,1,20) as "TABLESPACE_NAME",
                  substr(to_char(round(sum(a.bytes)/1024/1024,2)),1,10) as "TOTAL(MB)",
                  substr(to_char(round(sum(b.bytes)/1024/1024,2)),1,10) as "FREE(MB)",
                  substr(to_char(round((1-sum(b.bytes)/sum(a.bytes)),4)*100||'%'),1,10) as "USAGE%"
           from dba_data_files a,dba_free_space b
           where a.TABLESPACE_NAME=b.TABLESPACE_NAME
             and a.FILE_ID=b.FILE_ID
          group by a.tablespace_name  
          order by a.tablespace_name"""
    sqlSelect(db,sql)

def sqlSelectDataBase(db):
    sql="""SELECT open_mode,switchover_status,
                  dataguard_broker, guard_status,
                  database_role,force_logging,
                  log_mode, protection_mode, protection_level,
                  name,controlfile_type,fs_failover_status
         FROM V$DATABASE"""
    sqlSelectPortrait(db,sql)

def get_sql_val(db,sql):
    cr=db.cursor()
    cr.execute(sql)
    rs=cr.fetchone()	
    val=str(rs[0]).strip()
    cr.close()
    return val

def get_inst(db):	
    sql="""select substr(to_char(instance_number),1,5) as instance_number,
                  instance_name,
                  substr(host_name,1,30) as host_name ,
                  version,
                  substr(to_char(startup_time,'yyyy-mm-hh hh24:mi:ss'),1,20) as startup_time,
                  status,
                  substr(database_status,1,15) as database_status,
                  substr(instance_role,1,15) as instance_role,
		  active_state,blocked
	    from V$INSTANCE"""
    sqlSelectPortrait(db,sql)

def get_tran(db):
    sql="""select
             substr(to_char(to_date(t.start_time,'mm/dd/RRRR hh24:mi:ss'),'RRRR-MM-DD HH24:mi:ss'),1,20) as "START_RQ",
             substr(to_char(round((sysdate-to_date(t.start_time,'mm/dd/RRRR hh24:mi:ss'))*24*60,2)),1,10) as "RUN(MIN)", 
             substr(to_char(t.ubablk),1,10) as "UBABLK",
             substr(w.event,1,30) as "EVENT",
             substr(s.status,1,15) as "SES_STATUS", 
             substr(t.status,1,15) as "TRANS_STATUS"
           from v$transaction t ,v$session s,v$session_wait w
           where t.ses_addr=s.SADDR 
             and s.sid=w.sid
           order by (sysdate-to_date(start_time,'mm/dd/RRRR hh24:mi:ss'))*24*60 desc ,ubablk desc"""
    sqlSelect(db,sql)

def get_lock(db):
    sql="""select
             substr(to_char(sid),1,4) sid,
             substr(decode(type,
               'MR', 'Media Recovery',
               'RT', 'Redo Thread',
               'UN', 'User Name',
               'TX', 'Transaction',
               'TM', 'DML',
               'UL', 'PL/SQL User Lock',
               'DX', 'Distributed Xaction',
               'CF', 'Control File',
               'IS', 'Instance State',
               'FS', 'File Set',
               'IR', 'Instance Recovery',
               'ST', 'Disk Space Transaction',
               'TS', 'Temp Segment',
               'IV', 'Library Cache Invalidation',
               'LS', 'Log Start or Switch',
               'RW', 'Row Wait',
               'SQ', 'Sequence Number',
               'TE', 'Extend Table',
               'TT', 'Temp Table', type),1,20) lock_type,
             substr(decode(lmode,
               0, 'None',          
               1, 'Null',          
               2, 'Row-S (SS)',    
               3, 'Row-X (SX)',     
               4, 'Share',         
               5, 'S/Row-X (SSX)', 
               6, 'Exclusive',     
               to_char(lmode)),1,12) as "HELD",
             substr(decode(request,
               0, 'None',          
               1, 'Null',          
               2, 'Row-S (SS)',     
               3, 'Row-X (SX)',    
               4, 'Share',         
               5, 'S/Row-X (SSX)',  
               6, 'Exclusive',     
               to_char(request)),1,12) as "REQUESTED",
               substr((select object_name from user_objects where object_id=id1),1,20) as obj_name, 
               substr(to_char(decode(type,'TX',id2)),1,6) as "XIDSQN",
               substr(to_char(ctime),1,10) as "ELAPSED(s)",
             substr(decode(block,
               0, 'Not Blocking', 
               1, 'Blocking',  
               2, 'Global', 
               to_char(block)),1,15) as "STATUS"
             from v$lock     
             where type in('TX','TM') and (ctime>60 or block=1)   
             order by type"""
    sqlSelect(db,sql)

def get_server_info(db):
    cr=db.cursor()
    sql="select BANNER from v$version"
    cr.execute(sql)
    rs=cr.fetchall()
    #print(rs[0],type(rs[0]),rs[0][0])
    for i in rs:     
       print(i[0])
    cr.close()
    #return str(rs[0][0])

def get_table_defi(db,owner,obj):
    cr=db.cursor()
    if owner=="":
      sql="select dp_ops_build.get_tab_ddl(user,'{0}') from dual".format(obj.upper())
    else:
      sql="select dp_ops_build.get_tab_ddl('{0}','{1}') from dual".format(owner.upper(),obj.upper())
    cr.execute(sql)
    rs=cr.fetchone()
    cr.close()
    #print(rs)
    return rs[0]

def get_index_defi(db,owner,obj):
    cr=db.cursor()
    if owner=="":
      sql="select dp_ops_build.get_ind_ddl(user,'{0}') from dual".format(obj.upper())
    else:
      sql="select dp_ops_build.get_ind_ddl('{0}','{1}') from dual".format(owner.upper(),obj.upper())
    cr.execute(sql)
    rs=cr.fetchone()
    cr.close()
    return rs[0] 

def get_msg(sql,cnt):
   msg="" 
   if "update" == sql.lower().strip()[0:6] :
     msg="update {0} completed!".format(cnt)
   elif "delete" == sql.lower().strip()[0:6]:
     msg="delete {0} completed!".format(cnt)
   elif "select" == sql.lower().strip()[0:6]:
     msg="select {0} rows is ok!".format(cnt)
   elif "insert" == sql.lower().strip()[0:6]:
     msg="insert {0} completed!".format(cnt)
   elif "truncate" == sql.lower().strip()[0:8]:
     msg="Table Truncated!"
   elif "alter" == sql.lower().strip()[0:5] and "table" in sql.lower():
     msg="Table altered!"
   elif "drop" == sql.lower().strip()[0:4] and "table" in sql.lower():
     msg="Table dropped!"
   elif "drop" in sql.lower().strip()[0:4] and "index" in sql.lower():
     msg="Index dropped!"
   elif "create" == sql.lower().strip()[0:6] and "table" in sql.lower():
     msg="Table created!"
   elif "create" == sql.lower().strip()[0:6] and "index" in sql.lower():
     msg="Index created!"
   elif "create" == sql.lower().strip()[0:6] and "view" in sql.lower():
     msg="View Created!"
   elif "create" == sql.lower().strip()[0:6] and "user" in sql.lower():
     msg="User Created!"
   elif "create" == sql.lower().strip()[0:6] and "function" in sql.lower() and "package" not in sql.lower():
     msg="Function Created!"
   elif "create" == sql.lower().strip()[0:6] and "procedure" in sql.lower() and "package" not in sql.lower():
     msg="procedure Created!" 
   elif "create" == sql.lower().strip()[0:6] and "trigger" in sql.lower():
     msg="Trigger Created!"
   elif "create" == sql.lower().strip()[0:6] and "package body" in sql.lower():
     msg="Package Body Created!"
   elif "create" == sql.lower().strip()[0:6] and "package" in sql.lower() and "body" not in sql.lower():
     msg="Package spec Created!"
   elif "declare" == sql.lower().strip()[0:7] or "begin"== sql.lower().strip()[0:5]:
     msg="plsql block executed!"
   elif "grant" == sql.lower().strip()[0:5]:
     msg="Grant privileges success!"
   elif "revoke" == sql.lower().strip()[0:6]:
     msg="Revoke privileges success!"
   elif "call" in sql.lower().strip():
     msg="procedure executed!"
   elif "okill" in sql.lower().strip():
     msg="Process killed!"
   elif "kill" in sql.lower().strip():
     msg="Session killed!"
   elif "commit" in sql.lower().strip():
     msg="Committed!"
   elif "rollback" in sql.lower().strip():
     msg="Rollback!"
   else:
     ""
   print(msg)   

def sqlDML(db,sql,IF_COMMIT):
    cr=db.cursor()
    cr.execute(sql)
    rec=cr.rowcount
    cr.close()
    if IF_COMMIT.strip()=="Y":
       db.commit()
    get_msg(sql,rec)

def db_commit(db):
    db.commit()
    get_msg("commit","")

def db_rollback(db):
    db.rollback()
    get_msg("rollback","")

def sqlDDL(db,sql,is_debug):
    cr=db.cursor()
    if is_debug.upper()=="Y":
       print("DDL",sql)
    cr.execute(sql)
    cr.close()
    db.commit()
    get_msg(sql,"")

def killSESS(db,n_sid):
   cr=db.cursor()
   sql="select serial# from v$session where sid={0}".format(n_sid)
   cr.execute(sql)
   rs=cr.fetchall()
   n_serial=str(rs[0][0])
   sql="alter system kill session '{0},{1}' immediate".format(n_sid,n_serial)
   cr.execute(sql)
   cr.close()
   db.commit()
   get_msg("kill","")

def killSPID(spid):
   #print("killSPID")
   v=os.system("kill -9 "+spid)
   get_msg("okill","")

def callPROC(db,pname,param):
    cr=db.cursor()
    cr.callproc(pname,param)
    cr.close()
    get_msg("call","")

def sqlSelect(db,sql):
    cr=db.cursor()
    cr.execute(sql)
    desc=cr.description
    #for i in range(len(desc)):
    # print(desc[i])
    
    col_total_len=10
    col_len=0
    col_name=""
    col_type=""
    for i in range(len(desc)):
        if int(desc[i][2])>=len(desc[i][0]):
          col_len=int(desc[i][2])
        else:
          col_len=len(desc[i][0])
        
        col_type=str(desc[i][1]).split(".")[1].replace(">","").replace("'","")
        if col_type=="DATETIME":
          col_len=10
        if col_type=="NUMBER":
          col_len=10
        col_total_len+=col_len
        col_name=col_name+desc[i][0].ljust(col_len,' ')+'  '

    print(col_name)
    #print('-'.ljust(col_total_len,'-'))    
    print('-'.ljust(len(col_name),'-'))
    rs=cr.fetchall()
    for i in rs:
       s=''
       for j in range(len(i)):
           if int(desc[j][2])>=len(desc[j][0]):
              col_len=int(desc[j][2])
           else:
              col_len=len(desc[j][0])
           
           col_type=str(desc[j][1]).split(".")[1].replace(">","").replace("'","")    
           if col_type=="DATETIME":
             s=s+str(i[j])[0:10].ljust(10,' ')+'  '
           elif col_type=="NUMBER":
             s=s+str(i[j])[0:10].ljust(10,' ')+'  '
           else:
             s=s+str(i[j]).ljust(col_len,' ')+'  '
       print(s)
    cr.close()

def sqlSelectPortrait(db,sql):
    cr=db.cursor()
    cr.execute(sql)
    desc=cr.description
    #for i in range(len(desc)):
    #  print(desc[i])
    rs=cr.fetchone()	
    col_len=0
    col_name=""
    col_type=""
    col_desc=""
    col_max_len=0
    #calc col_max_len
    for i in range(len(rs)):
        if col_max_len<len(desc[i][0]):
           col_max_len=len(desc[i][0])

    print('-'.ljust(50,'-'))
    for i in range(len(rs)):
        col_desc=desc[i][0]
        if int(desc[i][2])>=len(desc[i][0]):
          col_len=int(desc[i][2])
        else:
          col_len=len(desc[i][0])

        col_type=str(desc[i][1]).split(".")[1].replace(">","").replace("'","")    
        if col_type=="DATETIME":
           col_name=str(rs[i])[0:9].ljust(10,' ')
        else:      
           col_name=str(rs[i]).ljust(col_len,' ')
        print(col_desc.ljust(col_max_len+6,' ')+":"+col_name)	 
    cr.close()

def get_transfer_db(fname):
    config=configparser.ConfigParser()
    config.read(fname)
    fromdb_ip    = config.get("TRANSFER","from_ip")
    fromdb_port  = config.get("TRANSFER","from_port")
    fromdb_sid   = config.get("TRANSFER","from_sid")
    fromdb_user  = config.get("TRANSFER","from_user")
    fromdb_pass  = config.get("TRANSFER","from_pass")
    todb_ip      = config.get("TRANSFER","to_ip")
    todb_port    = config.get("TRANSFER","to_port")
    todb_sid     = config.get("TRANSFER","to_sid")
    todb_user    = config.get("TRANSFER","to_user")
    todb_pass    = config.get("TRANSFER","to_pass")
    tname        = config.get("TRANSFER","tname")
    from_string  = fromdb_ip+":"+fromdb_port+":"+fromdb_sid
    to_string    = todb_ip+":"+todb_port+":"+todb_sid
    #print("fromdb info   :",fromdb_ip,fromdb_port,fromdb_sid,fromdb_user,fromdb_pass)
    #print("todb info     :",todb_ip,todb_port,todb_sid,todb_user,todb_pass)
    #print("trnsfer table :",tname)
    fromtns      = cx_Oracle.makedsn(fromdb_ip,fromdb_port,fromdb_sid)
    totns        = cx_Oracle.makedsn(todb_ip,todb_port,todb_sid)
    fromdb       = cx_Oracle.connect(fromdb_user,fromdb_pass,fromtns)
    todb         = cx_Oracle.connect(todb_user,todb_pass,totns)
    return fromdb,todb,fromdb_user,todb_user,tname,from_string,to_string

def get_sync_db(fname):
    config=configparser.ConfigParser()
    config.read(fname)
    fromdb_ip    = config.get("SYNC","from_ip")
    fromdb_port  = config.get("SYNC","from_port")
    fromdb_sid   = config.get("SYNC","from_sid")
    fromdb_user  = config.get("SYNC","from_user")
    fromdb_pass  = config.get("SYNC","from_pass")
    todb_ip      = config.get("SYNC","to_ip")
    todb_port    = config.get("SYNC","to_port")
    todb_sid     = config.get("SYNC","to_sid")
    todb_user    = config.get("SYNC","to_user")
    todb_pass    = config.get("SYNC","to_pass")
    tname        = config.get("SYNC","tname")
    from_string  = fromdb_ip+":"+fromdb_port+":"+fromdb_sid
    to_string    = todb_ip+":"+todb_port+":"+todb_sid
    #print("fromdb info   :",fromdb_ip,fromdb_port,fromdb_sid,fromdb_user,fromdb_pass)
    #print("todb info     :",todb_ip,todb_port,todb_sid,todb_user,todb_pass)
    #print("trnsfer table :",tname)
    fromtns      = cx_Oracle.makedsn(fromdb_ip,fromdb_port,fromdb_sid)
    totns        = cx_Oracle.makedsn(todb_ip,todb_port,todb_sid)
    fromdb       = cx_Oracle.connect(fromdb_user,fromdb_pass,fromtns)
    todb         = cx_Oracle.connect(todb_user,todb_pass,totns)
    return fromdb,todb,fromdb_user,todb_user,tname,from_string,to_string	

def get_format_tab(tname,touser,is_debug):
    s=tname[0:tname.rfind(")")+1].replace("\"","").upper().replace(touser.upper()+'.',"")
    if is_debug.upper()=="Y":
      print("get_format_tab=",s)
    return s    

def get_insert_header(fromdb,fromuser,touser,tname,is_debug):
    cr=fromdb.cursor()
    sql="select * from "+fromuser+"."+tname
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+touser+"."+tname+"("
    s2=" values("
    for i in range(len(desc)):
      s1=s1+desc[i][0].lower()+','  
      s2=s2+':'+str(i+1)+','
    s1=s1[0:-1]+')'
    s2=s2[0:-1]+')'
    if is_debug.upper()=="Y":
      print("insert_header=",s1+s2)
    cr.close()
    return s1+s2

def chk_syn_tab_rows(from_db,from_user,tname):
    from_cr=from_db.cursor()
    from_sql="select count(0) from SYN_LOG where if_syn='N' and tname='{0}'".format(tname.upper());
    from_cr.execute(from_sql)
    from_rs=from_cr.fetchone()
    n_tab_total_rows=from_rs[0]
    from_cr.close()
    return n_tab_total_rows

def prn_syn_tab_rows(from_db,from_user,tname):
    from_cr=from_db.cursor()
    from_sql="""select sum(decode(otype,'I',1,0)) as ins_rec, 
                       sum(decode(otype,'U',1,0)) as upd_rec, 
                       sum(decode(otype,'D',1,0)) as del_rec,
                       sum(decode(otype,'DDL',1,0)) as ddl_rec
                from SYN_LOG where if_syn='N' and tname='{0}'""".format(tname.upper()) 
    from_cr.execute(from_sql)
    from_rs=from_cr.fetchone()
    #print(from_rs[0],from_rs[1],from_rs[2])
    n_ins=str(from_rs[0])
    n_upd=str(from_rs[1])
    n_del=str(from_rs[2])
    n_ddl=str(from_rs[3])
    print("Sync time  :"+str(datetime.datetime.now())[0:20])
    print("Sync table :insert:{0} rows,update {1} rows ,delete {2} rows,ddl:{3} rows".format(n_ins,n_upd,n_del,n_ddl))
    from_cr.close()

def get_tab_rows(from_db,from_user,tname):
    from_cr=from_db.cursor()
    from_sql="select count(0) from "+from_user+"."+tname
    from_cr.execute(from_sql)
    from_rs=from_cr.fetchone()
    n_tab_total_rows=from_rs[0]
    from_cr.close()
    return n_tab_total_rows

def get_query_rows(from_db,from_user,query):
    from_cr=from_db.cursor()
    from_sql=query[1:-1]
    from_cr.execute(from_sql)
    from_rs=from_cr.fetchall()
    n_tab_total_rows=len(list(from_rs))
    from_cr.close()
    return n_tab_total_rows

def get_syn_tab_list(from_db,from_user,tname):
    from_cr=from_db.cursor()
    from_sql="select table_name from user_tables where table_name<>'SYN_LOG' order by table_name";
    from_cr.execute(from_sql)
    from_rs=from_cr.fetchall()
    v_tab_list=""
    for i in from_rs:
       v_tab_list=v_tab_list+i[0]+","
    from_cr.close()
    return v_tab_list[0:-1]

def get_batch_size(n_tab_rows):
    n_batch_size=0
    if n_tab_rows >= 50000:
      n_batch_size=1000
    elif n_tab_rows >= 10000:
      n_batch_size=800
    elif n_tab_rows >=5000:
      n_batch_size=600
    else:
      n_batch_size=n_tab_rows//2
    return n_batch_size


def transfer_data(from_db,to_db,from_user,to_user,tname,from_string,to_string):
    n_tab_total_rows=get_tab_rows(from_db,from_user,tname)
    n_batch_size=get_batch_size(n_tab_total_rows)
    t_syn_begin_time=datetime.datetime.now()
    print("*".ljust(52,"*"))   
    print("* From database".ljust(20,' ')+":"+from_string.ljust(30,' ')+"*")
    print("* From user".ljust(20,' ')+":"+from_user.ljust(30,' ')+"*")
    print("* To   database".ljust(20,' ')+":"+to_string.ljust(30,' ')+"*")
    print("* To   user".ljust(20,' ')+":"+to_user.ljust(30,' ')+"*")
    print("* Transfer table".ljust(20,' ')+":"+tname.ljust(30,' ')+"*")
    print("* Transfer rows".ljust(20,' ')+":"+str(n_tab_total_rows).ljust(30,' ')+"*")
    print("* Transfer batch".ljust(20,' ')+":"+str(n_batch_size).ljust(30,' ')+"*")
    print("*".ljust(52,"*"))
    if n_tab_total_rows!=0:
      from_cr=from_db.cursor()
      from_sql="select * from "+from_user+"."+tname
      from_cr.execute(from_sql)   
      desc=from_cr.description 
      from_rs=from_cr.fetchmany(n_batch_size)
      ins_sql_header = get_transfer_header(from_db,from_user,to_user,tname)
      to_cr=to_db.cursor()
      print("Transferring Table..."+tname.upper())
      bar = ProgressBar(total = n_tab_total_rows,width =43)
      ins_batch_val=""
      while from_rs:
          ins_batch_val="begin\n"
          for row in list(from_rs):
            ins_val=""
            for j in range(len(row)):
               col_type=str(desc[j][1]).split(".")[1].replace(">","").replace("'","")    
               if col_type=="NUMBER":
                 if row[j] is None:
                    ins_val=ins_val+"null,"
                 else: 
                    ins_val=ins_val+str(row[j])+","
               elif col_type=="STRING":
                 if row[j] is None:
                    ins_val=ins_val+"null,"
                 else:
                    ins_val=ins_val+"'"+row[j]+"',"
               elif col_type=="DATETIME":
                 if row[j] is None:
                     ins_val=ins_val+"null,"
                 else:
                     ins_val=ins_val+"to_date('"+str(row[j])[0:10]+"','yyyy-mm-dd'),"
               else:
                 if row[j] is None:
                    ins_val=ins_val+"null,"
                 else:
                    ins_val=ins_val+row[j]+","
            ins_sql=ins_sql_header+ins_val[0:-1]+');\n'
            ins_batch_val=ins_batch_val+ins_sql
            bar.move()
            bar.log("")
            time.sleep(0.00001)
          ins_batch_val=ins_batch_val+'commit;'+'\nend;'
          to_cr.execute(ins_batch_val)
          from_rs=from_cr.fetchmany(n_batch_size)
      from_cr.close()
      to_cr.close()
      t_syn_end_time=datetime.datetime.now()
      print("");
      print("Transfer complete!")
      print("Transfer total time:"+str((t_syn_end_time-t_syn_begin_time).seconds)+"s");
    
def transfer(fname):
    try:
       fromdb,todb,fromuser,touser,tname,from_string,to_string=get_transfer_db(fname)
       if tname=="*":
         tname=get_syn_tab_list(fromdb,fromuser,tname)
       for tab in tname.split(","):
         if if_exists_tab(todb,touser,tab)>0:
           print("Table {0}.{1} already exists!".format(touser,tab))
         else:      
           todb_tab_sql = get_table_defi(fromdb,fromuser,tab)
           try:         
              sqlDDL(todb,todb_tab_sql[0:-1],"")
           except BaseException:
              print("Transfering {0} Table Error!".format(tab))
              exception_info2()
           transfer_data(fromdb,todb,fromuser,touser,tab,from_string,to_string)
       todb.close()   
    except BaseException:
       exception_info2()

def if_exists_tab(fromdb,fromuser,tname):
   from_cr=fromdb.cursor()
   from_sql="select count(0) from dba_tables where owner='{0}' and table_name='{1}'".format(fromuser.upper(),tname.upper())
   from_cr.execute(from_sql)
   from_rs=from_cr.fetchone()
   n_tab_rows=from_rs[0]
   from_cr.close()
   return n_tab_rows

def if_syn_log_cre_tab(fromdb,fromuser,tname):
   from_cr=fromdb.cursor()
   from_sql="""select count(0) from syn_log 
               where if_syn='N' and tname='{0}'
                 and ddl_stat like '%CREATE%' 
                 and ddl_stat like '%TABLE%'""".format(tname.upper())
   from_cr.execute(from_sql)
   from_rs=from_cr.fetchone()
   n_tab_rows=from_rs[0]
   from_cr.close()
   return n_tab_rows
 
def init_syn_tab_list(from_db,tname):
   from_sql="""begin
                 dp_syn_build.set_syn_tab_list('{0}');
               end;""".format(tname)
   from_cr=from_db.cursor()
   from_cr.execute(from_sql)
   from_db.commit()
   from_cr.close()

def init_syn_tab(from_db,tname):
   from_sql="""begin
                 dp_syn_build.init_tab('{0}');
               end;""".format(tname)
   from_cr=from_db.cursor()
   from_cr.execute(from_sql)
   from_db.commit()
   from_cr.close()

def sync_data(from_db,to_db,from_user,to_user,tname,from_string,to_string,debug):
    v_upd_sql=""
    v_blk_sql=""
    v_syn_id=""
    n_tab_total_rows=chk_syn_tab_rows(from_db,from_user,tname)
    n_batch_size=get_batch_size(n_tab_total_rows)
    from_cr=from_db.cursor()
    from_cr2=from_db.cursor()
    if n_tab_total_rows!=0:
      t_syn_begin_time=datetime.datetime.now()
      prn_syn_tab_rows(from_db,from_user,tname) 
      from_sql="""select dp_syn_build.gen_syn_sql(syn_id) as exec_sql,syn_id,if_syn,
                         decode(otype,'D','DML','I','DML','U','DML',otype) as otype
                  from SYN_LOG 
                  where if_syn='N' and tname='{0}' 
                 order by syn_id""".format(tname.upper());
      if debug.upper()=="Y" :
        print(from_sql)
      from_cr.execute(from_sql)
      from_rs=from_cr.fetchmany(n_batch_size)
      to_cr=to_db.cursor()
      while from_rs:
         v_blk_sql=''
         v_syn_id=''
         i_counter=0
         for i in range(len(from_rs)):
            i_counter=i_counter+1
            if debug=="Y":
               print("from_rs.len="+str(len(from_rs))+" ,i_counter="+str(i_counter))
            if from_rs[i][3]=="SYN" :
               v_syn_id=str(from_rs[i][1])
               v_blk_sql="begin\n"+from_rs[i][0]+";\nend;"        
               v_upd_sql="update syn_log set if_syn='Y' where syn_id in({0})".format(v_syn_id)
               if debug=="Y":
                  print("SYN:----------------------\n"+v_blk_sql)
                  print("SYN:----------------------\n"+v_upd_sql)
               from_cr2.execute(v_blk_sql)
               from_cr2.execute(v_upd_sql)
               v_blk_sql="begin\n"
               v_syn_id=''
            elif from_rs[i][3]=="DDL" :
               v_syn_id=str(from_rs[i][1])
               v_blk_sql="begin\n"+from_rs[i][0]+";\nend;"
               v_upd_sql="update syn_log set if_syn='Y' where syn_id in({0})".format(v_syn_id)
               if debug=="Y":
                  print("DDL:----------------------\n"+v_blk_sql)
                  print("DDL:----------------------\n"+v_upd_sql)
               to_cr.execute(v_blk_sql)
               from_cr2.execute(v_upd_sql)
               v_blk_sql="begin\n"
               v_syn_id=''
            elif from_rs[i][3]=="DML" :
               if i_counter==1:
                  v_blk_sql="begin\n"+from_rs[i][0]+";\n"
                  v_syn_id=str(from_rs[i][1])+","
               else:
                  v_blk_sql=v_blk_sql+from_rs[i][0]+";\n"
                  v_syn_id=v_syn_id+str(from_rs[i][1])+","
               
               if i_counter==len(from_rs):
                  v_blk_sql=v_blk_sql+"end;"
                  v_syn_id=v_syn_id[0:-1]
                  v_upd_sql="update syn_log set if_syn='Y' where syn_id in({0})".format(v_syn_id)
                  if debug=="Y":
                     print("DML:--------last-------------\n"+v_blk_sql)
                     print("DML:--------last-------------\n"+v_upd_sql)
                  to_cr.execute(v_blk_sql)
                  from_cr2.execute(v_upd_sql)
                  v_blk_sql="begin\n"
                  v_syn_id=''
               elif from_rs[i+1][3]!="DML":
                   v_blk_sql=v_blk_sql+"end;"
                   v_syn_id=v_syn_id[0:-1]
                   v_upd_sql="update syn_log set if_syn='Y' where syn_id in({0})".format(v_syn_id)
                   if debug=="Y":
                      print("DML:------middle------------\n"+v_blk_sql)
                      print("DML:------middle------------\n"+v_upd_sql)
                   to_cr.execute(v_blk_sql)
                   from_cr2.execute(v_upd_sql)
                   v_blk_sql="begin\n"
                   v_syn_id=''
                  
                   
         if debug=="Y":
            print("exit for....\n"+v_blk_sql)
            print("exit for ...\n"+v_syn_id)
         from_db.commit()
         to_db.commit()
         from_rs=from_cr.fetchmany(n_batch_size)

      t_syn_end_time=datetime.datetime.now()
      print("Sync complete!,elapsed time:"+str((t_syn_end_time-t_syn_begin_time).seconds)+"s")
      from_cr.close()
      from_cr2.close()
      to_cr.close()

def sync(fname):
    fromdb,todb,fromuser,touser,tname,from_string,to_string=get_sync_db(fname)
    if tname=="*":
       tname=get_syn_tab_list(fromdb,fromuser,tname)
    print(tname)
    init_syn_tab_list(fromdb,tname);
    print("*".ljust(52,"*"))
    print("* From database".ljust(20,' ')+":"+from_string.ljust(30,' ')+"*")
    print("* From user".ljust(20,' ')+":"+fromuser.ljust(30,' ')+"*")
    print("* To   database".ljust(20,' ')+":"+to_string.ljust(30,' ')+"*")
    print("* To   user".ljust(20,' ')+":"+touser.ljust(30,' ')+"*")
    #print("* Sync table".ljust(20,' ')+":"+tname.ljust(30,' ')+"*")
    print("*".ljust(52,"*"))
    while True:  
     try:    
        for tab in tname.split(","):    
          try:
            if if_exists_tab(todb,touser,tab)==0 and if_syn_log_cre_tab(fromdb,fromuser,tab)==0:
               init_syn_tab(fromdb,tab) 
               todb_tab_sql = get_table_defi(fromdb,fromuser,tab)
               todb_cre_tab_fmt_sql = get_format_tab(todb_tab_sql,fromuser,"")
               sqlDDL(todb,todb_cre_tab_fmt_sql,"")
               transfer_data(fromdb,todb,fromuser,touser,tab,from_string,to_string)
            elif if_exists_tab(todb,touser,tab)==0 and if_syn_log_cre_tab(fromdb,fromuser,tab)>0:
               #init_syn_tab(fromdb,tab)
               pass
            sync_data(fromdb,todb,fromuser,touser,tab,from_string,to_string,"")
          except BaseException:
            exception_info3("Sync Table:{0} ERROR! ".format(tab))
            #exception_info2()
            time.sleep(1)
     except BaseException:
        exception_info3("Sync Table:{0} ERROR! ".format(tab))
        #exception_info2()
        fromdb.close()
        todb.close()
        sys.exit(0)
 
    fromdb.close()
    todb.close()

def get_exp_conf(fname):
    config=configparser.ConfigParser()
    config.read(fname)
    try:
      fromdb_ip    = config.get("EXP","from_ip")
    except:
      print("No option 'from_ip' in section: 'EXP'")
      return
    try:
      fromdb_port  = config.get("EXP","from_port")
    except:
      print("No option 'from_port' in section: 'EXP'")
      return
    try:
      fromdb_sid  = config.get("EXP","from_sid")
    except:
      print("No option 'from_sid' in section: 'EXP'")
      return
    try:
      fromdb_user  = config.get("EXP","from_user")
    except:
      print("No option 'from_user' in section: 'EXP'")
      return
    try:
      fromdb_pass  = config.get("EXP","from_pass")
    except:
      print("No option 'from_pass' in section: 'EXP'")
      return  
    try:
      exp_type     = config.get("EXP","exp_type")
      if exp_type.lower() not in ("sql","xls","csv"):
        print("exp_type is invalid,only support:sql,csv,xls!")
        return
    except:
      exp_type     = 'sql'
      print("Warning:exp_type not config,use default:sql replace!")
    try:
      tname        = config.get("EXP","tname") 
    except:
      tname        = ''
    try:
      query        = config.get("EXP","query")
    except:
      query        ='' 
    try:
      filename     = config.get("EXP","filename")
      if exp_type in("csv","xls") and ( filename is None or filename==''):
         print("Error:exp_type is csv,xls,must config \'filename\'!")
         return      
    except:
      if exp_type in("csv","xls") and ( filename is None or filename==''):
         print("Error:exp_type is csv,xls,must config \'filename\'!")
         return   

    from_string  = fromdb_ip+":"+fromdb_port+":"+fromdb_sid
    fromtns      = cx_Oracle.makedsn(fromdb_ip,fromdb_port,fromdb_sid)
    fromdb       = cx_Oracle.connect(fromdb_user,fromdb_pass,fromtns)
    return fromdb,fromdb_user,tname,from_string,exp_type,query,filename

def get_imp_conf(fname):
    config=configparser.ConfigParser()
    config.read(fname)
    todb_ip    = config.get("IMP","to_ip")
    todb_port  = config.get("IMP","to_port")
    todb_sid   = config.get("IMP","to_sid")
    todb_user  = config.get("IMP","to_user")
    todb_pass  = config.get("IMP","to_pass")
    filename   = config.get("IMP","filename")
    to_string  = todb_ip+":"+todb_port+":"+todb_sid
    totns      = cx_Oracle.makedsn(todb_ip,todb_port,todb_sid)
    todb       = cx_Oracle.connect(todb_user,todb_pass,totns)
    return todb,todb_user,to_string,filename

def get_mon_conf(fname):
    config=configparser.ConfigParser()
    config.read(fname)
    mondb_ip    = config.get("MONITOR","mon_ip")
    mondb_port  = config.get("MONITOR","mon_port")
    mondb_sid   = config.get("MONITOR","mon_sid")
    mondb_user  = config.get("MONITOR","mon_user")
    mondb_pass  = config.get("MONITOR","mon_pass")
    to_string   = mondb_ip+":"+mondb_port+":"+mondb_sid
    mon_tns     = cx_Oracle.makedsn(mondb_ip,mondb_port,mondb_sid)
    mon_db      = cx_Oracle.connect(mondb_user,mondb_pass,mon_tns)
    to_user     = config.get("MONITOR","to_user")
    from_user   = config.get("MONITOR","from_user")
    from_pass   = config.get("MONITOR","from_pass")
    return mon_db,from_user,from_pass,to_user

def get_mon_item_conf(fname,mon_item):
    config=configparser.ConfigParser()
    config.read(fname)
    mon_val= config.get("MONITOR",mon_item)
    return mon_val

def get_exp_header(fromdb,fromuser,tname):
    cr=fromdb.cursor()
    sql="select * from "+fromuser+"."+tname
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+fromuser+"."+tname+"("
    s2=" values("
    for i in range(len(desc)):
      s1=s1+desc[i][0].lower()+','  
    s1=s1[0:-1]+')'
    cr.close()
    return s1+s2

def get_transfer_header(fromdb,fromuser,touser,tname):
    cr=fromdb.cursor()
    sql="select * from "+fromuser+"."+tname
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+touser+"."+tname+"("
    s2=" values("
    for i in range(len(desc)):
      s1=s1+desc[i][0].lower()+','  
    s1=s1[0:-1]+')'
    cr.close()
    return s1+s2

def exp_sql_data(from_db,from_user,tname,from_string,filename):
    n_tab_total_rows=0
    n_tab_total_rows=get_tab_rows(from_db,from_user,tname)
    t_exp_begin_time=datetime.datetime.now()
    n_batch_size=get_batch_size(n_tab_total_rows)
    if n_tab_total_rows!=0:
      from_cr=from_db.cursor()
      from_sql=''
      from_sql="select * from "+from_user+"."+tname
      from_cr.execute(from_sql)   
      desc=from_cr.description
      #for i in range(len(desc)):
      #  print(desc[i]) 
      from_rs=from_cr.fetchmany(n_batch_size)
      ins_sql_header = get_exp_header(from_db,from_user,tname)
      ins_val=""
      print("Export Table..."+tname.upper()+" ,Batch="+str(n_batch_size)+" ,Rows="+str(n_tab_total_rows)+"...")
      bar = ProgressBar(total =n_tab_total_rows,width =43)
      while from_rs:
        for row in list(from_rs):
            ins_val=""
            for j in range(len(row)):
               #print(row[j])
               col_type=str(desc[j][1]).split(".")[1].replace(">","").replace("'","")    
               if col_type=="NUMBER":
                 if row[j] is None:
                    ins_val=ins_val+"null,"
                 else: 
                    ins_val=ins_val+str(row[j])+","
               elif col_type=="STRING":
                 if row[j] is None:
                    ins_val=ins_val+"null,"
                 else:
                    ins_val=ins_val+"'"+row[j]+"',"
               elif col_type=="BINARY":
                 if row[j] is None:
                    ins_val=ins_val+"null,"
                 else:
                    ins_val=ins_val+"'"+str(row[j])+"',"
               elif col_type=="DATETIME":
                 if row[j] is None:
                     ins_val=ins_val+"null,"
                 else:
                     ins_val=ins_val+"to_date('"+str(row[j])[0:10]+"','yyyy-mm-dd'),"
               elif col_type=="TIMESTAMP":
                 if row[j] is None:
                    ins_val=ins_val+"null,"
                 else:
                    ins_val=ins_val+"to_date('"+str(row[j])[0:19]+"','yyyy-mm-dd hh24:mi:ss'),"
               else:
                 if row[j] is None:
                    ins_val=ins_val+"null,"
                 else:
                    ins_val=ins_val+row[j]+","
            ins_sql=ins_sql_header+ins_val[0:-1]+');\n'
            #print(ins_sql)
            filename.write(ins_sql)
            bar.move()
            bar.log("")
            time.sleep(0.00001)
        filename.write('commit;\n') 
        from_rs=from_cr.fetchmany(n_batch_size)
      from_cr.close()
      t_exp_end_time=datetime.datetime.now()
      print("");
      print("Exporting complete!")
      print("Export total time:"+str((t_exp_end_time-t_exp_begin_time).seconds)+"s");

def exp_sql_data_csv(from_db,from_user,tname,from_string,filename,query):
    n_tab_total_rows=0 
    if tname is not None and tname !='' :
      n_tab_total_rows=get_tab_rows(from_db,from_user,tname)
    elif query is not None and query!='':
      n_tab_total_rows=get_query_rows(from_db,from_user,query)

    t_exp_begin_time=datetime.datetime.now()
    n_batch_size=get_batch_size(n_tab_total_rows)
    if n_tab_total_rows!=0:
      from_cr=from_db.cursor()
      from_sql=''
      if tname is not None and tname !='' : 
        from_sql="select * from "+from_user+"."+tname
      elif query is not None and query!='':
        from_sql=query[1:-1]
      print(from_sql)
      from_cr.execute(from_sql)   
      desc=from_cr.description
      from_rs=from_cr.fetchmany(n_batch_size)
      ins_val=""
      if tname is not None and tname !='' :
        print("Export Table csv..."+tname.upper()+" ,Batch="+str(n_batch_size)+" ,Rows="+str(n_tab_total_rows)+"...")
      elif query is not None and query!='':
        print("Export query csv..."+" ,Batch="+str(n_batch_size)+" ,Rows="+str(n_tab_total_rows)+"...")
      bar = ProgressBar(total =n_tab_total_rows,width =43)
      while from_rs:
        for row in list(from_rs):
            ins_val=""
            for j in range(len(row)):
               #print(row[j])
               col_type=str(desc[j][1]).split(".")[1].replace(">","").replace("'","")    
               if col_type=="NUMBER":
                 if row[j] is None:
                    ins_val=ins_val+","
                 else: 
                    ins_val=ins_val+str(row[j])+","
               elif col_type=="STRING":
                 if row[j] is None:
                    ins_val=ins_val+","
                 else:
                    ins_val=ins_val+"'"+row[j]+"',"
               elif col_type=="BINARY":
                 if row[j] is None:
                    ins_val=ins_val+","
                 else:
                    ins_val=ins_val+"'"+str(row[j])+"',"
               elif col_type=="DATETIME":
                 if row[j] is None:
                     ins_val=ins_val+","
                 else:
                     ins_val=ins_val+"'"+str(row[j])[0:10]+"'"
               elif col_type=="TIMESTAMP":
                 if row[j] is None:
                    ins_val=ins_val+","
                 else:
                    ins_val=ins_val+"'"+str(row[j])[0:19]+"'"
               else:
                 if row[j] is None:
                    ins_val=ins_val+","
                 else:
                    ins_val=ins_val+row[j]+","
            ins_sql=ins_val[0:-1]+'\n'
            filename.write(ins_sql)
            bar.move()
            bar.log("")
            time.sleep(0.00001)
        from_rs=from_cr.fetchmany(n_batch_size)
      from_cr.close()
      t_exp_end_time=datetime.datetime.now()
      print("");
      print("Exporting complete!")
      print("Export total time:"+str((t_exp_end_time-t_exp_begin_time).seconds)+"s");

def exp_sql_data_xls(from_db,from_user,tname,from_string,file_path,file_name,query):
    n_tab_total_rows=0
    if tname is not None and tname !='' :
      n_tab_total_rows=get_tab_rows(from_db,from_user,tname)
    elif query is not None and query!='':
      n_tab_total_rows=get_query_rows(from_db,from_user,query)
    t_exp_begin_time=datetime.datetime.now()
    n_batch_size=get_batch_size(n_tab_total_rows)
    if n_tab_total_rows!=0:
      from_cr=from_db.cursor()
      from_sql=''
      if tname is not None and tname !='' : 
        from_sql="select * from "+from_user+"."+tname
      elif query is not None and query!='':
        from_sql=query[1:-1]	 
      from_cr.execute(from_sql)
      desc=from_cr.description
      from_rs=from_cr.fetchmany(n_batch_size)
      ins_val=""
      if tname is not None and tname !='' :
        print("Export Table xls..."+tname.upper()+" ,Batch="+str(n_batch_size)+" ,Rows="+str(n_tab_total_rows)+"...")
      elif query is not None and query!='':
        print("Export query xls..."+" ,Batch="+str(n_batch_size)+" ,Rows="+str(n_tab_total_rows)+"...")
      bar = ProgressBar(total =n_tab_total_rows,width =43)
      style0 = xlwt.easyxf('font: name Times New Roman, color-index black, bold on',num_format_str='#,##0.00')
      style1 = xlwt.easyxf(num_format_str='YYYY-MM-DD')
      style2 = xlwt.easyxf(num_format_str='YYYY-MM-DD h:mm:ss')
      wb = xlwt.Workbook()
      r=0
      n_page=0
      r_threshold=65536
      while from_rs:
        for i in range(len(list(from_rs))):
            ins_val=""
            if r%r_threshold==0:
              if tname is not None and tname!='':
                ws = wb.add_sheet(tname+'-'+str(n_page))
              elif query is not None and query!='':
                ws = wb.add_sheet(file_name.split(".")[0]+'-'+str(n_page))
              n_page=n_page+1
              r=0
            row=list(from_rs)[i]
            for j in range(len(row)):
               #print(str(i)+'#'+str(j)+'#'+str(row[j]))
               col_type=str(desc[j][1]).split(".")[1].replace(">","").replace("'","")
               if col_type=="NUMBER":
                 if row[j] is None:
                    ws.write(r,j,None,style0)                 
                 else:
                    ws.write(r,j,row[j],style0)
               elif col_type=="STRING":
                 if row[j] is None:
                    ws.write(r,j,None)
                 else:
                    ws.write(r,j,row[j])
               elif col_type=="BINARY":
                 if row[j] is None:
                    ws.write(r,j,None)
                 else:
                    ws.write(r,j,row[j]) 
               elif col_type=="DATETIME":
                 if row[j] is None:
                    ws.write(r,j,None)
                 else:
                    ws.write(r,j,row[j],style1)
               elif col_type=="TIMESTAMP":
                 if row[j] is None:
                    ws.write(r,j,None)
                 else:
                    ws.write(r,j,row[j],style2)
               else:
                 if row[j] is None:
                    ws.write(r,j,None)
                 else:
                    ws.write(r,j,row[j])
            r=r+1
            bar.move()
            bar.log("")
            time.sleep(0.00001)
        from_rs=from_cr.fetchmany(n_batch_size)
        #wb.save(filename)
      from_cr.close()
      wb.save(file_path+file_name)
      t_exp_end_time=datetime.datetime.now()
      print("Exporting complete!")
      print("Export total time:"+str((t_exp_end_time-t_exp_begin_time).seconds)+"s");

def exp(fname):
    try:
       fromdb,fromuser,tname,from_string,exp_type,query,filename=get_exp_conf(fname)
       if (tname is None or tname=='')  and (query is None or query==''):
         print("Must config tname or query parameter in config.ini file!") 
         return
       elif (tname is not None and tname!='')  and (query is not None and query!=''):
         print("Can't be configured at the same time \'tname\' or \'query\'  in config.ini file!")
         return
       elif (query is not None and query!='') and (exp_type=="sql"):
         print("Not support exp \'query\' format is sql,Only support xls,csv in config.ini file!")
         return
       
       file_path=''
       try:
         os.mkdir("./"+exp_type)
       except:
         pass
       file_path=os.getcwd()+'/'+exp_type+'/'
       
       print("*".ljust(52,"*"))
       print("* From database".ljust(20,' ')+":"+from_string.ljust(30,' ')+"*")
       print("* From user".ljust(20,' ')+":"+fromuser.ljust(30,' ')+"*")
       print("* Exp  type".ljust(20,' ')+":"+exp_type.ljust(30,' ')+"*")
       print("* Exp  path".ljust(20,' ')+":"+file_path.ljust(30,' ')+"*")
       print("* Exp  table".ljust(20,' ')+":"+tname.ljust(30,' ')+"*")
       print("*".ljust(52,"*"))
       if tname is not None:
         if tname=="*":
            tname=get_syn_tab_list(fromdb,fromuser,tname)
         for tab in tname.split(","):
            if exp_type=="sql":
               todb_tab_sql = get_table_defi(fromdb,fromuser,tab)
               file_handle = open(file_path+tab+'.'+exp_type,'w')
               file_handle.writelines(todb_tab_sql.read()+'\n')
               exp_sql_data(fromdb,fromuser,tab,from_string,file_handle)
               file_handle.close()
            elif exp_type=="csv":
               file_handle = open(file_path+tab+'.'+exp_type,'w')
               exp_sql_data_csv(fromdb,fromuser,tab,from_string,file_handle,None)
               file_handle.close()
            elif exp_type=="xls":
               exp_sql_data_xls(fromdb,fromuser,tab,from_string,file_path,tab+'.'+exp_type,None) 
            else:
               pass
       if query is not None and query!='':
            if exp_type=="csv":
               file_handle = open(file_path+filename,'w')
               exp_sql_data_csv(fromdb,fromuser,None,from_string,file_handle,query)
               file_handle.close()
            elif exp_type=="xls":
               exp_sql_data_xls(fromdb,fromuser,None,from_string,file_path,filename,query)
            else:
               pass
    except BaseException:
       exception_info2()

def imp(fname):
    file_handle=""
    try:
       todb,touser,to_string,filename=get_imp_conf(fname)
       print("*".ljust(52,"*"))
       print("* To   database".ljust(20,' ')+":"+to_string.ljust(30,' ')+"*")
       print("* To   user".ljust(20,' ')+":"+touser.ljust(30,' ')+"*")
       print("*".ljust(52,"*"))
       file_handle = open(filename,'r')
       file_context=file_handle.read()
       for i in file_context.split(";"):          
         try:
            if "INSERT" in i.upper() or "DELETE" in i.upper() or "UPDATE" in i.upper():
               sqlDML(todb,i,"")
            elif "CREATE" in i.upper() or "ALTER" in i.upper() or \
                 "TRUNCATE" in i.upper() or "DROP" in i.upper():
               sqlDDL(todb,i,"")
            elif "COMMIT" in i.upper():
               db_commit(todb)
            elif "ROLLBACK" in i.upper():
               db_rollback(todb) 
         except:
           file_handle.close()
           exception_info()
       file_handle.close()
    except BaseException:
       file_handle.close()
       exception_info()

def send_mail(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    #to_user=['zhdn_791005@163.com','hcg@jusfoun.com','mafei@jusfoun.com']
    to_user=p_to_user.split(",")
    #print(to_user,type(to_user))
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP("smtp.163.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
        print("邮件发送成功!")
        return 0
    except smtplib.SMTPException as e:
        print("邮件发送失败！")
        print(e)
        return -1

def monitor(fname):
    try:
       while True: 
             mon_db,from_user,from_pass,to_user=get_mon_conf(fname)
             tablespace_usage_rate=get_mon_item_conf(fname,"tablespace_usage_rate")
             mon_sleep=get_mon_item_conf(fname,"mon_sleep")
             mon_cr=mon_db.cursor()
             mon_sql="""begin  
                          dp_mon_build.set_mon_threshold('tablespace_usage_rate','{0}');
                          dp_mon_build.start_monitor; 
                          dp_mon_build.check_monitor_log;
                          dp_mon_build.upd_monitor_status;
                        end;""".format(tablespace_usage_rate)
             time.sleep(int(mon_sleep))
             mon_cr.execute(mon_sql)
             mon_sql="""select mail_header,mail_body,fail_times,
                               first_send,second_send,third_send,mon_id
                         from MON_LOG 
                          where if_handle='N' order by mon_id"""
             mon_cr.execute(mon_sql)
             mon_rs=mon_cr.fetchall()
             for i in range(len(mon_rs)):
                title      =mon_rs[i][0]
                content    =mon_rs[i][1] 
                fail_times =mon_rs[i][2]
                first_send =mon_rs[i][3]
                second_send=mon_rs[i][4]
                third_send =mon_rs[i][5]
                mon_id     =mon_rs[i][6]

                if fail_times==1  and first_send==0:
                   result=send_mail(from_user,from_pass,to_user,title,content) 
                   if result==0:
                      mon_upd_sql="""update mon_log 
                                              set first_send=1 
                                              where mon_id={0}""".format(mon_id)
                      mon_cr.execute(mon_upd_sql)
                      mon_db.commit()    
                elif fail_times==2 and second_send==0:
                   result=send_mail(from_user,from_pass,to_user,title,content)
                   if result==0:
                      mon_upd_sql="""update mon_log 
                                              set second_send=1 
                                              where mon_id={0}""".format(mon_id)
                      mon_cr.execute(mon_upd_sql)
                      mon_db.commit()
                elif fail_times==3 and third_send==0:
                   result=send_mail(from_user,from_pass,to_user,title,content)
                   if result==0:
                      mon_upd_sql="""update mon_log 
                                              set third_send=1 
                                              where mon_id={0}""".format(mon_id)
                      mon_cr.execute(mon_upd_sql)
                      mon_db.commit()
                else:
                   pass 
                
             mon_detail_sql="""select mon_detail_id,mail_header,mail_body
                               from MON_LOG_DETAIL 
                               where if_handle='Y' and if_sendmail='N' order by mon_detail_id"""
             mon_detail_cr=mon_db.cursor()
             mon_detail_cr.execute(mon_detail_sql)
             mon_detail_rs=mon_detail_cr.fetchall()
             for j in range(len(mon_detail_rs)):
                mon_detail_id=mon_detail_rs[j][0]
                title        =mon_detail_rs[j][1]
                content      =mon_detail_rs[j][2]
                result=send_mail(from_user,from_pass,to_user,title,content)
                if result==0:
                  mon_detail_upd_sql="""update mon_log_detail 
                                         set if_sendmail='Y' 
                                       where mon_detail_id={0}""".format(mon_detail_id)
                  mon_detail_cr.execute(mon_detail_upd_sql)
                  mon_db.commit() 
              
    except smtplib.SMTPException as e:
       print(e)

def execfile(db,filename):
    file_handle=""
    try:
       file_handle = open(filename,'r')
       file_context=file_handle.read()
       for i in file_context.split(";"):
         try:
            if "INSERT" in i.upper() or "DELETE" in i.upper() or "UPDATE" in i.upper():
               sqlDML(db,i,"")
            elif "CREATE" in i.upper() or "ALTER" in i.upper() or "TRUNCATE" in i.upper() or "DROP" in i.upper():
               sqlDDL(db,i,"")
            elif "COMMIT" in i.upper():
               db_commit(db)
            elif "ROLLBACK" in i.upper():
               db_rollback(db)
         except:
           file_handle.close()
           exception_info()
       file_handle.close()
    except BaseException:
       exception_info2()

def usage():
    print("Oclient V2.0 console for help:")
    print(out_char.ljust(out_len-20,out_char)) 
    print("  \\q  or \\c                exit the program!")
    print("  \\p     <parameter name>  search oracle parameter")
    print("  \\dt    <table_name>      query table definition")
    print("  \\di    <index_name>      query index definition")
    print("  \\dp    <prop_name>       query database properties")
    print("  \\tp                      query tablespace usage")
    print("  \\db                      query v$database info")
    print("  \\inst                    query v$instance info")
    print("  \\lock                    query exception lock")
    print("  \\tran                    query exception transaction")    
    print("  \\sess  <sid>             query session information")
    print("  \\wait                    query wait event")
    print("  \\kill  <sid>             kill session")
    print("  \\okill <spid>            kill os process")
    print("  call   <procedure name>  execute procedure")
    print("  @<sql_file_name>         execute sql file")
    print("\nCommand line support the following operation.")
    print(out_char.ljust(out_len-20,out_char))
    print("  (1) select language,multi-table query")
    print("  (2) DML opertion")
    print("  (3) DDL operation,as create ,drop truncate and so on.")
    print("  (4) DDL operation,as create ,drop truncate.")
    print("  (5) DCL operation,as commit,rollback,grant,revoke.")
    print("  (6) EXECUTE pl/sql block")
    print("  (7) EXECUTE sql file\n")

def assist():
    print("\n oclient V2.0 tools for help:")
    print(out_char.ljust(out_len,out_char))
    print("  -h  or --help  or /?              query oclient help")
    print("  -c <host:port:instance>           connect oracle database tns string")
    print("  -u <user>:<password>              user and password information")
    print("  -q <QUERY>\|<DML>\|<DDL>\|<DVL>   execute sql language")
    print("  -p <parameter name>               query oracle parameter")
    print("  -console                          enter into interaction mode")
    print("  -consolehelp                      query interaction mode help")
    print("  -transfer -conf <config file>     transfer table in different oracle database")
    print("  -sync -conf <config file>         sync table in different oracle database")
    print("  -exp  -conf <config.ini>          export table from source database")
    print("  -imp  -conf <config.ini>          import table from  sql file\n")
    print("  -monitor -conf <config.ini>       monitor database\n")
    print(" oclient example:")
    print(out_char.ljust(out_len,out_char))
    print("   oclient -c 192.168.200.100:1521:orcl -u apps:apps -q \"select * from scott.emp\"")
    print("   oclient -c 192.168.200.100:1521:orcl -u apps:apps -q \"update scott.emp set sal=sal+1\"")
    print("   oclient -c 192.168.200.100:1521:orcl -u apps:apps -q \"create user lx1 identified by lx1\"")
    print("   oclient -c 192.168.200.100:1521:orcl -u apps:apps -q \"grant connect,resource to lx1\"")
    print("   oclient -c 192.168.200.100:1521:orcl -u apps:apps -p sga")
    print("   oclient -c 192.168.200.100:1521:orcl -u scott:tiger -e \"/ops/lx3.sql\"")
    print("   oclient -c 192.168.200.100:1521:orcl -u apps:apps -console")
    print("   oclient -consolehelp")
    print("   oclient -transfer -conf config.ini")
    print("   oclient -sync -conf config.ini")
    #print(out_char.ljust(out_len,out_char))
    sys.exit(0)

out_len=86
out_max_len=120
out_char='-'
if len(sys.argv)==1:
   print("Please input -h, or --help ,or /? for help!")
   sys.exit(0)
if sys.argv[1] in {"-h","--help","/?"}:
   assist()

if ('-c' not in sys.argv or '-u' not in sys.argv) and "-consolehelp" not in sys.argv:
   if "-transfer" not in sys.argv and \
      "-sync" not in sys.argv and \
      "-exp" not in sys.argv and \
      "-imp" not in sys.argv and \
      "-monitor" not in sys.argv:
     print("Must to be specified:'-c' and '-u' option at the same time!\n")
     sys.exit(0)

if "-console" not in sys.argv and "-consolehelp" not in sys.argv: 
  if "-transfer" not in sys.argv  and \
     "-sync" not in sys.argv and \
     "-exp" not in sys.argv and \
     "-imp" not in sys.argv and \
     "-monitor" not in sys.argv:
     if "-q" not in sys.argv and  "-p" not in sys.argv and "-e" not in sys.argv:
       print("Must to be specified:'-q' ,'-p','-e' or '-console'  option!\n")
       sys.exit(0)

conn_string=""
conn_user=""
exec_sql=""
exec_param=""
exec_file=""
exec_console="N"
db=""
for p in range(len(sys.argv)):
  #print(sys.argv[p])
  if  sys.argv[p] =="-c":
      conn_string = sys.argv[p+1].strip()
  elif sys.argv[p] == "-u":
      conn_user = sys.argv[p+1].strip()
  elif sys.argv[p] == "-q":
      exec_sql = sys.argv[p+1].strip()
  elif sys.argv[p] == "-p":
      exec_param = sys.argv[p+1].strip()
  elif sys.argv[p]=="-e":
      exec_file= sys.argv[p+1].strip()
      print(exec_file)
  elif sys.argv[p] == "-console":
      exec_console="Y"
  elif sys.argv[p] == "-consolehelp":
      exec_console="H"
  elif sys.argv[p] == "-transfer":
      transfer(sys.argv[p+2])
      sys.exit(0)
  elif sys.argv[p] == "-sync":
      sync(sys.argv[p+2])
      sys.exit(0)
  elif sys.argv[p] == "-exp":
      exp(sys.argv[p+2])
      sys.exit(0)
  elif sys.argv[p] == "-imp":
      imp(sys.argv[p+2])
      sys.exit(0)
  elif sys.argv[p] == "-monitor":
      monitor(sys.argv[p+2])
      sys.exit(0)
  else:
      ""
if exec_console!="H":
   try:
     db=get_db(conn_string,conn_user)
   except:
     exception_logon_info()
     sys.exit(0)

if exec_console=="H":
   usage()
   sys.exit(0)
elif exec_console=="Y":
   print(out_char.ljust(out_len,out_char))
   print("Welcome to the oclient V2.0 monitor.  Commands end with ;")
   sid=get_sid(db)
   print("Your Oracle connection sid is {0}".format(sid))
   get_server_info(db)
   l=""
   sign="oclient>"
   while True:
     s=input(sign)
     l=l+s+chr(10)+chr(13)
     if s=='\\q' or s=='\\c': 
        break
     elif "\\p" in s:
        try:
          exec_param=s.split(" ")[1]
          sqlSelectParam(db,exec_param)
          l=""
        except:
          print("Please input 'parameter name',For Details:\\p <parameter name>")
     elif "@" in s:
         execfile(db,s[1:])
         l=""
     elif "call" in s :
        if "(" in s:
          proc_name=s[0:-1].split(" ")[1].split("(")[0]
          proc_param=s[0:-1].split(" ")[1].split("(")[1].replace(")","").split(",")
        else:
          proc_name=s[0:-1].split(" ")[1].split("(")[0]
          proc_param=""
        callPROC(db,proc_name,proc_param)
        l=""
     elif s[0:6]=="commit":
        db_commit(db)
     elif s[0:8]=="rollback":
        db_rollback(db)
     elif "\\sess" in s:
        try:
          exec_param=s.split(" ")[1]
        except:
          exec_param=""
        if exec_param=="":
           get_sess(db)
        else:
           get_sess_sid(db,exec_param)
        l=""
     elif "\\inst" in s:
        get_inst(db)
        l=""
     elif "\\lock" in s:
        get_lock(db)
        l=""
     elif "\\tran" in s:
        get_tran(db)
        l=""
     elif "\\wait" in s:
        get_wait(db)
        l=""
     elif "\\kill" in s:
        try:
          killSESS(db,s.split(" ")[1])
          l=""
        except:
          print("Please input 'sid',For Details:\\kill <sid>")
     elif "\\okill" in s:
        try:
          killSPID(s.split(" ")[1])
          l=""
        except:
          print("Please input 'spid',For Details:\\okill <spid>")
     elif "\\dt" in s:
        try:
          if "." in s:
            owner=s.split(" ")[1].split('.')[0]
            tname=s.split(" ")[1].split('.')[1]
          else:
            owner=""
            tname=s.split(" ")[1]
          print('-'.ljust(60,'-'))
          print(get_table_defi(db,owner,tname))
          l=""
        except:
           exception_info()
           print("Please input 'Table Name',For Details:\\dt <table_name>")
     elif "\\di" in s:
        try:
          if "." in s:
            owner=s.split(" ")[1].split('.')[0]
            tname=s.split(" ")[1].split('.')[1]
          else:
            owner=""
            tname=s.split(" ")[1]
          print('-'.ljust(60,'-'))
          print(get_index_defi(db,owner,tname))
          l=""
        except:
          exception_info()
          print("Please input 'Table Name',For Details:\\di <table_name>")
     elif "\\dp" in s:
        try:
          exec_param=s.split(" ")[1]
          sqlSelectProp(db,exec_param)
          l=""
        except:
          print("Please input 'property name',For Details:\\dp <property name>")
     elif "\\tp" in s:
        sqlSelectTableSpace(db)
        l=""
     elif "\\db" in s:
        sqlSelectDataBase(db)
        l=""
     elif s=="\\h":
          usage()
          l=""
     elif s.strip()=='':
          continue
          l=""
     elif "function" in l.lower() or "procedure" in l.lower() or "trigger" in l.lower() or "package" in l.lower() or \
          "begin" in l.lower() or "declare" in l.lower():
          if s.lower()=="end;":
             sign="oclient>"
             source_sql=l[0:-2].lower().strip()
             exec_sql=l[0:-3].replace('\n\r','  ')
             debug_info("",source_sql,exec_sql)        
             sqlDDL(db,source_sql,"")
             l="" 
          else:
             sign=" ".ljust(len(sign)-1," ")+">"  
     elif ';' in s:
        sign="oclient>"
        source_sql=l[0:-3].lower().strip()
        exec_sql=l[0:-3].replace('\n\r','  ')  
        debug_info("N",source_sql,exec_sql)
        try:
          if source_sql[0:6]=="select" :
             sqlSelect(db,source_sql)
          elif source_sql[0:6] in ("insert","update","delete"):
             sqlDML(db,source_sql,"N")
          elif "create"==source_sql[0:6]  or "alter"==source_sql[0:5] or "drop"==source_sql[0:4]  or \
               "truncate"==source_sql[0:8] or "grant"==source_sql[0:5] or "revoke"==source_sql[0:6]:
             sqlDDL(db,source_sql,"")
          l=""
        except BaseException:
          exception_info()
          l=""
        s=""
     else:
        sign=" ".ljust(len(sign)-1," ")+">"
else:
    try: 
      if exec_sql !='':
        if exec_sql[0:6]=="select" :
          rs=sqlSelect(db,exec_sql)
        elif exec_sql[0:6] in ("insert","update","delete"):
          sqlDML(db,exec_sql,"Y")
        elif "create"==exec_sql[0:6] or "drop"==exec_sql[0:4] or \
             "alter"==exec_sql[0:5] or "truncate"==exec_sql[0:8] or "grant"==exec_sql[0:5]:  
          sqlDDL(db,exec_sql,"")
      if exec_param !='':
        rs=sqlSelectParam(db,exec_param)
      if exec_file !='':
        execfile(db,exec_file)
    except BaseException:
      exception_info()
db.close()
