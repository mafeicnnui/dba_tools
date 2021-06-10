create or replace package dp_mon_build is

  type t_str_tab is table of varchar2(100) index by BINARY_INTEGER;
  
  /*
    功能：启动数据库监控，并写监控日志
  */
  procedure  start_monitor ;
  
  /*
    功能：检测检控日志
  */
  procedure  check_monitor_log;
  
  
 /*
    功能：更新检测检控日志状态
  */
  procedure  upd_monitor_status;
  
  
  /*
    功能：通过监控项获取监控阀值
  */
  function   get_mon_threshold(p_mon_item varchar2) return varchar2;
  

  /*
    功能：oclient客户端调过过程将配置文件中的监控项写入mon_threshold
  */
  procedure set_mon_threshold(p_mon_item varchar2,p_mon_val varchar2);
  

  function get_host_name return varchar2 ;
  
  function get_host_addr return varchar2 ;
  
  function str_to_tab(v_str varchar2,v_split varchar2) return t_str_tab ;

end dp_mon_build;
/
create or replace package body dp_mon_build is
   type t_mon_tps_rec is record (tablespace_name varchar2(50),
                                 total_space varchar2(100),
                                 free_space varchar2(100),
                                 usage_rate varchar2(100),
                                 if_warn varchar2(1)
                            ); 
   type t_mon_tps_tab is table of t_mon_tps_rec;
   
   --type t_str_tab is table of varchar2(100);
   
   function get_host_name return varchar2 is
      v_host_name       varchar2(100);
   begin
     select name into v_host_name from server_name;
     return v_host_name;
   end;
   
   function get_host_addr return varchar2 is
      v_host_addr       varchar2(100);
   begin
     select name into v_host_addr from server_ip;     
     return v_host_addr;
   end;
   
   function get_host_os return varchar2 is
      v_host_os       varchar2(100);
   begin
     select name into v_host_os from operation;
     return v_host_os;
   end;
   
   function get_host_time return varchar2 is
      v_host_time       varchar2(100);
   begin
     select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') into v_host_time from dual;
     return v_host_time;
   end;
   
   function get_host_html return varchar2 is
      v_html varchar2(1000);
      v_host_name       varchar2(100);
      v_host_addr       varchar2(20);
      v_host_os         varchar2(100);
      v_host_time       varchar2(100);
   begin
      v_html:='
<h3><font color=''black''>主机信息：</font></h3>
<hr align=left width=700 />
<table>
  <tr>
    <td width=100><strong>主机名称：</strong></td>
    <td>$$HOST_NAME</td>
  </tr>
  <tr>
    <td><strong>主机地址：</strong></td>
    <td>$$HOST_ADDR</td>
  </tr>
  <tr>
    <td><strong>操作系统：</strong></td>
    <td>$$HOST_OS</td>
  </tr>
  <tr>
    <td><strong>主机时间：</strong></td>
    <td>$$HOST_TIME</td>
  </tr>
</table>';
       v_html:=replace(v_html,'$$HOST_NAME',get_host_name);
       v_html:=replace(v_html,'$$HOST_ADDR',get_host_addr);  
       v_html:=replace(v_html,'$$HOST_OS',get_host_os);  
       v_html:=replace(v_html,'$$HOST_TIME',get_host_time);  
    return v_html;
   end;
     
   function get_db_html return varchar2 is
       v_html varchar2(1000);
       v_db_version      varchar2(200);
       v_db_name         varchar2(100);
       v_db_logmoe       varchar2(100);
       v_db_created      varchar2(20);
       v_db_start_time   varchar2(20);
       v_db_runtime      varchar2(20);
   begin
       v_html:='
<p>       
<h3><font color=''black''>数据库信息：</font></h3>
<hr align=left width=700 />
<table>
  <tr>
    <td width=100><strong>软件版本：</strong></td>
    <td>$$DB_VERSION</td>
  </tr>
  <tr>
    <td><strong>数据库名：</strong></td>
    <td>$$DB_NAME</td>
  </tr>
  <tr>
    <td><strong>日志模式：</strong></td>
    <td>$$LOG_MODE</td>
  </tr>
  <tr>
    <td><strong>建库时间：</strong></td>
    <td>$$CREATED_RQ</td>
  </tr>
  <tr>
    <td><strong>启动时间：</strong></td>
    <td>$$STARTUP_RQ</td>
  </tr>
  <tr>
    <td><strong>运行时长：</strong></td>
    <td>$$RUN_TIME</td>
  </tr>
</table>';    
     select banner into v_db_version  from (select rownum xh,banner from v$version ) where xh=1;
     select name,
            decode(log_mode,'NOARCHIVELOG','非归档','归档'),
            to_char(created,'yyyy-mm-dd hh24:mi:ss')
          into v_db_name,v_db_logmoe,v_db_created from v$database;
     select round((sysdate-startup_time)*24)||' 小时',
            to_char(startup_time,'yyyy-mm-dd hh24:mi:ss')
          into v_db_runtime,v_db_start_time from v$instance;    
       
     v_html:=replace(v_html,'$$DB_VERSION',v_db_version);
     v_html:=replace(v_html,'$$DB_NAME',v_db_name);
     v_html:=replace(v_html,'$$LOG_MODE',v_db_logmoe);
     v_html:=replace(v_html,'$$CREATED_RQ',v_db_created);
     v_html:=replace(v_html,'$$RUN_TIME',v_db_runtime);
     v_html:=replace(v_html,'$$STARTUP_RQ',v_db_start_time);
     return v_html;
   end;
   
   function get_tablespace_html return varchar2 is
       v_html varchar2(1000);
   begin
       v_html:='
<p>       
<h3><font color=''black''>表空间预警信息：</font></h3>
<table border="1" cellpadding="0" cellspacing="0">
  <tr>
    <td width=200><strong>表空间名</strong></td>
    <td width=200><strong>总空间(MB)</strong></td>
    <td width=200><strong>可用空间(MB)</strong></td>
    <td width=100><strong>使用率(%)</strong></td>
  </tr> '||chr(10); 
     return v_html;
   end;
   
   function get_mon_id return int is
     n_mon_id int;
   begin
      select mon_log_seq.nextval into n_mon_id from dual;
      return n_mon_id;
   end;
   
   procedure wlog_tablespace(p_item varchar2,p_warn_message varchar2,p_cancel_message varchar2,v_mon_tps_tab t_mon_tps_tab) is
     v_html            varchar2(4000);
     v_warn_tablespace varchar2(100);
     v_cancel_tablespace varchar2(4000);
     v_cancel_tablespace_tmp varchar2(4000);
     n_exist           int;
     n_exist_detail    int;
     v_item_value      varchar2(100);
     n_mon_id          int;
   begin
       --生成主机信息
       v_html:=get_host_html; 
       
       --生成数据库信息 
       v_html:=v_html||get_db_html;
  
       --生成预警表空间信息
       v_html:=v_html||get_tablespace_html;
       
       --获取监控ID
       n_mon_id:=get_mon_id;
       
       dbms_output.put_line('v_mon_tps_tab.count='||v_mon_tps_tab.count);
       v_warn_tablespace:='';
       v_cancel_tablespace_tmp:=v_html;
       
       select count(0) into n_exist
        from mon_log 
          where if_handle='N' and item_name = 'TABLESPACE_USAGE_RATE';
       
       for j in 1..v_mon_tps_tab.count loop
          if v_mon_tps_tab(j).if_warn='1' then
               v_warn_tablespace:=v_warn_tablespace||v_mon_tps_tab(j).tablespace_name||',';
               v_html:=v_html||' <tr>'||chr(10);
               v_html:=v_html||'    <td><font color=''red''>'||v_mon_tps_tab(j).tablespace_name||'</font></td>'||chr(10);
               v_html:=v_html||'    <td><font color=''red''>'||v_mon_tps_tab(j).total_space||'</font></td>'||chr(10);
               v_html:=v_html||'    <td><font color=''red''>'||v_mon_tps_tab(j).free_space||'</font></td>'||chr(10);
               v_html:=v_html||'    <td><font color=''red''>'||v_mon_tps_tab(j).usage_rate||'</font></td>'||chr(10);
               v_html:=v_html||' </tr>'||chr(10);
               v_cancel_tablespace:=v_cancel_tablespace_tmp;
               v_cancel_tablespace:=v_cancel_tablespace||' <tr>'||chr(10);
               v_cancel_tablespace:=v_cancel_tablespace||'    <td><font color=''black''>'||v_mon_tps_tab(j).tablespace_name||'</font></td>'||chr(10);
               v_cancel_tablespace:=v_cancel_tablespace||'    <td><font color=''black''>'||v_mon_tps_tab(j).total_space||'</font></td>'||chr(10);
               v_cancel_tablespace:=v_cancel_tablespace||'    <td><font color=''black''>'||v_mon_tps_tab(j).free_space||'</font></td>'||chr(10);
               v_cancel_tablespace:=v_cancel_tablespace||'    <td><font color=''black''>'||v_mon_tps_tab(j).usage_rate||'</font></td>'||chr(10);
               v_cancel_tablespace:=v_cancel_tablespace||' </tr>'||chr(10);
               
               select count(0) into n_exist_detail
               from mon_log_detail
                   where if_handle='N'
                     and item_name = 'TABLESPACE_USAGE_RATE'
                     and item_value=v_mon_tps_tab(j).tablespace_name;
                     
               if n_exist_detail=0 then      
                   insert into mon_log_detail(mon_detail_id,item_name,item_value,mail_header,mail_body,if_sendmail,if_handle,mon_id)
                   values(mon_log_detail_seq.nextval,p_item,v_mon_tps_tab(j).tablespace_name,
                          p_cancel_message||'[主机名:'||get_host_name||',IP:'||get_host_addr||']',v_cancel_tablespace||'</table>'||chr(10),'N','N',n_mon_id);              
               else
                   update mon_log_detail 
                     set mail_body=v_cancel_tablespace||'</table>'
                      where mon_detail_id in(select mon_detail_id
                                             from mon_log_detail
                                             where item_name = 'TABLESPACE_USAGE_RATE'
                                               and item_value=v_mon_tps_tab(j).tablespace_name
                                               and if_handle='N');
               end if;
          else
               v_html:=v_html||' <tr>'||chr(10);
               v_html:=v_html||'    <td>'||v_mon_tps_tab(j).tablespace_name||'</td>'||chr(10);
               v_html:=v_html||'    <td>'||v_mon_tps_tab(j).total_space||'</td>'||chr(10);
               v_html:=v_html||'    <td>'||v_mon_tps_tab(j).free_space||'</td>'||chr(10);
               v_html:=v_html||'    <td>'||v_mon_tps_tab(j).usage_rate||'</td>'||chr(10);
               v_html:=v_html||' </tr>'||chr(10);
               
          end if;  
       end loop;
       v_warn_tablespace:=substr(v_warn_tablespace,1,length(v_warn_tablespace)-1);
       v_html:=v_html||'</table>'||chr(10);
                
       if n_exist>0  and v_warn_tablespace is not null then
           select item_value into v_item_value
            from mon_log
             where if_handle='N' and item_name = 'TABLESPACE_USAGE_RATE';
               
          if v_item_value<>v_warn_tablespace then
             update mon_log 
               set UPDATED=sysdate,
                   fail_times=1,
                   first_send=0,
                   second_send=0,
                   third_send=0,
                   item_value=v_warn_tablespace,
                   mail_body=v_html
                where item_name= 'TABLESPACE_USAGE_RATE'
                  and if_handle='N';  
              
              delete from mon_log_detail 
                where item_name = 'TABLESPACE_USAGE_RATE'  
                  and if_handle='N'
                  and instr(v_warn_tablespace,item_value)=0;
                              
          else
              update mon_log 
                 set  mail_body=v_html
                 where item_name= 'TABLESPACE_USAGE_RATE' and if_handle='N';
                   
               update mon_log 
                 set UPDATED=sysdate,
                     fail_times=fail_times+1
                 where item_name= 'TABLESPACE_USAGE_RATE' 
                   and if_handle='N'
                   and (sysdate-updated)*24*60*60>60;     
          
          end if;
        elsif n_exist>0  and v_warn_tablespace is null then
              update mon_log 
                 set mail_body=v_html
                 where item_name= 'TABLESPACE_USAGE_RATE' and if_handle='N';
                   
               update mon_log 
                 set UPDATED=sysdate,
                   fail_times=fail_times+1
                 where item_name= 'TABLESPACE_USAGE_RATE'
                   and if_handle='N'
                   and (sysdate-updated)*24*60*60>60;    
        end if;
        
        if n_exist=0  and  v_warn_tablespace is not null then
           insert into mon_log(mon_id,item_name,item_value,mail_header,mail_body,fail_times,if_handle)
               values(n_mon_id,p_item,v_warn_tablespace,
                     p_warn_message||'[主机名:'||get_host_name||',IP:'||get_host_addr||']',v_html,1,'N');
        end if;
        commit;           
   end;
   
   procedure wlog_detail_tablespace(p_item varchar2,v_mon_tps_tab t_mon_tps_tab,v_tablespace_name varchar2) is
     v_html            varchar2(4000);
   begin
       --生成主机信息
       v_html:=get_host_html; 
       
       --生成数据库信息 
       v_html:=v_html||get_db_html;
  
       --生成预警表空间信息
       v_html:=v_html||get_tablespace_html;
        
       for j in 1..v_mon_tps_tab.count loop
          if v_mon_tps_tab(j).tablespace_name=v_tablespace_name then
               v_html:=v_html||v_mon_tps_tab(j).tablespace_name||',';
               v_html:=v_html||' <tr>'||chr(10);
               v_html:=v_html||'    <td><font color=''black''>'||v_mon_tps_tab(j).tablespace_name||'</font></td>'||chr(10);
               v_html:=v_html||'    <td><font color=''black''>'||v_mon_tps_tab(j).total_space||'</font></td>'||chr(10);
               v_html:=v_html||'    <td><font color=''black''>'||v_mon_tps_tab(j).free_space||'</font></td>'||chr(10);
               v_html:=v_html||'    <td><font color=''black''>'||v_mon_tps_tab(j).usage_rate||'</font></td>'||chr(10);
               v_html:=v_html||' </tr>'||chr(10);
              
                update mon_log_detail 
                   set mail_body=v_html||'</table>'
                    where mon_detail_id in(select mon_detail_id
                                           from mon_log_detail
                                           where item_name = 'TABLESPACE_USAGE_RATE'
                                             and item_value=v_mon_tps_tab(j).tablespace_name
                                             and if_handle='N');
                                 
               
          end if;  
       end loop;
   end;
   
   procedure  start_monitor is
     type t_cur is ref cursor ;
     v_mon_tps_tab t_mon_tps_tab; 
     v_cur t_cur;
   begin
    for i in( select item,warn_script,warn_message,cancel_message from mon_script order by id) loop
       open v_cur for i.warn_script ;
       if i.item='TABLESPACE_USAGE_RATE' then
           fetch v_cur bulk collect into v_mon_tps_tab;
           wlog_tablespace(i.item,i.warn_message,i.cancel_message,v_mon_tps_tab);
        end if;   
      close v_cur; 
    end loop;
    
  end;
  
  procedure check_monitor_log is
     type t_cur is ref cursor ;
     v_mon_tps_tab t_mon_tps_tab; 
     v_cur t_cur;
     n_val int;
  begin
     for i in( select item,check_script,warn_script from mon_script order by id) loop
        for j in(select mon_detail_id,item_value from mon_log_detail where item_name=i.item order by mon_detail_id) loop
            if i.item='TABLESPACE_USAGE_RATE' then
              execute immediate i.check_script  into n_val using j.item_value;
              if n_val=1 then
                 open v_cur for i.warn_script ; 
                 fetch v_cur bulk collect into v_mon_tps_tab;
                 wlog_detail_tablespace(i.item,v_mon_tps_tab, j.item_value);
                 close v_cur; 
                 update mon_log_detail set if_handle='Y' where mon_detail_id=j.mon_detail_id;
                 commit;
              end if;
              dbms_output.put_line(j.item_value||'  n_val='||n_val);              
            end if;   
        end loop;
     end loop;
     
  end ;
  
  function str_to_tab(v_str varchar2,v_split varchar2) return t_str_tab is
     v_str_tab t_str_tab;
     v_tmp_str varchar2(100);
     n_begin_pos int;
     n_end_pos int;
     n_len int;
     i_index int:=1;
  begin
    n_begin_pos:=1;
    n_end_pos:=instr(v_str,v_split);
    while n_end_pos > 0 loop
      n_len:=n_end_pos-n_begin_pos;
      v_tmp_str:=substr(v_str,n_begin_pos,n_len);
      v_str_tab(i_index):=v_tmp_str;
      n_begin_pos:=n_end_pos+1;
      n_end_pos:=instr(v_str,v_split,n_begin_pos);
      i_index:=i_index+1;
    end loop;
    v_str_tab(i_index):=substr(v_str,n_begin_pos);
    return v_str_tab;
  end;
  
  procedure upd_monitor_status is
     v_list t_str_tab; 
     n_exist int;
     n_upd_bz varchar2(1):='Y';
  begin
     for i in(select mon_id,item_name,item_value from mon_log where if_handle='N' order by mon_id) loop
       v_list:=str_to_tab(i.item_value,',');
       for j in v_list.first.. v_list.last loop
           dbms_output.put_line('v_list['||j||']='||v_list(j));
           select count(0)  into n_exist
             from mon_log_detail 
               where mon_id=i.mon_id  and item_value=v_list(j) and if_handle='N';
            dbms_output.put_line('n_exist='||n_exist);
            if n_exist=1 then    
              n_upd_bz:='N'; 
            end if; 
       end loop;
       
       if n_upd_bz='Y' then  
         update mon_log set if_handle='Y' where mon_id=i.mon_id;
       end if; 
        
     end loop;
     commit;
  end ;
  
  function  get_mon_threshold(p_mon_item varchar2) return varchar2 is
     v_val varchar2(100);
  begin
    select t.mon_value into v_val from mon_threshold t where t.mon_item=p_mon_item;
    return v_val;
  end;
 
  procedure set_mon_threshold(p_mon_item varchar2,p_mon_val varchar2) is
    n_exist int;
  begin
    select count(0) into n_exist from mon_threshold t where upper(t.mon_item)=upper(p_mon_item);
    if n_exist=1 then
      update mon_threshold t set t.mon_value=p_mon_val where upper(t.mon_item)=upper(p_mon_item);
    else
      insert into mon_threshold(mon_item,mon_value) values( upper(p_mon_item), p_mon_val);
    end if;
    commit;
  end;
  
end dp_mon_build;
/
