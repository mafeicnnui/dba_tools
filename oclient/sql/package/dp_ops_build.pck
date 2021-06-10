create or replace package dp_ops_build is

  function get_ind_ddl(p_owner varchar2,p_tab varchar2) return varchar2;

  function get_tab_ddl(p_owner varchar2,p_tab varchar2) return clob;
  
  function get_run_sql(p_sid varchar2) return varchar2;
  
  function beatufy_sql(p_sql varchar2) return varchar2;
  
  function get_col_lists(v_col_str varchar2) return varchar2 ;
  
  function get_where_lists(v_col_str varchar2) return varchar2;

end dp_ops_build;
/
create or replace package body dp_ops_build is

  function get_where_lists(v_col_str varchar2) return varchar2 is
     type r_col_tab is table of varchar2(300) INDEX BY BINARY_INTEGER;  
     v_col_tab r_col_tab;
     v_col_val varchar2(4000);
     v_tmp_str varchar2(4000):=v_col_str;
     i_count int:=0;
  begin
     i_count:=0;
     v_col_val:='';
     while instr(upper(v_tmp_str),'AND')>0 loop
       v_col_tab(i_count):=substr(upper(v_tmp_str),1,instr(upper(v_tmp_str),'AND')-1);
       v_tmp_str:=substr(v_tmp_str,instr(upper(v_tmp_str),'AND')+3);
       i_count:=i_count+1;
     end loop;
     v_col_tab(i_count):=v_tmp_str;
    
     for i in v_col_tab.first..v_col_tab.last loop 
       v_col_tab(i):=replace(v_col_tab(i),chr(9));
       v_col_tab(i):=replace(v_col_tab(i),chr(10));
       v_col_tab(i):=replace(v_col_tab(i),chr(13));
       v_col_tab(i):=trim(v_col_tab(i));
       v_col_val:=v_col_val||' and '||v_col_tab(i)||chr(10);
     end loop;
     v_col_val:=substr(v_col_val,1,length(v_col_val)-1)||chr(10);
     return substr(v_col_val,6);    
   end;
   
   function get_col_lists(v_col_str varchar2) return varchar2 is
     type r_col_tab is table of varchar2(100) INDEX BY BINARY_INTEGER;  
     v_col_tab r_col_tab;
     v_tmp_str varchar2(4000):=v_col_str;
     v_col_val varchar2(4000);
     i_count int:=0;
   begin
     i_count:=0;
     v_col_val:='';
     while instr(v_tmp_str,',')>0 loop
       v_col_tab(i_count):=substr(v_tmp_str,1,instr(v_tmp_str,',')-1);
       v_tmp_str:=substr(v_tmp_str,instr(v_tmp_str,',')+1);
       i_count:=i_count+1;
     end loop;
     v_col_tab(i_count):=v_tmp_str;
    
     for i in v_col_tab.first..v_col_tab.last loop 
       v_col_tab(i):=replace(v_col_tab(i),chr(9));
       v_col_tab(i):=replace(v_col_tab(i),chr(10));
       v_col_tab(i):=replace(v_col_tab(i),chr(13));
       v_col_tab(i):=trim(v_col_tab(i));
       v_col_val:=v_col_val||lpad(' ',3,' ')||v_col_tab(i)||','||chr(10);
     end loop;
     v_col_val:=substr(v_col_val,1,length(v_col_val)-2)||chr(10);
     return lower(v_col_val);    
  end;
  
  function beatufy_sql(p_sql varchar2) return varchar2 is
    v_sql varchar2(4000):=trim(p_sql);
    n_select_end_pos int;
    n_from_begin_pos int;
    n_tab_begin_pos  int;
    n_tab_end_pos    int;
    n_ord_begin_pos  int;
    n_ord_end_pos    int;
    n_where_begin_pos  int:=0;
    n_where_end_pos    int:=0;
    n_grp_begin_pos    int:=0;
    n_grp_end_pos      int:=0; 
    v_col_list     varchar2(2000);
    v_col_values   varchar2(2000);
    v_tab_values   varchar2(1000);
    v_where_list     varchar2(2000);
    v_where_values varchar2(1000);
    v_ord_values   varchar2(1000);
    v_grp_values   varchar2(1000);
    v_sql_stat     varchar2(4000);
 begin
  if instr(upper(v_sql),'SELECT')=0 and  instr(upper(v_sql),'UPDATE')=0  and instr(upper(v_sql),'DELETE')=0 then
     return 'NULL';
  else
    if instr(upper(v_sql),'SELECT')>0 then
        
        n_select_end_pos:=instr(upper(v_sql),'SELECT')+6;
        n_from_begin_pos:=instr(upper(v_sql),'FROM');
        v_col_list:=trim(substr(v_sql,n_select_end_pos,n_from_begin_pos-n_select_end_pos));
        if v_col_list!='*' then
         v_col_values:=get_col_lists(v_col_list);
        else
          v_col_values:='*';
        end if;
        
        if instr(upper(v_sql),'FROM')>0 then
          n_tab_begin_pos:=n_from_begin_pos+4;
          if instr(upper(v_sql),'WHERE')>0 then
             n_tab_end_pos:=instr(upper(v_sql),'WHERE');
          elsif instr(upper(v_sql),'GROUP BY')>0 then
             n_tab_end_pos:=instr(upper(v_sql),'GROUP BY');   
          elsif instr(upper(v_sql),'ORDER BY')>0 then
             n_tab_end_pos:=instr(upper(v_sql),'ORDER BY');
          else
             n_tab_end_pos:=length(v_sql);
          end if;
          v_tab_values:=lower(' '||trim(substr(v_sql,n_tab_begin_pos,n_tab_end_pos-n_tab_begin_pos)));
        end if;
       
        if instr(upper(v_sql),'WHERE')>0 then
           n_where_begin_pos:=instr(upper(v_sql),'WHERE')+5;
           if instr(upper(v_sql),'GROUP BY')>0 then
             n_where_end_pos:=instr(upper(v_sql),'GROUP BY');
           elsif instr(upper(v_sql),'ORDER BY')>0 then
             n_where_end_pos:=instr(upper(v_sql),'ORDER BY');
           else
             n_where_end_pos:=length(v_sql);
           end if;
           v_where_list:=substr(v_sql,n_where_begin_pos,n_where_end_pos-n_where_begin_pos);
           v_where_values:=get_where_lists(v_where_list);
        end if;
        
        if instr(upper(v_sql),'GROUP BY')>0 then
           n_grp_begin_pos:=instr(upper(v_sql),'GROUP BY')+8;
           if instr(upper(v_sql),'ORDER BY')>0 then
             n_grp_end_pos:=instr(upper(v_sql),'ORDER BY');
           elsif instr(upper(v_sql),'HAVING')>0 then
             n_grp_end_pos:=instr(upper(v_sql),'HAVING');
           else
             n_grp_end_pos:=length(v_sql);
           end if;
           v_grp_values:=lower(' '||trim(substr(v_sql,n_grp_begin_pos,n_grp_end_pos-n_grp_begin_pos)));
        end if;
                
        if instr(upper(v_sql),'ORDER BY')>0 then
           n_ord_begin_pos:=instr(upper(upper(v_sql)),'ORDER BY')+8;
           n_ord_end_pos:=length(v_sql);
           v_ord_values:=lower(' '||trim(substr(v_sql,n_ord_begin_pos,n_ord_end_pos-n_ord_begin_pos+1)));
        end if;        
    end if;       
   end if; 
   
   v_sql_stat:='SELECT'||chr(10)||v_col_values||'FROM '||v_tab_values||chr(10);
               
   if instr(upper(v_sql),'WHERE')>0 then 
       v_sql_stat:=v_sql_stat||'WHERE '||v_where_values;
   end if;   
   if instr(upper(v_sql),'GROUP BY')>0 then 
       v_sql_stat:=v_sql_stat||'GROUP BY '||v_grp_values||chr(10);
   end if;   
   if instr(upper(v_sql),'ORDER BY')>0 then 
       v_sql_stat:=v_sql_stat||'ORDER BY '||v_ord_values||chr(10);
   end if;
   return v_sql_stat;  
   
  end;

  function get_run_sql(p_sid varchar2) return varchar2 is
   v_sql varchar2(24000);
  begin
   v_sql:=null;
   for i in(select sql_text from v$sqltext_with_newlines 
       where sql_id=(select sql_id from v$session where sid=p_sid) order by piece) loop
     v_sql:=v_sql||i.sql_text;
   end loop;
   --return beatufy_sql(v_sql)||chr(10)||chr(13);
   return v_sql||chr(10)||chr(13);
  exception
   when others then
    return ' ';
  end;

  function get_ind_ddl(p_owner varchar2,p_tab varchar2) return varchar2 is
    v_owner varchar2(1000):=p_owner;
    v_tab   varchar2(1000):=p_tab;
    v_tab_header varchar2(100):=initcap('create index ');
    v_col_expr varchar(200);
    v_col_expr_total varchar(20000);
    v_indent varchar2(10):=' ';
    n_col_max_len int;
    
    function get_ind_col_names(v_index_name varchar2) return varchar2 is
       v_val varchar2(2000);
    begin
      v_val:='';
      for i in( select column_name from dba_ind_columns where index_name=upper(v_index_name)) loop
        v_val:=v_val||i.column_name||',';
      end loop;
      return substr(v_val,1,length(v_val)-1);
    end;
 begin
   v_col_expr_total:='';
   for i in(select t.table_name,t.index_name,
                   t.uniqueness,t.index_type
              from dba_indexes t
              where owner=v_owner and table_name=v_tab) loop 
              
       if i.index_type='NORMAL' then
         if i.uniqueness='UNIQUE'  then 
             v_col_expr:=initcap('alter table ')||v_tab||' add primary key('||get_ind_col_names(i.index_name)||');';
         elsif  i.uniqueness='NONUNIQUE'  then    
             v_col_expr:=initcap('create index ')||i.index_name||' on '||v_tab||'('||get_ind_col_names(i.index_name)||');';
         end if;
       elsif i.index_type='BITMAP' then   
           v_col_expr:=initcap('create bitmap index ')||i.index_name||' on '||v_tab||'('||get_ind_col_names(i.index_name)||');';
       end if;
       v_col_expr_total:=v_col_expr_total||v_col_expr||chr(10);
   end loop;
   v_col_expr_total:=substr(v_col_expr_total,1,length(v_col_expr_total)-1);
   return v_col_expr_total;
  end;

  function get_tab_ddl(p_owner varchar2,p_tab varchar2) return clob is
    v_owner varchar2(32767):=upper(p_owner);
    v_tab   varchar2(32767):=upper(p_tab);
    v_tab_header varchar2(32767):=initcap('create table ')||v_tab||'('||chr(10);
    v_col_expr varchar(32767);
    v_col_expr_total varchar(32767);
    v_indent varchar2(10):=' ';
    n_col_max_len int;

    function get_pk_val(v_owner varchar2,v_table varchar2,v_col varchar2) return varchar2 is
      n_exist_pk int:=0;
      v_cons_name varchar2(1000);
      v_cons_col  varchar2(1000);
    begin
      select count(0) into n_exist_pk
        from DBA_CONSTRAINTS
        where owner = v_owner and table_name = v_table and constraint_type = 'P';

      if n_exist_pk>0 then
        select constraint_name into v_cons_name
         from DBA_CONSTRAINTS
         where owner = v_owner  and table_name = v_table and constraint_type = 'P';

         select column_name into v_cons_col
           from DBA_CONS_COLUMNS where constraint_name=v_cons_name;
      end if;

      if  v_cons_col=v_col then
        return ' primary key';
      else
        return '';
      end if;
    end;
 begin
   v_col_expr_total:='';

   select max(length(column_name))+4
       into n_col_max_len
     from dba_tab_columns t where owner=upper(p_owner) and table_name=upper(p_tab);

   for i in(select column_name,data_type,data_length,
                   data_precision,data_scale,
                   decode(nullable,'N',' not null','') nullable
              from dba_tab_columns t
              where owner=v_owner and table_name=v_tab order by column_id) loop

     if i.data_type='NUMBER' and i.data_precision is null and i.data_scale=0 then
         v_col_expr:=v_indent||rpad(i.column_name,n_col_max_len,' ')||'INTEGER';
     elsif  i.data_type='NUMBER' and i.data_precision is null and i.data_scale is null then
         v_col_expr:=v_indent||rpad(i.column_name,n_col_max_len,' ')||i.data_type;
     elsif  i.data_type='NUMBER' and i.data_precision is not null and i.data_scale=0 then
         v_col_expr:=v_indent||rpad(i.column_name,n_col_max_len,' ')||i.data_type||'('||i.data_precision||')';
     elsif  i.data_type='NUMBER' and i.data_precision is not null and i.data_scale>0 then
         v_col_expr:=v_indent||rpad(i.column_name,n_col_max_len,' ')||i.data_type||'('||i.data_precision||','||i.data_scale||')';
     elsif  i.data_type in('VARCHAR2','CHAR','RAW')  then
         v_col_expr:=v_indent||rpad(i.column_name,n_col_max_len,' ')||i.data_type||'('||i.data_length||')';
     elsif  i.data_type='DATE'  then
         v_col_expr:=v_indent||rpad(i.column_name,n_col_max_len,' ')||i.data_type;
     elsif  i.data_type='TIMESTAMP(6)'  then
         v_col_expr:=v_indent||rpad(i.column_name,n_col_max_len,' ')||i.data_type;
     end if;
     v_col_expr:=v_col_expr||i.nullable||get_pk_val(v_owner,v_tab,i.column_name)||',';
     v_col_expr_total:=v_col_expr_total||v_col_expr||chr(10);
   end loop;
   v_col_expr_total:=substr(v_col_expr_total,1,length(v_col_expr_total)-2)||chr(10)||');';   
   --dbms_output.put_line(length(v_col_expr_total));
   --dbms_output.put_line(length(v_col_expr));
   return v_tab_header||lower(v_col_expr_total);
 end;

 
end dp_ops_build;
/
