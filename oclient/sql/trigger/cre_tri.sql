sqlplus / as sysdba
create or replace trigger on_logon_trigger 
after logon on database 
begin 
    dbms_application_info.set_client_info(sys_context( 'userenv', 'ip_address' ) ); 
end;
/