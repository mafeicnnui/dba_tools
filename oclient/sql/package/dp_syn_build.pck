create or replace package dp_syn_build is

  procedure init_syn;
  procedure init_tab(v_tab varchar2);
  function  gen_syn_log(v_tab varchar2,op_type varchar2) return varchar2;
  function  gen_syn_sql(n_syn_id int) return varchar2 ;
  function  get_syn_tab_list return varchar2 ;
  procedure set_syn_tab_list(v_tab_list varchar2);
  
end dp_syn_build;
/
create or replace package body dp_syn_build is
  
  type t_syn_log_rec is record (
      syn_id     int ,
      tname      varchar2(40),
      otype      varchar2(3),
      pk_name    varchar2(40),
      pk_type    varchar2(40),
      pk_val     varchar2(40),
      col_name1  varchar2(4000),
      col_name2  varchar2(4000),
      col_name3  varchar2(4000),
      col_name4  varchar2(4000),
      col_name5  varchar2(4000),
      col_name6  varchar2(4000),
      col_name7  varchar2(4000),
      col_name8  varchar2(4000),
      col_name9  varchar2(4000),
      col_name10 varchar2(4000),
      col_name11 varchar2(4000),
      col_name12 varchar2(4000),
      col_name13 varchar2(4000),
      col_name14 varchar2(4000),
      col_name15 varchar2(4000),
      col_name16 varchar2(4000),
      col_name17 varchar2(4000),
      col_name18 varchar2(4000),
      col_name19 varchar2(4000),
      col_name20 varchar2(4000),
      col_name21  varchar2(4000),
      col_name22  varchar2(4000),
      col_name23  varchar2(4000),
      col_name24  varchar2(4000),
      col_name25  varchar2(4000),
      col_name26  varchar2(4000),
      col_name27  varchar2(4000),
      col_name28  varchar2(4000),
      col_name29  varchar2(4000),
      col_name30 varchar2(4000),
      col_name31 varchar2(4000),
      col_name32 varchar2(4000),
      col_name33 varchar2(4000),
      col_name34 varchar2(4000),
      col_name35 varchar2(4000),
      col_name36 varchar2(4000),
      col_name37 varchar2(4000),
      col_name38 varchar2(4000),
      col_name39 varchar2(4000),
      col_name40 varchar2(4000),      
      col_name41  varchar2(4000),
      col_name42  varchar2(4000),
      col_name43  varchar2(4000),
      col_name44  varchar2(4000),
      col_name45  varchar2(4000),
      col_name46  varchar2(4000),
      col_name47  varchar2(4000),
      col_name48  varchar2(4000),
      col_name49  varchar2(4000),
      col_name50 varchar2(4000),
      col_name51 varchar2(4000),
      col_name52 varchar2(4000),
      col_name53 varchar2(4000),
      col_name54 varchar2(4000),
      col_name55 varchar2(4000),
      col_name56 varchar2(4000),
      col_name57 varchar2(4000),
      col_name58 varchar2(4000),
      col_name59 varchar2(4000),
      col_name60 varchar2(4000),
      col_name61  varchar2(4000),
      col_name62  varchar2(4000),
      col_name63  varchar2(4000),
      col_name64  varchar2(4000),
      col_name65  varchar2(4000),
      col_name66  varchar2(4000),
      col_name67  varchar2(4000),
      col_name68  varchar2(4000),
      col_name69  varchar2(4000),
      col_name70 varchar2(4000),
      col_name71 varchar2(4000),
      col_name72 varchar2(4000),
      col_name73 varchar2(4000),
      col_name74 varchar2(4000),
      col_name75 varchar2(4000),
      col_name76 varchar2(4000),
      col_name77 varchar2(4000),
      col_name78 varchar2(4000),
      col_name79 varchar2(4000),
      col_name80 varchar2(4000),
      col_name81 varchar2(4000),
      col_name82 varchar2(4000),
      col_name83 varchar2(4000),
      col_name84 varchar2(4000),
      col_name85 varchar2(4000),
      col_name86 varchar2(4000),
      col_name87 varchar2(4000),
      col_name88 varchar2(4000),
      col_name89 varchar2(4000),
      col_name90 varchar2(4000),
      col_name91 varchar2(4000),
      col_name92 varchar2(4000),
      col_name93 varchar2(4000),
      col_name94 varchar2(4000),
      col_name95 varchar2(4000),
      col_name96 varchar2(4000),
      col_name97 varchar2(4000),
      col_name98 varchar2(4000),
      col_name99 varchar2(4000),
      col_name100 varchar2(4000),      
      col_type1  varchar2(4000),
      col_type2  varchar2(4000),
      col_type3  varchar2(4000),
      col_type4  varchar2(4000),
      col_type5  varchar2(4000),
      col_type6  varchar2(4000),
      col_type7  varchar2(4000),
      col_type8  varchar2(4000),
      col_type9  varchar2(4000),
      col_type10  varchar2(4000),
      col_type11  varchar2(4000),
      col_type12  varchar2(4000),
      col_type13  varchar2(4000),
      col_type14  varchar2(4000),
      col_type15  varchar2(4000),
      col_type16  varchar2(4000),
      col_type17  varchar2(4000),
      col_type18  varchar2(4000),
      col_type19  varchar2(4000),
      col_type20  varchar2(4000),
      col_type21  varchar2(4000),
      col_type22  varchar2(4000),
      col_type23  varchar2(4000),
      col_type24  varchar2(4000),
      col_type25  varchar2(4000),
      col_type26  varchar2(4000),
      col_type27  varchar2(4000),
      col_type28  varchar2(4000),
      col_type29  varchar2(4000),
      col_type30  varchar2(4000),
      col_type31  varchar2(4000),
      col_type32  varchar2(4000),
      col_type33  varchar2(4000),
      col_type34  varchar2(4000),
      col_type35  varchar2(4000),
      col_type36  varchar2(4000),
      col_type37  varchar2(4000),
      col_type38  varchar2(4000),
      col_type39  varchar2(4000),
      col_type40  varchar2(4000),
      col_type41  varchar2(4000),
      col_type42  varchar2(4000),
      col_type43  varchar2(4000),
      col_type44  varchar2(4000),
      col_type45  varchar2(4000),
      col_type46  varchar2(4000),
      col_type47  varchar2(4000),
      col_type48  varchar2(4000),
      col_type49  varchar2(4000),
      col_type50  varchar2(4000),
      col_type51  varchar2(4000),
      col_type52  varchar2(4000),
      col_type53  varchar2(4000),
      col_type54  varchar2(4000),
      col_type55  varchar2(4000),
      col_type56  varchar2(4000),
      col_type57  varchar2(4000),
      col_type58  varchar2(4000),
      col_type59  varchar2(4000),
      col_type60  varchar2(4000),
      col_type61  varchar2(4000),
      col_type62  varchar2(4000),
      col_type63  varchar2(4000),
      col_type64  varchar2(4000),
      col_type65  varchar2(4000),
      col_type66  varchar2(4000),
      col_type67  varchar2(4000),
      col_type68  varchar2(4000),
      col_type69  varchar2(4000),
      col_type70  varchar2(4000),
      col_type71  varchar2(4000),
      col_type72  varchar2(4000),
      col_type73  varchar2(4000),
      col_type74  varchar2(4000),
      col_type75  varchar2(4000),
      col_type76  varchar2(4000),
      col_type77  varchar2(4000),
      col_type78  varchar2(4000),
      col_type79  varchar2(4000),
      col_type80  varchar2(4000),
      col_type81  varchar2(4000),
      col_type82  varchar2(4000),
      col_type83  varchar2(4000),
      col_type84  varchar2(4000),
      col_type85  varchar2(4000),
      col_type86  varchar2(4000),
      col_type87  varchar2(4000),
      col_type88  varchar2(4000),
      col_type89  varchar2(4000),
      col_type90  varchar2(4000),
      col_type91  varchar2(4000),
      col_type92  varchar2(4000),
      col_type93  varchar2(4000),
      col_type94  varchar2(4000),
      col_type95  varchar2(4000),
      col_type96  varchar2(4000),
      col_type97  varchar2(4000),
      col_type98  varchar2(4000),
      col_type99  varchar2(4000),
      col_type100  varchar2(4000),
      col_new_val1  varchar2(4000),
      col_new_val2  varchar2(4000),
      col_new_val3  varchar2(4000),
      col_new_val4  varchar2(4000),
      col_new_val5  varchar2(4000),
      col_new_val6  varchar2(4000),
      col_new_val7  varchar2(4000),
      col_new_val8  varchar2(4000),
      col_new_val9  varchar2(4000),
      col_new_val10  varchar2(4000),
      col_new_val11  varchar2(4000),
      col_new_val12  varchar2(4000),
      col_new_val13  varchar2(4000),
      col_new_val14  varchar2(4000),
      col_new_val15  varchar2(4000),
      col_new_val16  varchar2(4000),
      col_new_val17  varchar2(4000),
      col_new_val18  varchar2(4000),
      col_new_val19  varchar2(4000),
      col_new_val20  varchar2(4000),
      col_new_val21  varchar2(4000),
      col_new_val22  varchar2(4000),
      col_new_val23  varchar2(4000),
      col_new_val24  varchar2(4000),
      col_new_val25  varchar2(4000),
      col_new_val26  varchar2(4000),
      col_new_val27  varchar2(4000),
      col_new_val28  varchar2(4000),
      col_new_val29  varchar2(4000),
      col_new_val30  varchar2(4000),
      col_new_val31  varchar2(4000),
      col_new_val32  varchar2(4000),
      col_new_val33  varchar2(4000),
      col_new_val34  varchar2(4000),
      col_new_val35  varchar2(4000),
      col_new_val36  varchar2(4000),
      col_new_val37  varchar2(4000),
      col_new_val38  varchar2(4000),
      col_new_val39  varchar2(4000),
      col_new_val40  varchar2(4000),
      col_new_val41  varchar2(4000),
      col_new_val42  varchar2(4000),
      col_new_val43  varchar2(4000),
      col_new_val44  varchar2(4000),
      col_new_val45  varchar2(4000),
      col_new_val46  varchar2(4000),
      col_new_val47  varchar2(4000),
      col_new_val48  varchar2(4000),
      col_new_val49  varchar2(4000),
      col_new_val50  varchar2(4000),
      col_new_val51  varchar2(4000),
      col_new_val52  varchar2(4000),
      col_new_val53  varchar2(4000),
      col_new_val54  varchar2(4000),
      col_new_val55  varchar2(4000),
      col_new_val56  varchar2(4000),
      col_new_val57  varchar2(4000),
      col_new_val58  varchar2(4000),
      col_new_val59  varchar2(4000),
      col_new_val60  varchar2(4000),
      col_new_val61  varchar2(4000),
      col_new_val62  varchar2(4000),
      col_new_val63  varchar2(4000),
      col_new_val64  varchar2(4000),
      col_new_val65  varchar2(4000),
      col_new_val66  varchar2(4000),
      col_new_val67  varchar2(4000),
      col_new_val68  varchar2(4000),
      col_new_val69  varchar2(4000),
      col_new_val70  varchar2(4000),
      col_new_val71  varchar2(4000),
      col_new_val72  varchar2(4000),
      col_new_val73  varchar2(4000),
      col_new_val74  varchar2(4000),
      col_new_val75  varchar2(4000),
      col_new_val76  varchar2(4000),
      col_new_val77  varchar2(4000),
      col_new_val78  varchar2(4000),
      col_new_val79  varchar2(4000),
      col_new_val80  varchar2(4000),
      col_new_val81  varchar2(4000),
      col_new_val82  varchar2(4000),
      col_new_val83  varchar2(4000),
      col_new_val84  varchar2(4000),
      col_new_val85  varchar2(4000),
      col_new_val86  varchar2(4000),
      col_new_val87  varchar2(4000),
      col_new_val88  varchar2(4000),
      col_new_val89  varchar2(4000),
      col_new_val90  varchar2(4000),
      col_new_val91  varchar2(4000),
      col_new_val92  varchar2(4000),
      col_new_val93  varchar2(4000),
      col_new_val94  varchar2(4000),
      col_new_val95  varchar2(4000),
      col_new_val96  varchar2(4000),
      col_new_val97  varchar2(4000),
      col_new_val98  varchar2(4000),
      col_new_val99  varchar2(4000),
      col_new_val100 varchar2(4000),
      col_old_val1   varchar2(4000),
      col_old_val2   varchar2(4000),
      col_old_val3   varchar2(4000),
      col_old_val4   varchar2(4000),
      col_old_val5   varchar2(4000),
      col_old_val6   varchar2(4000),
      col_old_val7   varchar2(4000),
      col_old_val8   varchar2(4000),
      col_old_val9   varchar2(4000),
      col_old_val10  varchar2(4000),
      col_old_val11  varchar2(4000),
      col_old_val12  varchar2(4000),
      col_old_val13  varchar2(4000),
      col_old_val14  varchar2(4000),
      col_old_val15  varchar2(4000),
      col_old_val16  varchar2(4000),
      col_old_val17  varchar2(4000),
      col_old_val18  varchar2(4000),
      col_old_val19  varchar2(4000),
      col_old_val20  varchar2(4000),
      col_old_val21  varchar2(4000),
      col_old_val22  varchar2(4000),
      col_old_val23  varchar2(4000),
      col_old_val24  varchar2(4000),
      col_old_val25  varchar2(4000),
      col_old_val26  varchar2(4000),
      col_old_val27  varchar2(4000),
      col_old_val28  varchar2(4000),
      col_old_val29  varchar2(4000),
      col_old_val30  varchar2(4000),
      col_old_val31  varchar2(4000),
      col_old_val32  varchar2(4000),
      col_old_val33  varchar2(4000),
      col_old_val34  varchar2(4000),
      col_old_val35  varchar2(4000),
      col_old_val36  varchar2(4000),
      col_old_val37  varchar2(4000),
      col_old_val38  varchar2(4000),
      col_old_val39  varchar2(4000),
      col_old_val40  varchar2(4000),
      col_old_val41  varchar2(4000),
      col_old_val42  varchar2(4000),
      col_old_val43  varchar2(4000),
      col_old_val44  varchar2(4000),
      col_old_val45  varchar2(4000),
      col_old_val46  varchar2(4000),
      col_old_val47  varchar2(4000),
      col_old_val48  varchar2(4000),
      col_old_val49  varchar2(4000),
      col_old_val50  varchar2(4000),
      col_old_val51  varchar2(4000),
      col_old_val52  varchar2(4000),
      col_old_val53  varchar2(4000),
      col_old_val54  varchar2(4000),
      col_old_val55  varchar2(4000),
      col_old_val56  varchar2(4000),
      col_old_val57  varchar2(4000),
      col_old_val58  varchar2(4000),
      col_old_val59  varchar2(4000),
      col_old_val60  varchar2(4000),
      col_old_val61  varchar2(4000),
      col_old_val62  varchar2(4000),
      col_old_val63  varchar2(4000),
      col_old_val64  varchar2(4000),
      col_old_val65  varchar2(4000),
      col_old_val66  varchar2(4000),
      col_old_val67  varchar2(4000),
      col_old_val68  varchar2(4000),
      col_old_val69  varchar2(4000),
      col_old_val70  varchar2(4000),
      col_old_val71  varchar2(4000),
      col_old_val72  varchar2(4000),
      col_old_val73  varchar2(4000),
      col_old_val74  varchar2(4000),
      col_old_val75  varchar2(4000),
      col_old_val76  varchar2(4000),
      col_old_val77  varchar2(4000),
      col_old_val78  varchar2(4000),
      col_old_val79  varchar2(4000),
      col_old_val80  varchar2(4000),
      col_old_val81  varchar2(4000),
      col_old_val82  varchar2(4000),
      col_old_val83  varchar2(4000),
      col_old_val84  varchar2(4000),
      col_old_val85  varchar2(4000),
      col_old_val86  varchar2(4000),
      col_old_val87  varchar2(4000),
      col_old_val88  varchar2(4000),
      col_old_val89  varchar2(4000),
      col_old_val90  varchar2(4000),
      col_old_val91  varchar2(4000),
      col_old_val92  varchar2(4000),
      col_old_val93  varchar2(4000),
      col_old_val94  varchar2(4000),
      col_old_val95  varchar2(4000),
      col_old_val96  varchar2(4000),
      col_old_val97  varchar2(4000),
      col_old_val98  varchar2(4000),
      col_old_val99  varchar2(4000),
      col_old_val100 varchar2(4000),
      if_syn         varchar2(1) ,
      syn_time       timestamp);

  procedure set_syn_tab_list(v_tab_list varchar2) is
  begin
    execute immediate 'delete from syn_param where id=1';
    execute immediate 'insert into syn_param(id,mc) values(:1,:2)' using 1,upper(v_tab_list);
    commit;
  end;
  
  function get_syn_tab_list return varchar2 is
     v_mc varchar2(2000);
  begin  
    execute immediate 'select mc from syn_param where id=1' into v_mc;
    return v_mc;
  exception
     when others then
       return '';
  end;

  procedure cre_seq is
    v_cre_seq_sql varchar2(4000);
    v_drp_seq_sql varchar2(4000);
  begin
    v_drp_seq_sql:='drop sequence syn_log_seq';
    v_cre_seq_sql:='create sequence syn_log_seq'||chr(10)||
                      'minvalue 1'||chr(10)||
                      'maxvalue 999999999999'||chr(10)||
                      'start with 1'||chr(10)||
                      'increment by 1'||chr(10)||
                      'cache 200'||chr(10)||
                      'order';

     begin
         execute immediate  v_drp_seq_sql;
     exception
         when others then
           null;
     end;
     execute immediate v_cre_seq_sql;
   
  exception
     when others then
        dbms_output.put_line(sqlcode||sqlerrm);
  end;
  procedure cre_syn_ind is
    v_cre_ind_sql1 varchar2(4000);
    v_cre_ind_sql2 varchar2(4000);
    v_drp_ind_sql1 varchar2(4000);
    v_drp_ind_sql2 varchar2(4000);
  begin
    v_drp_ind_sql1:='drop index ind_syn_log_n1';
    v_drp_ind_sql2:='drop index ind_syn_log_u1';
    v_cre_ind_sql1:='create index ind_syn_log_n1  on syn_log(if_syn,tname)';
    v_cre_ind_sql2:='create unique index ind_syn_log_u1  on syn_log(syn_id)';

     begin
         execute immediate  v_drp_ind_sql1;
         execute immediate  v_drp_ind_sql2;
     exception
         when others then
           null;
     end;
     execute immediate v_cre_ind_sql1;
     execute immediate v_cre_ind_sql2;

  exception
     when others then
        dbms_output.put_line(sqlcode||sqlerrm);
  end;

  procedure cre_syn_tab is
    v_cre_tab_sql varchar2(18000);
    v_drp_tab_sql varchar2(18000);
  begin
    v_drp_tab_sql:='drop table syn_log';
    v_cre_tab_sql:='create table syn_log('||
                      'syn_id    int not null,'||
                      'tname      varchar2(40) not null,'||
                      'otype      varchar2(3),'||
                      'pk_name    varchar2(40),'||
                      'pk_type    varchar2(40),'||
                      'pk_val     varchar2(40),'||
                      'col_name1  varchar2(4000),'||
                      'col_name2  varchar2(4000),'||
                      'col_name3  varchar2(4000),'||
                      'col_name4  varchar2(4000),'||
                      'col_name5  varchar2(4000),'||
                      'col_name6  varchar2(4000),'||
                      'col_name7  varchar2(4000),'||
                      'col_name8  varchar2(4000),'||
                      'col_name9  varchar2(4000),'||
                      'col_name10 varchar2(4000),'||
                      'col_name11 varchar2(4000),'||
                      'col_name12 varchar2(4000),'||
                      'col_name13 varchar2(4000),'||
                      'col_name14 varchar2(4000),'||
                      'col_name15 varchar2(4000),'||
                      'col_name16 varchar2(4000),'||
                      'col_name17 varchar2(4000),'||
                      'col_name18 varchar2(4000),'||
                      'col_name19 varchar2(4000),'||
                      'col_name20 varchar2(4000),'||
                      'col_name21  varchar2(4000),'||
                      'col_name22  varchar2(4000),'||
                      'col_name23  varchar2(4000),'||
                      'col_name24  varchar2(4000),'||
                      'col_name25  varchar2(4000),'||
                      'col_name26  varchar2(4000),'||
                      'col_name27  varchar2(4000),'||
                      'col_name28  varchar2(4000),'||
                      'col_name29  varchar2(4000),'||
                      'col_name30 varchar2(4000),'||
                      'col_name31 varchar2(4000),'||
                      'col_name32 varchar2(4000),'||
                      'col_name33 varchar2(4000),'||
                      'col_name34 varchar2(4000),'||
                      'col_name35 varchar2(4000),'||
                      'col_name36 varchar2(4000),'||
                      'col_name37 varchar2(4000),'||
                      'col_name38 varchar2(4000),'||
                      'col_name39 varchar2(4000),'||
                      'col_name40 varchar2(4000),'||
                      'col_name41  varchar2(4000),'||
                      'col_name42  varchar2(4000),'||
                      'col_name43  varchar2(4000),'||
                      'col_name44  varchar2(4000),'||
                      'col_name45  varchar2(4000),'||
                      'col_name46  varchar2(4000),'||
                      'col_name47  varchar2(4000),'||
                      'col_name48  varchar2(4000),'||
                      'col_name49  varchar2(4000),'||
                      'col_name50 varchar2(4000),'||
                      'col_name51 varchar2(4000),'||
                      'col_name52 varchar2(4000),'||
                      'col_name53 varchar2(4000),'||
                      'col_name54 varchar2(4000),'||
                      'col_name55 varchar2(4000),'||
                      'col_name56 varchar2(4000),'||
                      'col_name57 varchar2(4000),'||
                      'col_name58 varchar2(4000),'||
                      'col_name59 varchar2(4000),'||
                      'col_name60 varchar2(4000),'||
                      'col_name61  varchar2(4000),'||
                      'col_name62  varchar2(4000),'||
                      'col_name63  varchar2(4000),'||
                      'col_name64  varchar2(4000),'||
                      'col_name65  varchar2(4000),'||
                      'col_name66  varchar2(4000),'||
                      'col_name67  varchar2(4000),'||
                      'col_name68  varchar2(4000),'||
                      'col_name69  varchar2(4000),'||
                      'col_name70 varchar2(4000),'||
                      'col_name71 varchar2(4000),'||
                      'col_name72 varchar2(4000),'||
                      'col_name73 varchar2(4000),'||
                      'col_name74 varchar2(4000),'||
                      'col_name75 varchar2(4000),'||
                      'col_name76 varchar2(4000),'||
                      'col_name77 varchar2(4000),'||
                      'col_name78 varchar2(4000),'||
                      'col_name79 varchar2(4000),'||
                      'col_name80 varchar2(4000),'||
                      'col_name81  varchar2(4000),'||
                      'col_name82  varchar2(4000),'||
                      'col_name83  varchar2(4000),'||
                      'col_name84  varchar2(4000),'||
                      'col_name85  varchar2(4000),'||
                      'col_name86  varchar2(4000),'||
                      'col_name87  varchar2(4000),'||
                      'col_name88  varchar2(4000),'||
                      'col_name89  varchar2(4000),'||
                      'col_name90 varchar2(4000),'||
                      'col_name91 varchar2(4000),'||
                      'col_name92 varchar2(4000),'||
                      'col_name93 varchar2(4000),'||
                      'col_name94 varchar2(4000),'||
                      'col_name95 varchar2(4000),'||
                      'col_name96 varchar2(4000),'||
                      'col_name97 varchar2(4000),'||
                      'col_name98 varchar2(4000),'||
                      'col_name99 varchar2(4000),'||
                      'col_name100 varchar2(4000),'||
                      'col_type1  varchar2(4000),'||
                      'col_type2  varchar2(4000),'||
                      'col_type3  varchar2(4000),'||
                      'col_type4  varchar2(4000),'||
                      'col_type5  varchar2(4000),'||
                      'col_type6  varchar2(4000),'||
                      'col_type7  varchar2(4000),'||
                      'col_type8  varchar2(4000),'||
                      'col_type9  varchar2(4000),'||
                      'col_type10  varchar2(4000),'||
                      'col_type11  varchar2(4000),'||
                      'col_type12  varchar2(4000),'||
                      'col_type13  varchar2(4000),'||
                      'col_type14  varchar2(4000),'||
                      'col_type15  varchar2(4000),'||
                      'col_type16  varchar2(4000),'||
                      'col_type17  varchar2(4000),'||
                      'col_type18  varchar2(4000),'||
                      'col_type19  varchar2(4000),'||
                      'col_type20  varchar2(4000),'||
                      'col_type21  varchar2(4000),'||
                      'col_type22  varchar2(4000),'||
                      'col_type23  varchar2(4000),'||
                      'col_type24  varchar2(4000),'||
                      'col_type25  varchar2(4000),'||
                      'col_type26  varchar2(4000),'||
                      'col_type27  varchar2(4000),'||
                      'col_type28  varchar2(4000),'||
                      'col_type29  varchar2(4000),'||
                      'col_type30  varchar2(4000),'||
                      'col_type31  varchar2(4000),'||
                      'col_type32  varchar2(4000),'||
                      'col_type33  varchar2(4000),'||
                      'col_type34  varchar2(4000),'||
                      'col_type35  varchar2(4000),'||
                      'col_type36  varchar2(4000),'||
                      'col_type37  varchar2(4000),'||
                      'col_type38  varchar2(4000),'||
                      'col_type39  varchar2(4000),'||
                      'col_type40  varchar2(4000),'||                      
                      'col_type41  varchar2(4000),'||
                      'col_type42  varchar2(4000),'||
                      'col_type43  varchar2(4000),'||
                      'col_type44  varchar2(4000),'||
                      'col_type45  varchar2(4000),'||
                      'col_type46  varchar2(4000),'||
                      'col_type47  varchar2(4000),'||
                      'col_type48  varchar2(4000),'||
                      'col_type49  varchar2(4000),'||
                      'col_type50  varchar2(4000),'||
                      'col_type51  varchar2(4000),'||
                      'col_type52  varchar2(4000),'||
                      'col_type53  varchar2(4000),'||
                      'col_type54  varchar2(4000),'||
                      'col_type55  varchar2(4000),'||
                      'col_type56  varchar2(4000),'||
                      'col_type57  varchar2(4000),'||
                      'col_type58  varchar2(4000),'||
                      'col_type59  varchar2(4000),'||
                      'col_type60  varchar2(4000),'||
                      'col_type61  varchar2(4000),'||
                      'col_type62  varchar2(4000),'||
                      'col_type63  varchar2(4000),'||
                      'col_type64  varchar2(4000),'||
                      'col_type65  varchar2(4000),'||
                      'col_type66  varchar2(4000),'||
                      'col_type67  varchar2(4000),'||
                      'col_type68  varchar2(4000),'||
                      'col_type69  varchar2(4000),'||
                      'col_type70  varchar2(4000),'||
                      'col_type71  varchar2(4000),'||
                      'col_type72  varchar2(4000),'||
                      'col_type73  varchar2(4000),'||
                      'col_type74  varchar2(4000),'||
                      'col_type75  varchar2(4000),'||
                      'col_type76  varchar2(4000),'||
                      'col_type77  varchar2(4000),'||
                      'col_type78  varchar2(4000),'||
                      'col_type79  varchar2(4000),'||
                      'col_type80  varchar2(4000),'||
                      'col_type81  varchar2(4000),'||
                      'col_type82  varchar2(4000),'||
                      'col_type83  varchar2(4000),'||
                      'col_type84  varchar2(4000),'||
                      'col_type85  varchar2(4000),'||
                      'col_type86  varchar2(4000),'||
                      'col_type87  varchar2(4000),'||
                      'col_type88  varchar2(4000),'||
                      'col_type89  varchar2(4000),'||
                      'col_type90  varchar2(4000),'||
                      'col_type91  varchar2(4000),'||
                      'col_type92  varchar2(4000),'||
                      'col_type93  varchar2(4000),'||
                      'col_type94  varchar2(4000),'||
                      'col_type95  varchar2(4000),'||
                      'col_type96  varchar2(4000),'||
                      'col_type97  varchar2(4000),'||
                      'col_type98  varchar2(4000),'||
                      'col_type99  varchar2(4000),'||
                      'col_type100  varchar2(4000),'||                      
                      'col_new_val1  varchar2(4000),'||
                      'col_new_val2  varchar2(4000),'||
                      'col_new_val3  varchar2(4000),'||
                      'col_new_val4  varchar2(4000),'||
                      'col_new_val5  varchar2(4000),'||
                      'col_new_val6  varchar2(4000),'||
                      'col_new_val7  varchar2(4000),'||
                      'col_new_val8  varchar2(4000),'||
                      'col_new_val9  varchar2(4000),'||
                      'col_new_val10  varchar2(4000),'||
                      'col_new_val11  varchar2(4000),'||
                      'col_new_val12  varchar2(4000),'||
                      'col_new_val13  varchar2(4000),'||
                      'col_new_val14  varchar2(4000),'||
                      'col_new_val15  varchar2(4000),'||
                      'col_new_val16  varchar2(4000),'||
                      'col_new_val17  varchar2(4000),'||
                      'col_new_val18  varchar2(4000),'||
                      'col_new_val19  varchar2(4000),'||
                      'col_new_val20  varchar2(4000),'||
                      'col_new_val21  varchar2(4000),'||
                      'col_new_val22  varchar2(4000),'||
                      'col_new_val23  varchar2(4000),'||
                      'col_new_val24  varchar2(4000),'||
                      'col_new_val25  varchar2(4000),'||
                      'col_new_val26  varchar2(4000),'||
                      'col_new_val27  varchar2(4000),'||
                      'col_new_val28  varchar2(4000),'||
                      'col_new_val29  varchar2(4000),'||
                      'col_new_val30  varchar2(4000),'||
                      'col_new_val31  varchar2(4000),'||
                      'col_new_val32  varchar2(4000),'||
                      'col_new_val33  varchar2(4000),'||
                      'col_new_val34  varchar2(4000),'||
                      'col_new_val35  varchar2(4000),'||
                      'col_new_val36  varchar2(4000),'||
                      'col_new_val37  varchar2(4000),'||
                      'col_new_val38  varchar2(4000),'||
                      'col_new_val39  varchar2(4000),'||
                      'col_new_val40  varchar2(4000),'||                      
                      'col_new_val41  varchar2(4000),'||
                      'col_new_val42  varchar2(4000),'||
                      'col_new_val43  varchar2(4000),'||
                      'col_new_val44  varchar2(4000),'||
                      'col_new_val45  varchar2(4000),'||
                      'col_new_val46  varchar2(4000),'||
                      'col_new_val47  varchar2(4000),'||
                      'col_new_val48  varchar2(4000),'||
                      'col_new_val49  varchar2(4000),'||
                      'col_new_val50  varchar2(4000),'||
                      'col_new_val51  varchar2(4000),'||
                      'col_new_val52  varchar2(4000),'||
                      'col_new_val53  varchar2(4000),'||
                      'col_new_val54  varchar2(4000),'||
                      'col_new_val55  varchar2(4000),'||
                      'col_new_val56  varchar2(4000),'||
                      'col_new_val57  varchar2(4000),'||
                      'col_new_val58  varchar2(4000),'||
                      'col_new_val59  varchar2(4000),'||
                      'col_new_val60  varchar2(4000),'||
                      'col_new_val61  varchar2(4000),'||
                      'col_new_val62  varchar2(4000),'||
                      'col_new_val63  varchar2(4000),'||
                      'col_new_val64  varchar2(4000),'||
                      'col_new_val65  varchar2(4000),'||
                      'col_new_val66  varchar2(4000),'||
                      'col_new_val67  varchar2(4000),'||
                      'col_new_val68  varchar2(4000),'||
                      'col_new_val69  varchar2(4000),'||
                      'col_new_val70  varchar2(4000),'||
                      'col_new_val71  varchar2(4000),'||
                      'col_new_val72  varchar2(4000),'||
                      'col_new_val73  varchar2(4000),'||
                      'col_new_val74  varchar2(4000),'||
                      'col_new_val75  varchar2(4000),'||
                      'col_new_val76  varchar2(4000),'||
                      'col_new_val77  varchar2(4000),'||
                      'col_new_val78  varchar2(4000),'||
                      'col_new_val79  varchar2(4000),'||
                      'col_new_val80  varchar2(4000),'||
                      'col_new_val81  varchar2(4000),'||
                      'col_new_val82  varchar2(4000),'||
                      'col_new_val83  varchar2(4000),'||
                      'col_new_val84  varchar2(4000),'||
                      'col_new_val85  varchar2(4000),'||
                      'col_new_val86  varchar2(4000),'||
                      'col_new_val87  varchar2(4000),'||
                      'col_new_val88  varchar2(4000),'||
                      'col_new_val89  varchar2(4000),'||
                      'col_new_val90  varchar2(4000),'||
                      'col_new_val91  varchar2(4000),'||
                      'col_new_val92  varchar2(4000),'||
                      'col_new_val93  varchar2(4000),'||
                      'col_new_val94  varchar2(4000),'||
                      'col_new_val95  varchar2(4000),'||
                      'col_new_val96  varchar2(4000),'||
                      'col_new_val97  varchar2(4000),'||
                      'col_new_val98  varchar2(4000),'||
                      'col_new_val99  varchar2(4000),'||
                      'col_new_val100  varchar2(4000),'||                      
                      'col_old_val1  varchar2(4000),'||
                      'col_old_val2  varchar2(4000),'||
                      'col_old_val3  varchar2(4000),'||
                      'col_old_val4  varchar2(4000),'||
                      'col_old_val5  varchar2(4000),'||
                      'col_old_val6  varchar2(4000),'||
                      'col_old_val7  varchar2(4000),'||
                      'col_old_val8  varchar2(4000),'||
                      'col_old_val9  varchar2(4000),'||
                      'col_old_val10  varchar2(4000),'||
                      'col_old_val11  varchar2(4000),'||
                      'col_old_val12  varchar2(4000),'||
                      'col_old_val13  varchar2(4000),'||
                      'col_old_val14  varchar2(4000),'||
                      'col_old_val15  varchar2(4000),'||
                      'col_old_val16  varchar2(4000),'||
                      'col_old_val17  varchar2(4000),'||
                      'col_old_val18  varchar2(4000),'||
                      'col_old_val19  varchar2(4000),'||
                      'col_old_val20  varchar2(4000),'||
                      'col_old_val21  varchar2(4000),'||
                      'col_old_val22  varchar2(4000),'||
                      'col_old_val23  varchar2(4000),'||
                      'col_old_val24  varchar2(4000),'||
                      'col_old_val25  varchar2(4000),'||
                      'col_old_val26  varchar2(4000),'||
                      'col_old_val27  varchar2(4000),'||
                      'col_old_val28  varchar2(4000),'||
                      'col_old_val29  varchar2(4000),'||
                      'col_old_val30  varchar2(4000),'||
                      'col_old_val31  varchar2(4000),'||
                      'col_old_val32  varchar2(4000),'||
                      'col_old_val33  varchar2(4000),'||
                      'col_old_val34  varchar2(4000),'||
                      'col_old_val35  varchar2(4000),'||
                      'col_old_val36  varchar2(4000),'||
                      'col_old_val37  varchar2(4000),'||
                      'col_old_val38  varchar2(4000),'||
                      'col_old_val39  varchar2(4000),'||
                      'col_old_val40  varchar2(4000),'||                      
                      'col_old_val41  varchar2(4000),'||
                      'col_old_val42  varchar2(4000),'||
                      'col_old_val43  varchar2(4000),'||
                      'col_old_val44  varchar2(4000),'||
                      'col_old_val45  varchar2(4000),'||
                      'col_old_val46  varchar2(4000),'||
                      'col_old_val47  varchar2(4000),'||
                      'col_old_val48  varchar2(4000),'||
                      'col_old_val49  varchar2(4000),'||
                      'col_old_val50  varchar2(4000),'||
                      'col_old_val51  varchar2(4000),'||
                      'col_old_val52  varchar2(4000),'||
                      'col_old_val53  varchar2(4000),'||
                      'col_old_val54  varchar2(4000),'||
                      'col_old_val55  varchar2(4000),'||
                      'col_old_val56  varchar2(4000),'||
                      'col_old_val57  varchar2(4000),'||
                      'col_old_val58  varchar2(4000),'||
                      'col_old_val59  varchar2(4000),'||
                      'col_old_val60  varchar2(4000),'||
                      'col_old_val61  varchar2(4000),'||
                      'col_old_val62  varchar2(4000),'||
                      'col_old_val63  varchar2(4000),'||
                      'col_old_val64  varchar2(4000),'||
                      'col_old_val65  varchar2(4000),'||
                      'col_old_val66  varchar2(4000),'||
                      'col_old_val67  varchar2(4000),'||
                      'col_old_val68  varchar2(4000),'||
                      'col_old_val69  varchar2(4000),'||
                      'col_old_val70  varchar2(4000),'||
                      'col_old_val71  varchar2(4000),'||
                      'col_old_val72  varchar2(4000),'||
                      'col_old_val73  varchar2(4000),'||
                      'col_old_val74  varchar2(4000),'||
                      'col_old_val75  varchar2(4000),'||
                      'col_old_val76  varchar2(4000),'||
                      'col_old_val77  varchar2(4000),'||
                      'col_old_val78  varchar2(4000),'||
                      'col_old_val79  varchar2(4000),'||
                      'col_old_val80  varchar2(4000),'||
                      'col_old_val81  varchar2(4000),'||
                      'col_old_val82  varchar2(4000),'||
                      'col_old_val83  varchar2(4000),'||
                      'col_old_val84  varchar2(4000),'||
                      'col_old_val85  varchar2(4000),'||
                      'col_old_val86  varchar2(4000),'||
                      'col_old_val87  varchar2(4000),'||
                      'col_old_val88  varchar2(4000),'||
                      'col_old_val89  varchar2(4000),'||
                      'col_old_val90  varchar2(4000),'||
                      'col_old_val91  varchar2(4000),'||
                      'col_old_val92  varchar2(4000),'||
                      'col_old_val93  varchar2(4000),'||
                      'col_old_val94  varchar2(4000),'||
                      'col_old_val95  varchar2(4000),'||
                      'col_old_val96  varchar2(4000),'||
                      'col_old_val97  varchar2(4000),'||
                      'col_old_val98  varchar2(4000),'||
                      'col_old_val99  varchar2(4000),'||
                      'col_old_val100  varchar2(4000),'||                      
                      'ddl_stat       varchar2(4000),'||
                      'if_syn     varchar2(1) default ''N''  not null,'||
                      'syn_time   timestamp)';
       begin
         execute immediate  v_drp_tab_sql;
       exception
         when others then
           null;
       end;
       execute immediate  v_cre_tab_sql;
             
       begin
         execute immediate  'drop table syn_param';
       exception
          when others then
             null;             
       end;
       execute immediate 'create table syn_param(id int,mc varchar2(2000))';     
       
       begin
         execute immediate  'drop table syn_bz';
       exception
          when others then
             null;             
       end;
       execute immediate 'create table syn_bz(i int)'; 
       
  exception
     when others then
      dbms_output.put_line(sqlcode||sqlerrm);
  end;
  
 
  
  function gen_ins_syn_log(v_tab varchar2,op_type varchar2) return varchar2 is
         n_counter        int;
         v_col_name_list    varchar2(18000);
         v_col_val_list     varchar2(18000);
         v_col_val_list_t   varchar2(18000);

         v_col_key_val      varchar2(100);

         v_col_typ_list     varchar2(18000);
         v_col_typ_val_list varchar2(18000);

         v_col_val_name_list varchar2(18000);
         v_col_val_val_list  varchar2(18000);
     begin
          n_counter :=1;
          v_col_name_list:='insert into syn_log(syn_id,tname,otype,syn_time,pk_name,pk_type,pk_val,';
          v_col_val_list:='';
          v_col_val_list_t:=' values(syn_log_seq.nextval,'||''''||upper(v_tab)||''''||',''I'',systimestamp,';
          v_col_key_val:='';

          v_col_val_name_list:='';
          v_col_val_val_list:='';
          for k in ( select col_name,col_val,data_type,
                         'col_name' || rownum as syn_col_name,
                         'col_type' || rownum as syn_col_type,
                         'col_new_val' || rownum as syn_col_val,
                         if_key
                       from (select a.column_id,''''||column_name||'''' as col_name,
                                         ':new.'||lower(column_name) as col_val,
                                         ''''||data_type||'''' as data_type,
                                         (select 'Y' from user_cons_columns b
                                         where b.table_name = a.table_name
                                           and b.column_name= a.COLUMN_NAME
                                           and constraint_name=(select constraint_name
                                                   from  user_constraints
                                                   where constraint_type='P'
                                                     and table_name=upper(v_tab) )) as if_key
                                  from user_tab_columns a
                                  where a.table_name=upper(v_tab) order by a.column_id)) loop

             if k.if_key='Y' then
               v_col_key_val:=k.col_name||','||k.data_type||','||k.col_val||',';
             end if;

             v_col_name_list:=v_col_name_list||k.syn_col_name||',';
             v_col_val_list:= v_col_val_list||k.col_name||',';

             v_col_typ_list:=v_col_typ_list||k.syn_col_type||',';
             v_col_typ_val_list:=v_col_typ_val_list||k.data_type||',';

             v_col_val_name_list:=v_col_val_name_list||k.syn_col_val||',';
             v_col_val_val_list:=v_col_val_val_list||k.col_val||',';

          end loop;

          v_col_name_list:=v_col_name_list||v_col_typ_list||v_col_val_name_list;

          v_col_val_list_t:=v_col_val_list_t||v_col_key_val;
          v_col_val_list:=v_col_val_list_t||v_col_val_list||v_col_typ_val_list||v_col_val_val_list;

          v_col_name_list:=substr(v_col_name_list,1,length(v_col_name_list)-1)||')';
          v_col_val_list:=substr(v_col_val_list,1,length(v_col_val_list)-1)||')';
          return v_col_name_list||v_col_val_list;
   end;

  function gen_upd_syn_log(v_tab varchar2,op_type varchar2) return varchar2 is
         n_counter        int;
         v_col_name_list    varchar2(18000);
         v_col_val_list     varchar2(18000);
         v_col_val_list_t   varchar2(18000);
         v_col_key_val      varchar2(800);

         v_col_typ_list     varchar2(18000);
         v_col_typ_val_list varchar2(18000);

         v_col_name_new_list varchar2(18000);
         v_col_name_old_list varchar2(18000);
         v_col_val_new_list  varchar2(18000);
         v_col_val_old_list  varchar2(18000);
     begin
          n_counter :=1;
          v_col_name_list:='insert into syn_log(syn_id,tname,otype,syn_time,pk_name,pk_type,pk_val,';
          v_col_val_list:='';
          v_col_val_list_t:=' values(syn_log_seq.nextval,'||''''||upper(v_tab)||''''||',''U'',systimestamp,';
          v_col_key_val:='';

          v_col_name_new_list:='';
          v_col_name_old_list:='';
          v_col_val_new_list:='';
          v_col_val_old_list:='';

          for k in (select ''''||column_name||'''' as col_name,
                           ':new.'||lower(column_name) as col_val_new,
                           ':old.'||lower(column_name) as col_val_old,
                           ''''||data_type||'''' as data_type,
                           'col_name' || rownum as syn_col_name,
                           'col_type' || rownum as syn_col_type,
                           'col_new_val' || rownum as syn_col_new_val,
                           'col_old_val' || rownum as syn_col_old_val,
                           (select 'Y' from user_cons_columns b
                           where b.table_name = a.table_name
                             and b.column_name= a.COLUMN_NAME
                             and constraint_name=(select constraint_name
                                     from  user_constraints
                                     where constraint_type='P'
                                       and table_name=upper(v_tab) )) as if_key
                    from user_tab_columns a
                    where a.table_name=upper(v_tab) order by a.column_id) loop

             if k.if_key='Y' then
               v_col_key_val:=k.col_name||','||k.data_type||','||k.col_val_new||',';
             end if;

             --col name
             v_col_name_list:=v_col_name_list||k.syn_col_name||',';
             v_col_typ_list:=v_col_typ_list||k.syn_col_type||',';
             v_col_name_new_list:=v_col_name_new_list||k.syn_col_new_val||',';
             v_col_name_old_list:=v_col_name_old_list||k.syn_col_old_val||',';

             --col val
             v_col_val_list:= v_col_val_list||k.col_name||',';
             v_col_typ_val_list:=v_col_typ_val_list||k.data_type||',';
             v_col_val_new_list:=v_col_val_new_list||k.col_val_new||',';
             v_col_val_old_list:=v_col_val_old_list||k.col_val_old||',';

          end loop;

          v_col_name_list:=v_col_name_list||v_col_typ_list||v_col_name_new_list||v_col_name_old_list;

          v_col_val_list_t:=v_col_val_list_t||v_col_key_val;
          v_col_val_list:=v_col_val_list_t||v_col_val_list||v_col_typ_val_list||v_col_val_new_list||v_col_val_old_list;

          v_col_name_list:=substr(v_col_name_list,1,length(v_col_name_list)-1)||')';
          v_col_val_list:=substr(v_col_val_list,1,length(v_col_val_list)-1)||')';
          return v_col_name_list||v_col_val_list;
  end;
  function gen_del_syn_log(v_tab varchar2,op_type varchar2) return varchar2 is
         n_counter        int;
         v_col_name_list    varchar2(1000);
         v_col_val_list     varchar2(1000);
         v_col_val_list_t   varchar2(1000);
         v_col_key_val      varchar2(100);

         v_col_typ_list     varchar2(1000);
         v_col_typ_val_list varchar2(1000);

         v_col_val_name_list varchar2(1000);
         v_col_val_val_list  varchar2(1000);
      begin
          v_col_name_list:='insert into syn_log(syn_id,tname,otype,syn_time,pk_name,pk_type,pk_val )';
          v_col_val_list:='';
          v_col_val_list_t:=' values(syn_log_seq.nextval,'||''''||upper(v_tab)||''''||',''D'',systimestamp,';
          v_col_key_val:='';

          v_col_val_name_list:='';
          v_col_val_val_list:='';

          for k in (select ''''||column_name||'''' as col_name,
                           ':old.'||lower(column_name) as col_val,
                           ''''||data_type||'''' as data_type,
                           'col_name' || rownum as syn_col_name,
                           'col_type' || rownum as syn_col_type,
                           'col_val' || rownum as syn_col_val,
                           (select 'Y' from user_cons_columns b
                           where b.table_name = a.table_name
                             and b.column_name= a.COLUMN_NAME
                             and constraint_name=(select constraint_name
                                     from  user_constraints
                                     where constraint_type='P'
                                       and table_name=upper(v_tab) )) as if_key
                    from user_tab_columns a
                    where a.table_name=upper(v_tab) order by a.column_id) loop

             if k.if_key='Y' then
               v_col_key_val:=k.col_name||','||k.data_type||','||k.col_val||')';
             end if;
          end loop;
          v_col_val_list:=v_col_val_list_t||v_col_key_val;
          return v_col_name_list||v_col_val_list;
  end;
  
  function gen_syn_log(v_tab varchar2,op_type varchar2) return varchar2 is
  begin
      if op_type='I' then
         return gen_ins_syn_log(v_tab,op_type);
      elsif op_type='U' then
         return gen_upd_syn_log(v_tab,op_type);
      elsif op_type='D' then
         return gen_del_syn_log(v_tab,op_type);
      end if;
  end;

  procedure cre_tab_dml_trg(v_tab varchar2) is
    v_sql varchar2(30000);
  begin
    v_sql:='create or replace trigger trg_'||v_tab||chr(10)||
           'before delete or insert or update '||chr(10)||
           ' on '||v_tab||chr(10)||
           ' for each row '||chr(10)||
           'declare '||chr(10)||
           ' v_sql varchar2(4000); '||chr(10)||
           ' v_col varchar2(1000); '||chr(10)||
           ' n int; '||chr(10)||
           'begin '||chr(10)||
             ' execute immediate ''alter session set nls_date_format=''''YYYY-MM-DD HH24:MI:SS'''''';'||chr(10)||
             ' select count(0) into n from syn_bz; '||chr(10)||
             ' if n>0 then '||chr(10)||
             '     RAISE_APPLICATION_ERROR(-20002,''Table changed! Please run "exec dp_syn_build.init_tab(<table_name>));" resync table!'');'||chr(10)||
             ' end if; '||chr(10)||
             ' case '||chr(10)||
             '  when inserting then '||chr(10)||
             '    '||dp_syn_build.gen_syn_log(v_tab,'I')||';'||chr(10)||
             '  when updating then '||chr(10)||
             '    '||dp_syn_build.gen_syn_log(v_tab,'U')||';'||chr(10)||
             '  when deleting then '||chr(10)||
             '    '||dp_syn_build.gen_syn_log(v_tab,'D')||';'||chr(10)||
             ' end case;'||chr(10)||
            'end;';

      execute immediate v_sql;
    
  end;

  function gen_syn_sql(n_syn_id int) return varchar2 is
      c1      SYS_REFCURSOR;
      v_otype varchar2(3);

      function get_val(col_val varchar2,col_type varchar2) return varchar2 is
      begin
        if col_val is null then
          return 'NULL';
        end if;

        if col_type in('NUMBER','INT','INTEGER') then
           return col_val;
        elsif col_type in('VARCHAR2','CHAR','RAW') then
           return ''''||col_val||'''';
        elsif col_type ='DATE' then
           return 'TO_DATE('''||substr(col_val,1,10)||''',''YYYY-MM-DD'')';
        elsif col_type='TIMESTAMP(3)' then
           return 'TO_DATE('''||substr(col_val,1,19)||''',''YYYY-MM-DD HH24:MI:SS'')';
        else
           return col_val;
        end if;
      end;

      function get_ins_sql(n_syn_id int) return varchar2 is
          v_ins_header   varchar2(18000):='INSERT INTO ';
          v_ins_column   varchar2(18000):='';
          v_tab_name     varchar2(300):='';
          v_ins_value    varchar2(2000):='';
          v_syn_log_rec  t_syn_log_rec;
          c1             SYS_REFCURSOR;
      begin
        open  c1  for 'select syn_id,tname,otype,pk_name,pk_type,pk_val,                              
                              col_name1,col_name2,col_name3,col_name4,col_name5,
                              col_name6,col_name7,col_name8,col_name9,col_name10,
                              col_name11,col_name12,col_name13,col_name14,col_name15,
                              col_name16,col_name17,col_name18,col_name19,col_name20,
                              col_name21,col_name22,col_name23,col_name24,col_name25,
                              col_name26,col_name27,col_name28,col_name29,col_name30,
                              col_name31,col_name32,col_name33,col_name34,col_name35,
                              col_name36,col_name37,col_name38,col_name39,col_name40,
                              col_name41,col_name42,col_name43,col_name44,col_name45,
                              col_name46,col_name47,col_name48,col_name49,col_name50,
                              col_name51,col_name52,col_name53,col_name54,col_name55,
                              col_name56,col_name57,col_name58,col_name59,col_name60,
                              col_name61,col_name62,col_name63,col_name64,col_name65,
                              col_name66,col_name67,col_name68,col_name69,col_name70,
                              col_name71,col_name72,col_name73,col_name74,col_name75,
                              col_name76,col_name77,col_name78,col_name79,col_name80,
                              col_name81,col_name82,col_name83,col_name84,col_name85,
                              col_name86,col_name87,col_name88,col_name89,col_name90,
                              col_name91,col_name92,col_name93,col_name94,col_name95,
                              col_name96,col_name97,col_name98,col_name99,col_name100,                              
                              col_type1,col_type2,col_type3,col_type4,col_type5,
                              col_type6,col_type7,col_type8,col_type9,col_type10,
                              col_type11,col_type12,col_type13,col_type14,col_type15,
                              col_type16,col_type17,col_type18,col_type19,col_type20,
                              col_type21,col_type22,col_type23,col_type24,col_type25,
                              col_type26,col_type27,col_type28,col_type29,col_type30,
                              col_type31,col_type32,col_type33,col_type34,col_type35,
                              col_type36,col_type37,col_type38,col_type39,col_type40,
                              col_type41,col_type42,col_type43,col_type44,col_type45,
                              col_type46,col_type47,col_type48,col_type49,col_type50,
                              col_type51,col_type52,col_type53,col_type54,col_type55,
                              col_type56,col_type57,col_type58,col_type59,col_type60,
                              col_type61,col_type62,col_type63,col_type64,col_type65,
                              col_type66,col_type67,col_type68,col_type69,col_type70,
                              col_type71,col_type72,col_type73,col_type74,col_type75,
                              col_type76,col_type77,col_type78,col_type79,col_type80,
                              col_type81,col_type82,col_type83,col_type84,col_type85,
                              col_type86,col_type87,col_type88,col_type89,col_type90,
                              col_type91,col_type92,col_type93,col_type94,col_type95,
                              col_type96,col_type97,col_type98,col_type99,col_type100,
                              col_new_val1,col_new_val2,col_new_val3,col_new_val4,col_new_val5,
                              col_new_val6,col_new_val7,col_new_val8,col_new_val9,col_new_val10,
                              col_new_val11,col_new_val12,col_new_val13,col_new_val14,col_new_val15,
                              col_new_val16,col_new_val17,col_new_val18,col_new_val19,col_new_val20,                              
                              col_new_val21,col_new_val22,col_new_val23,col_new_val24,col_new_val25,
                              col_new_val26,col_new_val27,col_new_val28,col_new_val29,col_new_val30,
                              col_new_val31,col_new_val32,col_new_val33,col_new_val34,col_new_val35,
                              col_new_val36,col_new_val37,col_new_val38,col_new_val39,col_new_val40, 
                              col_new_val41,col_new_val42,col_new_val43,col_new_val44,col_new_val45,
                              col_new_val46,col_new_val47,col_new_val48,col_new_val49,col_new_val50,
                              col_new_val51,col_new_val52,col_new_val53,col_new_val54,col_new_val55,
                              col_new_val56,col_new_val57,col_new_val58,col_new_val59,col_new_val60,                              
                              col_new_val61,col_new_val62,col_new_val63,col_new_val64,col_new_val65,
                              col_new_val66,col_new_val67,col_new_val68,col_new_val69,col_new_val70,
                              col_new_val71,col_new_val72,col_new_val73,col_new_val74,col_new_val75,
                              col_new_val76,col_new_val77,col_new_val78,col_new_val79,col_new_val80,
                              col_new_val81,col_new_val82,col_new_val83,col_new_val84,col_new_val85,
                              col_new_val86,col_new_val87,col_new_val88,col_new_val89,col_new_val90,
                              col_new_val91,col_new_val92,col_new_val93,col_new_val94,col_new_val95,
                              col_new_val96,col_new_val97,col_new_val98,col_new_val99,col_new_val100, 
                              col_old_val1,col_old_val2,col_old_val3,col_old_val4,col_old_val5,
                              col_old_val6,col_old_val7,col_old_val8,col_old_val9,col_old_val10,
                              col_old_val11,col_old_val12,col_old_val13,col_old_val14,col_old_val15,
                              col_old_val16,col_old_val17,col_old_val18,col_old_val19,col_old_val20,                              
                              col_old_val21,col_old_val22,col_old_val23,col_old_val24,col_old_val25,
                              col_old_val26,col_old_val27,col_old_val28,col_old_val29,col_old_val30,
                              col_old_val31,col_old_val32,col_old_val33,col_old_val34,col_old_val35,
                              col_old_val36,col_old_val37,col_old_val38,col_old_val39,col_old_val40,
                              col_old_val41,col_old_val42,col_old_val43,col_old_val44,col_old_val45,
                              col_old_val46,col_old_val47,col_old_val48,col_old_val49,col_old_val50,
                              col_old_val51,col_old_val52,col_old_val53,col_old_val54,col_old_val55,
                              col_old_val56,col_old_val57,col_old_val58,col_old_val59,col_old_val60,                              
                              col_old_val61,col_old_val62,col_old_val63,col_old_val64,col_old_val65,
                              col_old_val66,col_old_val67,col_old_val68,col_old_val69,col_old_val70,
                              col_old_val71,col_old_val72,col_old_val73,col_old_val74,col_old_val75,
                              col_old_val76,col_old_val77,col_old_val78,col_old_val79,col_old_val80,
                              col_old_val81,col_old_val82,col_old_val83,col_old_val84,col_old_val85,
                              col_old_val86,col_old_val87,col_old_val88,col_old_val89,col_old_val90,
                              col_old_val91,col_old_val92,col_old_val93,col_old_val94,col_old_val95,
                              col_old_val96,col_old_val97,col_old_val98,col_old_val99,col_old_val100,
                              if_syn,syn_time from syn_log where syn_id=:1' using n_syn_id ;
        fetch c1 into v_syn_log_rec;
             v_tab_name:=v_syn_log_rec.tname;
             if v_syn_log_rec.col_name1 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name1||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val1,v_syn_log_rec.col_type1)||',';
             end if;

             if v_syn_log_rec.col_name2 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name2||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val2,v_syn_log_rec.col_type2)||',';
             end if;

             if v_syn_log_rec.col_name3 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name3||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val3,v_syn_log_rec.col_type3)||',';
             end if;
             if v_syn_log_rec.col_name4 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name4||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val4,v_syn_log_rec.col_type4)||',';
             end if;

             if v_syn_log_rec.col_name5 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name5||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val5,v_syn_log_rec.col_type5)||',';
             end if;
             if v_syn_log_rec.col_name6 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name6||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val6,v_syn_log_rec.col_type6)||',';
             end if;

             if v_syn_log_rec.col_name7 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name7||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val7,v_syn_log_rec.col_type7)||',';
             end if;
             if v_syn_log_rec.col_name8 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name8||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val8,v_syn_log_rec.col_type8)||',';
             end if;

             if v_syn_log_rec.col_name9 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name9||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val9,v_syn_log_rec.col_type9)||',';
             end if;
             if v_syn_log_rec.col_name10 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name10||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val10,v_syn_log_rec.col_type10)||',';
             end if;

             if v_syn_log_rec.col_name11 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name11||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val11,v_syn_log_rec.col_type11)||',';
             end if;
             if v_syn_log_rec.col_name12 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name12||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val12,v_syn_log_rec.col_type12)||',';
             end if;

             if v_syn_log_rec.col_name13 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name13||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val13,v_syn_log_rec.col_type13)||',';
             end if;
             if v_syn_log_rec.col_name14 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name14||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val14,v_syn_log_rec.col_type14)||',';
             end if;

             if v_syn_log_rec.col_name15 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name15||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val15,v_syn_log_rec.col_type15)||',';
             end if;
             if v_syn_log_rec.col_name16 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name16||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val16,v_syn_log_rec.col_type16)||',';
             end if;

             if v_syn_log_rec.col_name17 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name17||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val17,v_syn_log_rec.col_type17)||',';
             end if;
             if v_syn_log_rec.col_name18 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name18||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val18,v_syn_log_rec.col_type18)||',';
             end if;

             if v_syn_log_rec.col_name19 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name19||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val19,v_syn_log_rec.col_type19)||',';
             end if;
             if v_syn_log_rec.col_name20 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name20||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val20,v_syn_log_rec.col_type20)||',';
             end if;
             
             if v_syn_log_rec.col_name21 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name21||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val21,v_syn_log_rec.col_type21)||',';
             end if;

             if v_syn_log_rec.col_name22 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name22||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val22,v_syn_log_rec.col_type22)||',';
             end if;

             if v_syn_log_rec.col_name23 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name23||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val23,v_syn_log_rec.col_type23)||',';
             end if;
             if v_syn_log_rec.col_name24 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name24||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val24,v_syn_log_rec.col_type24)||',';
             end if;

             if v_syn_log_rec.col_name25 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name25||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val25,v_syn_log_rec.col_type25)||',';
             end if;
             if v_syn_log_rec.col_name26 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name26||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val26,v_syn_log_rec.col_type26)||',';
             end if;

             if v_syn_log_rec.col_name27 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name27||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val27,v_syn_log_rec.col_type27)||',';
             end if;
             if v_syn_log_rec.col_name28 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name28||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val28,v_syn_log_rec.col_type28)||',';
             end if;

             if v_syn_log_rec.col_name29 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name29||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val29,v_syn_log_rec.col_type29)||',';
             end if;
             if v_syn_log_rec.col_name30 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name30||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val30,v_syn_log_rec.col_type30)||',';
             end if;

             if v_syn_log_rec.col_name31 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name31||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val31,v_syn_log_rec.col_type31)||',';
             end if;
             if v_syn_log_rec.col_name32 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name32||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val32,v_syn_log_rec.col_type32)||',';
             end if;

             if v_syn_log_rec.col_name33 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name33||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val33,v_syn_log_rec.col_type33)||',';
             end if;
             if v_syn_log_rec.col_name34 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name34||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val34,v_syn_log_rec.col_type34)||',';
             end if;

             if v_syn_log_rec.col_name35 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name35||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val35,v_syn_log_rec.col_type35)||',';
             end if;
             if v_syn_log_rec.col_name36 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name36||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val36,v_syn_log_rec.col_type36)||',';
             end if;

             if v_syn_log_rec.col_name37 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name37||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val37,v_syn_log_rec.col_type37)||',';
             end if;
             if v_syn_log_rec.col_name38 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name38||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val38,v_syn_log_rec.col_type38)||',';
             end if;

             if v_syn_log_rec.col_name39 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name39||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val39,v_syn_log_rec.col_type39)||',';
             end if;

             if v_syn_log_rec.col_name40 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name40||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val40,v_syn_log_rec.col_type40)||',';
             end if;
             
             if v_syn_log_rec.col_name41 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name41||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val41,v_syn_log_rec.col_type41)||',';
             end if;
             if v_syn_log_rec.col_name42 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name42||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val42,v_syn_log_rec.col_type42)||',';
             end if;

             if v_syn_log_rec.col_name43 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name43||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val43,v_syn_log_rec.col_type43)||',';
             end if;
             if v_syn_log_rec.col_name44 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name44||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val44,v_syn_log_rec.col_type44)||',';
             end if;

             if v_syn_log_rec.col_name45 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name45||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val45,v_syn_log_rec.col_type45)||',';
             end if;
             if v_syn_log_rec.col_name46 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name46||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val46,v_syn_log_rec.col_type46)||',';
             end if;

             if v_syn_log_rec.col_name47 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name47||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val47,v_syn_log_rec.col_type47)||',';
             end if;
             if v_syn_log_rec.col_name48 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name48||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val48,v_syn_log_rec.col_type48)||',';
             end if;

             if v_syn_log_rec.col_name49 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name49||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val49,v_syn_log_rec.col_type49)||',';
             end if;

             if v_syn_log_rec.col_name50 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name50||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val50,v_syn_log_rec.col_type50)||',';
             end if;
             
             if v_syn_log_rec.col_name51 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name51||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val51,v_syn_log_rec.col_type51)||',';
             end if;
             if v_syn_log_rec.col_name52 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name52||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val52,v_syn_log_rec.col_type52)||',';
             end if;

             if v_syn_log_rec.col_name53 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name53||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val53,v_syn_log_rec.col_type53)||',';
             end if;
             if v_syn_log_rec.col_name54 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name54||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val54,v_syn_log_rec.col_type54)||',';
             end if;

             if v_syn_log_rec.col_name55 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name55||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val55,v_syn_log_rec.col_type55)||',';
             end if;
             if v_syn_log_rec.col_name56 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name56||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val56,v_syn_log_rec.col_type56)||',';
             end if;

             if v_syn_log_rec.col_name57 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name57||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val57,v_syn_log_rec.col_type57)||',';
             end if;
             if v_syn_log_rec.col_name58 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name58||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val58,v_syn_log_rec.col_type58)||',';
             end if;

             if v_syn_log_rec.col_name59 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name59||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val59,v_syn_log_rec.col_type59)||',';
             end if;

             if v_syn_log_rec.col_name60 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name60||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val60,v_syn_log_rec.col_type60)||',';
             end if;
             
             if v_syn_log_rec.col_name61 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name61||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val61,v_syn_log_rec.col_type61)||',';
             end if;
             if v_syn_log_rec.col_name62 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name62||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val62,v_syn_log_rec.col_type62)||',';
             end if;

             if v_syn_log_rec.col_name63 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name63||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val63,v_syn_log_rec.col_type63)||',';
             end if;
             if v_syn_log_rec.col_name64 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name64||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val64,v_syn_log_rec.col_type64)||',';
             end if;

             if v_syn_log_rec.col_name65 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name65||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val65,v_syn_log_rec.col_type65)||',';
             end if;
             if v_syn_log_rec.col_name66 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name66||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val66,v_syn_log_rec.col_type66)||',';
             end if;

             if v_syn_log_rec.col_name67 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name67||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val67,v_syn_log_rec.col_type67)||',';
             end if;
             if v_syn_log_rec.col_name68 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name68||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val68,v_syn_log_rec.col_type68)||',';
             end if;

             if v_syn_log_rec.col_name69 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name69||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val69,v_syn_log_rec.col_type69)||',';
             end if;

             if v_syn_log_rec.col_name70 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name70||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val70,v_syn_log_rec.col_type70)||',';
             end if;
             
             if v_syn_log_rec.col_name71 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name71||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val71,v_syn_log_rec.col_type71)||',';
             end if;
             if v_syn_log_rec.col_name72 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name72||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val72,v_syn_log_rec.col_type72)||',';
             end if;

             if v_syn_log_rec.col_name73 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name73||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val73,v_syn_log_rec.col_type73)||',';
             end if;
             if v_syn_log_rec.col_name74 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name74||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val74,v_syn_log_rec.col_type74)||',';
             end if;

             if v_syn_log_rec.col_name75 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name75||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val75,v_syn_log_rec.col_type75)||',';
             end if;
             if v_syn_log_rec.col_name76 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name76||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val76,v_syn_log_rec.col_type76)||',';
             end if;

             if v_syn_log_rec.col_name77 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name77||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val77,v_syn_log_rec.col_type77)||',';
             end if;
             if v_syn_log_rec.col_name78 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name78||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val78,v_syn_log_rec.col_type78)||',';
             end if;

             if v_syn_log_rec.col_name79 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name79||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val79,v_syn_log_rec.col_type79)||',';
             end if;

             if v_syn_log_rec.col_name80 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name80||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val80,v_syn_log_rec.col_type80)||',';
             end if;
             
             if v_syn_log_rec.col_name81 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name81||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val81,v_syn_log_rec.col_type81)||',';
             end if;
             
             if v_syn_log_rec.col_name82 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name82||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val82,v_syn_log_rec.col_type82)||',';
             end if;

             if v_syn_log_rec.col_name83 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name83||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val83,v_syn_log_rec.col_type83)||',';
             end if;
             if v_syn_log_rec.col_name84 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name84||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val84,v_syn_log_rec.col_type84)||',';
             end if;

             if v_syn_log_rec.col_name85 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name85||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val85,v_syn_log_rec.col_type85)||',';
             end if;
             if v_syn_log_rec.col_name86 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name86||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val86,v_syn_log_rec.col_type86)||',';
             end if;

             if v_syn_log_rec.col_name87 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name87||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val87,v_syn_log_rec.col_type87)||',';
             end if;
             if v_syn_log_rec.col_name88 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name88||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val88,v_syn_log_rec.col_type88)||',';
             end if;

             if v_syn_log_rec.col_name89 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name89||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val89,v_syn_log_rec.col_type89)||',';
             end if;

             if v_syn_log_rec.col_name90 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name90||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val90,v_syn_log_rec.col_type90)||',';
             end if;
             
             if v_syn_log_rec.col_name91 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name91||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val91,v_syn_log_rec.col_type91)||',';
             end if;

             if v_syn_log_rec.col_name92 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name92||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val92,v_syn_log_rec.col_type92)||',';
             end if;

             if v_syn_log_rec.col_name93 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name93||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val93,v_syn_log_rec.col_type93)||',';
             end if;
             if v_syn_log_rec.col_name94 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name94||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val94,v_syn_log_rec.col_type94)||',';
             end if;

             if v_syn_log_rec.col_name95 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name95||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val95,v_syn_log_rec.col_type95)||',';
             end if;
             if v_syn_log_rec.col_name96 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name96||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val96,v_syn_log_rec.col_type96)||',';
             end if;

             if v_syn_log_rec.col_name97 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name97||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val97,v_syn_log_rec.col_type97)||',';
             end if;
             if v_syn_log_rec.col_name98 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name98||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val98,v_syn_log_rec.col_type98)||',';
             end if;

             if v_syn_log_rec.col_name99 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name99||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val99,v_syn_log_rec.col_type99)||',';
             end if;

             if v_syn_log_rec.col_name100 is not null then
                v_ins_column:=v_ins_column||v_syn_log_rec.col_name100||',';
                v_ins_value :=v_ins_value||get_val(v_syn_log_rec.col_new_val100,v_syn_log_rec.col_type100)||',';
             end if;
             
         close c1;
        -- end loop;
         v_ins_header:=v_ins_header||v_tab_name||'('||v_ins_column;
         v_ins_header:=substr(v_ins_header,1,length(v_ins_header)-1)||')';
         v_ins_value :=substr(v_ins_value,1,length(v_ins_value)-1)||')';
         return v_ins_header||' values ('||v_ins_value;
      end;

      function get_upd_sql(n_syn_id int) return varchar2 is
          v_upd_header   varchar2(18000):='';
          v_upd_column   varchar2(18000):='';
          v_upd_where    varchar2(2000):='';
          v_syn_log_rec  t_syn_log_rec;
          c1             SYS_REFCURSOR;
      begin
              open  c1  for 'select syn_id,tname,otype,pk_name,pk_type,pk_val,
                                    col_name1,col_name2,col_name3,col_name4,col_name5,
                                    col_name6,col_name7,col_name8,col_name9,col_name10,
                                    col_name11,col_name12,col_name13,col_name14,col_name15,
                                    col_name16,col_name17,col_name18,col_name19,col_name20,
                                    col_name21,col_name22,col_name23,col_name24,col_name25,
                                    col_name26,col_name27,col_name28,col_name29,col_name30,
                                    col_name31,col_name32,col_name33,col_name34,col_name35,
                                    col_name36,col_name37,col_name38,col_name39,col_name40,
                                    col_name51,col_name52,col_name53,col_name54,col_name55,
                                    col_name56,col_name57,col_name58,col_name59,col_name60,
                                    col_name61,col_name62,col_name63,col_name64,col_name65,
                                    col_name66,col_name67,col_name68,col_name69,col_name70,
                                    col_name71,col_name72,col_name73,col_name74,col_name75,
                                    col_name76,col_name77,col_name78,col_name79,col_name80,
                                    col_name81,col_name82,col_name83,col_name84,col_name85,
                                    col_name86,col_name87,col_name88,col_name89,col_name90,
                                    col_name91,col_name92,col_name93,col_name94,col_name95,
                                    col_name96,col_name97,col_name98,col_name99,col_name100,
                                    col_type1,col_type2,col_type3,col_type4,col_type5,
                                    col_type6,col_type7,col_type8,col_type9,col_type10,
                                    col_type11,col_type12,col_type13,col_type14,col_type15,
                                    col_type16,col_type17,col_type18,col_type19,col_type20,
                                    col_type21,col_type22,col_type23,col_type24,col_type25,
                                    col_type26,col_type27,col_type28,col_type29,col_type30,
                                    col_type31,col_type32,col_type33,col_type34,col_type35,
                                    col_type36,col_type37,col_type38,col_type39,col_type40,
                                    col_type41,col_type42,col_type43,col_type44,col_type45,
                                    col_type46,col_type47,col_type48,col_type49,col_type50,
                                    col_type51,col_type52,col_type53,col_type54,col_type55,
                                    col_type56,col_type57,col_type58,col_type59,col_type60,
                                    col_type61,col_type62,col_type63,col_type64,col_type65,
                                    col_type66,col_type67,col_type68,col_type69,col_type70,
                                    col_type71,col_type72,col_type73,col_type74,col_type75,
                                    col_type76,col_type77,col_type78,col_type79,col_type80,
                                    col_type81,col_type82,col_type83,col_type84,col_type85,
                                    col_type86,col_type87,col_type88,col_type89,col_type90,
                                    col_type91,col_type92,col_type93,col_type94,col_type95,
                                    col_type96,col_type97,col_type98,col_type99,col_type100,                                    
                                    col_new_val1,col_new_val2,col_new_val3,col_new_val4,col_new_val5,
                                    col_new_val6,col_new_val7,col_new_val8,col_new_val9,col_new_val10,
                                    col_new_val11,col_new_val12,col_new_val13,col_new_val14,col_new_val15,
                                    col_new_val16,col_new_val17,col_new_val18,col_new_val19,col_new_val20,                              
                                    col_new_val21,col_new_val22,col_new_val23,col_new_val24,col_new_val25,
                                    col_new_val26,col_new_val27,col_new_val28, col_new_val29,col_new_val30,
                                    col_new_val31,col_new_val32,col_new_val33,col_new_val34,col_new_val35,
                                    col_new_val36,col_new_val37,col_new_val38,col_new_val39,col_new_val40, 
                                    col_new_val41,col_new_val42,col_new_val43,col_new_val44,col_new_val45,
                                    col_new_val46,col_new_val47,col_new_val48,col_new_val49,col_new_val50,
                                    col_new_val51,col_new_val52,col_new_val53,col_new_val54,col_new_val55,
                                    col_new_val56,col_new_val57,col_new_val58,col_new_val59,col_new_val60,                              
                                    col_new_val61,col_new_val62,col_new_val63,col_new_val64,col_new_val65,
                                    col_new_val66,col_new_val67,col_new_val68, col_new_val69,col_new_val70,
                                    col_new_val71,col_new_val72,col_new_val73,col_new_val74,col_new_val75,
                                    col_new_val76,col_new_val77,col_new_val78,col_new_val79,col_new_val80, 
                                    col_new_val81,col_new_val82,col_new_val83,col_new_val84,col_new_val85,
                                    col_new_val86,col_new_val87,col_new_val88, col_new_val89,col_new_val90,
                                    col_new_val91,col_new_val92,col_new_val93,col_new_val94,col_new_val95,
                                    col_new_val96,col_new_val97,col_new_val98,col_new_val99,col_new_val100, 
                                    col_old_val1,col_old_val2,col_old_val3,col_old_val4,col_old_val5,
                                    col_old_val6,col_old_val7,col_old_val8,col_old_val9,col_old_val10,
                                    col_old_val11,col_old_val12,col_old_val13,col_old_val14,col_old_val15,
                                    col_old_val16,col_old_val17,col_old_val18,col_old_val19,col_old_val20,                              
                                    col_old_val21,col_old_val22,col_old_val23,col_old_val24,col_old_val25,
                                    col_old_val26,col_old_val27,col_old_val28,col_old_val29,col_old_val30,
                                    col_old_val31,col_old_val32,col_old_val33,col_old_val34,col_old_val35,
                                    col_old_val36,col_old_val37,col_old_val38,col_old_val39,col_old_val40,
                                    col_old_val41,col_old_val42,col_old_val43,col_old_val44,col_old_val45,
                                    col_old_val46,col_old_val47,col_old_val48,col_old_val49,col_old_val50,
                                    col_old_val51,col_old_val52,col_old_val53,col_old_val54,col_old_val55,
                                    col_old_val56,col_old_val57,col_old_val58,col_old_val59,col_old_val60,                              
                                    col_old_val61,col_old_val62,col_old_val63,col_old_val64,col_old_val65,
                                    col_old_val66,col_old_val67,col_old_val68,col_old_val69,col_old_val70,
                                    col_old_val71,col_old_val72,col_old_val73,col_old_val74,col_old_val75,
                                    col_old_val76,col_old_val77,col_old_val78,col_old_val79,col_old_val80,
                                    col_old_val81,col_old_val82,col_old_val83,col_old_val84,col_old_val85,
                                    col_old_val86,col_old_val87,col_old_val88,col_old_val89,col_old_val90,
                                    col_old_val91,col_old_val92,col_old_val93,col_old_val94,col_old_val95,
                                    col_old_val96,col_old_val97,col_old_val98,col_old_val99,col_old_val100,
                                    if_syn,syn_time from syn_log where syn_id=:1' using n_syn_id ;
            fetch c1 into v_syn_log_rec;
             v_upd_header:='UPDATE '||v_syn_log_rec.tname||' SET ';
             v_upd_where :=' WHERE '||v_syn_log_rec.pk_name||'='||get_val(v_syn_log_rec.pk_val,v_syn_log_rec.pk_type);

             if nvl(v_syn_log_rec.col_new_val1,'Y')<>nvl(v_syn_log_rec.col_old_val1,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name1||'='||get_val(v_syn_log_rec.col_new_val1,v_syn_log_rec.col_type1)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val2,'Y')<>nvl(v_syn_log_rec.col_old_val2,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name2||'='||get_val(v_syn_log_rec.col_new_val2,v_syn_log_rec.col_type2)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val3,'Y')<>nvl(v_syn_log_rec.col_old_val3,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name3||'='||get_val(v_syn_log_rec.col_new_val3,v_syn_log_rec.col_type3)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val4,'Y')<>nvl(v_syn_log_rec.col_old_val4,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name4||'='||get_val(v_syn_log_rec.col_new_val4,v_syn_log_rec.col_type4)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val5,'Y')<>nvl(v_syn_log_rec.col_old_val5,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name5||'='||get_val(v_syn_log_rec.col_new_val5,v_syn_log_rec.col_type5)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val6,'Y')<>nvl(v_syn_log_rec.col_old_val6,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name6||'='||get_val(v_syn_log_rec.col_new_val6,v_syn_log_rec.col_type6)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val7,'Y')<>nvl(v_syn_log_rec.col_old_val7,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name7||'='||get_val(v_syn_log_rec.col_new_val7,v_syn_log_rec.col_type7)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val8,'Y')<>nvl(v_syn_log_rec.col_old_val8,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name8||'='||get_val(v_syn_log_rec.col_new_val8,v_syn_log_rec.col_type8)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val9,'Y')<>nvl(v_syn_log_rec.col_old_val9,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name9||'='||get_val(v_syn_log_rec.col_new_val9,v_syn_log_rec.col_type9)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val10,'Y')<>nvl(v_syn_log_rec.col_old_val10,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name10||'='||get_val(v_syn_log_rec.col_new_val10,v_syn_log_rec.col_type10)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val11,'Y')<>nvl(v_syn_log_rec.col_old_val11,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name11||'='||get_val(v_syn_log_rec.col_new_val11,v_syn_log_rec.col_type11)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val12,'Y')<>nvl(v_syn_log_rec.col_old_val12,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name12||'='||get_val(v_syn_log_rec.col_new_val12,v_syn_log_rec.col_type12)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val13,'Y')<>nvl(v_syn_log_rec.col_old_val13,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name13||'='||get_val(v_syn_log_rec.col_new_val13,v_syn_log_rec.col_type13)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val14,'Y')<>nvl(v_syn_log_rec.col_old_val14,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name14||'='||get_val(v_syn_log_rec.col_new_val14,v_syn_log_rec.col_type14)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val15,'Y')<>nvl(v_syn_log_rec.col_old_val15,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name15||'='||get_val(v_syn_log_rec.col_new_val15,v_syn_log_rec.col_type15)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val16,'Y')<>nvl(v_syn_log_rec.col_old_val16,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name16||'='||get_val(v_syn_log_rec.col_new_val16,v_syn_log_rec.col_type16)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val17,'Y')<>nvl(v_syn_log_rec.col_old_val17,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name17||'='||get_val(v_syn_log_rec.col_new_val17,v_syn_log_rec.col_type17)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val18,'Y')<>nvl(v_syn_log_rec.col_old_val18,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name18||'='||get_val(v_syn_log_rec.col_new_val18,v_syn_log_rec.col_type18)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val19,'Y')<>nvl(v_syn_log_rec.col_old_val19,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name19||'='||get_val(v_syn_log_rec.col_new_val19,v_syn_log_rec.col_type19)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val20,'Y')<>nvl(v_syn_log_rec.col_old_val20,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name20||'='||get_val(v_syn_log_rec.col_new_val20,v_syn_log_rec.col_type20)||',';
             end if;
             
             if nvl(v_syn_log_rec.col_new_val21,'Y')<>nvl(v_syn_log_rec.col_old_val21,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name21||'='||get_val(v_syn_log_rec.col_new_val21,v_syn_log_rec.col_type21)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val22,'Y')<>nvl(v_syn_log_rec.col_old_val22,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name22||'='||get_val(v_syn_log_rec.col_new_val22,v_syn_log_rec.col_type22)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val23,'Y')<>nvl(v_syn_log_rec.col_old_val23,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name23||'='||get_val(v_syn_log_rec.col_new_val23,v_syn_log_rec.col_type23)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val24,'Y')<>nvl(v_syn_log_rec.col_old_val24,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name24||'='||get_val(v_syn_log_rec.col_new_val24,v_syn_log_rec.col_type24)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val25,'Y')<>nvl(v_syn_log_rec.col_old_val25,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name25||'='||get_val(v_syn_log_rec.col_new_val25,v_syn_log_rec.col_type25)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val26,'Y')<>nvl(v_syn_log_rec.col_old_val26,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name26||'='||get_val(v_syn_log_rec.col_new_val26,v_syn_log_rec.col_type26)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val27,'Y')<>nvl(v_syn_log_rec.col_old_val27,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name27||'='||get_val(v_syn_log_rec.col_new_val27,v_syn_log_rec.col_type27)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val28,'Y')<>nvl(v_syn_log_rec.col_old_val28,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name28||'='||get_val(v_syn_log_rec.col_new_val28,v_syn_log_rec.col_type28)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val29,'Y')<>nvl(v_syn_log_rec.col_old_val29,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name29||'='||get_val(v_syn_log_rec.col_new_val29,v_syn_log_rec.col_type29)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val30,'Y')<>nvl(v_syn_log_rec.col_old_val30,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name30||'='||get_val(v_syn_log_rec.col_new_val30,v_syn_log_rec.col_type30)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val31,'Y')<>nvl(v_syn_log_rec.col_old_val31,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name31||'='||get_val(v_syn_log_rec.col_new_val31,v_syn_log_rec.col_type31)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val32,'Y')<>nvl(v_syn_log_rec.col_old_val32,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name32||'='||get_val(v_syn_log_rec.col_new_val32,v_syn_log_rec.col_type32)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val33,'Y')<>nvl(v_syn_log_rec.col_old_val33,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name33||'='||get_val(v_syn_log_rec.col_new_val33,v_syn_log_rec.col_type33)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val34,'Y')<>nvl(v_syn_log_rec.col_old_val34,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name34||'='||get_val(v_syn_log_rec.col_new_val34,v_syn_log_rec.col_type34)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val35,'Y')<>nvl(v_syn_log_rec.col_old_val35,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name35||'='||get_val(v_syn_log_rec.col_new_val35,v_syn_log_rec.col_type35)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val36,'Y')<>nvl(v_syn_log_rec.col_old_val36,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name36||'='||get_val(v_syn_log_rec.col_new_val36,v_syn_log_rec.col_type36)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val37,'Y')<>nvl(v_syn_log_rec.col_old_val37,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name37||'='||get_val(v_syn_log_rec.col_new_val37,v_syn_log_rec.col_type37)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val38,'Y')<>nvl(v_syn_log_rec.col_old_val38,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name38||'='||get_val(v_syn_log_rec.col_new_val38,v_syn_log_rec.col_type38)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val39,'Y')<>nvl(v_syn_log_rec.col_old_val39,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name39||'='||get_val(v_syn_log_rec.col_new_val39,v_syn_log_rec.col_type39)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val40,'Y')<>nvl(v_syn_log_rec.col_old_val40,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name40||'='||get_val(v_syn_log_rec.col_new_val40,v_syn_log_rec.col_type40)||',';
             end if;  
             
             if nvl(v_syn_log_rec.col_new_val41,'Y')<>nvl(v_syn_log_rec.col_old_val41,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name41||'='||get_val(v_syn_log_rec.col_new_val41,v_syn_log_rec.col_type41)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val42,'Y')<>nvl(v_syn_log_rec.col_old_val42,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name42||'='||get_val(v_syn_log_rec.col_new_val42,v_syn_log_rec.col_type42)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val43,'Y')<>nvl(v_syn_log_rec.col_old_val43,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name43||'='||get_val(v_syn_log_rec.col_new_val43,v_syn_log_rec.col_type43)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val44,'Y')<>nvl(v_syn_log_rec.col_old_val44,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name44||'='||get_val(v_syn_log_rec.col_new_val44,v_syn_log_rec.col_type44)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val45,'Y')<>nvl(v_syn_log_rec.col_old_val45,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name45||'='||get_val(v_syn_log_rec.col_new_val45,v_syn_log_rec.col_type45)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val46,'Y')<>nvl(v_syn_log_rec.col_old_val46,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name46||'='||get_val(v_syn_log_rec.col_new_val46,v_syn_log_rec.col_type46)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val47,'Y')<>nvl(v_syn_log_rec.col_old_val47,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name47||'='||get_val(v_syn_log_rec.col_new_val47,v_syn_log_rec.col_type47)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val48,'Y')<>nvl(v_syn_log_rec.col_old_val48,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name48||'='||get_val(v_syn_log_rec.col_new_val48,v_syn_log_rec.col_type48)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val49,'Y')<>nvl(v_syn_log_rec.col_old_val49,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name49||'='||get_val(v_syn_log_rec.col_new_val49,v_syn_log_rec.col_type49)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val50,'Y')<>nvl(v_syn_log_rec.col_old_val50,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name50||'='||get_val(v_syn_log_rec.col_new_val50,v_syn_log_rec.col_type50)||',';
             end if;  
             
             if nvl(v_syn_log_rec.col_new_val51,'Y')<>nvl(v_syn_log_rec.col_old_val51,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name51||'='||get_val(v_syn_log_rec.col_new_val51,v_syn_log_rec.col_type51)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val52,'Y')<>nvl(v_syn_log_rec.col_old_val52,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name52||'='||get_val(v_syn_log_rec.col_new_val52,v_syn_log_rec.col_type52)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val53,'Y')<>nvl(v_syn_log_rec.col_old_val53,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name53||'='||get_val(v_syn_log_rec.col_new_val53,v_syn_log_rec.col_type53)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val54,'Y')<>nvl(v_syn_log_rec.col_old_val54,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name54||'='||get_val(v_syn_log_rec.col_new_val54,v_syn_log_rec.col_type54)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val55,'Y')<>nvl(v_syn_log_rec.col_old_val55,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name55||'='||get_val(v_syn_log_rec.col_new_val55,v_syn_log_rec.col_type55)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val56,'Y')<>nvl(v_syn_log_rec.col_old_val56,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name56||'='||get_val(v_syn_log_rec.col_new_val56,v_syn_log_rec.col_type56)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val57,'Y')<>nvl(v_syn_log_rec.col_old_val57,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name57||'='||get_val(v_syn_log_rec.col_new_val57,v_syn_log_rec.col_type57)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val58,'Y')<>nvl(v_syn_log_rec.col_old_val58,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name58||'='||get_val(v_syn_log_rec.col_new_val58,v_syn_log_rec.col_type58)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val59,'Y')<>nvl(v_syn_log_rec.col_old_val59,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name59||'='||get_val(v_syn_log_rec.col_new_val59,v_syn_log_rec.col_type59)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val60,'Y')<>nvl(v_syn_log_rec.col_old_val60,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name60||'='||get_val(v_syn_log_rec.col_new_val60,v_syn_log_rec.col_type60)||',';
             end if;  
             
             if nvl(v_syn_log_rec.col_new_val61,'Y')<>nvl(v_syn_log_rec.col_old_val61,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name61||'='||get_val(v_syn_log_rec.col_new_val61,v_syn_log_rec.col_type61)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val62,'Y')<>nvl(v_syn_log_rec.col_old_val62,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name62||'='||get_val(v_syn_log_rec.col_new_val62,v_syn_log_rec.col_type62)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val63,'Y')<>nvl(v_syn_log_rec.col_old_val63,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name63||'='||get_val(v_syn_log_rec.col_new_val63,v_syn_log_rec.col_type63)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val64,'Y')<>nvl(v_syn_log_rec.col_old_val64,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name64||'='||get_val(v_syn_log_rec.col_new_val64,v_syn_log_rec.col_type64)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val65,'Y')<>nvl(v_syn_log_rec.col_old_val65,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name65||'='||get_val(v_syn_log_rec.col_new_val65,v_syn_log_rec.col_type65)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val66,'Y')<>nvl(v_syn_log_rec.col_old_val66,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name66||'='||get_val(v_syn_log_rec.col_new_val66,v_syn_log_rec.col_type66)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val67,'Y')<>nvl(v_syn_log_rec.col_old_val67,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name67||'='||get_val(v_syn_log_rec.col_new_val67,v_syn_log_rec.col_type67)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val68,'Y')<>nvl(v_syn_log_rec.col_old_val68,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name68||'='||get_val(v_syn_log_rec.col_new_val68,v_syn_log_rec.col_type68)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val69,'Y')<>nvl(v_syn_log_rec.col_old_val69,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name69||'='||get_val(v_syn_log_rec.col_new_val69,v_syn_log_rec.col_type69)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val70,'Y')<>nvl(v_syn_log_rec.col_old_val70,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name70||'='||get_val(v_syn_log_rec.col_new_val70,v_syn_log_rec.col_type70)||',';
             end if;  
             
             if nvl(v_syn_log_rec.col_new_val71,'Y')<>nvl(v_syn_log_rec.col_old_val71,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name71||'='||get_val(v_syn_log_rec.col_new_val71,v_syn_log_rec.col_type71)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val72,'Y')<>nvl(v_syn_log_rec.col_old_val72,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name72||'='||get_val(v_syn_log_rec.col_new_val72,v_syn_log_rec.col_type72)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val73,'Y')<>nvl(v_syn_log_rec.col_old_val73,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name73||'='||get_val(v_syn_log_rec.col_new_val73,v_syn_log_rec.col_type73)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val74,'Y')<>nvl(v_syn_log_rec.col_old_val74,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name74||'='||get_val(v_syn_log_rec.col_new_val74,v_syn_log_rec.col_type74)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val75,'Y')<>nvl(v_syn_log_rec.col_old_val75,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name75||'='||get_val(v_syn_log_rec.col_new_val75,v_syn_log_rec.col_type75)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val76,'Y')<>nvl(v_syn_log_rec.col_old_val76,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name76||'='||get_val(v_syn_log_rec.col_new_val76,v_syn_log_rec.col_type76)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val77,'Y')<>nvl(v_syn_log_rec.col_old_val77,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name77||'='||get_val(v_syn_log_rec.col_new_val77,v_syn_log_rec.col_type77)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val78,'Y')<>nvl(v_syn_log_rec.col_old_val78,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name78||'='||get_val(v_syn_log_rec.col_new_val78,v_syn_log_rec.col_type78)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val79,'Y')<>nvl(v_syn_log_rec.col_old_val79,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name79||'='||get_val(v_syn_log_rec.col_new_val79,v_syn_log_rec.col_type79)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val80,'Y')<>nvl(v_syn_log_rec.col_old_val80,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name80||'='||get_val(v_syn_log_rec.col_new_val80,v_syn_log_rec.col_type80)||',';
             end if;  
             
             if nvl(v_syn_log_rec.col_new_val81,'Y')<>nvl(v_syn_log_rec.col_old_val81,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name81||'='||get_val(v_syn_log_rec.col_new_val81,v_syn_log_rec.col_type81)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val82,'Y')<>nvl(v_syn_log_rec.col_old_val82,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name82||'='||get_val(v_syn_log_rec.col_new_val82,v_syn_log_rec.col_type82)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val83,'Y')<>nvl(v_syn_log_rec.col_old_val83,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name83||'='||get_val(v_syn_log_rec.col_new_val83,v_syn_log_rec.col_type83)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val84,'Y')<>nvl(v_syn_log_rec.col_old_val84,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name84||'='||get_val(v_syn_log_rec.col_new_val84,v_syn_log_rec.col_type84)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val85,'Y')<>nvl(v_syn_log_rec.col_old_val85,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name85||'='||get_val(v_syn_log_rec.col_new_val85,v_syn_log_rec.col_type85)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val86,'Y')<>nvl(v_syn_log_rec.col_old_val86,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name86||'='||get_val(v_syn_log_rec.col_new_val86,v_syn_log_rec.col_type86)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val87,'Y')<>nvl(v_syn_log_rec.col_old_val87,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name87||'='||get_val(v_syn_log_rec.col_new_val87,v_syn_log_rec.col_type87)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val88,'Y')<>nvl(v_syn_log_rec.col_old_val88,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name88||'='||get_val(v_syn_log_rec.col_new_val88,v_syn_log_rec.col_type88)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val89,'Y')<>nvl(v_syn_log_rec.col_old_val89,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name89||'='||get_val(v_syn_log_rec.col_new_val89,v_syn_log_rec.col_type89)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val90,'Y')<>nvl(v_syn_log_rec.col_old_val90,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name90||'='||get_val(v_syn_log_rec.col_new_val90,v_syn_log_rec.col_type90)||',';
             end if;  
             
             if nvl(v_syn_log_rec.col_new_val91,'Y')<>nvl(v_syn_log_rec.col_old_val91,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name91||'='||get_val(v_syn_log_rec.col_new_val91,v_syn_log_rec.col_type91)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val92,'Y')<>nvl(v_syn_log_rec.col_old_val92,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name92||'='||get_val(v_syn_log_rec.col_new_val92,v_syn_log_rec.col_type92)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val93,'Y')<>nvl(v_syn_log_rec.col_old_val93,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name93||'='||get_val(v_syn_log_rec.col_new_val93,v_syn_log_rec.col_type93)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val94,'Y')<>nvl(v_syn_log_rec.col_old_val94,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name94||'='||get_val(v_syn_log_rec.col_new_val94,v_syn_log_rec.col_type94)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val95,'Y')<>nvl(v_syn_log_rec.col_old_val95,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name95||'='||get_val(v_syn_log_rec.col_new_val95,v_syn_log_rec.col_type95)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val96,'Y')<>nvl(v_syn_log_rec.col_old_val96,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name96||'='||get_val(v_syn_log_rec.col_new_val96,v_syn_log_rec.col_type96)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val97,'Y')<>nvl(v_syn_log_rec.col_old_val97,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name97||'='||get_val(v_syn_log_rec.col_new_val97,v_syn_log_rec.col_type97)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val98,'Y')<>nvl(v_syn_log_rec.col_old_val98,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name98||'='||get_val(v_syn_log_rec.col_new_val98,v_syn_log_rec.col_type98)||',';
             end if;

             if nvl(v_syn_log_rec.col_new_val99,'Y')<>nvl(v_syn_log_rec.col_old_val99,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name99||'='||get_val(v_syn_log_rec.col_new_val99,v_syn_log_rec.col_type99)||',';
             end if;
             if nvl(v_syn_log_rec.col_new_val100,'Y')<>nvl(v_syn_log_rec.col_old_val100,'Y') then
                v_upd_column:=v_upd_column||v_syn_log_rec.col_name100||'='||get_val(v_syn_log_rec.col_new_val100,v_syn_log_rec.col_type100)||',';
             end if;  
                       
             if v_upd_column is null then
                v_upd_column:=v_syn_log_rec.pk_name||'='||get_val(v_syn_log_rec.pk_val,v_syn_log_rec.pk_type)||',';
             end if;

         close c1;
         v_upd_header:=v_upd_header||substr(v_upd_column,1,length(v_upd_column)-1)||v_upd_where;
         return v_upd_header;
      exception
        when others then
            return '';
      end;

      function get_del_sql(n_syn_id int) return varchar2 is
          v_del_header   varchar2(1000):='';
          v_del_where    varchar2(1000):='';
          v_syn_log_rec  t_syn_log_rec;
          c1             SYS_REFCURSOR;
      begin
          open  c1  for 'select syn_id,tname,otype,pk_name,pk_type,pk_val,
                               if_syn,syn_time from syn_log where syn_id=:1' using n_syn_id ;
         fetch c1 into v_syn_log_rec;
             v_del_header:='DELETE FROM '||v_syn_log_rec.tname;
             v_del_where :=' WHERE '||v_syn_log_rec.pk_name||'='||get_val(v_syn_log_rec.pk_val,v_syn_log_rec.pk_type);
         close c1;
         return v_del_header||v_del_where;
      exception
        when others then
            return '';
      end;
      
      function get_ddl_sql(n_syn_id int) return varchar2 is
          v_ddl_stat   varchar2(1000):='';
          v_syn_log_rec  t_syn_log_rec;
          c1             SYS_REFCURSOR;
      begin
         open c1 for 'select ddl_stat from syn_log where syn_id=:1' using n_syn_id ;
         fetch c1 into v_ddl_stat;            
         close c1;
         return v_ddl_stat;
      exception
        when others then
            return '';
      end;
      
      function get_syn_sql(n_syn_id int) return varchar2 is
          v_ddl_stat   varchar2(1000):='';
          v_syn_log_rec  t_syn_log_rec;
          c1             SYS_REFCURSOR;
      begin
         open c1 for 'select ddl_stat from syn_log where syn_id=:1' using n_syn_id ;
         fetch c1 into v_ddl_stat;            
         close c1;
         return v_ddl_stat;
      exception
        when others then
            return '';
      end;

  begin
     open  c1  for 'select otype from syn_log where syn_id=:1' using n_syn_id ;
     fetch c1 into v_otype;
     if v_otype ='I' then
        return get_ins_sql(n_syn_id);
     elsif v_otype='U' then
        return get_upd_sql(n_syn_id);
     elsif v_otype='D' then
        return get_del_sql(n_syn_id);
     elsif v_otype='DDL' then
        return get_ddl_sql(n_syn_id);   
     elsif v_otype='SYN' then
        return get_syn_sql(n_syn_id);      
     else
        return '';
     end if;
     close c1;
  end;
  procedure cre_tab_ddl_trg is
     v_sql varchar2(30000);
  begin
     v_sql:='create or replace trigger trg_'||user||chr(10)||          
            '  after  ddl on '||user||'.schema'||chr(10)||       
            'declare '||chr(10)||
            '  tr_sql_text ora_name_list_t; '||chr(10)||
            '  tr_n number; '||chr(10)||
            '  tr_stmt varchar2(2000) := null; '||chr(10)||
            '  n int; '||chr(10)||
            'begin '||chr(10)||            
             ' begin '||chr(10)|| 
             '   tr_n := ora_sql_txt(tr_sql_text); '||chr(10)|| 
             '   for i in 1 .. tr_n loop '||chr(10)|| 
             '     tr_stmt := tr_stmt || tr_sql_text(i); '||chr(10)|| 
             '   end loop; '||chr(10)|| 
             ' exception when others then '||chr(10)|| 
             '   tr_stmt := null; '||chr(10)|| 
             ' end; '||chr(10)|| 
             ' tr_stmt:=replace(upper(tr_stmt),'||chr(39)||user||'.'||chr(39)||'); '||chr(10)|| 
             ' tr_stmt:=replace(tr_stmt,chr(10)); '||chr(10)|| 
             ' tr_stmt:=replace(tr_stmt,chr(13)); '||chr(10)|| 
             ' tr_stmt:=replace(tr_stmt,chr(0)); '||chr(10)|| 
             ' tr_stmt:=ltrim(rtrim(tr_stmt,chr(32))); '||chr(10)|| 
             ' tr_stmt:=trim(''EXECUTE IMMEDIATE ''||chr(39)||tr_stmt||chr(39)); '||chr(10)|| 
             ' '||chr(10)||
             ' select count(0) into n from syn_bz; '||chr(10)||     
             ' if n>0 then '||chr(10)|| 
             '     if instr(upper(tr_stmt),''DP_SYN_BUILD.INIT_TAB'')=0 then '||chr(10)||        
             '        RAISE_APPLICATION_ERROR(-20002,''Table changed!!! Please run "exec dp_syn_build.init_tab(<table_name>));" resync table!'');'||chr(10)||      
             '     end if; '||chr(10)||     
             ' end if; '||chr(10)||      
             ' if instr(dp_syn_build.get_syn_tab_list,ORA_DICT_OBJ_NAME)>0 then  '||chr(10)||  
             ' if instr(tr_stmt,''CREATE OR REPLACE TRIGGER'')>0 then '||chr(10)||       
             '    null; '||chr(10)|| 
             ' elsif instr(tr_stmt,''CREATE OR REPLACE PACKAGE'')>0 then '||chr(10)|| 
             '    null; '||chr(10)||         
             ' elsif instr(tr_stmt,''CREATE UNIQUE INDEX'')>0 then  '||chr(10)||           
             '    null; '||chr(10)|| 
             ' elsif instr(upper(tr_stmt),''CREATE'')>0   '||chr(10)|| 
             '     and instr(upper(tr_stmt),''TABLE'')>0  '||chr(10)|| 
             '        and  instr(upper(tr_stmt),''SELECT'')>0  then  '||chr(10)|| 
             '    null;  '||chr(10)||    
             ' elsif instr(upper(tr_stmt),''TRUNCATE'')>0       '||chr(10)|| 
             '       and instr(upper(tr_stmt),''TABLE'')>0      '||chr(10)||    
             '         and instr(upper(tr_stmt),''SYN_LOG'')>0  '||chr(10)|| 
             '           and instr(upper(tr_stmt),''TRUNCATE'')<instr(upper(tr_stmt),''TABLE'')  '||chr(10)|| 
             '             and  instr(upper(tr_stmt),''TABLE'')<instr(upper(tr_stmt),''SYN_LOG'') then '||chr(10)|| 
             '    null;  '||chr(10)|| 
             ' else      '||chr(10)||      
             '    insert into syn_log(syn_id,tname,otype,ddl_stat,syn_time) '||chr(10)|| 
             '       values(syn_log_seq.nextval,ORA_DICT_OBJ_NAME,''DDL'',tr_stmt,systimestamp); '||chr(10)||        
             ' end if;   '||chr(10)|| 
             ' '||chr(10)||    
             ' if instr(upper(tr_stmt),''ALTER'')>0        '||chr(10)|| 
             '     and  instr(upper(tr_stmt),''TABLE'')>0  '||chr(10)|| 
             '        and instr(upper(tr_stmt),''ADD'')>0  '||chr(10)||  
             '          and instr(upper(tr_stmt),''PRIMARY'')=0  '||chr(10)|| 
             '           and instr(upper(tr_stmt),''ALTER'')<instr(upper(tr_stmt),''TABLE'') '||chr(10)|| 
             '             and  instr(upper(tr_stmt),''TABLE'')<instr(upper(tr_stmt),''ADD'') then '||chr(10)|| 
             '      insert into syn_log(syn_id,tname,otype,ddl_stat,syn_time) '||chr(10)|| 
             '         values(syn_log_seq.nextval,ORA_DICT_OBJ_NAME,''SYN'',''DP_SYN_BUILD.INIT_TAB('||'''''''||ORA_DICT_OBJ_NAME||'||''''''')'',systimestamp); '||chr(10)|| 
             '      insert into syn_bz(i) values(1); '||chr(10)||           
             '  end if; '||chr(10)|| 
             '  '||chr(10)|| 
             '  if instr(upper(tr_stmt),''ALTER'')>0  '||chr(10)|| 
             '     and  instr(upper(tr_stmt),''TABLE'')>0  '||chr(10)|| 
             '        and instr(upper(tr_stmt),''MODIFY'')>0  '||chr(10)|| 
             '           and instr(upper(tr_stmt),''ALTER'')<instr(upper(tr_stmt),''TABLE'') '||chr(10)|| 
             '             and  instr(upper(tr_stmt),''TABLE'')<instr(upper(tr_stmt),''MODIFY'') then '||chr(10)|| 
             '      insert into syn_log(syn_id,tname,otype,ddl_stat,syn_time) '||chr(10)|| 
             '         values(syn_log_seq.nextval,ORA_DICT_OBJ_NAME,''SYN'',''DP_SYN_BUILD.INIT_TAB('||'''''''||ORA_DICT_OBJ_NAME||'||''''''')'',systimestamp); '||chr(10)|| 
             '      insert into syn_bz(i) values(2); '||chr(10)||   
             '  end if; '||chr(10)|| 
             ''||chr(10)|| 
             '  if instr(upper(tr_stmt),''ALTER'')>0       '||chr(10)|| 
             '     and  instr(upper(tr_stmt),''TABLE'')>0  '||chr(10)|| 
             '        and instr(upper(tr_stmt),''DROP'')>0 '||chr(10)|| 
             '           and instr(upper(tr_stmt),''ALTER'')<instr(upper(tr_stmt),''TABLE'')   '||chr(10)|| 
             '             and  instr(upper(tr_stmt),''TABLE'')<instr(upper(tr_stmt),''DROP'') '||chr(10)|| 
             '               and  instr(upper(tr_stmt),''DROP'')<instr(upper(tr_stmt),''COLUMN'') then '||chr(10)|| 
             '      insert into syn_log(syn_id,tname,otype,ddl_stat,syn_time) '||chr(10)|| 
             '         values(syn_log_seq.nextval,ORA_DICT_OBJ_NAME,''SYN'',''DP_SYN_BUILD.INIT_TAB('||'''''''||ORA_DICT_OBJ_NAME||'||''''''')'',systimestamp); '||chr(10)|| 
             '      insert into syn_bz(i) values(3); '||chr(10)||                
             '  end if; '||chr(10)|| 
             ''||chr(10)|| 
             '  if instr(upper(tr_stmt),''CREATE'')>0          '||chr(10)|| 
             '     and  instr(upper(tr_stmt),''TABLE'')>0      '||chr(10)||    
             '        and instr(upper(tr_stmt),''SELECT'')=0   '||chr(10)||        
             '          and instr(upper(tr_stmt),''CREATE'')<instr(upper(tr_stmt),''TABLE'') then '||chr(10)|| 
             '      insert into syn_log(syn_id,tname,otype,ddl_stat,syn_time) '||chr(10)|| 
             '         values(syn_log_seq.nextval,ORA_DICT_OBJ_NAME,''SYN'',''DP_SYN_BUILD.INIT_TAB('||'''''''||ORA_DICT_OBJ_NAME||'||''''''')'',systimestamp); '||chr(10)||
             ''||chr(10)|| 
             'end if; '||chr(10)|| 
             'end if; '||chr(10)|| 
            'end;'; 

      execute immediate v_sql;

  end;

  procedure chk_tab(v_tab varchar2) is
     n_cnt int:=0;
  begin
     select count(0) into n_cnt from dba_tables
       where owner=user and table_name=upper(v_tab);
     if n_cnt=0 then
       RAISE_APPLICATION_ERROR(-20001,upper(user)||'.'||upper(v_tab)||' NOT EXIST!');
     end if;

     select count(0) into n_cnt from user_constraints
       where constraint_type='P'
         and owner=user
         and table_name=upper(v_tab);
     if n_cnt=0 then
       RAISE_APPLICATION_ERROR(-20002,upper(user)||'.'||upper(v_tab)||' NOT EXIST PRIMARY KEY!');
     end if;
     
  end;
  
  procedure del_syn_bz is
  begin
     execute immediate 'delete  from syn_bz';
     commit;
  end;

  procedure init_tab(v_tab varchar2) is
  begin
    chk_tab(v_tab);
    cre_tab_dml_trg(v_tab);
    del_syn_bz;
  end;

  procedure init_syn is
  begin
    begin
      execute immediate 'drop trigger trg_'||user;
    exception
     when others then
        null;  
    end;    
    begin
      for i in(select trigger_name from user_triggers where trigger_name like 'TRG_%') loop
        execute immediate 'drop trigger '||i.trigger_name;
      end loop;      
    exception
      when others then
        null;
    end;   
    
    cre_seq;
    cre_syn_tab;
    cre_syn_ind;   
    cre_tab_ddl_trg;
    execute immediate 'alter trigger trg_'||user||' compile';
    execute immediate 'alter trigger trg_'||user||' enable';
  end;

end dp_syn_build;
/
