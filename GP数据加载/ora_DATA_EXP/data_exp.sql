--	/home/oracle/data_exp
create or replace directory data_exp as '/home/oracle/data_exp';
grant all on directory data_exp to public;

--

declare
    outfile utl_file.file_type;
    str	varchar2(50);
begin
    outfile := utl_file.fopen('DATA_EXP','hr.countries.txt','W');
    --
    select to_char(sysdate,'yyyy/mm/dd hh24:mi:ss') into str from dual;
    utl_file.put_line(outfile, str);
    for rec in (select country_id, country_name, region_id from hr.countries where 1=1)
    loop
       utl_file.put_line(outfile, rec.country_id||chr(15)||rec.country_name||chr(15)||rec.region_id);
    end loop;
    select to_char(sysdate,'yyyy/mm/dd hh24:mi:ss') into str from dual;
    utl_file.put_line(outfile, str);    
    utl_file.fclose(outfile);
end;
/


---------6000184343/1024/1024 ~= 5722MB ,994second
--zip WHLOAN.TB_CON_BORR_ACCT_SUMM_YUQI.zip ./WHLOAN.TB_CON_BORR_ACCT_SUMM_YUQI.txt 
----adding: WHLOAN.TB_CON_BORR_ACCT_SUMM_YUQI.txt (deflated 92%)
declare
    outfile utl_file.file_type;
    str	varchar2(50);
begin
    outfile := utl_file.fopen('DMP_DIR','WHLOAN.TB_CON_BORR_ACCT_SUMM_YUQI.txt','W');
    --
    select to_char(sysdate,'yyyy/mm/dd hh24:mi:ss') into str from dual;
    utl_file.put_line(outfile, str);
    for rec in (select info_id, 
borrow_num, 
overdue_30_balance_principal, 
overdue_60_balance_principal, 
overdue_90_balance_principal, 
overdue_180_balance_principal, 
overdue_270_balance_principal, 
overdue_360_balance_principal, 
overdue_other_balance_principa, 
overdue_30_balance_interest, 
overdue_60_balance_interest, 
overdue_90_balance_interest, 
overdue_180_balance_interest, 
overdue_270_balance_interest, 
overdue_360_balance_interest, 
overdue_other_balance_interest, 
year_month, 
op_date
 from WHLOAN.TB_CON_BORR_ACCT_SUMM_YUQI where 1=1)
    loop
       utl_file.put_line(outfile, rec.info_id||chr(15)|| 
rec.borrow_num||chr(15)|| 
rec.overdue_30_balance_principal||chr(15)|| 
rec.overdue_60_balance_principal||chr(15)|| 
rec.overdue_90_balance_principal||chr(15)|| 
rec.overdue_180_balance_principal||chr(15)|| 
rec.overdue_270_balance_principal||chr(15)|| 
rec.overdue_360_balance_principal||chr(15)|| 
rec.overdue_other_balance_principa||chr(15)|| 
rec.overdue_30_balance_interest||chr(15)|| 
rec.overdue_60_balance_interest||chr(15)|| 
rec.overdue_90_balance_interest||chr(15)|| 
rec.overdue_180_balance_interest||chr(15)|| 
rec.overdue_270_balance_interest||chr(15)|| 
rec.overdue_360_balance_interest||chr(15)|| 
rec.overdue_other_balance_interest||chr(15)|| 
rec.year_month||chr(15)|| 
rec.op_date);
    end loop;
    select to_char(sysdate,'yyyy/mm/dd hh24:mi:ss') into str from dual;
    utl_file.put_line(outfile, str);    
    utl_file.fclose(outfile);
end;
/

----------------------------------------------------
----------------------------------------------------
declare
/*--一个表生成一个匿名SQL语句块
var_dir必须是oracle里面存在的directory,并且执行文件导出的用户有读写权限,大写字母;
导出文件字段分隔符以var_delimiter的值来定;
位置:var_dir,文件名:schema_table_name.txt
\x09 TAB; \x0d  换行; \x0a  回车; \x0f 本次分隔符
*/
var_dir          varchar2(20):='DMP_DIR';
var_schema       varchar2(20):='OE';
var_filename     varchar2(50):='';
var_delimiter    char;
outfile          utl_file.file_type;
var_cols1        varchar2(2000):='';
var_cols2        varchar2(2000):='';
var_sql          varchar2(2000):='';
var_str          varchar2(4000):='';
begin
  var_delimiter := chr(15);
  --outfile := utl_file.fopen(var_dir,'data_exp_sql1.txt','W');
  for tr in (select * from dba_tables t where t.owner=var_schema) loop
    var_filename := tr.owner||'_'||tr.table_name||'.txt';
    outfile := utl_file.fopen(var_dir,var_filename,'W',32767);  
    for cr in (select * from dba_tab_columns t 
      where t.owner='OE' and t.table_name=tr.table_name
      and t.DATA_TYPE in ('NVARCHAR2','INTERVAL YEAR(2) TO MONTH','TIMESTAMP(6) WITH LOCAL TIME ZONE','NUMBER','CHAR','DATE','VARCHAR2') 
      order by t.OWNER,t.TABLE_NAME,t.COLUMN_ID) loop
      var_cols1 := var_cols1||','||cr.column_name;
      var_cols2 := var_cols2||'||chr(15)||rec.'||cr.column_name;
    end loop;--for cr
    --去掉第一个','
    var_cols1 := substr(var_cols1,2);
    --去掉第一个'||chr(15)||'
    var_cols2 := substr(var_cols2,12);
    var_sql := 'SELECT '||var_cols1||' FROM '||tr.owner||'.'||tr.table_name||' where 1=1';
    --导出文本文件的[匿名SQL语句块]
    var_str := 'declare'||chr(13)||chr(10)||'outfile utl_file.file_type;'||chr(13)||chr(10)||'begin'||chr(13)||chr(10)||'outfile := utl_file.fopen('''||var_dir||''','''||var_filename||''',''W'');'
            ||chr(13)||chr(10)||'for rec in ('||var_sql||')'||chr(13)||chr(10)||'loop'||chr(13)||chr(10)||'utl_file.put_line(outfile, '||var_cols2||');'||chr(13)||chr(10)||'end loop;'
            ||chr(13)||chr(10)||'utl_file.fclose(outfile);'||chr(13)||chr(10)||'end;'||chr(13)||chr(10)||'/';
    utl_file.put_line(outfile, var_str); 
    utl_file.fclose(outfile);
  end loop;--for tr
  --utl_file.fclose(outfile);
end;
/
-------------------------------------
-------------------------------------
declare
/*--生成的SQL匿名语句块放在一个文件
var_dir必须是oracle里面存在的directory,并且执行文件导出的用户有读写权限,大写字母;
导出文件字段分隔符以var_delimiter的值来定;
位置:var_dir,文件名:schema_table_name.txt
\x09 TAB; \x0d  换行; \x0a  回车; \x0f 本次分隔符
*/
var_dir          varchar2(20):='DMP_DIR';
var_schema       varchar2(20):='OE';
var_filename     varchar2(50):='';
var_delimiter    char;
outfile          utl_file.file_type;
var_cols1        varchar2(2000):='';
var_cols2        varchar2(2000):='';
var_sql          varchar2(2000):='';
var_str          varchar2(4000):='';
begin
  var_delimiter := chr(15);
  
  for tr in (select * from dba_tables t where t.owner=var_schema) loop
    outfile := utl_file.fopen(var_dir,'data_exp_sql.txt','A',32767);
    var_filename := tr.owner||'_'||tr.table_name||'.txt';
    --outfile := utl_file.fopen(var_dir,var_filename,'W',32767);  
    for cr in (select * from dba_tab_columns t 
      where t.owner='OE' and t.table_name=tr.table_name
      and t.DATA_TYPE in ('NVARCHAR2','INTERVAL YEAR(2) TO MONTH','TIMESTAMP(6) WITH LOCAL TIME ZONE','NUMBER','CHAR','DATE','VARCHAR2') 
      order by t.OWNER,t.TABLE_NAME,t.COLUMN_ID) loop
      var_cols1 := var_cols1||','||cr.column_name;
      var_cols2 := var_cols2||'||chr(15)||rec.'||cr.column_name;
    end loop;--for cr
    --去掉第一个','
    var_cols1 := substr(var_cols1,2);
    --去掉第一个'||chr(15)||'
    var_cols2 := substr(var_cols2,12);
    var_sql := 'SELECT '||var_cols1||' FROM '||tr.owner||'.'||tr.table_name||' where 1=1';
    --导出文本文件的[匿名SQL语句块]
    var_str := 'declare'||chr(10)||'outfile utl_file.file_type;'||chr(10)||'begin'||chr(10)||'outfile := utl_file.fopen('''||var_dir||''','''||var_filename||''',''W'');'
            ||chr(10)||'for rec in ('||var_sql||')'||chr(10)||'loop'||chr(10)||'utl_file.put_line(outfile, '||var_cols2||');'||chr(10)||'end loop;'
            ||chr(10)||'utl_file.fclose(outfile);'||chr(10)||'end;'||chr(10)||'/';
    utl_file.put_line(outfile, var_str); 
    utl_file.put_line(outfile, '-- '||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss'));
    utl_file.fclose(outfile);
    
    var_cols1 := '';
    var_cols2 := '';
    
  end loop;--for tr
  --utl_file.fclose(outfile);
end;
/

--#############################
--测试生成后台job,dbms_scheduler.create_job
--#############################
declare
/*--生成的SQL匿名语句块放在一个文件
var_dir必须是oracle里面存在的directory,并且执行文件导出的用户有读写权限,大写字母;
导出文件字段分隔符以var_delimiter的值来定;
位置:var_dir,文件名:schema_table_name.txt
\x09 TAB; \x0d  换行; \x0a  回车; \x0f 本次分隔符
*/
var_dir          varchar2(20):='DMP_DIR';
var_schema       varchar2(20):='OE';
var_filename     varchar2(50):='';
var_jobname      varchar2(50):='';
var_delimiter    char;
outfile          utl_file.file_type;
var_cols1        varchar2(2000):='';
var_cols2        varchar2(2000):='';
var_sql          varchar2(2000):='';
var_str          varchar2(4000):='';
begin
  var_delimiter := chr(15);
  
  for tr in (select * from dba_tables t where t.owner=var_schema) loop
    outfile := utl_file.fopen(var_dir,'data_exp_sql.txt','A',32767);
    var_filename := tr.owner||'_'||tr.table_name||'.txt';
    var_jobname :=  'EXP_'||tr.owner||'_'||tr.table_name;
    --outfile := utl_file.fopen(var_dir,var_filename,'W',32767);  
    for cr in (select * from dba_tab_columns t 
      where t.owner='OE' and t.table_name=tr.table_name
      and t.DATA_TYPE in ('NVARCHAR2','INTERVAL YEAR(2) TO MONTH','TIMESTAMP(6) WITH LOCAL TIME ZONE','NUMBER','CHAR','DATE','VARCHAR2') 
      order by t.OWNER,t.TABLE_NAME,t.COLUMN_ID) loop
      var_cols1 := var_cols1||','||cr.column_name;
      var_cols2 := var_cols2||'||chr(15)||rec.'||cr.column_name;
    end loop;--for cr
    --去掉第一个','
    var_cols1 := substr(var_cols1,2);
    --去掉第一个'||chr(15)||'
    var_cols2 := substr(var_cols2,12);
    var_sql := 'SELECT '||var_cols1||' FROM '||tr.owner||'.'||tr.table_name||' where 1=1';
    --导出文本文件的[匿名SQL语句块]
    var_str := 'declare'||chr(10)||'outfile utl_file.file_type;'||chr(10)||'begin'||chr(10)||'outfile := utl_file.fopen('''||var_dir||''','''||var_filename||''',''W'');'
            ||chr(10)||'for rec in ('||var_sql||')'||chr(10)||'loop'||chr(10)||'utl_file.put_line(outfile, '||var_cols2||');'||chr(10)||'end loop;'
            --||chr(10)||'utl_file.fclose(outfile);'||chr(10)||'end;'||chr(10)||'/';
      ||chr(10)||'utl_file.fclose(outfile);'||chr(10)||'end;';
    --utl_file.put_line(outfile, var_str); 
    utl_file.put_line(outfile, var_str||chr(10)||'/'); 
    utl_file.put_line(outfile, '-- '||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss'));
    utl_file.fclose(outfile);
    --为了能被作业使用,需要对一个单引号变成两个单引号
    var_str := replace(var_str,'''','''''');
       dbms_scheduler.create_job(job_name    => substr(var_jobname,1,30),
                             job_type        => 'PLSQL_BLOCK',
                             job_action      => var_str,
                             start_date      => sysdate,
                             enabled         => TRUE,
                             comments        => 'exppot data to flatfile');
    var_cols1 := '';
    var_cols2 := '';
    ----dbms_output.put_line(var_str);
  end loop;--for tr
  --utl_file.fclose(outfile);
end;
/
