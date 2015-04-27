-- 统计SDSDATA下面表的记录数
select 'select '''||t.table_schema||'.'||t.table_name||''',tab_rowcount('''||t.table_schema||'.'||t.table_name||''');' from information_schema.tables t where t.table_schema='sdsdata' and t.is_insertable_into='YES' order by 1;

-- select tab_rowcount('sdsdata.s01_bmsbussreg_err');

-- 统计表记录数的函数

-- drop FUNCTION tab_rowcount(varchar);

CREATE OR REPLACE FUNCTION tab_rowcount(tname varchar) RETURNS integer AS $$
DECLARE
       rcnt	integer;
       strsql	varchar(200);
       quantity integer;
BEGIN
         strsql := 'select count(*) from '||tname;
         RAISE NOTICE 'SQL IS %',strsql;
         execute strsql into rcnt;
         RAISE NOTICE 'Table of % have % record ',tname,rcnt;
       
       RETURN rcnt;
END;
$$ LANGUAGE plpgsql
;

-- 

select * from etlrep.control_task;

-- 查看SDSDATA下普通表的数据情况limit 5
select 'select * from '||t.table_schema||'.'||t.table_name||' limit 5;' from information_schema.tables t where t.table_schema='sdsdata' and t.is_insertable_into='YES';
-- 查看SDSDATA下外部表的数据情况limit 5
select 'select * from '||t.table_schema||'.'||t.table_name||' limit 5;' from information_schema.tables t where t.table_schema='sdsdata' and t.is_insertable_into='NO';


--select * from sdsdata.s01_amsassetbook_err;

CREATE or REPLACE FUNCTION stat_control()  RETURNS varchar AS $$
declare
       ctab 	record;
       rcnt	integer:=0;
       rcnt_err	integer:=0;
       strsql	varchar(200);
       quantity integer;
       tcnt	integer:=0;
BEGIN
       for ctab in (select * from etlrep.control_task order by 1) loop
          select coalesce(count(*),0) into tcnt from information_schema.tables t where t.table_schema='sdsdata' and t.is_insertable_into='YES' and t.table_name=lower(ctab.task_name||'0526');
          --select *  into rcnt from tab_rowcount('sdsdata.'||ctab.task_name||'0526' order by 1);
          if (tcnt=1) then 
          	rcnt := tab_rowcount('sdsdata.'||ctab.task_name||'0526');
          else
            rcnt := 0;
          end if;

	  select coalesce(count(*),0) into tcnt from information_schema.tables t where t.table_schema='sdsdata' and t.is_insertable_into='YES' and t.table_name=lower(ctab.task_name)||'_err';
          --select * into rcnt_err from tab_rowcount('sdsdata.'||ctab.task_name||'_err');
          if (tcnt=1) then 
          	rcnt_err := tab_rowcount('sdsdata.'||ctab.task_name||'_err');
          else
            rcnt := 0;
          end if;

          select coalesce(count(*),0) into tcnt from etlrep.control_task1 t where t.task_name=ctab.task_name;
          --if (tcnt=1) then 
          --	delete from etlrep.control_task1 t where t.task_name=ctab.task_name;
          --else
          	insert into etlrep.control_task1(task_name,sys_name,src_table,tgt_table,table_cn,file_name,status)
          	values (ctab.task_name,ctab.sys_name,ctab.src_table,rcnt,ctab.task_name||'_err',rcnt_err,null);
          --end if;
          
       rcnt := 0;
       rcnt_err := 0;
       end loop;
  
  RETURN 'success';
END;
$$ LANGUAGE plpgsql
;


/*
--S02\4\5\6\8,S10  whrcbloan/whrcbloan@31.2.1.13/sample  
信贷、理财、网银、IC卡、票据、小额系统访问方式：31.2.1.13/sample  sjck/sjck03
核心  21.1.1.45/wsbank  sjck/sjck03  (有权限 wsbank/wsbank2013)
国结  21.1.1.45/utandb  sjck/sjck03  (utan/utan)
前置  21.1.1.45/cspdb   sjck/sjck03  (csprun/csprun)
农信银访问方式 telnet  ip 31.2.1.241 telnet用户名/密码 mif/mif 
-- S07（农信银是db2环境：   db2 "select * from table"）
  
-- S09 资金系统(不是表，是视图)  21.1.1.45/wsbank   customer/customer

*/
---------------------------------------------------------
-- 
-- oracle work area
-- connect sys/oracle@31.2.2.62:1521/dqadb as sysdba
-- connect bi/wangjinyu@31.2.2.62:1521/dqadb
-- alter user bi identified by wangjinyu account unlock;
-- bi.control_sdsdata
---------------------------------------------------------
-- drop table control_sdsdata;

create table control_sdsdata(
systabname   varchar2(100),
syscode      varchar2(10),
tablename    varchar2(50),
systabname1  varchar2(100),
tabrowcount  integer,
errtabname   varchar2(50),
err_rowcount integer,
true_ratio   number(5,2),
memo         varchar2(200)
);

select t.* from control_sdsdata t order by 1

-- drop table syscode;

create table syscode(
syscode      varchar2(10),
sysname      varchar2(100),
connstr      varchar2(50),
dbtype       varchar2(20),
langstr      varchar2(50),
nlslangstr   varchar2(100),
nlsdatefmt   varchar2(100)
);

select t.* from syscode t order by 1;

-- drop database link s01;
-- Create database link ,S01,S03,S09
create database link s01
  connect to sjck identified by sjck03
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 21.1.1.45)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = wsbank)    )  )';
create database link s03
  connect to sjck identified by sjck03
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 21.1.1.45)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = utandb)    )  )';
create database link s09
  connect to customer identified by customer
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 21.1.1.45)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = wsbank)    )  )';
  
-- Create database link ,S02,S04,S05,S06,S08,S10,S15 ==> S02

create database link s02
  connect to whrcbloan identified by whrcbloan
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 31.2.1.13)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = sample)    )  )';
create database link s04
  connect to whrcbloan identified by whrcbloan
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 31.2.1.13)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = sample)    )  )';
create database link s05
  connect to whrcbloan identified by whrcbloan
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 31.2.1.13)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = sample)    )  )';
create database link s06
  connect to whrcbloan identified by whrcbloan
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 31.2.1.13)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = sample)    )  )';
create database link s08
  connect to whrcbloan identified by whrcbloan
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 31.2.1.13)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = sample)    )  )';
create database link s10
  connect to whrcbloan identified by whrcbloan
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 31.2.1.13)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = sample)    )  )';
create database link s15
  connect to whrcbloan identified by whrcbloan
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 31.2.1.13)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = sample)    )  )';

--------------------------- examples for dblinks
select * from AMSASSETBOOK@s01;
select * from GRT_SEA_USED@s02; --s02
select * from COUNTRY@s03;
select * from TB_GRT_GUARANTEE@s02;--s04
select * from TBASSETACC@s02;--s05
select * from BMTRANSFERHIS@s02;--s06
select * from COLL_ACCT_LIST@s02;--s08
select * from V_LDREPODEALS@s09;--
select * from TBL_CARD_PROD_CFG@s02;--s10
select * from CMSCHANNELDEF@s02;--s15
---------------------------wsbank/wsbank2013@21.1.1.45/wsbank  (sjck/sjck03)
-- 
---------------------------whrcbloan/whrcbloan@31.2.1.13/sample
drop public database link bi;
create public database link bi
  connect to bi identified by wangjinyu
  using '(DESCRIPTION =    (ADDRESS_LIST =      (ADDRESS = (PROTOCOL = TCP)(HOST = 31.2.2.62)(PORT = 1521))    )    (CONNECT_DATA =      (SERVER = DEDICATED)        (SERVICE_NAME = dqadb)    )  )';

select * from syscode@bi;
--##################################################################################################
--
--去源系统建好了bi的dblink后,生成建表语句 create table tab as select * from tab@s0n where rownum<1000
--
--##################################################################################################
--####### whrcbloan/whrcbloan@31.2.1.13:1521/sample
-- 'S02','S04','S05','S06','S08','S10'
-- 'S02' 只取前30位作表名
set echo off
set head off
set linesize 1000
col copystr format a4000

select '-- connect whrcbloan/whrcbloan@31.2.1.13/sample' from dual;

select 'copy from '||a.connstr||'  create '||t.syscode||'_'||t.tablename||' using select * from '||t.tablename||' where rownum<1000;' copystr
--select 'create table '||t.syscode||'_'||t.tablename||' as select '||get_columns(t.tablename)||' from '||t.tablename||'@'||decode(t.syscode,'S01','S01','S03','S03','S09','S09','S02')||' WHERE ROWNUM<1000;'
  --a.connstr,t.* 
from control_sdsdata@bi t,syscode@bi a,all_all_tables b
where t.syscode in ('S04','S05','S06','S08','S10') and t.syscode=a.syscode and t.tablename=b.table_name
      and (t.syscode||'_'||t.tablename) in 
        (select t.syscode||'_'||t.tablename from control_sdsdata@bi t
        minus
        select tname from tab@bi where regexp_instr(tname,'^S[0-9][0-9]_') = 1)
order by 1;

-- 'S02'
--select 'copy from '||'whrcbloan/whrcbloan@31.2.1.13:1521/sample'||'  create '||t.syscode||'_'||t.tablename||' using select * from '||t.tablename||' where rownum<1000;' copystr
select 'create table S2_'||substr(t.tablename,4)||' as select * from '||t.tablename||'@'||decode(t.syscode,'S01','S01','S03','S03','S09','S09','S02')||' WHERE ROWNUM<1000;'
  --a.connstr,t.* 
from control_sdsdata@bi t,syscode@bi a,all_all_tables b
where t.syscode in ('S02') and t.syscode=a.syscode and t.tablename=b.table_name
      and (t.syscode||'_'||t.tablename) in 
        (select t.syscode||'_'||t.tablename from control_sdsdata@bi t
        minus
        select tname from tab@bi where regexp_instr(tname,'^S[0-9][0-9]_') = 1)
order by 1;
--####### s01 sjck/sjck03@21.1.1.45:1521/wsbank
set echo off
set head off
set linesize 1000
col copystr format a4000

select '-- connect whrcbloan/whrcbloan@31.2.2.13/sample' from dual;

-- select 'copy from '||a.connstr||'  create '||t.syscode||'_'||t.tablename||' using select '||get_columns(t.tablename)||' from '||t.tablename||' where rownum<1000;' copystr
select 'create table '||t.syscode||'_'||t.tablename||' as select '||get_columns(t.tablename)||' from '||t.tablename||'@'||decode(t.syscode,'S01','S01','S03','S03','S09','S09','S02')||' WHERE ROWNUM<1000;'
  --a.connstr,t.* 
from control_sdsdata@bi t,syscode@bi a,all_all_tables b
where t.syscode in ('S01') and t.syscode=a.syscode and t.tablename=b.table_name
      and (t.syscode||'_'||t.tablename) in 
        (select t.syscode||'_'||t.tablename from control_sdsdata@bi t
        minus
        select tname from tab@bi where regexp_instr(tname,'^S[0-9][0-9]_') = 1)
order by 1;
--####### s03
set echo off
set head off
set linesize 1000
col copystr format a4000

select '-- sjck/sjck03@21.1.1.45:1521/utandb' from dual;

-- select 'copy from '||a.connstr||'  create '||t.syscode||'_'||t.tablename||' using select '||get_columns(t.tablename)||' from '||t.tablename||' where rownum<1000;' copystr
select 'create table '||t.syscode||'_'||t.tablename||' as select '||get_columns(t.tablename)||' from '||t.tablename||'@'||decode(t.syscode,'S01','S01','S03','S03','S09','S09','S02')||' WHERE ROWNUM<1000;'
  --a.connstr,t.* 
from control_sdsdata@bi t,syscode@bi a,all_all_tables b
where t.syscode in ('S03') and t.syscode=a.syscode and t.tablename=b.table_name
      and (t.syscode||'_'||t.tablename) in 
        (select t.syscode||'_'||t.tablename from control_sdsdata@bi t
        minus
        select tname from tab@bi where regexp_instr(tname,'^S[0-9][0-9]_') = 1)
order by 1;
--####### s09
set echo off
set head off
set linesize 1000
col copystr format a4000

select '-- customer/customer@21.1.1.45:1521/wsbank' from dual;

select 'copy from '||a.connstr||'  create '||t.syscode||'_'||t.tablename||' using select * from '||t.tablename||' where rownum<1000;' copystr
-- select 'create table '||t.syscode||'_'||t.tablename||' as select '||get_columns(t.tablename)||' from '||t.tablename||'@'||decode(t.syscode,'S01','S01','S03','S03','S09','S09','S02')||' WHERE ROWNUM<1000;'
  --a.connstr,t.* 
from control_sdsdata@bi t,syscode@bi a,all_views b
where t.syscode in ('S09') and t.syscode=a.syscode and t.tablename=b.VIEW_NAME
      and (t.syscode||'_'||t.tablename) in 
        (select t.syscode||'_'||t.tablename from control_sdsdata@bi t
        minus
        select tname from tab@bi where regexp_instr(tname,'^S[0-9][0-9]_') = 1)
order by 1;

--####### s15   sjck/sjck03@21.1.1.45:1521/cspdb
set echo off
set head off
set linesize 1000
col copystr format a4000

select '-- sjck/sjck03@21.1.1.45:1521/cspdb' from dual;

select 'copy from '||a.connstr||'  create '||t.syscode||'_'||t.tablename||' using select * from '||t.tablename||' where rownum<1000;' copystr
-- select 'create table '||t.syscode||'_'||t.tablename||' as select '||get_columns(t.tablename)||' from '||t.tablename||'@'||decode(t.syscode,'S01','S01','S03','S03','S09','S09','S02')||' WHERE ROWNUM<1000;'
  --a.connstr,t.* 
from control_sdsdata@bi t,syscode@bi a,all_all_tables b
where t.syscode in ('S15') and t.syscode=a.syscode and t.tablename=b.table_name
      and (t.syscode||'_'||t.tablename) in 
        (select t.syscode||'_'||t.tablename from control_sdsdata@bi t
        minus
        select tname from tab@bi where regexp_instr(tname,'^S[0-9][0-9]_') = 1)
order by 1;
---------------------------查找没有的源系统表 bi/wangjinyu@31.2.2.62/dqadb
select t.syscode||'_'||t.tablename from control_sdsdata t
        minus --S02_TP_SCU_USER
select decode(substr(tname,1,2),'S2',decode(tname,'S2_SCU_USER','S02_TP_SCU_USER','S02_TB'||substr(tname,3)),tname) from tab where regexp_instr(tname,'^S[0-9][0-9]_') = 1 or regexp_instr(tname,'^S2_') = 1
;
-- 除s07以外的表,377张
create or replace view view_sdsdata_n07 as
select decode(substr(tname,1,2),'S2',decode(tname,'S2_SCU_USER','S02_TP_SCU_USER','S02_TB'||substr(tname,3)),tname) fullname
,decode(substr(tname,1,2),'S2',decode(tname,'S2_SCU_USER','TP_SCU_USER','TB'||substr(tname,3)),substr(tname,5)) srcname
,tname
from tab where regexp_instr(tname,'^S[0-9][0-9]_') = 1 or regexp_instr(tname,'^S2_') = 1
order by 1;
--------------
select a.*,b.*
from view_sdsdata_n07 a,control_sdsdata b
where a.srcname=b.errtabname;
--------------查看需要导出的表信息,卸数参数文件
select b.syscode||'#'||a.srcname||'#'||'select '||'#'
--a.*,b.*
from view_sdsdata_n07 a,control_sdsdata b
where a.srcname=b.errtabname
order by a.fullname;
-----------------------------
create or replace function get_columns_clr(tabname varchar2)
/*
作者：Kenny.Wang	
日期：20130531
功能：对varchar2,char,varchar字段替换【回车chr(10),换行chr(13),分隔符chr(7)等不可见字符】为【空格chr(32)】
环境：oracle  database: connect bi/wangjinyu@31.2.2.62:1521/dqadb
*/
return varchar2 is
delimiter varchar2(10);
colstr    varchar2(4000):='';
rec       all_tab_cols%ROWTYPE;
sleng     integer;
str       varchar2(100);
begin
  delimiter := ',';
  for rec in (select * from all_tab_cols t where t.TABLE_NAME=tabname order by t.COLUMN_ID) loop
    begin
      -- [[:space:]] 匹配任何空白字母
--      select case when instr(rec.data_type,'CHAR')<>0 then 'regexp_replace('||rec.column_name||',''['||chr(10)||'|'||chr(13)||'|'||chr(7)||']'',chr(32))' else rec.column_name end case into str from dual;
--      select case when instr(rec.data_type,'CHAR')<>0 then 'regexp_replace('||rec.column_name||',''[''||chr(10)||''|''||chr(13)||''|''||chr(7)||'']'',chr(32))' else rec.column_name end case into str from dual;
--      select case when instr(rec.data_type,'CHAR')<>0 then 'regexp_replace('||rec.column_name||',''[[:space:]]'',chr(32))' else rec.column_name end case into str from dual;
      select case when instr(rec.data_type,'CHAR')<>0 then 'regexp_replace('||rec.column_name||',''[[:space:]]'','' '')' else rec.column_name end case into str from dual;      	
      colstr := colstr||str||delimiter;  ---------regexp_replace(ASSETNO,'['||chr(10)||'|'||chr(13)||'|'||chr(7)||']',chr(32))
    exception
      when others then
        return rec.table_name||'+++++++ ';
    end;
  end loop;
  colstr := substr(colstr,1,length(colstr)-1);
  return colstr||' ';
end;
/
---------------------------
select b.syscode||'#'||a.srcname||'#'||'select '||get_columns_clr(a.tname)||' from '||a.tname||'#'
--a.*,b.*
from view_sdsdata_n07 a,control_sdsdata b
where a.srcname=b.errtabname
order by a.fullname;
---------------------------wsbank/wsbank2013@21.1.1.45/wsbank  (sjck/sjck03) 按表的字段顺序返回字段列表函数 get_columns(tabname varchar2)
-- 
---------------------------whrcbloan/whrcbloan@31.2.1.13/sample 按表的字段顺序返回字段列表函数 get_columns(tabname varchar2)
create or replace function get_columns(tabname varchar2)
return varchar2 is
delimiter varchar2(10);
colstr    varchar2(4000):='';
rec       all_tab_cols%rowtype;
sleng     integer;
begin
  delimiter := ',';
  for rec in (select * from all_tab_cols t where t.TABLE_NAME=tabname order by t.COLUMN_ID) loop
    if regexp_instr(rec.data_type,'TIMESTAMP')<>0 or regexp_instr(rec.data_type,'DATE')<>0 then
      colstr := colstr||'to_char('||rec.column_name||',''yyyymmdd hh24:mi:ss'')'||delimiter;
    else
      colstr := colstr||rec.column_name||delimiter;
    end if;
  end loop;
  colstr := substr(colstr,1,length(colstr)-1);
  return colstr||' ';
end;
/

--------------------------- 拷贝源系统表到：bi/wangjinyu@31.2.2.62:1521/dqadb
-- t.tablename  -->  t.syscode||'_'||t.tablename
-- copy表的例子,字段日期(date,timestamp)要转换为'yyyymmdd hh42:mi:ss'
copy from customer/customer@21.1.1.45:1521/wsbank -
  create S09_V_BONDSDEALS using select  -
deal_id, -
deal_tablename, -
aspclient_id, -
bondscode, -
bondsname, -
bondstype, -
serial_number, -
tradedate, -
settledate, -
buyorsell, -
cleanprice, -
dirtyprice, -
yieldtomaturity, -
settleamount, -
portfolio_id, -
portfolio_name, -
keepfolder_id, -
keepfolder_shortname, -
folderatts, -
classfyname, -
cptys_shortname, -
cptys_id, -
settletype, -
dealer_id, -
dealer_name, -
ref_number, -
feeamount, -
taxamount, -
brokeramount, -
note, -
nominal, -
accruedamount, -
cfets_from, -
source, -
to_char(lastmodified,'yyyymmdd hh24:mi:ss'), -
datasymbol_id -
 from V_BONDSDEALS where rownum<1000;
-- 生成的记录保存到 f:\temp\fileout17.txt
select 'copy from '||a.connstr||' -'||chr(10)||'  create '||t.syscode||'_'||t.tablename||' using select * from '||t.tablename||' where rownum<1000;'
  --a.connstr,t.* 
from control_sdsdata t,syscode a
where t.syscode<>'S07' and t.syscode=a.syscode
order by 1;

-- 开始copy表操作
-- 在 DOS  :   f:\temp\ 下,F:\temp>sqlplus bi/wangjinyu@31.2.2.62/dqadb <fileout17.txt >copy_tobi.log
-- copy完成后,可查看
select * from tab where regexp_instr(tname,'^S[0-9][0-9]_') = 1;
-- 查看没拷贝成功的表
select t.syscode||'_'||t.tablename from control_sdsdata t
minus
select tname from tab where regexp_instr(tname,'^S[0-9][0-9]_') = 1;
---------------------------whrcbloan/whrcbloan@31.2.1.13/sample
-- wsbank/wsbank2013@21.1.1.45/wsbank 再次生成拷贝表的语句
--
set echo off
set head off
set linesize 1000
col copystr format a4000

select 'copy from '||a.connstr||'  create '||t.syscode||'_'||t.tablename||' using select '||get_columns(t.tablename)||' from '||t.tablename||' where rownum<1000;' copystr
  --a.connstr,t.* 
from control_sdsdata@bi t,syscode@bi a
where t.syscode<>'S07' and t.syscode=a.syscode
      and (t.syscode||'_'||t.tablename) in 
        (select t.syscode||'_'||t.tablename from control_sdsdata@bi t
        minus
        select tname from tab@bi where regexp_instr(tname,'^S[0-9][0-9]_') = 1)
order by 1;
