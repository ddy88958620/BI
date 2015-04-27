--------------------------------------------------------------------------------
-- 脚本说明：武汉农商行数据仓库建库、用户、角色脚本                           --
--           此脚本请在PSQL中执行                                             --
-- 内容：创建用户、角色、授权、注释等                                     --
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- 使用角色 gpadmin 登录 postgres ,继续后面操作 ******************************
-- psql -h 31.2.2.69 -p 5432 -d postgres -U gpadmin 
-- 第一步，以gpadmin用户登录目标GP服务器的postgres数据库，执行下用户、角色、新数据库的创建
-- 用户和角色已经存在的话，可以略过，或者创建提示已存在报错没有关系
--------------------------------------------------------------------------------
-- 创建用户(USER/ROLE)及角色(ROLE GROUP)

CREATE ROLE EDWADMIN LOGIN  ENCRYPTED PASSWORD 'edwadmin' SUPERUSER INHERIT CREATEDB CREATEROLE; 
COMMENT ON ROLE EDWADMIN IS '数据仓库管理员';

CREATE ROLE ETLUSER LOGIN  ENCRYPTED PASSWORD 'etluser'; COMMENT ON ROLE ETLUSER IS 'ETL跑批用户';

CREATE ROLE FRTUSER LOGIN  ENCRYPTED PASSWORD 'frtuser'; COMMENT ON ROLE FRTUSER IS '应用查询用户';

CREATE ROLE ARCUSER LOGIN  ENCRYPTED PASSWORD 'arcuser'; COMMENT ON ROLE ARCUSER IS '备份用户';

CREATE ROLE DQCUSER LOGIN  ENCRYPTED PASSWORD 'dqcuser'; COMMENT ON ROLE DQCUSER IS '数据质量检查用户';

CREATE ROLE MDSUSER LOGIN  ENCRYPTED PASSWORD 'mdsuser'; COMMENT ON ROLE MDSUSER IS '元数据管理用户';

CREATE ROLE RPTUSER LOGIN  ENCRYPTED PASSWORD 'rptuser'; COMMENT ON ROLE RPTUSER IS '综合报表平台管理用户';

CREATE ROLE MNTROLE;
COMMENT ON ROLE MNTROLE  IS '维护角色(IT)';

CREATE ROLE QRYROLE;
COMMENT ON ROLE QRYROLE  IS '查询角色';

-- 创建模式(SCHEMA)

CREATE DATABASE EDW869
  WITH ENCODING='UTF8'
       OWNER=EDWADMIN
       CONNECTION LIMIT=-1;
 COMMENT ON DATABASE EDW869  IS '武汉农商行企业级数据仓库';

--------------------------------------------------------------------------------
-- 使用角色 EDWADMIN 登录 EDW869，继续后面操作 ******************************
-- psql -h 31.2.2.69 -p 5432 -d EDW869 -U edwadmin 
-- 第二步，以edwadmin用户登录目标GP服务器刚新建的数据库，执行下schema的创建和授权等
-- 若是刚新建的库，以下执行都得提示成功才可以
--------------------------------------------------------------------------------

-- schema: sdsddl,sdsdata,odsdata,btemp,bdsdata,wrkdata,adsdata,adsview,bdsview,odsview,tmpdata,dqcrep,mdsrep,rptrep,etlrep,arcdata,dmsdata

CREATE SCHEMA SDSDATA;
COMMENT ON SCHEMA SDSDATA IS '缓冲区';

CREATE SCHEMA SDSDDL;
COMMENT ON SCHEMA SDSDDL IS '缓冲区定义库';

CREATE SCHEMA ODSDATA;
COMMENT ON SCHEMA ODSDATA IS '操作型数据存储库';

CREATE SCHEMA BTEMP;
COMMENT ON SCHEMA BTEMP IS '基础层临时';

CREATE SCHEMA BDSDATA;
COMMENT ON SCHEMA BDSDATA IS '基础数据库';

CREATE SCHEMA WRKDATA;
COMMENT ON SCHEMA WRKDATA IS 'ETL跑批临时库';

CREATE SCHEMA ADSDATA;
COMMENT ON SCHEMA ADSDATA IS '集市库';

CREATE SCHEMA ADSVIEW;
COMMENT ON SCHEMA ADSVIEW IS '集市视图库';

CREATE SCHEMA BDSVIEW;
COMMENT ON SCHEMA BDSVIEW IS '基础数据视图库';

CREATE SCHEMA ODSVIEW;
COMMENT ON SCHEMA ODSVIEW IS '操作型数据视图库';

CREATE SCHEMA TMPDATA;
COMMENT ON SCHEMA TMPDATA IS '数据实验室';

CREATE SCHEMA USDSDDL;
COMMENT ON SCHEMA USDSDDL IS '缓冲区卸数定义库';

CREATE SCHEMA UBDSDDL;
COMMENT ON SCHEMA UBDSDDL IS '基础层卸数定义库';

CREATE SCHEMA UADSDDL;
COMMENT ON SCHEMA UADSDDL IS '集市卸数定义库';

CREATE SCHEMA UESDSDDL;
COMMENT ON SCHEMA UESDSDDL IS 'EAST切换卸数定义库';

CREATE SCHEMA DMSDATA;
COMMENT ON SCHEMA DMSDATA IS '指标数据库[dimension]';

CREATE SCHEMA DQCREP;
ALTER SCHEMA DQCREP OWNER TO DQCUSER;
COMMENT ON SCHEMA DQCREP IS '数据质量检查资料库';

CREATE SCHEMA MDSREP;
ALTER SCHEMA MDSREP OWNER TO MDSUSER;
COMMENT ON SCHEMA MDSREP IS '元数据库';

CREATE SCHEMA RPTREP;
ALTER SCHEMA RPTREP OWNER TO RPTUSER;
COMMENT ON SCHEMA RPTREP IS '报表平台资料库';

CREATE SCHEMA ETLREP;
ALTER SCHEMA ETLREP OWNER TO ETLUSER;
COMMENT ON SCHEMA ETLREP IS 'ETL资料库';

CREATE SCHEMA ARCDATA;
ALTER SCHEMA ARCDATA OWNER TO ARCUSER;
COMMENT ON SCHEMA ARCDATA IS '备份库';

-- GRANT权限
GRANT RPTUSER TO QRYROLE;
GRANT DQCUSER TO QRYROLE;
GRANT MDSUSER TO QRYROLE;
GRANT ARCUSER TO QRYROLE;

grant usage on schema TMPDATA to public;
grant create on schema TMPDATA to public;

------------------- 对部分角色和用户授权
-- 授权外部表创建
alter role etluser with createexttable (type='readable',protocol='gpfdist');
alter role arcuser with createexttable (type='readable',protocol='gpfdist');
alter role mntrole with createexttable (type='readable',protocol='gpfdist');

alter role etluser with createexttable (type='writable',protocol='gpfdist');
alter role arcuser with createexttable (type='writable',protocol='gpfdist');
alter role mntrole with createexttable (type='writable',protocol='gpfdist');

-- schema权限对其它用户或角色授权可用
grant usage on schema sdsddl,sdsdata,odsdata,bdsdata,wrkdata,adsdata,adsview,bdsview,odsview,tmpdata,dqcrep,mdsrep,rptrep,etlrep,arcdata,dmsdata,USDSDDL,UBDSDDL,UADSDDL,UESDSDDL to etluser,arcuser,mntrole with grant option;
grant create on schema sdsddl,sdsdata,odsdata,bdsdata,wrkdata,adsdata,adsview,bdsview,odsview,tmpdata,dqcrep,mdsrep,rptrep,etlrep,arcdata,dmsdata,USDSDDL,UBDSDDL,UADSDDL,UESDSDDL to etluser with grant option;

-- 反洗钱
CREATE SCHEMA UFXQDATA;
COMMENT ON SCHEMA UFXQDATA IS '反洗钱卸数存储库';

grant usage on schema UFXQDATA to etluser,arcuser,mntrole with grant option;
grant create on schema UFXQDATA to etluser with grant option;


-- 报表集市
drop SCHEMA  if exists dmrdata;
drop SCHEMA  if exists dmrdat;
CREATE SCHEMA dmrdata;
COMMENT ON SCHEMA dmrdata IS '报表集市数库';

grant usage on schema dmrdata to etluser,arcuser,mntrole with grant option;
grant create on schema dmrdata to etluser with grant option;

-- 脚本临时表表结构
-- DROP SCHEMA  if exists tmpmap;
-- CREATE SCHEMA tmpmap  AUTHORIZATION edwadmin;
-- grant usage on schema tmpmap to etluser,arcuser,mntrole with grant option;
-- grant create on schema tmpmap to etluser with grant option;
-- COMMENT ON SCHEMA tmpmap IS 'temporary table mapping[临时表表结构存储-用于元数据分析]';

