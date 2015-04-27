--------------------------------------------------------------------------------
-- �ű�˵�����人ũ�������ݲֿ⽨�⡢�û�����ɫ�ű�                           --
--           �˽ű�����PSQL��ִ��                                             --
-- ���ݣ������û�����ɫ����Ȩ��ע�͵�                                     --
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- ʹ�ý�ɫ gpadmin ��¼ postgres ,����������� ******************************
-- psql -h 31.2.2.69 -p 5432 -d postgres -U gpadmin 
-- ��һ������gpadmin�û���¼Ŀ��GP��������postgres���ݿ⣬ִ�����û�����ɫ�������ݿ�Ĵ���
-- �û��ͽ�ɫ�Ѿ����ڵĻ��������Թ������ߴ�����ʾ�Ѵ��ڱ���û�й�ϵ
--------------------------------------------------------------------------------
-- �����û�(USER/ROLE)����ɫ(ROLE GROUP)

CREATE ROLE EDWADMIN LOGIN  ENCRYPTED PASSWORD 'edwadmin' SUPERUSER INHERIT CREATEDB CREATEROLE; 
COMMENT ON ROLE EDWADMIN IS '���ݲֿ����Ա';

CREATE ROLE ETLUSER LOGIN  ENCRYPTED PASSWORD 'etluser'; COMMENT ON ROLE ETLUSER IS 'ETL�����û�';

CREATE ROLE FRTUSER LOGIN  ENCRYPTED PASSWORD 'frtuser'; COMMENT ON ROLE FRTUSER IS 'Ӧ�ò�ѯ�û�';

CREATE ROLE ARCUSER LOGIN  ENCRYPTED PASSWORD 'arcuser'; COMMENT ON ROLE ARCUSER IS '�����û�';

CREATE ROLE DQCUSER LOGIN  ENCRYPTED PASSWORD 'dqcuser'; COMMENT ON ROLE DQCUSER IS '������������û�';

CREATE ROLE MDSUSER LOGIN  ENCRYPTED PASSWORD 'mdsuser'; COMMENT ON ROLE MDSUSER IS 'Ԫ���ݹ����û�';

CREATE ROLE RPTUSER LOGIN  ENCRYPTED PASSWORD 'rptuser'; COMMENT ON ROLE RPTUSER IS '�ۺϱ���ƽ̨�����û�';

CREATE ROLE MNTROLE;
COMMENT ON ROLE MNTROLE  IS 'ά����ɫ(IT)';

CREATE ROLE QRYROLE;
COMMENT ON ROLE QRYROLE  IS '��ѯ��ɫ';

-- ����ģʽ(SCHEMA)

CREATE DATABASE EDW869
  WITH ENCODING='UTF8'
       OWNER=EDWADMIN
       CONNECTION LIMIT=-1;
 COMMENT ON DATABASE EDW869  IS '�人ũ������ҵ�����ݲֿ�';

--------------------------------------------------------------------------------
-- ʹ�ý�ɫ EDWADMIN ��¼ EDW869������������� ******************************
-- psql -h 31.2.2.69 -p 5432 -d EDW869 -U edwadmin 
-- �ڶ�������edwadmin�û���¼Ŀ��GP���������½������ݿ⣬ִ����schema�Ĵ�������Ȩ��
-- ���Ǹ��½��Ŀ⣬����ִ�ж�����ʾ�ɹ��ſ���
--------------------------------------------------------------------------------

-- schema: sdsddl,sdsdata,odsdata,btemp,bdsdata,wrkdata,adsdata,adsview,bdsview,odsview,tmpdata,dqcrep,mdsrep,rptrep,etlrep,arcdata,dmsdata

CREATE SCHEMA SDSDATA;
COMMENT ON SCHEMA SDSDATA IS '������';

CREATE SCHEMA SDSDDL;
COMMENT ON SCHEMA SDSDDL IS '�����������';

CREATE SCHEMA ODSDATA;
COMMENT ON SCHEMA ODSDATA IS '���������ݴ洢��';

CREATE SCHEMA BTEMP;
COMMENT ON SCHEMA BTEMP IS '��������ʱ';

CREATE SCHEMA BDSDATA;
COMMENT ON SCHEMA BDSDATA IS '�������ݿ�';

CREATE SCHEMA WRKDATA;
COMMENT ON SCHEMA WRKDATA IS 'ETL������ʱ��';

CREATE SCHEMA ADSDATA;
COMMENT ON SCHEMA ADSDATA IS '���п�';

CREATE SCHEMA ADSVIEW;
COMMENT ON SCHEMA ADSVIEW IS '������ͼ��';

CREATE SCHEMA BDSVIEW;
COMMENT ON SCHEMA BDSVIEW IS '����������ͼ��';

CREATE SCHEMA ODSVIEW;
COMMENT ON SCHEMA ODSVIEW IS '������������ͼ��';

CREATE SCHEMA TMPDATA;
COMMENT ON SCHEMA TMPDATA IS '����ʵ����';

CREATE SCHEMA USDSDDL;
COMMENT ON SCHEMA USDSDDL IS '������ж�������';

CREATE SCHEMA UBDSDDL;
COMMENT ON SCHEMA UBDSDDL IS '������ж�������';

CREATE SCHEMA UADSDDL;
COMMENT ON SCHEMA UADSDDL IS '����ж�������';

CREATE SCHEMA UESDSDDL;
COMMENT ON SCHEMA UESDSDDL IS 'EAST�л�ж�������';

CREATE SCHEMA DMSDATA;
COMMENT ON SCHEMA DMSDATA IS 'ָ�����ݿ�[dimension]';

CREATE SCHEMA DQCREP;
ALTER SCHEMA DQCREP OWNER TO DQCUSER;
COMMENT ON SCHEMA DQCREP IS '��������������Ͽ�';

CREATE SCHEMA MDSREP;
ALTER SCHEMA MDSREP OWNER TO MDSUSER;
COMMENT ON SCHEMA MDSREP IS 'Ԫ���ݿ�';

CREATE SCHEMA RPTREP;
ALTER SCHEMA RPTREP OWNER TO RPTUSER;
COMMENT ON SCHEMA RPTREP IS '����ƽ̨���Ͽ�';

CREATE SCHEMA ETLREP;
ALTER SCHEMA ETLREP OWNER TO ETLUSER;
COMMENT ON SCHEMA ETLREP IS 'ETL���Ͽ�';

CREATE SCHEMA ARCDATA;
ALTER SCHEMA ARCDATA OWNER TO ARCUSER;
COMMENT ON SCHEMA ARCDATA IS '���ݿ�';

-- GRANTȨ��
GRANT RPTUSER TO QRYROLE;
GRANT DQCUSER TO QRYROLE;
GRANT MDSUSER TO QRYROLE;
GRANT ARCUSER TO QRYROLE;

grant usage on schema TMPDATA to public;
grant create on schema TMPDATA to public;

------------------- �Բ��ֽ�ɫ���û���Ȩ
-- ��Ȩ�ⲿ����
alter role etluser with createexttable (type='readable',protocol='gpfdist');
alter role arcuser with createexttable (type='readable',protocol='gpfdist');
alter role mntrole with createexttable (type='readable',protocol='gpfdist');

alter role etluser with createexttable (type='writable',protocol='gpfdist');
alter role arcuser with createexttable (type='writable',protocol='gpfdist');
alter role mntrole with createexttable (type='writable',protocol='gpfdist');

-- schemaȨ�޶������û����ɫ��Ȩ����
grant usage on schema sdsddl,sdsdata,odsdata,bdsdata,wrkdata,adsdata,adsview,bdsview,odsview,tmpdata,dqcrep,mdsrep,rptrep,etlrep,arcdata,dmsdata,USDSDDL,UBDSDDL,UADSDDL,UESDSDDL to etluser,arcuser,mntrole with grant option;
grant create on schema sdsddl,sdsdata,odsdata,bdsdata,wrkdata,adsdata,adsview,bdsview,odsview,tmpdata,dqcrep,mdsrep,rptrep,etlrep,arcdata,dmsdata,USDSDDL,UBDSDDL,UADSDDL,UESDSDDL to etluser with grant option;

-- ��ϴǮ
CREATE SCHEMA UFXQDATA;
COMMENT ON SCHEMA UFXQDATA IS '��ϴǮж���洢��';

grant usage on schema UFXQDATA to etluser,arcuser,mntrole with grant option;
grant create on schema UFXQDATA to etluser with grant option;


-- ������
drop SCHEMA  if exists dmrdata;
drop SCHEMA  if exists dmrdat;
CREATE SCHEMA dmrdata;
COMMENT ON SCHEMA dmrdata IS '����������';

grant usage on schema dmrdata to etluser,arcuser,mntrole with grant option;
grant create on schema dmrdata to etluser with grant option;

-- �ű���ʱ���ṹ
-- DROP SCHEMA  if exists tmpmap;
-- CREATE SCHEMA tmpmap  AUTHORIZATION edwadmin;
-- grant usage on schema tmpmap to etluser,arcuser,mntrole with grant option;
-- grant create on schema tmpmap to etluser with grant option;
-- COMMENT ON SCHEMA tmpmap IS 'temporary table mapping[��ʱ���ṹ�洢-����Ԫ���ݷ���]';

