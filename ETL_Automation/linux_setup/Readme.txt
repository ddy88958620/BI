��ͬ���ݿ�汾��Ҫע�����������SQL��䡢�﷨����(���¼����ļ��в��)��
	etlmaster.pl,etlrcv.pl,etl_unix.pm
##�����汾��binĿ¼����
gp_etlrep_Auto.tar
ora_etlrep_Auto.tar
td_etlrep_Auto.tar
##�汾�л������Ͽ��û�������ı�ʱ����Ҫ�������������ļ�${AUTO_HOME}/etc/ETL_LOGON
perl EncodeLogon.pl
  ETL_LOGON
  etluser
  etluser
##start.sh,stop.sh�������õ�profileĿ¼�µĻ���������Ϣ
##һ�����������߼�ETL�������ڵ�ֻ��Ҫ�����ڵ���etlscheduler�����������ڵ㲻��Ҫ
=================

��װ˵��

����Ҫ�� 
	1)Teradata Client(CLI, bteq, fastload, fexp, tdodbc, mload ��)
	2)Perl, DBI, DBD-ODBC
	3)unix configure file(/etc/hosts)
��װ���裺
  1)Create user "etl" for ETL Automation with home directory /ETL
    ��ȡ Auto261.tar.gz ���У� tar -zxvf Auto261.tar.gz
  2)׷��profile�����ݵ� $HOME/.profile
    ����ʵ�����л�����������صĻ����������޸ĺ�����login
    Automation ������Ŀ¼�� AUTO_HOME=$HOME; export AUTO_HOME
    Automation �����������ƣ� AUTO_SERVER=ETL1; export AUTO_SERVER
    Automation �������Ͽ⣨DataSource���� AUTO_DSN=etldb; export AUTO_DSN
    Automation �������Ͽ⣨Repository���� AUTO_DB=etl; export AUTO_DB
    Automation ����������IP��ַ�� AUTO_SERVER_IP=192.168.30.130; export AUTO_SERVER_IP
	3)����ODBC ����(.odbc.ini)
	  ���� odbc.ini $HOME/.odbc.ini
	  �޸� $HOME/.odbc.ini �����ݣ���[etldb]�����£��޸�Terdataϵͳ�ڵ��IP��ַ
			DBCName=128.64.96.56	
			DBCName2=128.64.96.57
			DBCName3=128.64.96.58
			DBCName4=128.64.96.59
	4)���� login Linux, ִ��perl install/set_wrkdir.pl, ����Automation ����Ŀ¼��
	5)mv install/bin/* $AUTO_HOME/bin
	6)Automation������Ĵ�����ʹ��Teradata���ߴ����û� etl��������etl�û�������� EtlRepository.sql
	7)����Automation���ݿ�������Ϣ
	  Linux:/ETL>EncodeLogon.pl
Please input the logon file name:ETL_LOGON
Please input the user name:etl
Please input the password:etl
Output the logon file...
Logon file was generated.
	8)ʹ�� EtlAdmin.jar, ���� ETL Server��Ϣ��ETLRepository�С�
	9)��ETL Server������ Automation
	  start.sh
  Automation����������װ���

==========================================������������ETL1.profile
# ����˿�,����ǰ��Ӧ�õĿ���ָ��
export AUTO_AGENT_PORT=6346
# dbi::Oracle�����Ͽ����Ӵ�
export AUTO_DSN="host=11.1.1.167;sid=etldb;port=1521"
# ETL SERVER��HOME
export AUTO_HOME=/EDW/ETL/INSTANCE/ETL1
# Running״̬�����񲢷���
export AUTO_JOB_COUNT=18
# ETL����������־Ϊ1
export AUTO_PRIMARY_SERVER=1
# ETL��������IP
export AUTO_PRIMARY_SERVER_IP=11.1.1.167
# ETL����������
export AUTO_SERVER=ETL1
# ETL������IP
export AUTO_SERVER_IP=11.1.1.167
# ETLˢ�����Ͽ��Ƶ��
export AUTO_SLEEP=5
export AUTO_SDSDATADB=SDSDATA;
export AUTO_SDSDDLDB=SDSDDL;
# ETL�ػ����̶˿�
export AUTO_WDOG_PORT=6346;

# ��С����
export AUTO_MINDATE=19000101
# �������
export AUTO_MAXDATE=30001231
# Ϊ������
export AUTO_NULLDATE=19000103
# ��������
export AUTO_ILLDATE=19000102

export PM_PATH=$AUTO_HOME/ADS
export AUTO_GPFDIST_IP=172.28.4.167
export GPFDIST_EXP_PORT=8083
export GPFDIST_IMP_PORT=8082
export AUTO_GREENPLUM_IP=11.1.1.153
export AUTO_GREENPLUM_DB=whrcb_edw
# ETL���Ͽ��schema
export AUTO_DB=etlrep