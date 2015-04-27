不同数据库版本，要注意区分里面的SQL语句、语法区别(以下几个文件有差别)：
	etlmaster.pl,etlrcv.pl,etl_unix.pm
##三个版本的bin目录内容
gp_etlrep_Auto.tar
ora_etlrep_Auto.tar
td_etlrep_Auto.tar
##版本切换或资料库用户及密码改变时，需要重新生成密码文件${AUTO_HOME}/etc/ETL_LOGON
perl EncodeLogon.pl
  ETL_LOGON
  etluser
  etluser
##start.sh,stop.sh里面有用到profile目录下的环境变量信息
##一个多个物理或逻辑ETL服务器节点只需要在主节点有etlscheduler，其它非主节点不需要
=================

安装说明

环境要求： 
	1)Teradata Client(CLI, bteq, fastload, fexp, tdodbc, mload 等)
	2)Perl, DBI, DBD-ODBC
	3)unix configure file(/etc/hosts)
安装步骤：
  1)Create user "etl" for ETL Automation with home directory /ETL
    获取 Auto261.tar.gz 运行， tar -zxvf Auto261.tar.gz
  2)追加profile中内容到 $HOME/.profile
    根据实际运行环境，调整相关的环境变量，修改后重新login
    Automation 工作主目录； AUTO_HOME=$HOME; export AUTO_HOME
    Automation 服务器的名称； AUTO_SERVER=ETL1; export AUTO_SERVER
    Automation 工作资料库（DataSource）； AUTO_DSN=etldb; export AUTO_DSN
    Automation 工作资料库（Repository）； AUTO_DB=etl; export AUTO_DB
    Automation 服务器工作IP地址； AUTO_SERVER_IP=192.168.30.130; export AUTO_SERVER_IP
	3)配置ODBC 环境(.odbc.ini)
	  复制 odbc.ini $HOME/.odbc.ini
	  修改 $HOME/.odbc.ini 的内容，在[etldb]子项下，修改Terdata系统节点的IP地址
			DBCName=128.64.96.56	
			DBCName2=128.64.96.57
			DBCName3=128.64.96.58
			DBCName4=128.64.96.59
	4)重新 login Linux, 执行perl install/set_wrkdir.pl, 创建Automation 工作目录树
	5)mv install/bin/* $AUTO_HOME/bin
	6)Automation工作库的创建，使用Teradata工具创建用户 etl，并把以etl用户身份运行 EtlRepository.sql
	7)配置Automation数据库连接信息
	  Linux:/ETL>EncodeLogon.pl
Please input the logon file name:ETL_LOGON
Please input the user name:etl
Please input the password:etl
Output the logon file...
Logon file was generated.
	8)使用 EtlAdmin.jar, 增加 ETL Server信息到ETLRepository中。
	9)在ETL Server上启动 Automation
	  start.sh
  Automation工作环境安装完毕

==========================================生产环境变量ETL1.profile
# 代理端口,接收前端应用的控制指令
export AUTO_AGENT_PORT=6346
# dbi::Oracle的资料库连接串
export AUTO_DSN="host=11.1.1.167;sid=etldb;port=1521"
# ETL SERVER的HOME
export AUTO_HOME=/EDW/ETL/INSTANCE/ETL1
# Running状态的任务并发数
export AUTO_JOB_COUNT=18
# ETL主服务器标志为1
export AUTO_PRIMARY_SERVER=1
# ETL主服务器IP
export AUTO_PRIMARY_SERVER_IP=11.1.1.167
# ETL服务器名称
export AUTO_SERVER=ETL1
# ETL服务器IP
export AUTO_SERVER_IP=11.1.1.167
# ETL刷新资料库的频率
export AUTO_SLEEP=5
export AUTO_SDSDATADB=SDSDATA;
export AUTO_SDSDDLDB=SDSDDL;
# ETL守护进程端口
export AUTO_WDOG_PORT=6346;

# 最小日期
export AUTO_MINDATE=19000101
# 最大日期
export AUTO_MAXDATE=30001231
# 为空日期
export AUTO_NULLDATE=19000103
# 错误日期
export AUTO_ILLDATE=19000102

export PM_PATH=$AUTO_HOME/ADS
export AUTO_GPFDIST_IP=172.28.4.167
export GPFDIST_EXP_PORT=8083
export GPFDIST_IMP_PORT=8082
export AUTO_GREENPLUM_IP=11.1.1.153
export AUTO_GREENPLUM_DB=whrcb_edw
# ETL资料库的schema
export AUTO_DB=etlrep