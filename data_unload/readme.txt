###############################################################################
# Program  : unload_main.sh                                                    
# Argument :                                                                   
#     参数1: SYSID-系统ID                                                      
#     参数2: FLG-程序触发标志 1-等待OK文件触发 2-直接执行                      
# Created by : WangYi 2013-7-6                                                 
# Modified by : WangYi                                                         
# Function : unload data from oracle database                                  
###############################################################################
部署说明：以系统编号为XXX的系统为例

1、设置环境变量,在.profile文件中增加以下内容
   export UNLOAD_WORKDIR=$HOME/data_unload

2、配置$UNLOAD_WORKDIR/.sys.lst文件，在文件结尾增加以下内容
   XXX|数据库连接串|数据保留天数|系统名称（例如S08系统：S08|gtp/gtp@11.1.1.123:1521/bbsp|10000|票据

3、配置卸数日期参数
   mkdir $UNLOAD_WORKDIR/cfg/XXX
   cd $UNLOAD_WORKDIR/cfg/XXX
   echo "YYYYMMDD" > XXX.cfg（YYYYMMDD为程序初次卸数日期，该日期不能大于当前系统日期）
   说明：程序每成功执行一次该日期会自动加一，若需重复卸载同一日期数据，需手工修改XXX.cfg文件中的日期
   
4、配置卸数脚本文件
   mkdir $UNLOAD_WORKDIR/sql/XXX
   cd $UNLOAD_WORKDIR/sql/XXX
   拷贝或编辑所有卸数脚本文件到$UNLOAD_WORKDIR/sql/XXX目录下（卸数脚本文件名为[源系统表名全大写].sql，如abc表脚本文件为ABC.sql）
   脚本文件内容为select语句，建议多行规范编写
   数据表卸数条件为按日期取增量时，日期变量使用#*P_DATE*#字符串替换（如abc表按tran_date字段取增量，条件部分书写为where tran_date='#*P_DATE*#'）

5、配置卸数目标目录
   确认$UNLOAD_WORKDIR/FILE目录存在
   建议$UNLOAD_WORKDIR/FILE目录设置为链接，本项目可指向/EDW/DATA/SOURCE
   

使用说明：以系统编号为S08的系统卸载2013年7月6日数据为例
1、手工执行
   确认$UNLOAD_WORKDIR/cfg/S08/S08.cfg文件第一行内容为20130706
   等待OK文件触发：$UNLOAD_WORKDIR/bin/unload_main.sh S08 1
   直接执行：      $UNLOAD_WORKDIR/bin/unload_main.sh S08 2

2、定时任务：以每晚21点30分启动为例
   确认$UNLOAD_WORKDIR/cfg/S08/S08.cfg文件第一行内容为20130706
   crontab -e，并增加一下内容
   等待OK文件触发：30 21 * * * /绝对路径/unload_main.sh S08 1 >>/日志文件绝对路径/S08.log 2>&1   （日志文件绝对路径建议为$UNLOAD_WORKDIR/log/S08）
   直接执行：      30 21 * * * /绝对路径/unload_main.sh S08 2 >>/日志文件绝对路径/S08.log 2>&1   （日志文件绝对路径建议为$UNLOAD_WORKDIR/log/S08）

00 05 * * * /home/gtp/data_unload/bin/unload_main.sh S01 1 >> /home/gtp/data_unload/log/S01.log 2>&1
15 00 * * * /home/gtp/data_unload/bin/unload_main.sh S04 2 >> /home/gtp/data_unload/log/S08.log 2>&1
00 21 * * * /home/gtp/data_unload/bin/unload_main.sh S06 2 >> /home/gtp/data_unload/log/S06.log 2>&1
00 02 * * * /home/gtp/data_unload/bin/unload_main.sh S08 2 >> /home/gtp/data_unload/log/S08.log 2>&1
15 00 * * * /home/gtp/data_unload/bin/unload_main.sh S09 2 >> /home/gtp/data_unload/log/S09.log 2>&1
15 00 * * * /home/gtp/data_unload/bin/unload_main.sh S10 2 >> /home/gtp/data_unload/log/S10.log 2>&1