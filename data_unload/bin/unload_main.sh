#! /bin/sh
###############################################################################
# Program  : unload_main.sh
# Argument :
#     参数1: SYSID-系统ID
#     参数2: FLG-程序触发标志 1-等待OK文件触发 2-直接执行
# Created by : WangYi 2013-7-6
# Modified by : WangYi 
#   20130820  : WangYi  .sys.lst配置文件倒数第二列增加DBTYPE,1-ORACLE 2-DB2
#                       208行增加从.sys.lst中读取DBTYPE变量
#                       280,292行在原语句上增加对DBTYPE条件判断 DBTYPE -eq 1为真时，执行原有语句
# Function : unload data from oracle database
###############################################################################

#加载用户环境变量
. ~/.bash_profile

export UNLOAD_WORKDIR=$HOME/data_unload
#全局变量定义
MAXNUM=30                     #单系统卸数最大并发数
DEL='0x07'                    #文件分割符
SLEEP_LONG=5                  #长等待时间/秒
SLEEP_SHORT=300000            #短等待时间/微秒

PNAME=`basename $0 .sh`       #取命令名称
SYSDT=`date +"%Y%m%d"`        #取系统日期

#日志函数
LOG ()
{
  LOGDT=`date +"%Y%m%d"`
  LOGTM=`date +"%T"`          #取系统时间
  echo "$LOGTM|$1"
}

_EXIT ()
{
  rm -f $TMPDIR/$PNAME
  exit
}

###########检查程序参数#############
#参数说明
#参数1:系统ID，具体ID编号为S01,S02......详见系统设计文档
#参数2:处罚类型，1-等待OK文件，2-不等待
####################################
if [ $# -ne 2 ]
then
  echo "错误 !!! USAGE: $PNAME.sh SYSID FLG"
  exit
fi

echo "`date +'%Y%m%d %T'`|检查程序参数成功"

#检查工作目录环境变量
if [ -z $UNLOAD_WORKDIR ]
then
  echo "环境变量UNLOAD_WORKDIR未定义"
  exit
fi

#检查系统ID定义文件
if [ ! -f $UNLOAD_WORKDIR/.sys.lst ]
then
  echo "$UNLOAD_WORKDIR/.sys.lst文件不存在"
  exit
fi

#检查参数1是否在列表文件中存在
SYS=`grep -e "\b$1\b" $UNLOAD_WORKDIR/.sys.lst|awk -F\| '{print $1}'`
if [ -z $SYS ]
then
  echo "第一个参数必须在$UNLOAD_WORKDIR/.sys.lst文件中定义"
  exit
fi

#检查参数2合法性，仅为1或2
if [ $2 -ne 1 -a $2 -ne 2 ]
then
  echo "第二个参数只能是1或2，1表示等待OK文件触发，2表示直接执行"
  exit
fi

#读取卸数日期控制文件,获取卸数日期
CFGFILE=$UNLOAD_WORKDIR/cfg/$SYS/$SYS.cfg
if [ ! -s $CFGFILE ]
then
  echo "$CFGFILE不存在或文件内容为空"
  exit
fi

#ULDT日期为要卸数的日期，直接使用
ULDT=`cat $CFGFILE|head -n 1|cut -c 1-8`

#定义日志文件
LOGDIR=$UNLOAD_WORKDIR/log/$SYS
if [ ! -d $LOGDIR ]
then
  mkdir $LOGDIR
fi
LOGFILE=$LOGDIR/$PNAME.$ULDT

#卸数日期不能大于系统日期
if [ $ULDT -gt $SYSDT ]
then
  LOG "卸数日期$ULDT不能大于系统日期$SYSDT" >> $LOGFILE
  exit
fi

echo "!!$SYSDT" >> $LOGFILE
LOG "###########程序启动##############" >> $LOGFILE

#防止同一源系统多次卸载
TMPDIR=$UNLOAD_WORKDIR/tmp/$SYS
if [ ! -d $TMPDIR ]
then
  mkdir $TMPDIR
fi

if [ -f $TMPDIR/$PNAME ]
then
  LOG "$SYS正在卸数，请确认后重新启动" >> $LOGFILE
  exit
fi

echo $$ > $TMPDIR/$PNAME
# touch $TMPDIR/$PNAME

###在此处之后，程序异常退出，一律调用_EXIT函数####
#定义bin目录
BINDIR=$UNLOAD_WORKDIR/bin
if [ ! -d $BINDIR ]
then
  LOG "$BINDIR目录不存在" >> $LOGFILE
  _EXIT
fi

#加载日期函数包
if [ ! -f $BINDIR/date_add.awk ]
then
  LOG "日期函数$BINDIR/date_add.awk不存在" >> $LOGFILE
  _EXIT
fi
. $BINDIR/date_add.awk

TMPDT=`date_add $ULDT -1`     #计算卸数日期前一日
if [ $? -ne 0 ]
then
  LOG "卸数日期不合法$ULDT" >> $LOGFILE
  _EXIT
fi
UL_DT=`echo $ULDT|cut -c 1-4`-`echo $ULDT|cut -c 5-6`-`echo $ULDT|cut -c 7-8`

#检查OK文件目录
OKDIR=$UNLOAD_WORKDIR/OK/$SYS
if [ ! -d $OKDIR ]
then
  mkdir $OKDIR
fi

LOG "检查环境变量及程序预处理成功" >> $LOGFILE

#参数2等于1时，检查并等待OK文件到位
if [ $2 -eq 1 ]
then
  while true
  do
    if [ -s $OKDIR/OK ]        #OK文件存在且不为空，则退出循环
    then
      break;
    fi
    
    sleep $SLEEP_LONG          #否则等待
  done

  while true
  do
    OKDT=`cat $OKDIR/OK|head -n 1|cut -c 1-8`   #读取OK文件日期
    if [ $OKDT -eq $ULDT ]     #OK文件日期等于卸数日期,则退出循环
    then
      break
    fi

    if [ $TMPDT -eq $OKDT ]
    then
      sleep $SLEEP_LONG
      continue
    fi

    LOG "卸数日期是$ULDT,OK文件日期只能是$ULDT或$TMPDT" >> $LOGFILE
    _EXIT
    
  done
fi

LOG "卸数程序触发条件已具备" >> $LOGFILE


# 设置ORACLE卸数的语言及字符集参数
export LANG='en_US.utf8'
export NLS_LANG='American_america.AL32UTF8'
export NLS_DATE_FORMAT='"YYYY-MM-DD HH24:MI:SS"'

#读取数据库连接串
LINKSTR=`grep $SYS $UNLOAD_WORKDIR/.sys.lst|awk -F\| '{print $2}'`

#读取数据保留天数
BAKDAYS=`grep $SYS $UNLOAD_WORKDIR/.sys.lst|awk -F\| '{print $3}'`

#读取卸数系统数据库类型 1-ORACLE 2-DB2
DBTYPE=`grep $SYS $UNLOAD_WORKDIR/.sys.lst|awk -F\| '{print $4}'`

#删除保留天数前一天的数据目录,20130708
RMDT=`date_add $ULDT -$BAKDAYS`     #计算要删除的卸数日期
if [ -d $UNLOAD_WORKDIR/FILE/$RMDT/$SYS ]
then
  rm -rf $UNLOAD_WORKDIR/FILE/$RMDT/$SYS
fi

#检查SQL目录
SQLDIR=$UNLOAD_WORKDIR/sql/$SYS
if [ ! -d $SQLDIR ]
then
  LOG "$SQLDIR目录不存在" >> $LOGFILE
  _EXIT
fi

#检查卸数目录，创建当日卸数目录
if [ ! -d $UNLOAD_WORKDIR/FILE ]
then
  LOG "$UNLOAD_WORKDIR/FILE目录不存在" >> $LOGFILE
  _EXIT
fi

if [ ! -d $UNLOAD_WORKDIR/FILE/$ULDT ]
then
  mkdir $UNLOAD_WORKDIR/FILE/$ULDT
fi

if [ ! -d $UNLOAD_WORKDIR/FILE/$ULDT/$SYS ]
then
  mkdir $UNLOAD_WORKDIR/FILE/$ULDT/$SYS
fi

FILEDIR=$UNLOAD_WORKDIR/FILE/$ULDT/$SYS

#统计源系统卸数表总数
TABCNT=`ls $SQLDIR|grep -c sql`
if [ $TABCNT -eq 0 ]
then
  LOG "没有数据需要卸载" >> $LOGFILE
  _EXIT
fi

TABSUM=0       #统计处理
CURCNT=`ls $TMPDIR|grep -c run`       #当前卸载进程数量
if [ $CURCNT -ne 0 ]
then
  LOG "有$CURCNT个进程正在卸数" >> $LOGFILE
  _EXIT
fi

LOG "***************开始卸数***************" >> $LOGFILE
for i in `ls $SQLDIR/*.sql`
do
  while true
  do
    if [ $CURCNT -lt  $MAXNUM ]    #当前卸载进程总数小于最大并发数时继续处理
    then
      break
    fi

    usleep $SLEEP_SHORT
    CURCNT=`ls $TMPDIR|grep -c run`
  done

  TABNAME=`basename $i .sql`     #数据表名称

  ULFILE=$FILEDIR/$TABNAME.txt

  #替换sql文件中的日期变量
  if [ $DBTYPE -eq 1 ]
  then
    sed "s/#\*P_DATE\*#/$UL_DT/g" $SQLDIR/$TABNAME.sql >$TMPDIR/$TABNAME.run
  elif [ $DBTYPE -eq 2 ]
  then
    echo "connect to $LINKSTR;" >$TMPDIR/$TABNAME.run
    echo "export to $ULFILE of del modified by STRIPLZEROS NOCHARDEL COLDEL$DEL" >>$TMPDIR/$TABNAME.run
    sed "s/#\*P_DATE\*#/$UL_DT/g" $SQLDIR/$TABNAME.sql >>$TMPDIR/$TABNAME.run
    echo ";" >>$TMPDIR/$TABNAME.run
  fi

  #生成卸数脚本
  echo '#! /bin/sh' > $TMPDIR/$TABNAME.sh
  echo 'STARTTM=`date +"%Y%m%d%H%M%S"`' >> $TMPDIR/$TABNAME.sh
  
  if [ $DBTYPE -eq 1 ]
  then
    echo "$BINDIR/sqluldr.bin user=$LINKSTR field=$DEL file=$ULFILE sql=$TMPDIR/$TABNAME.run" >> $TMPDIR/$TABNAME.sh
  elif [ $DBTYPE -eq 2 ]
  then
    echo "db2 -tvf $TMPDIR/$TABNAME.run" >> $TMPDIR/$TABNAME.sh
  fi

  echo 'ENDTM=`date +"%Y%m%d%H%M%S"`' >> $TMPDIR/$TABNAME.sh
  echo "usleep $SLEEP_SHORT" >> $TMPDIR/$TABNAME.sh
  echo "if [ -f $ULFILE ]" >> $TMPDIR/$TABNAME.sh
  echo "then" >> $TMPDIR/$TABNAME.sh
  echo "  FILESIZE=\`stat -c %s $ULFILE\`" >> $TMPDIR/$TABNAME.sh
  echo "  echo \"$TABNAME.txt|\$FILESIZE|\$STARTTM|\$ENDTM|0\" > $ULFILE.ok" >> $TMPDIR/$TABNAME.sh
  echo "  rm -f  $TMPDIR/$TABNAME.run" >> $TMPDIR/$TABNAME.sh
  echo "fi" >> $TMPDIR/$TABNAME.sh

  #执行卸数子程序
  eval "sh $TMPDIR/$TABNAME.sh >> $LOGDIR/$TABNAME.log 2>&1 &"

  TABSUM=`expr $TABSUM + 1`        #处理表数量累加

  LOG "已启动第$TABSUM个表的卸数程序，表名为$TABNAME" >> $LOGFILE

  usleep $SLEEP_SHORT              #启动程序后等待
  CURCNT=`ls $TMPDIR|grep -c run`  #计算当前正在卸数的进程数
done

wait                               #等待所有进程结束

LOG "所有卸数进程结束" >> $LOGFILE
  
if [ $TABSUM -ne $TABCNT ]         #检查卸数总数
then
  LOG "共卸载$TABSUM张表数据，实际需要卸数$TABCNT张表数据" >> $LOGFILE
  _EXIT
fi

#统计源系统成功卸数文件总数
TXTCNT=`ls $FILEDIR/*.txt|grep -c txt`
if [ $TXTCNT -ne $TABSUM ]
then
  LOG "应卸载$TABSUM个数据文件，实际卸载$TXTCNT个数据文件" >> $LOGFILE
  _EXIT
fi

#卸数日期翻牌
ULDT=`date_add $ULDT +1`     #计算下一卸数日期
if [ $? -ne 0 ]
then
  LOG "翻牌日期不合法$ULDT" >> $LOGFILE
  _EXIT
fi

#更新配置文件中的卸数日期
echo $ULDT > $CFGFILE

LOG "###################### 卸数完成 ######################" >> $LOGFILE
echo "" >> $LOGFILE
echo "`date +'%Y%m%d %T'`|###################### OK ######################"

_EXIT                               #正常退出
