#! /bin/sh
###############################################################################
# Program  : unload_main.sh
# Argument :
#     ����1: SYSID-ϵͳID
#     ����2: FLG-���򴥷���־ 1-�ȴ�OK�ļ����� 2-ֱ��ִ��
# Created by : WangYi 2013-7-6
# Modified by : WangYi 
#   20130820  : WangYi  .sys.lst�����ļ������ڶ�������DBTYPE,1-ORACLE 2-DB2
#                       208�����Ӵ�.sys.lst�ж�ȡDBTYPE����
#                       280,292����ԭ��������Ӷ�DBTYPE�����ж� DBTYPE -eq 1Ϊ��ʱ��ִ��ԭ�����
# Function : unload data from oracle database
###############################################################################

#�����û���������
. ~/.bash_profile

export UNLOAD_WORKDIR=$HOME/data_unload
#ȫ�ֱ�������
MAXNUM=30                     #��ϵͳж����󲢷���
DEL='0x07'                    #�ļ��ָ��
SLEEP_LONG=5                  #���ȴ�ʱ��/��
SLEEP_SHORT=300000            #�̵ȴ�ʱ��/΢��

PNAME=`basename $0 .sh`       #ȡ��������
SYSDT=`date +"%Y%m%d"`        #ȡϵͳ����

#��־����
LOG ()
{
  LOGDT=`date +"%Y%m%d"`
  LOGTM=`date +"%T"`          #ȡϵͳʱ��
  echo "$LOGTM|$1"
}

_EXIT ()
{
  rm -f $TMPDIR/$PNAME
  exit
}

###########���������#############
#����˵��
#����1:ϵͳID������ID���ΪS01,S02......���ϵͳ����ĵ�
#����2:�������ͣ�1-�ȴ�OK�ļ���2-���ȴ�
####################################
if [ $# -ne 2 ]
then
  echo "���� !!! USAGE: $PNAME.sh SYSID FLG"
  exit
fi

echo "`date +'%Y%m%d %T'`|����������ɹ�"

#��鹤��Ŀ¼��������
if [ -z $UNLOAD_WORKDIR ]
then
  echo "��������UNLOAD_WORKDIRδ����"
  exit
fi

#���ϵͳID�����ļ�
if [ ! -f $UNLOAD_WORKDIR/.sys.lst ]
then
  echo "$UNLOAD_WORKDIR/.sys.lst�ļ�������"
  exit
fi

#������1�Ƿ����б��ļ��д���
SYS=`grep -e "\b$1\b" $UNLOAD_WORKDIR/.sys.lst|awk -F\| '{print $1}'`
if [ -z $SYS ]
then
  echo "��һ������������$UNLOAD_WORKDIR/.sys.lst�ļ��ж���"
  exit
fi

#������2�Ϸ��ԣ���Ϊ1��2
if [ $2 -ne 1 -a $2 -ne 2 ]
then
  echo "�ڶ�������ֻ����1��2��1��ʾ�ȴ�OK�ļ�������2��ʾֱ��ִ��"
  exit
fi

#��ȡж�����ڿ����ļ�,��ȡж������
CFGFILE=$UNLOAD_WORKDIR/cfg/$SYS/$SYS.cfg
if [ ! -s $CFGFILE ]
then
  echo "$CFGFILE�����ڻ��ļ�����Ϊ��"
  exit
fi

#ULDT����ΪҪж�������ڣ�ֱ��ʹ��
ULDT=`cat $CFGFILE|head -n 1|cut -c 1-8`

#������־�ļ�
LOGDIR=$UNLOAD_WORKDIR/log/$SYS
if [ ! -d $LOGDIR ]
then
  mkdir $LOGDIR
fi
LOGFILE=$LOGDIR/$PNAME.$ULDT

#ж�����ڲ��ܴ���ϵͳ����
if [ $ULDT -gt $SYSDT ]
then
  LOG "ж������$ULDT���ܴ���ϵͳ����$SYSDT" >> $LOGFILE
  exit
fi

echo "!!$SYSDT" >> $LOGFILE
LOG "###########��������##############" >> $LOGFILE

#��ֹͬһԴϵͳ���ж��
TMPDIR=$UNLOAD_WORKDIR/tmp/$SYS
if [ ! -d $TMPDIR ]
then
  mkdir $TMPDIR
fi

if [ -f $TMPDIR/$PNAME ]
then
  LOG "$SYS����ж������ȷ�Ϻ���������" >> $LOGFILE
  exit
fi

echo $$ > $TMPDIR/$PNAME
# touch $TMPDIR/$PNAME

###�ڴ˴�֮�󣬳����쳣�˳���һ�ɵ���_EXIT����####
#����binĿ¼
BINDIR=$UNLOAD_WORKDIR/bin
if [ ! -d $BINDIR ]
then
  LOG "$BINDIRĿ¼������" >> $LOGFILE
  _EXIT
fi

#�������ں�����
if [ ! -f $BINDIR/date_add.awk ]
then
  LOG "���ں���$BINDIR/date_add.awk������" >> $LOGFILE
  _EXIT
fi
. $BINDIR/date_add.awk

TMPDT=`date_add $ULDT -1`     #����ж������ǰһ��
if [ $? -ne 0 ]
then
  LOG "ж�����ڲ��Ϸ�$ULDT" >> $LOGFILE
  _EXIT
fi
UL_DT=`echo $ULDT|cut -c 1-4`-`echo $ULDT|cut -c 5-6`-`echo $ULDT|cut -c 7-8`

#���OK�ļ�Ŀ¼
OKDIR=$UNLOAD_WORKDIR/OK/$SYS
if [ ! -d $OKDIR ]
then
  mkdir $OKDIR
fi

LOG "��黷������������Ԥ����ɹ�" >> $LOGFILE

#����2����1ʱ����鲢�ȴ�OK�ļ���λ
if [ $2 -eq 1 ]
then
  while true
  do
    if [ -s $OKDIR/OK ]        #OK�ļ������Ҳ�Ϊ�գ����˳�ѭ��
    then
      break;
    fi
    
    sleep $SLEEP_LONG          #����ȴ�
  done

  while true
  do
    OKDT=`cat $OKDIR/OK|head -n 1|cut -c 1-8`   #��ȡOK�ļ�����
    if [ $OKDT -eq $ULDT ]     #OK�ļ����ڵ���ж������,���˳�ѭ��
    then
      break
    fi

    if [ $TMPDT -eq $OKDT ]
    then
      sleep $SLEEP_LONG
      continue
    fi

    LOG "ж��������$ULDT,OK�ļ�����ֻ����$ULDT��$TMPDT" >> $LOGFILE
    _EXIT
    
  done
fi

LOG "ж�����򴥷������Ѿ߱�" >> $LOGFILE


# ����ORACLEж�������Լ��ַ�������
export LANG='en_US.utf8'
export NLS_LANG='American_america.AL32UTF8'
export NLS_DATE_FORMAT='"YYYY-MM-DD HH24:MI:SS"'

#��ȡ���ݿ����Ӵ�
LINKSTR=`grep $SYS $UNLOAD_WORKDIR/.sys.lst|awk -F\| '{print $2}'`

#��ȡ���ݱ�������
BAKDAYS=`grep $SYS $UNLOAD_WORKDIR/.sys.lst|awk -F\| '{print $3}'`

#��ȡж��ϵͳ���ݿ����� 1-ORACLE 2-DB2
DBTYPE=`grep $SYS $UNLOAD_WORKDIR/.sys.lst|awk -F\| '{print $4}'`

#ɾ����������ǰһ�������Ŀ¼,20130708
RMDT=`date_add $ULDT -$BAKDAYS`     #����Ҫɾ����ж������
if [ -d $UNLOAD_WORKDIR/FILE/$RMDT/$SYS ]
then
  rm -rf $UNLOAD_WORKDIR/FILE/$RMDT/$SYS
fi

#���SQLĿ¼
SQLDIR=$UNLOAD_WORKDIR/sql/$SYS
if [ ! -d $SQLDIR ]
then
  LOG "$SQLDIRĿ¼������" >> $LOGFILE
  _EXIT
fi

#���ж��Ŀ¼����������ж��Ŀ¼
if [ ! -d $UNLOAD_WORKDIR/FILE ]
then
  LOG "$UNLOAD_WORKDIR/FILEĿ¼������" >> $LOGFILE
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

#ͳ��Դϵͳж��������
TABCNT=`ls $SQLDIR|grep -c sql`
if [ $TABCNT -eq 0 ]
then
  LOG "û��������Ҫж��" >> $LOGFILE
  _EXIT
fi

TABSUM=0       #ͳ�ƴ���
CURCNT=`ls $TMPDIR|grep -c run`       #��ǰж�ؽ�������
if [ $CURCNT -ne 0 ]
then
  LOG "��$CURCNT����������ж��" >> $LOGFILE
  _EXIT
fi

LOG "***************��ʼж��***************" >> $LOGFILE
for i in `ls $SQLDIR/*.sql`
do
  while true
  do
    if [ $CURCNT -lt  $MAXNUM ]    #��ǰж�ؽ�������С����󲢷���ʱ��������
    then
      break
    fi

    usleep $SLEEP_SHORT
    CURCNT=`ls $TMPDIR|grep -c run`
  done

  TABNAME=`basename $i .sql`     #���ݱ�����

  ULFILE=$FILEDIR/$TABNAME.txt

  #�滻sql�ļ��е����ڱ���
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

  #����ж���ű�
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

  #ִ��ж���ӳ���
  eval "sh $TMPDIR/$TABNAME.sh >> $LOGDIR/$TABNAME.log 2>&1 &"

  TABSUM=`expr $TABSUM + 1`        #����������ۼ�

  LOG "��������$TABSUM�����ж�����򣬱���Ϊ$TABNAME" >> $LOGFILE

  usleep $SLEEP_SHORT              #���������ȴ�
  CURCNT=`ls $TMPDIR|grep -c run`  #���㵱ǰ����ж���Ľ�����
done

wait                               #�ȴ����н��̽���

LOG "����ж�����̽���" >> $LOGFILE
  
if [ $TABSUM -ne $TABCNT ]         #���ж������
then
  LOG "��ж��$TABSUM�ű����ݣ�ʵ����Ҫж��$TABCNT�ű�����" >> $LOGFILE
  _EXIT
fi

#ͳ��Դϵͳ�ɹ�ж���ļ�����
TXTCNT=`ls $FILEDIR/*.txt|grep -c txt`
if [ $TXTCNT -ne $TABSUM ]
then
  LOG "Ӧж��$TABSUM�������ļ���ʵ��ж��$TXTCNT�������ļ�" >> $LOGFILE
  _EXIT
fi

#ж�����ڷ���
ULDT=`date_add $ULDT +1`     #������һж������
if [ $? -ne 0 ]
then
  LOG "�������ڲ��Ϸ�$ULDT" >> $LOGFILE
  _EXIT
fi

#���������ļ��е�ж������
echo $ULDT > $CFGFILE

LOG "###################### ж����� ######################" >> $LOGFILE
echo "" >> $LOGFILE
echo "`date +'%Y%m%d %T'`|###################### OK ######################"

_EXIT                               #�����˳�
