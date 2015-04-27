#!/bin/sh

############   Basic common functions    ##############
#1)check the file/dir whether exists.
checkFile()
{
  FileNamePath="$1"
  Type="$2"
  logName="$3"
  
  if [ "$Type" = "1" ]
  then 
     if [ ! -d "$FileNamePath" ]
     then
        echo "The "$FileNamePath" does not exist.">>"$logName" 
        flag=1
     fi
  elif [ "$Type" = "2" ]
  then
     if [ ! -f "$FileNamePath" ]
     then 
        echo "The "$FileNamePath" does not exist.">>"$logName" 
        flag=1
     fi 
  fi
  return
}

#2)check connect whether alive 
checkConnection()
{
Connstr="$1"
sqlplus -S "$Connstr" <<EOF
SET SERVEROUTPUT ON 100000;
set FEEDBACK OFF;
set HEADING ON;
set ECHO OFF;
set TERM OFF;
set LINESIZE 255;
DECLARE
	  m_time DATE;
BEGIN
    select sysdate into m_time from dual;
    DBMS_OUTPUT.PUT_LINE('OK');
EXCEPTION 
WHEN OTHERS THEN
	  DBMS_OUTPUT.PUT_LINE(SQLERRM(SQLCODE));
END;
/
quit
EOF
return
}

#3)check the parameter whether empty
checkEmpty()
{
   parameter="$1"
   parameterName="$2"
   logName="$3"
   if [ -z "$parameter" ]
   then
       echo "The parameter $parameterName is empty.">>"$logName" 
       isOk=1
   fi
   return
}

#4)check the parameter whether is digit
checkValid()
{
    parameter="$1"
    parameterName="$2"
    logName="$3"
    if [ -z `echo "$parameter" |grep "^[0-9]*$"` ]
    then
         echo "$parameterName is invalid,please check it.">>"$logName" 
         isOk=1
    else
       if [ "$parameter" = "0" ]
       then
           echo "$parameterName value must great than zero.">>"$logName"
           isOk=1
       fi
    fi
    return
}

#5)spool the data,you can chang the SQL
spooldata()
{
Connstr="$1"
sqlplus -S "$Connstr" <<EOF>/dev/null
SET FEEDBACK OFF;
SET HEADING ON;
SET VERIFY OFF;
SET ECHO OFF;
SET TERM OFF;
SET LINESIZE 255;
SET PAGESIZE 200;
COL  EDW_STAGE  FORMAT a10;
COL  TXDATE  FORMAT a10;
COL  JOBSTATUS  FORMAT a10;
COL  CNT FORMAT 9999;

COL  ETLDATE FORMAT a15;
COL  SYS_TYPE FORMAT a10;
COL  STATUS FORMAT 99;

SPOOL  $logPath/spldata;

select t.*
       ,case substr(t.EDW_STAGE,2,1) 
       when 'S' then 1 when 'V' then 2 when 'T' then 3 when 'C' then 4 when 'M' then 5 when 'R' then 6 when 'U' then 7 
       end seqno
from (
select '#'||substr(etl_job,1,1)||'#' EDW_STAGE,to_char(LAST_TXDATE,'YYYYMMDD') TXDATE,LAST_JOBSTATUS JOBSTATUS,count(*) CNT from etl_job where etl_job not like 'U%' and etl_job in (select ETL_JOB from etl_job_ini) group by substr(etl_job,1,1),to_char(LAST_TXDATE,'YYYYMMDD'),LAST_JOBSTATUS
union all
select '#'||substr(etl_job,1,3)||'#' EDW_STAGE,to_char(LAST_TXDATE,'YYYYMMDD'),LAST_JOBSTATUS,count(*) from etl_job where etl_job like 'U%' and etl_job in (select ETL_JOB from etl_job_ini) group by substr(etl_job,1,3),to_char(LAST_TXDATE,'YYYYMMDD'),LAST_JOBSTATUS
) t order by 5,1,2,3
;

SPOOL OFF;
quit
EOF
}

#6)get the ETL txdate
################################################################## 计算当前业务日期和下一天业务日期
# 参数：TNS连接串 仓库区
get_txdate()
{
Connstr="$1"
Stage="$2"

sqlplus -S "$Connstr" <<EOF>/dev/null
SET FEEDBACK OFF;
SET HEADING ON;
SET VERIFY OFF;
SET ECHO OFF;
SET TERM OFF;
SET LINESIZE 255;
SET PAGESIZE 200;
COL  EDW_STAGE  FORMAT a10;
COL  TXDATE  FORMAT a10;
COL  JOBSTATUS  FORMAT a10;
COL  CNT FORMAT 9999;
COL  SEQNO FORMAT 9;

SPOOL $logPath/.ifnext

select t.*
       ,case substr(t.EDW_STAGE,2,1) 
       when 'S' then 1 when 'V' then 2 when 'T' then 3 when 'C' then 4 when 'M' then 5 when 'R' then 6 when 'U' then 7 
       end seqno
from (
select '#'||substr(etl_job,1,1) EDW_STAGE,to_char(LAST_TXDATE,'yyyymmdd') TXDATE,LAST_JOBSTATUS JOBSTATUS,count(*) CNT from etl_job 
where etl_job not like 'U%' and etl_job like '${Stage}%'
      and LAST_JOBSTATUS='Done'
       and etl_job in (select ETL_JOB from etl_job_ini)
      and LAST_TXDATE=(select max(LAST_TXDATE) from etl_job where etl_job like '${Stage}%' and etl_job in (select ETL_JOB from etl_job_ini))
group by LAST_TXDATE,LAST_JOBSTATUS,'#'||substr(etl_job,1,1)
union all
select '#'||substr(etl_job,1,3) EDW_STAGE,to_char(LAST_TXDATE,'yyyymmdd') TXDATE,LAST_JOBSTATUS JOBSTATUS,count(*) CNT from etl_job
where etl_job like 'U%' and etl_job like '${Stage}%'
      and LAST_JOBSTATUS='Done'
       and etl_job in (select ETL_JOB from etl_job_ini)
      and LAST_TXDATE=(select max(LAST_TXDATE) from etl_job
        where etl_job like '${Stage}%' and etl_job in (select ETL_JOB from etl_job_ini))
group by LAST_TXDATE,LAST_JOBSTATUS,'#'||substr(etl_job,1,3)
) t
order by 5,1,2
;
SPOOL OFF;
quit
EOF
# consider here, #S or #T , (normal trigger etl_job) or (force trigger etl_job)
cur_txdate=`grep "#${Stage}" $logPath/.ifnext | grep "Done" | awk -F " " '{ print $2 }' | sort -r | head -n 1`
nxt_txdate=`date -d "$cur_txdate 1 days" +"%Y%m%d"`
}

#7)invoker etl_job task for (T,C,M,U,I ...)
################################################################## ETL任务调度
# 参数：TNS连接串 仓库区 当前业务日期 e.g.: "$DBTNS" "T" $nxt_txdate
etl_job_trigger()
{
Connstr="$1"
Stage="$2"
Nxt_txdate="$3"

sqlplus -S "$Connstr" <<EOF>/dev/null
SET FEEDBACK OFF;
SET HEADING OFF;
SET VERIFY OFF;
SET ECHO OFF;
SET TERM OFF;
SET LINESIZE 400;
COL CMDSTR FORMAT a400;

SPOOL $logPath/.jobtrigger
select 'touch /EDW/ETL/INSTANCE/ETL1/DATA/receive/dir.'||lower(t.etl_job)||to_char(last_txdate+1,'yyyymmdd') cmdstr
from etl_job t
where t.etl_job like '${Stage}%' 
			 and etl_job in (select ETL_JOB from etl_job_ini)
      and t.LAST_JOBSTATUS in ('Done','Failed') and t.last_txdate=to_date('$Nxt_txdate','yyyymmdd')-1;
SPOOL OFF
quit
EOF

sh $logPath/.jobtrigger
sleep 3
}

#8)Failed etl_job Output (T,C,M,U,I ...)
################################################################## ETL失败任务输出
# 参数：TNS连接串 仓库区/层 当前业务日期 e.g.: "$DBTNS" "T" $cur_txdate
failed_job_output()
{
Connstr="$1"
Stage="$2"
Cur_txdate="$3"

sqlplus -S "$Connstr" <<EOF>/dev/null
SET FEEDBACK OFF;
SET HEADING ON;
SET VERIFY OFF;
SET ECHO OFF;
SET TERM OFF;
SET LINESIZE 400;
COL CURDATE FORMAT a20;
COL ETL_JOB FORMAT a50;
COL LAST_TXDATE FORMAT a10;
COL LAST_JOBSTATUS FORMAT a10;
COL commcurdate FORMAT a100;


--已经跑完的仓库区任务状态不更新 not in ('S','V','T','C','M','R','I','U')
SPOOL $logPath/.jobfailure
select to_char(sysdate,'yyyymmdd hh24:mi:ss') curdate,t.etl_job,to_char(t.last_txdate,'yyyymmdd'),t.last_jobstatus
from etl_job t
where LAST_JOBSTATUS='Failed' and etl_job like '${Stage}%'
	 and etl_job in (select ETL_JOB from etl_job_ini);
SET HEADING OFF;
select '######Stage=$Stage#####Cur_txdate=$Cur_txdate######## '||to_char(sysdate,'yyyymmdd hh24:mi:ss') commcurdate from dual;
SPOOL OFF

--对于跑批失败Failed的任务设置为Done
update etl_job set LAST_JOBSTATUS='Done' 
where LAST_JOBSTATUS='Failed' and etl_job like '${Stage}%' and last_txdate=to_date('$Cur_txdate','yyyymmdd')
	 and etl_job in (select ETL_JOB from etl_job_ini);
commit;
quit
EOF

# tmpsize=402
tmpsize=`stat -c %s $logPath/.jobfailure`
if [ $tmpsize -gt 402 ]; then
  cat $logPath/.jobfailure >> $logName
  echo "Please use [ more $logName ] to see the Failed ETL_JOB ..."
  sleep 2
fi
# echo tmpsize=$tmpsize >> $logName
# cat $logPath/.jobfailure >> $logName
rm -rf $logPath/.jobfailure
sleep 1
}
      
####################  Main logic ######################
#main 1) check the input parameter
if [ $# -ne 2 ]
then
   echo "Usage: sh ETLmonitor.sh T|C|M|R|U05|U06 MAX_DATE_YYYYMMDD"
   select Edw_stg in T C M R U05 U06;
   do
   break;
   done
   if [ -z $Edw_stg ]; then
   echo "Please rerun this program,and select the [EDW_STAGE NUMBER] for running etl_job."
   exit
   fi
else
   Edw_stg=$1
   MAX_DATE=$2
fi

echo Edw_stg=$Edw_stg,MAX_DATE=$MAX_DATE

#main 2) get the run path and log Name
runPath=`dirname $0`
cd "$runPath"
# cd ..
parentPath=`pwd`
binpath="$parentPath"
logPath="$parentPath/log"
confPath="$parentPath"
mkdir -p "$logPath"
# chmod -R 777 "$logPath"
isOk=0
flag=0
icount=0
logName="$logPath/`date +%Y%m%d_%H%M`_error.log"
rm -fr "$logName"
echo " ">"$logName"

#main 3) check the file or dir
echo " "
echo "Start to check the files  ETLmonitor.cfg."
checkFile "$confPath/ETLmonitor.cfg" 2 "$logName"
echo " "
echo "End to check the files ETLmonitor.cfg."

##########
source /home/etl/.bash_profile
etl1

rm -fr "$logPath/output.txt"
rm -fr "$logPath/spldata.lst"
rm -fr "$logPath/.ifnext"
rm -fr "$logPath/.jobtrigger"
##########

#main 4) log ETLmonitor.cfg
if [ $flag -eq 0 ]
then
#  echo " "
#  echo "Start to load the files ETLmonitor.cfg."
  . $confPath/ETLmonitor.cfg
#  echo " "
#  echo "End to load the files and ETLmonitor.cfg."
fi

if [ $Edw_stg = "T" ]; then
dcnt=$cdt
elif [ $Edw_stg = "C" ]; then
dcnt=$cdc
elif [ $Edw_stg = "M" ]; then
dcnt=$cdm
elif [ $Edw_stg = "R" ]; then
dcnt=$cdr
elif [ $Edw_stg = "U05" ]; then
dcnt=$cdu05
elif [ $Edw_stg = "U06" ]; then
dcnt=$cdu06
fi

echo Edw_stg=$Edw_stg,dcnt=$dcnt

#main 5) check the input parameter whether empty
if [ $flag -eq 0 ]
then
#    echo " "
#    echo "Start to check the parameters."
    # checkEmpty "$Mail_Box" "Mail_Box" "$logName"
    checkEmpty "$DBTNS" "DBTNS" "$logName"
    checkEmpty "$AttemptNumber" "AttemptNumber" "$logName"
    checkEmpty "$WaitTime" "WaitTime" "$logName"
#    echo " "
#    echo "End to check the parameters."
fi

while true
do
if [ $isOk -eq 0 ] && [ $flag -eq 0 ]
then
    #main 6) check the parameter valid
#    echo " "
#    echo "Start to check the parameters whether is Valid in "ETLmonitor.cfg"."
    checkValid "$AttemptNumber" "AttemptNumber" "$logName"
    checkValid "$WaitTime" "WaitTime" "$logName"
#    echo " "
#    echo "End to check the parameters whether is Valid in "ETLmonitor.cfg"."
    
    #main 7)check sqlplus whether alive and try the time from the config file
#    echo " "
#    echo "Start to check the DB Tns string whether is Valid."
    icount=$AttemptNumber
    ckRs=`checkConnection "$DBTNS"|grep "OK"`
    if [ -z "$ckRs" ]
    then
       while [ $icount -gt 0 ]
       do 
            sleepTime=`expr $WaitTime \* 60`
            sleep  $sleepTime
            icount=`expr $icount - 1`
            ckRs=`checkConnection "$DBTNS"|grep "OK"`
            if [ -z "$ckRs" ]
            then 
               continue
            else 
               break
            fi
       done   
       if [ $icount -eq 0 ]
       then
          oraMsg=`echo "$ckRs"|grep "ORA-"`
          echo "$oraMsg">>"$logName"
          isOk=1
       fi
   fi
   usedTime=`expr $AttemptNumber - $icount`
   usedTime=`expr $usedTime \*  $WaitTime`
#   echo " "
#   echo "End to check the DB Tns string whether is Valid."
fi

#main 8)get the monitor result
if [ $isOk -eq 0 ] && [ $flag -eq 0 ]
then 
#      echo " "
#      echo "Start to get the data from DB."
     `spooldata "$DBTNS"`
#      echo " "
#      echo "End to get the data from DB."
     logCount=`wc -l "$logPath/spldata.lst"|awk ' { print $1 }'`
     if [ "$logCount" != "0"  ]
     then
#         echo " "
#         echo " ">>"$logPath/output.txt"
         `cat $logPath/spldata.lst>>"$logPath/output.txt"`
#         echo " ">>"$logPath/output.txt"
         clear
         echo "   |S-临时|V-虚拟|T-基础|C-汇总|M-集市|I-补入|U-卸数|   "
         cat $logPath/output.txt
         ################################################################## 计算当前业务日期和下一天业务日期
         # 参数：TNS连接串 仓库区/层
         # 'S','V','T','C','M','R','I','U'
         get_txdate "$DBTNS" "$Edw_stg"
         echo "###########   cur_txdate=$cur_txdate,nxt_txdate=$nxt_txdate    ###########"
         cat $logPath/.ifnext
         if [ ${#cur_txdate} -ne 8 ]; then
         echo "   ETL_JOB is running for any Done status    "
         echo "###########   Edw_stg=$Edw_stg,dcnt=$dcnt,maxdate=$maxdate    ###########"
         sleep 5
         continue
         fi
         if [ $cur_txdate -gt $MAX_DATE ]; then
           break;
         fi
         dtmp=`grep "^#${Edw_stg}" $logPath/.ifnext | awk '{ print $4 }'`
         echo "###########   Edw_stg=$Edw_stg,dtmp=$dtmp,dcnt=$dcnt,maxdate=$maxdate    ###########"
         if [ $cur_txdate -gt $maxdate ]; then
         echo "   cur_txdate($cur_txdate) > maxdate($maxdate),ETL_JOB Finish!    "
         break;
         fi
         ################################################################## ETL失败任务输出
         # 参数：TNS连接串 仓库区/层 当前业务日期 e.g.: "$DBTNS" "T" $cur_txdate
         failed_job_output "$DBTNS" "$Edw_stg" $cur_txdate
         sleep 3
         if [ $dtmp -eq $dcnt -a $nxt_txdate -le $maxdate ]; then
           echo etl_job_trigger "$DBTNS" "$Edw_stg" $nxt_txdate
         ################################################################## ETL任务调度,当完成预期的任务数时，触发下一天任务
         # 参数：TNS连接串 仓库区/层 要触发的业务日期 e.g.: "$DBTNS" "T" $nxt_txdate
           etl_job_trigger "$DBTNS" "$Edw_stg" $nxt_txdate
           echo "Waiting 20s ... " && sleep 20
         fi
         if [ $dtmp -eq $dcnt -a $cur_txdate -eq $maxdate ]; then
           echo "跑批完成退出：Edw_stg=$Edw_stg,dtmp=$dtmp,dcnt=$dcnt,cur_txdate=$cur_txdate,maxdate=$maxdate"
           break;
         fi
     fi
else
     if [ $flag -eq 0 ]
     then
#         echo " "
         echo " ">>"$logPath/output.txt"
         echo "Has Oracle error occured,the error messages are listed below:">>"$logPath/output.txt"
         `cat $logName>>"$logPath/output.txt"`
         clear && cat $logPath/output.txt
     fi
fi
rm -fr "$logPath/output.txt"
rm -fr "$logPath/spldata.lst"
rm -fr "$logPath/.ifnext"
rm -fr "$logPath/.jobtrigger"

sleep 3
done
echo " "
echo "Finished."
echo " "
if [ $flag -gt 0 ] || [ $isOk -gt 0 ]
then 
   echo "Please use [ more $logName ] to see the error log..."
fi
echo " "
