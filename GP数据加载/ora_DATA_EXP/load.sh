#!/usr/bin/ksh
# 描述: 根据一个表名生成该表的控制文件
generate_control_file()
{
	if [ ! $# -eq 3 ]
	then
		echo "Usage: $0 {filename} {table}"
		exit
	else
		userid=$1
		filename=$2
		table=$3
	fi

## 执行下载操作
	lv_temp="wk_${table}.test"
	lv_temp1="wk_${table}.test1"
	lv_temp2="wk_${table}.test2"
	lv_control="${table}.ctl"

sqlplus ${userid} <<! >/dev/null
	spool ${lv_temp};
	desc ${table}
	spool off;
	exit
!
if [ "$?" -ne 0 ]
then
	echo "Error:sqlplus ${userid} error in generate control file for table ${table} !"
	echo "please check userid and passwd or oracle_sid."
exit
fi

if [ -f ${lv_temp} ]
then
	cat ${lv_temp}|grep -v "^SQL>" |grep -v " Name " |grep -v " -------" |awk '{print $1}' > ${lv_temp1}
	lv_line_num=`cat ${lv_temp1} | wc -l`
	lv_line_num=`expr ${lv_line_num} - 2`
	lv_index=0

	rm -f ${lv_temp2}
	for lineinfo in `cat ${lv_temp1}`
	do
		if [ ${lv_index} -eq ${lv_line_num} ]
		then
			echo "${lineinfo}" >> ${lv_temp2}
		else
			echo "${lineinfo}," >> ${lv_temp2}
			lv_index=`expr ${lv_index} + 1`
		fi
	done
else
	echo "$0 error :not find ${lv_temp} file."
	exit
fi

lv_str="LOAD DATA INFILE '${filename}' BADFILE 'bad_${table}.bad' APPEND INTO TABLE ${table} FIELDS TERMINATEd BY \"|\""
echo ${lv_str} > ${lv_control}
echo "(" >> ${lv_control}
cat ${lv_temp2} >> ${lv_control}
echo ")" >> ${lv_control}

rm -f ${lv_temp}
rm -f ${lv_temp1}
rm -f ${lv_temp2}

sed -e 's/desc,/"desc",/' ${lv_control} | sed 's/date,/"date",/' > ${lv_temp}
mv ${lv_temp} ${lv_control}

}



################################################################################
#
# 模块: load.sh
#
# 描述: 装载文件数据到指定表
#
# 参数 1 = 文件名
# 参数 2 = 表名
#
#
# 修改记录
# 日期 修改人 修改描述
#
################################################################################

################################################################################
## 主程序入口

if [ ! $# -eq 2 ]
then
	echo "Usage: $0 {filename} {table}"
	exit
fi
userid=WHRCBLOAN/WHRCBLOAN2013
filename=$1
table=$2

if [ ! -f $filename ]
then
	echo "File [$filename] is not exist !"
	exit
fi
## 局部变量定义区域
lv_rows=10000
lv_bindsize=8192000
lv_readsize=8192000

#生成控制文件

generate_control_file ${userid} $filename $table

echo "sqlldr ${userid} control=${table}.ctl rows=${lv_rows} bindsize=${lv_bindsize} readsize=${lv_readsize} log=log_${table}.log bad=bad_${table}.bad direct=false"
sqlldr ${userid} control=${table}.ctl rows=${lv_rows} bindsize=${lv_bindsize} readsize=${lv_readsize} log=log_${table}.log bad=bad_${table}.bad direct=false

if [ $? -ne 0 ]
then
	cat log_${table}.log
	echo "load [$filename] into [$table] error"
	echo "Please see log file log_${table}.log"
else
	echo "load [$filename] into [$table ok"
fi
rm -f ${table}.ctl

################################################################################
