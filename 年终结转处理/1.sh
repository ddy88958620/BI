#!/bin/sh
source /EDW/ETL/INSTANCE/ETL1/profile/ETL1.profile 
export GPIP=$AUTO_GREENPLUM_IP
export GPDB=$AUTO_GREENPLUM_DB
export FDIR=`pwd`
read -n 1 -p '准备执行年终结转_1数据加工,请按回车键继续.....'

psql -a -h $GPIP -d $GPDB -U etluser -f $FDIR/just_do_it_1.SQL 

read -n 1 -p '年终结转_1数据加工结束,请按回车键继续.....'
