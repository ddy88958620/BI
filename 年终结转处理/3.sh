#!/bin/sh
source /EDW/ETL/INSTANCE/ETL1/profile/ETL1.profile 
export GPIP=$AUTO_GREENPLUM_IP
export GPDB=$AUTO_GREENPLUM_DB
export FDIR=`pwd`
read -n 1 -p '׼��ִ�����ս�ת_3���ݼӹ�,�밴�س�������.....'

psql -a -h $GPIP -d $GPDB -U etluser -f $FDIR/just_do_it_3.SQL 

read -n 1 -p '���ս�ת_3���ݼӹ�����,�밴�س�������.....'
