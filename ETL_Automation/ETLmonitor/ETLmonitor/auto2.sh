#!/bin/sh

if [ $# -ne 2 ]; then
echo "Usage: sh auto.sh BGN_YYYYMMDD END_YYYYMMDD"
exit
else
txdate=$1
maxdate=$2
echo "  ###### txdate=$txdate,maxdate=$maxdate ######  "
sleep 10
fi

while [ $txdate -le $maxdate ]
do
cp -f ETLmonitor.cfg.bak ETLmonitor.cfg
sed -i "s/20130903/$txdate/" ETLmonitor.cfg
cat ETLmonitor.cfg
echo "
              #################################               
"
sleep 10
sh ETLmonitor2.sh T $txdate
#sh ETLmonitor2.sh C $txdate
#sh ETLmonitor2.sh M $txdate
#sh ETLmonitor2.sh R $txdate
#sh ETLmonitor2.sh U05 $txdate
#sh ETLmonitor2.sh U06 $txdate

txdate=`date -d "$txdate 1 days" +"%Y%m%d"`
echo "  ###### txdate=$txdate,maxdate=$maxdate ######  "
sleep 10
done

