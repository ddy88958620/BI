#!/bin/sh
nohup perl ${AUTO_HOME}/bin/etlagent.pl >> ${AUTO_HOME}/LOG/etlagent.out 2>&1 &
nohup perl ${AUTO_HOME}/bin/etlclean.pl >> ${AUTO_HOME}/LOG/etlclean.out 2>&1 &
nohup perl ${AUTO_HOME}/bin/etlmaster.pl >> ${AUTO_HOME}/LOG/etlmaster.out 2>&1 &
nohup perl ${AUTO_HOME}/bin/etlmsg.pl >> ${AUTO_HOME}/LOG/etlmsg.out 2>&1 &
nohup perl ${AUTO_HOME}/bin/etlrcv.pl >> ${AUTO_HOME}/LOG/etlrcv.out 2>&1 &
nohup perl ${AUTO_HOME}/bin/etlwdog.pl >> ${AUTO_HOME}/LOG/etlwdog.out 2>&1 &
nohup perl ${AUTO_HOME}/bin/etlschedule.pl >> ${AUTO_HOME}/LOG/etlschedule.out 2>&1 &
