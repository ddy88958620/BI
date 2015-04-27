#!/bin/sh
pid=`cat ../lock/etlrcv.lock`
kill -9 $pid
pid=`cat ../lock/etlagent.lock`
kill -9 $pid
pid=`cat ../lock/etlclean.lock`
kill -9 $pid
pid=`cat ../lock/etlmaster.lock`
kill -9 $pid
pid=`cat ../lock/etlmsg.lock`
kill -9 $pid
pid=`cat ../lock/etlwdog.lock`
kill -9 $pid
pid=`cat ../lock/etlschedule.lock`
kill -9 $pid
rm ../lock/*
