#!/usr/bin/sh

if [ $# -lt 3 ]
then
   echo "Error - Incorrect argument";
   echo "Usage: genbtqm.sh <sys> <db> <table>";
   exit 1;
fi

perl ${AUTO_HOME}/bin/scriptwz.pl -ds ${AUTO_DSN} -logon etl,etl -sys $1 -db $2 -table $3 -job bteqm -home ${AUTO_HOME} -otype shell



