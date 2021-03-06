#!/usr/bin/sh

if [ $# -lt 3 ]
then
   echo "Error - Incorrect argument";
   echo "Usage: genfld.sh <sys> <db> <table>";
   echo "For example, genfld.sh TST dp_mcif CUST_MASTER";
   exit 1;
fi

perl ${AUTO_HOME}/bin/scriptwz.pl -ds ${AUTO_DSN} -logon etl,etl -sys $1 -db $2 -table $3 -job fload -home ${AUTO_HOME} -otype shell -dformat unformatted



