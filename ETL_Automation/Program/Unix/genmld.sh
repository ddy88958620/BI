#!/usr/bin/sh

if [ $# -lt 3 ]
then
   echo "Error - Incorrect argument";
   echo "Usage: genmld.sh <sys> <db> <table>";
   echo "For example, genmld.sh TST dp_mcif CUST_MASTER";
   exit 1;
fi

perl ${AUTO_HOME}/bin/scriptwz.pl -ds ${AUTO_DSN} -logon etl,etl -sys $1 -db $2 -table $3 -job mload -home ${AUTO_HOME} -otype shell -dformat unformatted



