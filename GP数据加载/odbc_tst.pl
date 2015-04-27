#!/usr/bin/perl
use strict;
use DBI;
use DBD::ODBC;
#建立数据库连接
print "visit----\n";
my $dbh = DBI->connect("dbi:ODBC:Greenplum","gpadmin","gpadmin",{AutoCommit=>1,PrintError=>0,RaiseError=>0})|| die "Error : $DBI::errstr\n";
#建立会话
my $sth = $dbh->prepare("select * from petl.etl_job") || die "Error : $DBI::errstr\n";
#执行SQL语句
$sth->execute() || die "Error : $DBI::errstr\n";
my @row;
#$sth->fetchrow() : 返回结果
while(@row = $sth->fetchrow())
{
     print "@row\n";
}
$sth->finish;
#关闭数据库连接
$dbh->disconnect();
