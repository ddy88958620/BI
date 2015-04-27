#!/usr/bin/perl
use strict;
use DBI;
use DBD::ODBC;
#建立数据库连接
print "visit----\n";
my $dbh = DBI->connect("dbi:ODBC:Greenplum","gpadmin","gpadmin",{AutoCommit=>1,PrintError=>0,RaiseError=>0})|| die "Error : $DBI::errstr\n";
#建立会话

sub testsql{
my $sth = $dbh->prepare("SELECT Job_Priority FROM ETL_Job a,etl_job_source b WHERE a.ETL_System = b.ETL_System AND a.ETL_Job = b.ETL_Job and a.ETL_System = 'TST' AND b.Conv_File_Head ='TST_JOB1';") || die "Error : $DBI::errstr\n";
#my $sth = $dbh->prepare("select 12345678;");
#执行SQL语句
$sth->execute() || die "Error : $DBI::errstr\n";
my @row;
#$sth->fetchrow() : 返回结果
while(@row = $sth->fetchrow())
{
     print "@row\n";
}
$sth->finish;
}
#关闭数据库连接



testsql();

testsql();
$dbh->disconnect();
