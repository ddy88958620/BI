#!/usr/bin/perl
use strict;
use DBI;
use DBD::ODBC;
#�������ݿ�����
print "visit----\n";
my $dbh = DBI->connect("dbi:ODBC:Greenplum","gpadmin","gpadmin",{AutoCommit=>1,PrintError=>0,RaiseError=>0})|| die "Error : $DBI::errstr\n";
#�����Ự
my $sth = $dbh->prepare("select * from petl.etl_job") || die "Error : $DBI::errstr\n";
#ִ��SQL���
$sth->execute() || die "Error : $DBI::errstr\n";
my @row;
#$sth->fetchrow() : ���ؽ��
while(@row = $sth->fetchrow())
{
     print "@row\n";
}
$sth->finish;
#�ر����ݿ�����
$dbh->disconnect();
