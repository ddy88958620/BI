#!/usr/bin/perl
###############################################################################
# Program  : unload_from_ora.pl
# Argument : none
# Modified by Kenny Wang 2013-5-11
#   unload from oracle database vir sqlfile(tablename#select * from tablename)
###############################################################################
my $maxJobCount = 20; #����10����������ͬʱ����
my $nowJobCount = `ps -ef | grep -v grep | grep ociuldr.bin | wc -l`;	#��ǰ������
my $binfile = "/EDW/ETL/unload_from_source/ociuldr/ociuldr.bin"; #/data/app/ociuldr/ociuldr.bin
my $uidstr01 = 'sjck/sjck03@21.1.1.45:1521/wsbank';
my $uidstr03 = 'sjck/sjck03@21.1.1.45:1521/utandb';
my $uidstr09 = 'customer/customer@21.1.1.45:1521/wsbank';
my $uidstr15 = 'sjck/sjck03@21.1.1.45:1521/cspdb';
my $uidstr02 = 'whrcbloan/whrcbloan@31.2.1.13:1521/sample';
my $uidstr04 = $uidstr02;
my $uidstr05 = $uidstr02;
my $uidstr06 = $uidstr02;
my $uidstr08 = $uidstr02;
my $uidstr10 = $uidstr02;
my $uidstr;
my $sqlfile = "/EDW/ETL/unload_from_source/ociuldr/src_exp_all.sql"; #/data/app/ociuldr/iccard.sql
my $datadir01 = "/EDW/ETL/unload_from_source/ociuldr/S01/"; #/data/app/ociuldr/data/ #�������"/"
my $datadir02 = "/EDW/ETL/unload_from_source/ociuldr/S02/";
my $datadir03 = "/EDW/ETL/unload_from_source/ociuldr/S03/";
my $datadir04 = "/EDW/ETL/unload_from_source/ociuldr/S04/";
my $datadir05 = "/EDW/ETL/unload_from_source/ociuldr/S05/";
my $datadir06 = "/EDW/ETL/unload_from_source/ociuldr/S06/";
my $datadir08 = "/EDW/ETL/unload_from_source/ociuldr/S08/";
my $datadir09 = "/EDW/ETL/unload_from_source/ociuldr/S09/";
my $datadir10 = "/EDW/ETL/unload_from_source/ociuldr/S10/";
my $datadir15 = "/EDW/ETL/unload_from_source/ociuldr/S15/";
my $datadir;
#�����ı����ݵ��ֶηָ��� 0X07
##################�������� : LANG	NLS_LANG	NLS_DATE_FORMAT
my $LANG = "";
my $NLS_LANG = "";
my $NLS_DATE_FORMAT = "";

$LANG = $ENV{"LANG"};
unless ( $LANG =~ /en_US.UTF-8/i ) {
   print STDERR "����ȡ�� LANG ����, ������ֹ���˳�!\n";
   print "�����ڲ���ϵͳ����������: export LANG=en_US.utf8 \n";
   exit(1);
}

$NLS_LANG = $ENV{"NLS_LANG"};
unless ( $NLS_LANG =~ /American_america.AL32UTF8/i ) {
   print STDERR "����ȡ�� NLS_LANG ����, ������ֹ���˳�!\n";
   print "�����ڲ���ϵͳ����������: export NLS_LANG=American_america.AL32UTF8 \n";
   exit(1);
}

$NLS_DATE_FORMAT = $ENV{"NLS_DATE_FORMAT"};
unless ( $NLS_DATE_FORMAT =~ /YYYY-MM-DD HH24:MI:SS/i ) {
   print STDERR "����ȡ�� NLS_DATE_FORMAT ����, ������ֹ���˳�!\n";
   print "�����ڲ���ϵͳ����������: export NLS_DATE_FORMAT=\"YYYY-MM-DD HH24:MI:SS\" \n";
   exit(1);
}

##################���ں�ʱ��
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   $year  += 1900;
   $mon    = sprintf("%02d", $mon + 1);
   $mday   = sprintf("%02d", $mday);
   $TODAY  = "${year}${mon}${mday}";
   $nowtime = "${hour}:${min}:${sec}";
print "TODAY = ",$TODAY,"\tTIME = ",$nowtime,"\n";
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time() - 60*60*24);
   $year  += 1900;
   $mon    = sprintf("%02d", $mon + 1);
   $mday   = sprintf("%02d", $mday);
   $YESTODAY  = "${year}${mon}${mday}";
print "YESTODAY = ",$YESTODAY,"\n";
##################
#while(1 eq 1) 
{
my @proc=`ps -ef | grep -v grep | grep ociuldr.bin | wc -l`;
print @proc,"\n";
print @proc."\n";
}

  # open file to the handle <FILE>
  open(FILE, $sqlfile) || die "�ļ� $sqlfile ��ʧ��, ������ֹ.\n";
  # read the first line, and chomp off the newline
  chomp(my $firstline = <FILE>);
  print $firstline;
  # read other into array
  my @other = <FILE>;
  #print @other;
  close FILE; 
##################
# open file to the handle <FILE>
open(FILE, $sqlfile) || die "�ļ� $sqlfile ��ʧ��, ������ֹ.\n";
# read other into array
my @lines = <FILE>;
# close file
close FILE;

#print @lines;

my @fields;
my $schema;
my $tablename;
my $sqlstr;

for ($n = 0; $n <= $#lines ; $n++) {
  chomp($lines[$n]);
  @fields = split(/#/, $lines[$n]);
  ($schema,$tablename,$sqlstr) = ($fields[0],$fields[1],$fields[2]);
  #print $fields[0],"\t",$fields[1],"\t",$fields[2],"\n";
  # choose the source database's connection string vir $fields[0](that is syscode:S01,S02,...)
  if ($schema eq "S15") {
  	print '$schema='.$schema."\n";
  	$uidstr = $uidstr15; $datadir = $datadir15;
  	print '$uidstr='.$uidstr."\n".'$datadir='.$datadir."\n";
  	}
  elsif ($schema eq "S01") {
  	print '$schema='.$schema."\n";
  	$uidstr = $uidstr01; $datadir = $datadir01;
  	print '$uidstr='.$uidstr."\n".'$datadir='.$datadir."\n";
  	}
  elsif ($schema eq "S02") {
  	print '$schema='.$schema."\n";
  	$uidstr = $uidstr02; $datadir = $datadir02;
  	print '$uidstr='.$uidstr."\n".'$datadir='.$datadir."\n";
  	}
  elsif ($schema eq "S03") {
  	print '$schema='.$schema."\n";
  	$uidstr = $uidstr03; $datadir = $datadir03;
  	print '$uidstr='.$uidstr."\n".'$datadir='.$datadir."\n";
  	}
  elsif ($schema eq "S04") {
  	print '$schema='.$schema."\n";
  	$uidstr = $uidstr04; $datadir = $datadir04;
  	print '$uidstr='.$uidstr."\n".'$datadir='.$datadir."\n";
  	}
  elsif ($schema eq "S05") {
  	print '$schema='.$schema."\n";
  	$uidstr = $uidstr05; $datadir = $datadir05;
  	print '$uidstr='.$uidstr."\n".'$datadir='.$datadir."\n";
  	}
  elsif ($schema eq "S06") {
  	print '$schema='.$schema."\n";
  	$uidstr = $uidstr06; $datadir = $datadir06;
  	print '$uidstr='.$uidstr."\n".'$datadir='.$datadir."\n";
  	}
  elsif ($schema eq "S08") {
  	print '$schema='.$schema."\n";
  	$uidstr = $uidstr08; $datadir = $datadir08;
  	print '$uidstr='.$uidstr."\n".'$datadir='.$datadir."\n";
  	}
  elsif ($schema eq "S09") {
  	print '$schema='.$schema."\n";
  	$uidstr = $uidstr09; $datadir = $datadir09;
  	print '$uidstr='.$uidstr."\n".'$datadir='.$datadir."\n";
  	}
  elsif ($schema eq "S10") {
  	print '$schema='.$schema."\n";
  	$uidstr = $uidstr10; $datadir = $datadir10;
  	print '$uidstr='.$uidstr."\n".'$datadir='.$datadir."\n";
  	}
JobCount:
  $nowJobCount = `ps -ef | grep -v grep | grep ociuldr.bin | wc -l`;
  print 'maxJobCount = '.$maxJobCount,"\nnowJobCount = ".$nowJobCount."\n"; 
  if ($nowJobCount < $maxJobCount) {
  	# After a flatfile is unloaded,sleep 10 for the file to finish from swapspace to disk
  	print $binfile.' user='.$uidstr.' field=0x07 file='.$datadir.$tablename.'.txt query="'.$sqlstr.'" && sleep 10 && touch '.$datadir.$tablename.'.txt.ok &',"\n";
  	system ($binfile.' user='.$uidstr.' field=0x07 file='.$datadir.$tablename.'.txt query="'.$sqlstr.'" && sleep 10 && touch '.$datadir.$tablename.'.txt.ok &');
  }else{
    sleep 10;
    goto  JobCount;
  }
      

}

##���õ����ı�ʵ��
#system ('/data/app/ociuldr/ociuldr.bin user=iccard/iccard@orcl field=0x07 file=/data/app/ociuldr/data/Iccard.Tbl_Add_Acct_Info.Txt query="select * from iccard.TBL_ADD_ACCT_INFO"' && 'touch /data/app/ociuldr/data/iccard.TBL_ADD_ACCT_INFO.txt.ok');
