#!/usr/bin/perl
###############################################################################
# Program  : unload_from_ora.pl
# Argument : none
# Created by Kenny Wang 2013-5-11
# Modified by Kenny Wang 2013-6-6,增加S06四张不同源的表,直接设定语言及字符集变量
#   unload from oracle database vir sqlfile(tablename#select * from tablename)
###############################################################################
my $maxJobCount = 100; #可以10个导出进程同时工作
my $nowJobCount = `ps -ef | grep -v grep | grep ociuldr.bin | wc -l`;	#当前进程数
my $binfile = "/EDW/ETL/unload_from_source/ociuldr/ociuldr.bin"; #/data/app/ociuldr/ociuldr.bin
# SNN 对应的数据源oracle连接串
my $uidstr01 = 'sjck/sjck03@21.1.1.45:1521/wsbank';
my $uidstr03 = 'utan/utan@31.2.2.108:1521/orcl';
my $uidstr09 = 'customer/customer@21.1.1.45:1521/wsbank';
my $uidstr15 = 'csprun/csprun@31.2.2.108:1521/orcl';
my $uidstr02 = 'whrcbloan/whrcbloan@31.2.2.108:1521/orcl';
my $uidstr04 = $uidstr02;
my $uidstr05 = $uidstr02;
my $uidstr06 = $uidstr02;
my $uidstr061 = 'cnaps2/cnaps2@21.1.1.45:1521/wsbank'; # T_WMP_BANKINFO, T_WMP_CUSSIGNMANAGE, T_WMP_PAYMENTBOOK
my $uidstr08 = $uidstr02;
my $uidstr10 = $uidstr02;
my $uidstr;
# 进行卸对的参数文件(SNN,表,对应的SQL,全、增量，增量条件)
my $sqlfile = "/EDW/ETL/unload_from_source/ociuldr/src_exp_all_388_999_trm.sql"; #/data/app/ociuldr/iccard.sql
# 卸数后文本文件的存放目录
my $datadir01 = "/EDW/ETL/unload_from_source/oradata/S01/"; #/data/app/ociuldr/data/ #包含最后"/"
my $datadir02 = "/EDW/ETL/unload_from_source/oradata/S02/";
my $datadir03 = "/EDW/ETL/unload_from_source/oradata/S03/";
my $datadir04 = "/EDW/ETL/unload_from_source/oradata/S04/";
my $datadir05 = "/EDW/ETL/unload_from_source/oradata/S05/";
my $datadir06 = "/EDW/ETL/unload_from_source/oradata/S06/";
my $datadir08 = "/EDW/ETL/unload_from_source/oradata/S08/";
my $datadir09 = "/EDW/ETL/unload_from_source/oradata/S09/";
my $datadir10 = "/EDW/ETL/unload_from_source/oradata/S10/";
my $datadir15 = "/EDW/ETL/unload_from_source/oradata/S15/";
my $datadir;
# 导出文本数据的字段分隔符 0X07
my $deli = '0x07';
# 卸数生成文本文件后,停顿 n 秒,给系统保存文本数据留足时间后再生成相应的 ok 文件
my $sleepn = 'sleep 10';
##################环境变量 : LANG	NLS_LANG	NLS_DATE_FORMAT
my $LANG = "";
my $NLS_LANG = "";
my $NLS_DATE_FORMAT = "";
# 设置oracle卸数的语言及字符集参数
#`export LANG=en_US.UTF-8`;
#`export NLS_LANG=American_america.AL32UTF8`;
#`export NLS_DATE_FORMAT="YYYY-MM-DD HH24:MI:SS"`;
# 获取oracle卸数的语言及字符集参数
$LANG = $ENV{"LANG"};
$NLS_LANG = $ENV{"NLS_LANG"};
$NLS_DATE_FORMAT = $ENV{"NLS_DATE_FORMAT"};

# 打印、显示oracle卸数的语言及字符集参数
print 'export LANG = '.$LANG."\n";
print 'export NLS_LANG = '.$NLS_LANG."\n";
print 'export NLS_DATE_FORMAT = '.$NLS_DATE_FORMAT."\n";

sleep 5;

##################日期和时间
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
  open(FILE, $sqlfile) || die "文件 $sqlfile 打开失败, 程序终止.\n";
  # read the first line, and chomp off the newline
  chomp(my $firstline = <FILE>);
  print $firstline;
  # read other into array
  my @other = <FILE>;
# print @other;
  close FILE; 
##################
# open file to the handle <FILE>
open(FILE, $sqlfile) || die "文件 $sqlfile 打开失败, 程序终止.\n";
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
  	# After a flatfile is unloaded,${sleepn} for the file to finish from swapspace to disk
  	print $binfile.' user='.$uidstr.' field='.$deli.' file='.$datadir.$tablename.'.txt query="'.$sqlstr.'" && '.${sleepn}.' && touch '.$datadir.$tablename.'.txt.ok &',"\n";
  	system ($binfile.' user='.$uidstr.' field='.$deli.' file='.$datadir.$tablename.'.txt query="'.$sqlstr.'" && '.${sleepn}.' && touch '.$datadir.$tablename.'.txt.ok &');
  }else{
    sleep 10;
    goto  JobCount;
  }
      

}

######### 下面四个数据是不规律的数据来源,单独列出,都是 S06 ##################
#已经在S01,S06的卸数源创建了同义词,与S01,S06卸数方法保持一致
##调用导出文本实例
#system ('/data/app/ociuldr/ociuldr.bin user=iccard/iccard@orcl field=0x07 file=/data/app/ociuldr/data/Iccard.Tbl_Add_Acct_Info.Txt query="select * from iccard.TBL_ADD_ACCT_INFO"' && 'touch /data/app/ociuldr/data/iccard.TBL_ADD_ACCT_INFO.txt.ok');
