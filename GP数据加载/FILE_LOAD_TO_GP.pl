#!/usr/bin/perl
#########################################################################
# 任务名：      
# 前置任务名：  无
# 目标表：      
# 源表：        
# 方式：        全量更新
# 运行频度:     每日
# 任务功能说明: 加载数据文件到GP数据库
# 作者：        陈丽辉
# 创建日期:     2011-08-26
# 修改记录
#########################################################################
# 修改人        修改日期     修改内容
#
#########################################################################
# package section
#########################################################################
use strict; # Declare using Perl strict syntax
use File::Basename;
use File::Path;
use Date::Manip;
use Cwd 'abs_path';

#########################################################################
# variable section
#########################################################################
my $SCRIPT_NAME = basename("$0");
##my $SCRIPT_PATH = dirname("$0");
my $PERL_SCRIPT_PATH="/home/ap/ods/shared/perl/pub";     ##指定脚本存放目录，主要用于读取数据库参数配置文件
my $PERL_LOG_PATH=$ENV{ODS_HOME}."/file/audit/log/perl/pdm";  ##指定日志存放目录，主要用于生成日志文件

my @contents;
if (!defined(@contents)){
   @contents=connect_db();
}
my $SDB_DB = $ARGV[4];
if ( !defined($SDB_DB) ) {
   $SDB_DB = $contents[1];
}
my $SDB_HOST = $ARGV[5];
if ( !defined($SDB_HOST) ) {
    $SDB_HOST = $contents[2];
}
my $SDB_PORT = $ARGV[6];
if ( !defined($SDB_PORT) ) {
    $SDB_PORT = $contents[3];
}
my $SDB_USER = $ARGV[7];
if ( !defined($SDB_USER) ) {
    $SDB_USER = $contents[4];
}
my $FDM_SCH = $ARGV[8];
if ( !defined($FDM_SCH) ) {
    $FDM_SCH = $contents[5];
}
my $LAM_SCH = $ARGV[9];
if ( !defined($LAM_SCH) ) {
    $LAM_SCH = $contents[6];
}
my $PDM_SCH = $ARGV[10];
if ( !defined($PDM_SCH) ) {
    $PDM_SCH = $contents[7];
}
my $LDM_SCH = $ARGV[11];
if ( !defined($LDM_SCH) ) {
    $LDM_SCH = $contents[8];
}
my $FILE_INPUT_GPFDIST_PATH = $ARGV[12];
if ( !defined($FILE_INPUT_GPFDIST_PATH) ) {
    $FILE_INPUT_GPFDIST_PATH = $contents[9];
}
my $FILE_INPUT_GPFDIST_PORT = $ARGV[13];
if ( !defined($FILE_INPUT_GPFDIST_PORT) ) {
    $FILE_INPUT_GPFDIST_PORT = $contents[10]; 
}

#############################日期参数的各种形式
my $WORK_DAY     = $ARGV[0];

##YYYYMM#
my $WORK_DAY_YYYYMM =substr $WORK_DAY,0,6;

##YYYY#
my $WORK_DAY_YYYY =substr $WORK_DAY,0,4;

##YYYYQQ#
my $MM = $WORK_DAY;
   $MM =~ s/(\d{4})(\d{2})(\d{2})/$2/g;
   $MM = int($MM/3);
my $WORK_DAY_YYYYQQ = $WORK_DAY_YYYY."0".$MM;

##############################参数检查

my $GPFDIST_FILE_PATH = $ARGV[1];
my $TABLE_ID 		    	= $ARGV[2];
my $HOSTNAME	 				= $ARGV[3];

if (!($HOSTNAME))
{
   	print "运行格式:perl 脚本名 运行日期 配置文件(绝对路径) 装载文件名 [HOSTNAME]\n";
   	print "HOSTNAME不能为空,参考:\$HOSTNAME\n";
   	print "#######脚本执行失败#######\n";
    		exit(1);
}
     
sub date_tran
{
#############读取SQL语句##########
   my $i = 0;
   my $j = 0;   
   my $k = 0;
   my $FILE_ALL_INFO;
   my $FILE_ALL_NAME;
   my $FILE_PATH;
   my $TABLE_NAME;
   my $TABLE_NAME2;
   my $SQL0;
   my $SQL1;
   my $SQL2;
   my $SQL3;
   my $SQL4;
   my $SQL5;
   my $SQL;
	 my @FILE_INFO;
	 my $ADD_TYPE; 
	 my $TRUNCATE_APPEND;
	 my $FILE_DELIMITER;
	 
   if(open(ADDFILE,"$GPFDIST_FILE_PATH"))
   		{
   			open(ADDFILE,"$GPFDIST_FILE_PATH") or die $!;
   		}
   else{
   	print "没有找到对应的配置文件:${GPFDIST_FILE_PATH}\n";
   	print "#######脚本执行失败#######\n";
   	exit(1);
   	} 		

   while(<ADDFILE>){
     if(m/^#/){
        next;
      }
     elsif(m/^$TABLE_ID\s{0,}\:/){
       chomp $_;
       $j = 1;
       $FILE_ALL_INFO=$FILE_ALL_INFO.$_;
       if( !(m/\}/)){
       while(<ADDFILE>){
         chomp $_;
         $FILE_ALL_INFO=$FILE_ALL_INFO.$_;
       if( (m/\}/)){
          close(ADDFILE);
         }
       }
       }	   	
      else{
         close(ADDFILE);
       }} 
 	}	   
############修改SQL语句，替换相应变量########
  if ( $j == 1){
   	
   #######读取配置文件,获取对应信息(表名,文件名,装载方式,入库方式,分隔符)
	 $FILE_ALL_INFO=~s/\$WORKDAY/$WORK_DAY/g;
	 $FILE_ALL_INFO=~s/\$FDM_SCH/$FDM_SCH/g;	
	 $FILE_ALL_INFO=~s/\$LAM_SCH/$LAM_SCH/g;	 
	 $FILE_ALL_INFO=~s/\$PDM_SCH/$PDM_SCH/g;	 
	 $FILE_ALL_INFO=~s/\$LDM_SCH/$LDM_SCH/g;	
   $FILE_ALL_INFO=~s/^.{1,}\{//g;
   $FILE_ALL_INFO=~s/\}//g;    
   @FILE_INFO = split /:/,$FILE_ALL_INFO;
   $TABLE_NAME=$FILE_INFO[0];
   $FILE_ALL_NAME=$FILE_INFO[1];  
   $ADD_TYPE=$FILE_INFO[2]; 
   $TRUNCATE_APPEND=$FILE_INFO[3];
   my $all_length=length($TABLE_NAME)+length($FILE_ALL_NAME)+length($ADD_TYPE)+length($TRUNCATE_APPEND);   
   $FILE_DELIMITER=substr($FILE_ALL_INFO,$all_length+4,length($FILE_ALL_INFO)-$all_length); 
   $TABLE_NAME=~s/\s+$//; 
   $TABLE_NAME=~s/^\s+//; 
   $FILE_ALL_NAME=~s/\s+$//;
   $FILE_ALL_NAME=~s/^\s+//;
   $ADD_TYPE=~s/\s+$//;
   $ADD_TYPE=~s/^\s+//;
   $TRUNCATE_APPEND=~s/\s+$//;
   $TRUNCATE_APPEND=~s/^\s+//;
   $FILE_DELIMITER=~s/\s+$//;
   $FILE_DELIMITER=~s/^\s+//g;
   print "HOSTNAME: $HOSTNAME\n";  
   print "表名    : ${TABLE_NAME}\n";	
   print "文件名  : ${FILE_ALL_NAME}\n";   
   print "装载方式: ${ADD_TYPE}\n";
   print "入库方式: ${TRUNCATE_APPEND}\n";	
   
   #####检查文件是否存在###########
   if(!( -e ${FILE_ALL_NAME})){   	
   	print "文件错误,请检查配置文件是否正确,检查对应路径下是否有该文件!\n";
   	print "#######脚本执行失败#######\n";
   	exit(1);
   	}   	
   
   #######检查装载方式(可选值为:GPFDIST/COPY)   
   $ADD_TYPE=~tr/a-z/A-Z/;
   if ( $ADD_TYPE eq "GPFDIST") {$k=1;}   
   elsif ($ADD_TYPE eq "COPY") {$k=2;}
   else {
   			print "装载方式参数不对,可选值:GPFDIST/COPY\n";
   			print "#######脚本执行失败#######\n";
    		exit(1);
    		} 
   
   #######检查入库方式(可选值为:APPEND/TRUNCATE入库)
	 $TRUNCATE_APPEND=~tr/a-z/A-Z/;
   if ( $TRUNCATE_APPEND eq "APPEND") {$i=1;}  
   elsif ($TRUNCATE_APPEND eq "TRUNCATE") {$i=2;}
   else {
   			print "入库方式参数不对,可选值:APPEND/TRUNCATE\n";
   			print "#######脚本执行失败#######\n";
    		exit(1);
    		} 
    		
   #######检查分隔符(默认值为:\x1,为空给默认值)   
   if(!defined($FILE_DELIMITER) || $FILE_DELIMITER eq ''){
   				$FILE_DELIMITER ='\x1';
   				print "分隔符  : \\x1\n";} 
   else {print "分隔符  : ${FILE_DELIMITER}\n";} 

   ######GPFDIST方式###############
   if( $k == 1) {  		 
  			 #######处理传入文件的路径
  			 	if (substr($FILE_ALL_NAME,0,length(${FILE_INPUT_GPFDIST_PATH})) eq ${FILE_INPUT_GPFDIST_PATH})
  			 			{
							$FILE_PATH =substr(${FILE_ALL_NAME},length(${FILE_INPUT_GPFDIST_PATH}),length(${FILE_ALL_NAME}) - length(${FILE_INPUT_GPFDIST_PATH}));
							}
					else 	{
							$FILE_PATH =${FILE_ALL_NAME};		
								}								
					#######端口处理			
					if($FILE_INPUT_GPFDIST_PORT){$FILE_INPUT_GPFDIST_PORT=":".$FILE_INPUT_GPFDIST_PORT ;}					
  				#######分全量和增量入库两种方式生成SQL语句			
  			  if( $i == 1){ 
  			  			$SQL0="SET CLIENT_ENCODING=GB18030;";          	    	
					      $SQL1="DROP EXTERNAL TABLE IF EXISTS ${TABLE_NAME}_TEMP;";
  			        $SQL2="CREATE EXTERNAL TABLE ${TABLE_NAME}_TEMP (LIKE ${TABLE_NAME}) LOCATION ('gpfdist://${HOSTNAME}${FILE_INPUT_GPFDIST_PORT}${FILE_PATH}') FORMAT 'TEXT'(DELIMITER '${FILE_DELIMITER}');";
  			        $SQL3="INSERT INTO ${TABLE_NAME} SELECT * FROM ${TABLE_NAME}_TEMP;";
					      $SQL4="DROP EXTERNAL TABLE IF EXISTS ${TABLE_NAME}_TEMP;";
  			        $SQL=$SQL0.$SQL1.$SQL2.$SQL3.$SQL4;		
  			        print "执行过程:\n第一步  :${SQL0}\n第二步  :${SQL1}\n第三步  :${SQL2}\n第四步  :${SQL3}\n最后一步:${SQL4}\n";         
  			      }
  			  elsif( $i == 2){
  			  			$SQL0="SET CLIENT_ENCODING=GB18030;"; 
  			        $SQL1="DROP EXTERNAL TABLE IF EXISTS ${TABLE_NAME}_TEMP;";			   
  			        $SQL2="CREATE EXTERNAL TABLE ${TABLE_NAME}_TEMP (LIKE ${TABLE_NAME}) LOCATION ('gpfdist://${HOSTNAME}${FILE_INPUT_GPFDIST_PORT}${FILE_PATH}') FORMAT 'TEXT'(DELIMITER '${FILE_DELIMITER}');";
  			        $SQL3="TRUNCATE TABLE ${TABLE_NAME};"; 			        
  			        $SQL4="INSERT INTO ${TABLE_NAME} SELECT * FROM ${TABLE_NAME}_TEMP;";
					      $SQL5="DROP EXTERNAL TABLE IF EXISTS ${TABLE_NAME}_TEMP;";
  			        $SQL=$SQL0.$SQL1.$SQL2.$SQL3.$SQL4;			  			        
  			        print "执行过程:\n第一步  :${SQL0}\n第二步  :${SQL1}\n第三步  :${SQL2}\n第四步  :${SQL3}\n第五步  :${SQL4}\n最后一步:${SQL5}\n";       
  			      }  
  		}
   ######COPY方式###############
   elsif( $k == 2) {	
   		 #######处理传入文件的路径    	
    	 $FILE_PATH =${FILE_ALL_NAME};
 
  			 #######分全量和增量入库两种方式生成SQL语句	    	 
     		  if( $i == 1){ 
  			  		$SQL0="SET CLIENT_ENCODING=GB18030;";           	    	
		      		$SQL1="COPY ${TABLE_NAME} FROM '${FILE_PATH}' WITH DELIMITER '${FILE_DELIMITER}';";
		      		$SQL=$SQL0.$SQL1;
  						print "执行过程:\n第一步  :${SQL0}\n最后一步:${SQL1}\n";
        				}
    			elsif( $i == 2){
  			  		$SQL0="SET CLIENT_ENCODING=GB18030;";  
          		$SQL1="TRUNCATE TABLE ${TABLE_NAME};";	   
          		$SQL2="COPY ${TABLE_NAME} FROM '${FILE_PATH}' WITH DELIMITER '${FILE_DELIMITER}';";	
  			  		$SQL=$SQL0.$SQL1.$SQL2;	
  			  		print "执行过程:\n第一步  :${SQL0}\n第二步  :${SQL1}\n最后一步:${SQL2}\n";          
        			}        				     	
    	} 
   }
   else{
    print "文件中没有对应的信息:${TABLE_ID}\n";
    print "#######脚本执行失败#######\n";
    exit(1);
   }
  return $SQL;
}


############################################################################
# PSQL Function
############################################################################
sub run_psql_command {
   my ($SQL,$LOGFILE) = @_ ;
   my $rc = open(PSQL, "|  PSQL -d $SDB_DB -h $SDB_HOST -p $SDB_PORT -U $SDB_USER -a -v ON_ERROR_STOP=1 >>$LOGFILE 2>&1");
   # To see if psql command invoke ok?
   unless ($rc) {
      print "Could not invoke PSQL command,$!";
      return 1;
   }
   print PSQL <<ENDOFINPUT;


\\timing
----'程序开始

    $SQL;

----'程序结束';
\\q
ENDOFINPUT
   close( PSQL ) ;
   ##### return 0 means ok
   my $RET_CODE = $?;
   return $RET_CODE;
}

######################################################################
#read config parameter
######################################################################
sub connect_db
{
   my $CFG="$PERL_SCRIPT_PATH/FILE_LOAD_TO_GP_CONN.cfg";  #######配置数据库参数文件，如有需要请自行配置########
   my $rc=open(FILE,"$CFG");
   my @content;
   if ($rc == 1 ){
   	   @content=<FILE>;
   	   close(FILE);
   	              }
       else {
   my $CFG="$PERL_SCRIPT_PATH/FILE_LOAD_TO_GP_CONN.cfg";
   my $rc=open(FILE,"$CFG");
        if($rc != 1 ) {
            print "open FILE_LOAD_TO_GP_CONN.cfg file error";
        }
            @content=<FILE>;
            close(FILE);
        }
        close(FILE);
        $content[1]=~s/^.{1,}\=//g;
        $content[2]=~s/^.{1,}\=//g;
        $content[3]=~s/^.{1,}\=//g;
        $content[4]=~s/^.{1,}\=//g;
        $content[5]=~s/^.{1,}\=//g;
        $content[6]=~s/^.{1,}\=//g;
        $content[7]=~s/^.{1,}\=//g;
        $content[8]=~s/^.{1,}\=//g;        
        $content[9]=~s/^.{1,}\=//g;       
        $content[10]=~s/^.{1,}\=//g;   
        chomp $content[1];
        chomp $content[2];
        chomp $content[3];
        chomp $content[4];
        chomp $content[5];
        chomp $content[6];
        chomp $content[7];
        chomp $content[8];
        chomp $content[9];   
        chomp $content[10];     
        return @content;
}
############################################################################
# Return localtime
############################################################################
sub local_time {
	 ##读取本地系统时间
	 my $now;
	 my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   $year  += 1900;
   $mon    = sprintf("%02d", $mon + 1);
   $mday   = sprintf("%02d", $mday);
   $hour   = sprintf("%02d", $hour);
   $min    = sprintf("%02d", $min);
   $sec    = sprintf("%02d", $sec);
   my $now = "${year}${mon}${mday}${hour}${min}${sec}";
	 return $now;
}

######################################################################
# create log file
######################################################################
sub set_env {
   ##如果不存在即创建日期目录，并在该目录下对应每个脚本生成一个日志文件(日志文件存在则不创建)
   mkdir("$PERL_LOG_PATH/$WORK_DAY", 0755)  || die "$!"  unless (-e "$PERL_LOG_PATH/$WORK_DAY");
   $_ = $SCRIPT_NAME;
   s/\.pl$//i;
   unless (-e "$PERL_LOG_PATH/$WORK_DAY/FILE_LOAD_TO_GP_$TABLE_ID.log") {
   open(FILE,">$PERL_LOG_PATH/$WORK_DAY/FILE_LOAD_TO_GP_$TABLE_ID.log") or die "Couldn't create $PERL_LOG_PATH/$WORK_DAY/FILE_LOAD_TO_GP_$TABLE_ID.log:$!"  ;
   }
   close FILE;
   chmod 0755,"$PERL_LOG_PATH/$WORK_DAY/FILE_LOAD_TO_GP_$TABLE_ID.log";
   return "$PERL_LOG_PATH/$WORK_DAY/FILE_LOAD_TO_GP_$TABLE_ID.log";
}
######################################################################
# main function
######################################################################
sub main {
   my $LOGFILE = set_env();
   my $startdate = local_time();
   print "详细日志信息请看: $LOGFILE \n";
   print "开始时间: $startdate \n";
   my $SQL     = date_tran();
   my $ret     = run_psql_command("$SQL","$LOGFILE" ) ;
   my $enddate = local_time();
   my $sum_t   = $enddate - $startdate;
   print "结束时间: $enddate \n";
   print "程序总运行时间: $sum_t 秒\n";
   return $ret;
}

########################################################################
# promgram section
########################################################################
if ( $#ARGV < 0 ) {
   print "请输入参数 \n";
   exit(1);
}

my $CONTROL_FILE = $ARGV[0];
if ($CONTROL_FILE =~/[0-9]{8}($|\.)/) {
   $WORK_DAY = substr($CONTROL_FILE,0,8);
   ParseDate($WORK_DAY) or die "错误信息：不是合法日期数据! \n";
}
else{
   print "错误信息: 日期位数不够或者包含非法字符! \n";
   exit(1);
}
    my $ret = main();

if( $ret == 0 ){
	  print "#######脚本执行成功#######\n";
    exit(0);
}
else{
	  print "#######脚本执行失败#######\n";
    exit(1);
}

