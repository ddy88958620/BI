#!/usr/bin/perl
#########################################################################
# ��������      
# ǰ����������  ��
# Ŀ���      
# Դ��        
# ��ʽ��        ȫ������
# ����Ƶ��:     ÿ��
# ������˵��: ���������ļ���GP���ݿ�
# ���ߣ�        ������
# ��������:     2011-08-26
# �޸ļ�¼
#########################################################################
# �޸���        �޸�����     �޸�����
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
my $PERL_SCRIPT_PATH="/home/ap/ods/shared/perl/pub";     ##ָ���ű����Ŀ¼����Ҫ���ڶ�ȡ���ݿ���������ļ�
my $PERL_LOG_PATH=$ENV{ODS_HOME}."/file/audit/log/perl/pdm";  ##ָ����־���Ŀ¼����Ҫ����������־�ļ�

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

#############################���ڲ����ĸ�����ʽ
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

##############################�������

my $GPFDIST_FILE_PATH = $ARGV[1];
my $TABLE_ID 		    	= $ARGV[2];
my $HOSTNAME	 				= $ARGV[3];

if (!($HOSTNAME))
{
   	print "���и�ʽ:perl �ű��� �������� �����ļ�(����·��) װ���ļ��� [HOSTNAME]\n";
   	print "HOSTNAME����Ϊ��,�ο�:\$HOSTNAME\n";
   	print "#######�ű�ִ��ʧ��#######\n";
    		exit(1);
}
     
sub date_tran
{
#############��ȡSQL���##########
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
   	print "û���ҵ���Ӧ�������ļ�:${GPFDIST_FILE_PATH}\n";
   	print "#######�ű�ִ��ʧ��#######\n";
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
############�޸�SQL��䣬�滻��Ӧ����########
  if ( $j == 1){
   	
   #######��ȡ�����ļ�,��ȡ��Ӧ��Ϣ(����,�ļ���,װ�ط�ʽ,��ⷽʽ,�ָ���)
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
   print "����    : ${TABLE_NAME}\n";	
   print "�ļ���  : ${FILE_ALL_NAME}\n";   
   print "װ�ط�ʽ: ${ADD_TYPE}\n";
   print "��ⷽʽ: ${TRUNCATE_APPEND}\n";	
   
   #####����ļ��Ƿ����###########
   if(!( -e ${FILE_ALL_NAME})){   	
   	print "�ļ�����,���������ļ��Ƿ���ȷ,����Ӧ·�����Ƿ��и��ļ�!\n";
   	print "#######�ű�ִ��ʧ��#######\n";
   	exit(1);
   	}   	
   
   #######���װ�ط�ʽ(��ѡֵΪ:GPFDIST/COPY)   
   $ADD_TYPE=~tr/a-z/A-Z/;
   if ( $ADD_TYPE eq "GPFDIST") {$k=1;}   
   elsif ($ADD_TYPE eq "COPY") {$k=2;}
   else {
   			print "װ�ط�ʽ��������,��ѡֵ:GPFDIST/COPY\n";
   			print "#######�ű�ִ��ʧ��#######\n";
    		exit(1);
    		} 
   
   #######�����ⷽʽ(��ѡֵΪ:APPEND/TRUNCATE���)
	 $TRUNCATE_APPEND=~tr/a-z/A-Z/;
   if ( $TRUNCATE_APPEND eq "APPEND") {$i=1;}  
   elsif ($TRUNCATE_APPEND eq "TRUNCATE") {$i=2;}
   else {
   			print "��ⷽʽ��������,��ѡֵ:APPEND/TRUNCATE\n";
   			print "#######�ű�ִ��ʧ��#######\n";
    		exit(1);
    		} 
    		
   #######���ָ���(Ĭ��ֵΪ:\x1,Ϊ�ո�Ĭ��ֵ)   
   if(!defined($FILE_DELIMITER) || $FILE_DELIMITER eq ''){
   				$FILE_DELIMITER ='\x1';
   				print "�ָ���  : \\x1\n";} 
   else {print "�ָ���  : ${FILE_DELIMITER}\n";} 

   ######GPFDIST��ʽ###############
   if( $k == 1) {  		 
  			 #######�������ļ���·��
  			 	if (substr($FILE_ALL_NAME,0,length(${FILE_INPUT_GPFDIST_PATH})) eq ${FILE_INPUT_GPFDIST_PATH})
  			 			{
							$FILE_PATH =substr(${FILE_ALL_NAME},length(${FILE_INPUT_GPFDIST_PATH}),length(${FILE_ALL_NAME}) - length(${FILE_INPUT_GPFDIST_PATH}));
							}
					else 	{
							$FILE_PATH =${FILE_ALL_NAME};		
								}								
					#######�˿ڴ���			
					if($FILE_INPUT_GPFDIST_PORT){$FILE_INPUT_GPFDIST_PORT=":".$FILE_INPUT_GPFDIST_PORT ;}					
  				#######��ȫ��������������ַ�ʽ����SQL���			
  			  if( $i == 1){ 
  			  			$SQL0="SET CLIENT_ENCODING=GB18030;";          	    	
					      $SQL1="DROP EXTERNAL TABLE IF EXISTS ${TABLE_NAME}_TEMP;";
  			        $SQL2="CREATE EXTERNAL TABLE ${TABLE_NAME}_TEMP (LIKE ${TABLE_NAME}) LOCATION ('gpfdist://${HOSTNAME}${FILE_INPUT_GPFDIST_PORT}${FILE_PATH}') FORMAT 'TEXT'(DELIMITER '${FILE_DELIMITER}');";
  			        $SQL3="INSERT INTO ${TABLE_NAME} SELECT * FROM ${TABLE_NAME}_TEMP;";
					      $SQL4="DROP EXTERNAL TABLE IF EXISTS ${TABLE_NAME}_TEMP;";
  			        $SQL=$SQL0.$SQL1.$SQL2.$SQL3.$SQL4;		
  			        print "ִ�й���:\n��һ��  :${SQL0}\n�ڶ���  :${SQL1}\n������  :${SQL2}\n���Ĳ�  :${SQL3}\n���һ��:${SQL4}\n";         
  			      }
  			  elsif( $i == 2){
  			  			$SQL0="SET CLIENT_ENCODING=GB18030;"; 
  			        $SQL1="DROP EXTERNAL TABLE IF EXISTS ${TABLE_NAME}_TEMP;";			   
  			        $SQL2="CREATE EXTERNAL TABLE ${TABLE_NAME}_TEMP (LIKE ${TABLE_NAME}) LOCATION ('gpfdist://${HOSTNAME}${FILE_INPUT_GPFDIST_PORT}${FILE_PATH}') FORMAT 'TEXT'(DELIMITER '${FILE_DELIMITER}');";
  			        $SQL3="TRUNCATE TABLE ${TABLE_NAME};"; 			        
  			        $SQL4="INSERT INTO ${TABLE_NAME} SELECT * FROM ${TABLE_NAME}_TEMP;";
					      $SQL5="DROP EXTERNAL TABLE IF EXISTS ${TABLE_NAME}_TEMP;";
  			        $SQL=$SQL0.$SQL1.$SQL2.$SQL3.$SQL4;			  			        
  			        print "ִ�й���:\n��һ��  :${SQL0}\n�ڶ���  :${SQL1}\n������  :${SQL2}\n���Ĳ�  :${SQL3}\n���岽  :${SQL4}\n���һ��:${SQL5}\n";       
  			      }  
  		}
   ######COPY��ʽ###############
   elsif( $k == 2) {	
   		 #######�������ļ���·��    	
    	 $FILE_PATH =${FILE_ALL_NAME};
 
  			 #######��ȫ��������������ַ�ʽ����SQL���	    	 
     		  if( $i == 1){ 
  			  		$SQL0="SET CLIENT_ENCODING=GB18030;";           	    	
		      		$SQL1="COPY ${TABLE_NAME} FROM '${FILE_PATH}' WITH DELIMITER '${FILE_DELIMITER}';";
		      		$SQL=$SQL0.$SQL1;
  						print "ִ�й���:\n��һ��  :${SQL0}\n���һ��:${SQL1}\n";
        				}
    			elsif( $i == 2){
  			  		$SQL0="SET CLIENT_ENCODING=GB18030;";  
          		$SQL1="TRUNCATE TABLE ${TABLE_NAME};";	   
          		$SQL2="COPY ${TABLE_NAME} FROM '${FILE_PATH}' WITH DELIMITER '${FILE_DELIMITER}';";	
  			  		$SQL=$SQL0.$SQL1.$SQL2;	
  			  		print "ִ�й���:\n��һ��  :${SQL0}\n�ڶ���  :${SQL1}\n���һ��:${SQL2}\n";          
        			}        				     	
    	} 
   }
   else{
    print "�ļ���û�ж�Ӧ����Ϣ:${TABLE_ID}\n";
    print "#######�ű�ִ��ʧ��#######\n";
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
----'����ʼ

    $SQL;

----'�������';
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
   my $CFG="$PERL_SCRIPT_PATH/FILE_LOAD_TO_GP_CONN.cfg";  #######�������ݿ�����ļ���������Ҫ����������########
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
	 ##��ȡ����ϵͳʱ��
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
   ##��������ڼ���������Ŀ¼�����ڸ�Ŀ¼�¶�Ӧÿ���ű�����һ����־�ļ�(��־�ļ������򲻴���)
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
   print "��ϸ��־��Ϣ�뿴: $LOGFILE \n";
   print "��ʼʱ��: $startdate \n";
   my $SQL     = date_tran();
   my $ret     = run_psql_command("$SQL","$LOGFILE" ) ;
   my $enddate = local_time();
   my $sum_t   = $enddate - $startdate;
   print "����ʱ��: $enddate \n";
   print "����������ʱ��: $sum_t ��\n";
   return $ret;
}

########################################################################
# promgram section
########################################################################
if ( $#ARGV < 0 ) {
   print "��������� \n";
   exit(1);
}

my $CONTROL_FILE = $ARGV[0];
if ($CONTROL_FILE =~/[0-9]{8}($|\.)/) {
   $WORK_DAY = substr($CONTROL_FILE,0,8);
   ParseDate($WORK_DAY) or die "������Ϣ�����ǺϷ���������! \n";
}
else{
   print "������Ϣ: ����λ���������߰����Ƿ��ַ�! \n";
   exit(1);
}
    my $ret = main();

if( $ret == 0 ){
	  print "#######�ű�ִ�гɹ�#######\n";
    exit(0);
}
else{
	  print "#######�ű�ִ��ʧ��#######\n";
    exit(1);
}

