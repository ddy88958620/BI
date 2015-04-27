#!/usr/bin/perl
#######################################Head Section##################################################
#开发日期：2007/04/19                                                                                
#脚本功能：
#	1、检查当日S09数据目录，数据是否准备好；
#	2、获取所需源数据列表；
#	3、检查数据文件大小是否正确；
#	4、检查源数据是否已处理；
#	5、生成List文件和Dir文件。
#	6、追加S23_log
#创 建 人：郝艳丰
#修改日期：2006/11/03；增加插入BOCN_CTL_LOAD_LOG物理表模块
#%INTF::LoadTableDef 结构说明
#         ->LogFileName  存放DATA的处理成功结果日志文件名
#		  ->$DataFile	 数据文件定义及状态	
#				->format 数据文件实际名称
#				->flag	数据文件标志
#				->Column 数据文件字段定义及状态
#					->$ColumnName 字段名称
#						->SEQ	字段序号
#						->TYPE	字段类型
#						->LEN	字段长度
#						->MARK	字段标志
#				->CURColumn 数据文件字段定义及状态
#					->$ColumnName 字段名称
#						->SEQ	字段序号
#						->TYPE	字段类型
#						->LEN	字段长度
#						->MARK	字段标志
#				->ChkFlag 数据检查标志
#				->RecNum  列表文件中数据记录数
#				->RecLen  配置文件中指定记录长度
#				->CalFileSize 定义的数据大小
#				->FileSize 实际的数据文件大小
#
#flag   0---所需数据正常；
#		    1---所需数据存在，但文件大小与定义不一致；
#		    2---所需数据存在，但文件定义有变化；
#		    3---所需数据存在，但文件已处理；
#		    -1--所需数据不存在，但在列表文件中存在；
#		    -2--所需数据不存在，在列表文件中也不存在。
#
#
#####################################################################################################
use strict; 		# Declare using Perl strict syntax
use DBI;  			# If you are using other Perl's package, declare here 
use Time::Local;

#####################################################################################################
# ------------ Variable Section ------------
my $AUTO_HOME = $ENV{"AUTO_HOME"};
my $AUTO_CORRUPT = "$AUTO_HOME/DATA/fail/corrupt";
my $AUTO_LOG = "$AUTO_HOME/LOG";

my $CTS_SRCFILE_DIR='/home/stage/CTS';

my $S23_ETC_DIR = "$AUTO_HOME/etc";
my $DIRDELI;
my $os = $^O;

$os =~ tr [A-Z][a-z];

if ( $os eq "svr4" || $os eq "aix") {
   $DIRDELI = "/";
   unshift(@INC, "$AUTO_HOME/bin");
   require etl_unix;
   require etl_interface;
}

elsif ( $os eq "mswin32" ) {
   $DIRDELI = "\\";
   unshift(@INC, "$AUTO_HOME\\bin");
   require gftp_nt;
}

my $FLG_RCV = "${ETL::ETL_RECEIVE}";
my $ETL_DB = $ETL::ETLDB ;
my ($CONTROL_FILE,$DayOfData);
my $TXDATE;
my $SUBSYS;
my $logfile;
my $S23_log;

my @CheckFileHead=('CTS_ODS_');
my (%seen,$checkfilename);

#####################################################################################################
# function ChkDataArrived--检查数据状态，确定是否数据已传输完毕
sub ChkDataArrived
{
	my ($DataDirectory) = @_;
#	print "Director $DataDirectory \n";
#	my $DataDirectory = "$s02_Data_Home"."$DateOfData";
	######检查目录是否建立######
	unless ( -d $DataDirectory ) { 
		ETL::showTime(); print "目录【$DataDirectory】未建立\n";
		return 0 ;
	} 
	ETL::showTime(); print "目录【$DataDirectory】已建立，开始检测$CheckFileHead[0]\n";
	unless ( opendir(DATADIR_H,"$DataDirectory")){return 0;}
	my $checkfileCount = 0 ;
	while ( my $DataFileName = readdir(DATADIR_H)){
		if ( $DataFileName eq "." || $DataFileName eq ".." ) { next; }
	    if ( -d "${DataDirectory}/${DataFileName}" ) { next;}
#	    print "$DataFileName \n";
	    unless ( defined $seen{INTF::trimstr($DataFileName)} ){ next; }
	    unless ( $seen{INTF::trimstr($DataFileName)} ){ next; }
	    $checkfileCount +=1 ;
	}
	close(DATADIR_H);
	if ( $checkfileCount > 0 ) {
		ETL::showTime(); print "数据准备完成\n";
		return 1;
	}else{
		return 0;
	}
}

#####################################################################################################
# function ChkRecData--检查文件大小及定义，并设置flag标示
sub ChkRecData
{
	my ( $DataDir,$CtlFile ) = @_;
	unless ( open ( CTL_H, "$DataDir/$CtlFile" )){
		print "ERROR -- Failed to open file $DataDir/$CtlFile \n";
		return 0;
	}
	my ($TarTableName,$datafilename,$datasizedef,$datafiledef,$filesize,$CalFileSize,$filerownum);
	while ( my $datafilestr = <CTL_H> ){
		$datafilestr =~ s/([\n])//g;
		$datasizedef = INTF::trimstr((split('\|',$datafilestr))[2]);
		my $datafilename1 = INTF::trimstr((split('\|',$datafilestr))[0]);
		my $datafilehead= substr($datafilename1,0,length($datafilename1)-13);
		$datafilename = "${datafilehead}";
		unless (defined $INTF::SrcTableDef{$datafilename}  ){next;}
		$TarTableName = $INTF::SrcTableDef{$datafilename}->{tartable};
		print "$datasizedef $datafilename $TarTableName \n";
		$INTF::LoadTableDef{$TarTableName}->{RecLen}=$datasizedef;
		printf "[%8s]: ",$datafilename1;
		if ( ! -f "$DataDir/$datafilename1" ){
			$INTF::LoadTableDef{"$TarTableName"}->{FileSize} = -1;
			$INTF::LoadTableDef{"$TarTableName"}->{flag} = -1;
			print "[Result]: ERROR--数据文件$datafilename1不存在 \n";
		}else {
			$filesize = (stat("${DataDir}/$datafilename1"))[7];
			#$filesize = INTF::trimstr((split('\|',$datafilestr))[1]);
			$filerownum = INTF::trimstr((split('\|',$datafilestr))[1]);
			$INTF::LoadTableDef{"$TarTableName"}->{RecNum}=$filerownum;
			$CalFileSize = $datasizedef;
			printf "[List]:%-9s[Size]:%-9s",$CalFileSize,$filesize ;
			printf "[RowNum]:%-6s ",$INTF::LoadTableDef{"$TarTableName"}->{RecNum} ; 
			if ( $filesize == $CalFileSize ) {
				$INTF::LoadTableDef{"$TarTableName"}->{FileSize} = $filesize;
				$INTF::LoadTableDef{"$TarTableName"}->{flag} = 0;
				$INTF::LoadTableDef{"$TarTableName"}->{FileList} = "$datafilename1";
				print "[Result]: 文件大小一致, ";
			}else{
				$INTF::LoadTableDef{"$TarTableName"}->{FileSize} = $filesize;
				$INTF::LoadTableDef{"$TarTableName"}->{flag} = 1;
				$INTF::LoadTableDef{"$TarTableName"}->{FileList} = "$datafilename";
			}
		}
		print "\n";
	}
	close(CTL_H);
	#####检查List文件中没有，而所需数据文件存在的
	my $capword;
	foreach $capword (sort keys(%INTF::LoadTableDef)) {
		if ( $capword eq 'LogFileName' ) { next;}
		unless ( defined $INTF::LoadTableDef{"$capword"} ) { next ; }
		unless ( defined $INTF::LoadTableDef{"$capword"}->{flag} ) { next ; }
		unless ( $INTF::LoadTableDef{"$capword"}->{flag} eq -2 ) { next ; }
		my $datafilename1 = $INTF::LoadTableDef{"$capword"}->{format};
		my $datafilehead = substr($datafilename1,0,length($datafilename1)-13);
		$datafilehead = "${datafilehead}$DayOfData";
		
		opendir(DH,"${DataDir}");
		my @FileName = grep { /^${datafilehead}/i } readdir(DH);
		close(DH);
		my $FileName = $FileName[0];
		if ( $#FileName >= 0 ){
			printf "[%8s]: ",$FileName;
			$filesize = (stat("${DataDir}/$FileName"))[7];
			$filerownum = INTF::filecnt("${DataDir}/$FileName");

			printf "[List]:%-9s[Size]:%-9s",$filesize,$filesize ;
			$INTF::LoadTableDef{"$capword"}->{RecNum}=$filerownum;
			printf "[RowNum]:%-6s ",$INTF::LoadTableDef{"$capword"}->{RecNum} ;
				$INTF::LoadTableDef{"$capword"}->{FileSize} = $filesize;
				$INTF::LoadTableDef{"$capword"}->{flag} = 1;
				$INTF::LoadTableDef{"$capword"}->{FileList} = "$FileName";
				print "[Result]: 文件存在且为所需数据 ==$FileName \n";
		}else{
				$INTF::LoadTableDef{"$capword"}->{FileList} = "$datafilename1";
		}
	}	
	return 1;
}

#####################################################################################################
# function main
sub main
{
	my $CurrentDT = $DayOfData;  
	my @SrcTableList;
	my $SrcTableCNT;
	my $retCode;
	my $errNum=0;
	
	ETL::showTime(); print "CurrentDT Date: [ $CurrentDT ]...\n\n";	
	ETL::showTime(); print "Begin checking data status----arrived or not ...\n";	
	my $Data_Arrived_Mark=0;
	my $DataDirectory = "${CTS_SRCFILE_DIR}";

	#####检查数据传输状态，检查检查定义文件是否传到
	#####	对于该系统，有多个list文件，当第一个list文件到达后，就可开始处理
	while ( $Data_Arrived_Mark == ChkDataArrived($DataDirectory)){
		sleep 10;
	}
	#####数据已准备
	#####休眠1分钟
	ETL::showTime(); print "休眠1分钟\n";
	sleep 60;
	ETL::showTime(); print "休眠完成，开始处理数据\n";
	
	##### 建立与ETL Automation Repository的连接
	# Build the connection to ETLDB
	ETL::showTime(); print "Begin to connect ETL Automation Repository... \n";
	my $dbDh = ETL::connectETL(); 
	unless ( defined($dbDh) ) {
		ETL::showTime(); print "ERROR - Unable to connect to ETL Automation repository!\n";
		my $errstr = $DBI::errstr;
		ETL::showTime(); print "$errstr\n";
		return $ETL::FALSE;
	}
	ETL::showTime(); print "Connect ETL Automation Repository Successful \n";

	#####从库表：${ETL_DB}BOCN_CTL_TABLE_DEF中获取数据列表、数据Format(真实文件名称)、检查标志（Chk_Flag）	

	ETL::showTime(); print "Geting Necessary Source Data List \n";
	
	# Get the load table list
	unless(INTF::GetLoadTableList($dbDh,${SUBSYS})){
		ETL::showTime(); print "不能获取加载列表\n";
		return 12;
	}

	ETL::showTime(); print "Necessary Source Data List Gotten \n";
	#####获取数据列表完成

	ETL::showTime(); print "Data arrived \n";

	####开始数据处理
	my $listfilenum = $#CheckFileHead +1;
	while ( $listfilenum > 0 ){
		####Step 1，扫描数据目录中的list文件，如不在%seen中，则将该list文件加入s02_ctl_list中
		unless ( opendir(DATADIR_H,"$DataDirectory")){
			print "ERROR -- Fail to Open Directory $DataDirectory \n";
			return 0;
		}
		my $listfilename='';
		my $findlist=0;
		my @listF;
		#####检索 ${DataDirectory}，找到一个list文件
		while ( $listfilename = readdir(DATADIR_H)){
			if ( $listfilename eq "." || $listfilename eq ".." ) { next; }
		    if ( -d "${DataDirectory}/${listfilename}" ) { next;}
		    unless ( defined $seen{INTF::trimstr($listfilename)} ){ next; }
		    unless ( $seen{INTF::trimstr($listfilename)} ){ next; }
		    $listfilenum -=1;
	    	$findlist = 1;
		    last;
		}
		close(DATADIR_H);

		unless ( $findlist ) {
			sleep 20;
			next;
		}
		####	设置该$seen{$checkfilename}=0
    	$seen{$listfilename}=0;
		
		####Step 2, 打开$checkfilename,确定文件记录数、给定文件长度、实际文件长度，对比文件长度，根据结果设置Flag标志
		unless ( ChkRecData($DataDirectory,$listfilename)){
			print "ERROR -- Failed to deal $listfilename \n";
			return 0;
		}
		####Step 3, 根据flag标志，处理任务所需文件（list文件，Dir文件等)
		unless(INTF::TrigJob($dbDh,$SUBSYS,${CTS_SRCFILE_DIR},$S23_log,$DayOfData,0)){return 0;}
#		unless ( TrigJob($dbDh)){
#			print "ERROR -- Failed to Generate Job File due to $listfilename \n";
#			return 0;
#		}
	}
#	unless ( genempfile($dbDh)){
#		return 0;
#	}
	unless(INTF::TrigJob($dbDh,$SUBSYS,${CTS_SRCFILE_DIR},$S23_log,$DayOfData,1)){return 0;}

#	unless ( chkjobstatus()){
#		return 0;
#	}
	return 1;
}
#####################################################################################################
# program section

# To see if there is one parameter,
# if there is no parameter, exit program
if ( $#ARGV < 0 ) {
   exit(1);
}

# Get the first argument as control file
# The control file: CTL_S02_00_JOB_START_YYYYMMDD.dir
$CONTROL_FILE = $ARGV[0];

my $Control_File_Len=length($CONTROL_FILE);
$DayOfData = substr $CONTROL_FILE, $Control_File_Len-12,8;
$SUBSYS = substr( $CONTROL_FILE,4,3);
$TXDATE = substr($DayOfData,0,4) . "-" . substr($DayOfData,4,2) . "-" . substr($DayOfData,6,2);

$INTF::LoadTableDef{LogFileName} = "$AUTO_LOG/"."S23_${DayOfData}\.log";

# Define the data dictionary of ibs system
$CTS_SRCFILE_DIR = ${CTS_SRCFILE_DIR} . ${DIRDELI} . ${DayOfData};

$S23_log = $INTF::LoadTableDef{LogFileName};

open(STDERR, ">&STDOUT");

print "My process id is $$ \n";

foreach $checkfilename (@CheckFileHead) {
	$checkfilename = "${checkfilename}${DayOfData}.list"; 
	$seen{$checkfilename} = 1 
}

my $rc = main();
print "main() = $rc \n";
if ( $rc != 1 ) {
	exit (1);
} else {
	exit (0);
}
