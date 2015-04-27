#!/usr/bin/perl
#######################################Head Section##################################################
#�������ڣ�2007/04/19                                                                                
#�ű����ܣ�
#	1����鵱��S09����Ŀ¼�������Ƿ�׼���ã�
#	2����ȡ����Դ�����б�
#	3����������ļ���С�Ƿ���ȷ��
#	4�����Դ�����Ƿ��Ѵ���
#	5������List�ļ���Dir�ļ���
#	6��׷��S23_log
#�� �� �ˣ����޷�
#�޸����ڣ�2006/11/03�����Ӳ���BOCN_CTL_LOAD_LOG�����ģ��
#%INTF::LoadTableDef �ṹ˵��
#         ->LogFileName  ���DATA�Ĵ���ɹ������־�ļ���
#		  ->$DataFile	 �����ļ����弰״̬	
#				->format �����ļ�ʵ������
#				->flag	�����ļ���־
#				->Column �����ļ��ֶζ��弰״̬
#					->$ColumnName �ֶ�����
#						->SEQ	�ֶ����
#						->TYPE	�ֶ�����
#						->LEN	�ֶγ���
#						->MARK	�ֶα�־
#				->CURColumn �����ļ��ֶζ��弰״̬
#					->$ColumnName �ֶ�����
#						->SEQ	�ֶ����
#						->TYPE	�ֶ�����
#						->LEN	�ֶγ���
#						->MARK	�ֶα�־
#				->ChkFlag ���ݼ���־
#				->RecNum  �б��ļ������ݼ�¼��
#				->RecLen  �����ļ���ָ����¼����
#				->CalFileSize ��������ݴ�С
#				->FileSize ʵ�ʵ������ļ���С
#
#flag   0---��������������
#		    1---�������ݴ��ڣ����ļ���С�붨�岻һ�£�
#		    2---�������ݴ��ڣ����ļ������б仯��
#		    3---�������ݴ��ڣ����ļ��Ѵ���
#		    -1--�������ݲ����ڣ������б��ļ��д��ڣ�
#		    -2--�������ݲ����ڣ����б��ļ���Ҳ�����ڡ�
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
# function ChkDataArrived--�������״̬��ȷ���Ƿ������Ѵ������
sub ChkDataArrived
{
	my ($DataDirectory) = @_;
#	print "Director $DataDirectory \n";
#	my $DataDirectory = "$s02_Data_Home"."$DateOfData";
	######���Ŀ¼�Ƿ���######
	unless ( -d $DataDirectory ) { 
		ETL::showTime(); print "Ŀ¼��$DataDirectory��δ����\n";
		return 0 ;
	} 
	ETL::showTime(); print "Ŀ¼��$DataDirectory���ѽ�������ʼ���$CheckFileHead[0]\n";
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
		ETL::showTime(); print "����׼�����\n";
		return 1;
	}else{
		return 0;
	}
}

#####################################################################################################
# function ChkRecData--����ļ���С�����壬������flag��ʾ
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
			print "[Result]: ERROR--�����ļ�$datafilename1������ \n";
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
				print "[Result]: �ļ���Сһ��, ";
			}else{
				$INTF::LoadTableDef{"$TarTableName"}->{FileSize} = $filesize;
				$INTF::LoadTableDef{"$TarTableName"}->{flag} = 1;
				$INTF::LoadTableDef{"$TarTableName"}->{FileList} = "$datafilename";
			}
		}
		print "\n";
	}
	close(CTL_H);
	#####���List�ļ���û�У������������ļ����ڵ�
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
				print "[Result]: �ļ�������Ϊ�������� ==$FileName \n";
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

	#####������ݴ���״̬������鶨���ļ��Ƿ񴫵�
	#####	���ڸ�ϵͳ���ж��list�ļ�������һ��list�ļ�����󣬾Ϳɿ�ʼ����
	while ( $Data_Arrived_Mark == ChkDataArrived($DataDirectory)){
		sleep 10;
	}
	#####������׼��
	#####����1����
	ETL::showTime(); print "����1����\n";
	sleep 60;
	ETL::showTime(); print "������ɣ���ʼ��������\n";
	
	##### ������ETL Automation Repository������
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

	#####�ӿ��${ETL_DB}BOCN_CTL_TABLE_DEF�л�ȡ�����б�����Format(��ʵ�ļ�����)������־��Chk_Flag��	

	ETL::showTime(); print "Geting Necessary Source Data List \n";
	
	# Get the load table list
	unless(INTF::GetLoadTableList($dbDh,${SUBSYS})){
		ETL::showTime(); print "���ܻ�ȡ�����б�\n";
		return 12;
	}

	ETL::showTime(); print "Necessary Source Data List Gotten \n";
	#####��ȡ�����б����

	ETL::showTime(); print "Data arrived \n";

	####��ʼ���ݴ���
	my $listfilenum = $#CheckFileHead +1;
	while ( $listfilenum > 0 ){
		####Step 1��ɨ������Ŀ¼�е�list�ļ����粻��%seen�У��򽫸�list�ļ�����s02_ctl_list��
		unless ( opendir(DATADIR_H,"$DataDirectory")){
			print "ERROR -- Fail to Open Directory $DataDirectory \n";
			return 0;
		}
		my $listfilename='';
		my $findlist=0;
		my @listF;
		#####���� ${DataDirectory}���ҵ�һ��list�ļ�
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
		####	���ø�$seen{$checkfilename}=0
    	$seen{$listfilename}=0;
		
		####Step 2, ��$checkfilename,ȷ���ļ���¼���������ļ����ȡ�ʵ���ļ����ȣ��Ա��ļ����ȣ����ݽ������Flag��־
		unless ( ChkRecData($DataDirectory,$listfilename)){
			print "ERROR -- Failed to deal $listfilename \n";
			return 0;
		}
		####Step 3, ����flag��־���������������ļ���list�ļ���Dir�ļ���)
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
