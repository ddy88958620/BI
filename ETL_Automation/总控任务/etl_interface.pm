###############################################################################
# Program: etl_interface.pm
#	  ����ETL �ӿڵĹ���ģ��
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
#		1---�������ݴ��ڣ����ļ���С�붨�岻һ�£�
#		2---�������ݴ��ڣ����ļ������б仯��
#		3---�������ݴ��ڣ����ļ��Ѵ���
#		-1--�������ݲ����ڣ������б��ļ��д��ڣ�
#		-2--�������ݲ����ڣ����б��ļ���Ҳ�����ڡ�
#
#
#####################################################################################################

use strict;

#use DBI;
#use Time::Local;

package INTF;
my %LoadTableDef;
my %SrcTableDef;
my $ETL_DB = $ETL::ETLDB ;
my $FLG_RCV = "${ETL::ETL_RECEIVE}";
###############################################################################
# variable section
###############################################################################
my $os = $^O;

my $DIRDELI = "/";

if ( $os eq "svr4" || $os eq "aix") {
   $DIRDELI = "/";
 }
elsif ( $os eq "mswin32" ) {
   $DIRDELI = "\\";
}

sub trimstr {
    my @out = @_;
    for (@out) {
        s/^\s+//;          # trim left
        s/\s+$//;          # trim right
    }
    return @out == 1 
              ? $out[0]   # only one to return
              : @out;     # or many
}

#####################################################################################################
# function ChkRecData--����flag��ʾ�������ݿ��м�¼������־��������list�ļ���dir�ļ�
sub TrigJob
{
	my ($dbh,$SUBSYS,$SRCFILE_DIR,$LOGFILE,$DayOfData,$EndFlag) = @_;
	unless ( defined $EndFlag ){
		$EndFlag = 1;
	}
	my ($capword,$SrcDataFileName,$SrcListFileName,$SrcCtlFileName);
	
	if ( -f $LOGFILE ) {
#		%INTF::LoadTableDef = INTF::LOG_Filter();
		INTF::LOG_Filter();
	}
	unless (open(S01LOG_H,">>$LOGFILE")){
		ETL::showTime();print "ERROR-- Failed to open $LOGFILE \n";
	}
	my $curtime;
	my $recode=1;
	foreach $capword (sort keys(%INTF::LoadTableDef)) {
#		$SrcDataFileName = $DataFileFormat{$capword};
		if ( $capword eq 'LogFileName' ) { next;}
		unless ( defined $INTF::LoadTableDef{"$capword"} ) { next ; }
		unless ( defined $INTF::LoadTableDef{"$capword"}->{flag} ) { next ; }
		$SrcDataFileName = $INTF::LoadTableDef{"$capword"}->{FileList};
		if ($INTF::LoadTableDef{"$capword"}->{flag} != 0 ) {
			unless ( $EndFlag ) {next;}
			if ( $INTF::LoadTableDef{"$capword"}->{flag} == 1 ){
				print "[RECEIVE]: $SRCFILE_DIR$DIRDELI$SrcDataFileName \n";
				unless( INTF::WriteDataLog($dbh,1,$DayOfData,$SUBSYS,$capword)){
					print "ERROR -- Fail to write log \n";
				}
				print "[ERROR]: - File Corrupt - $SrcDataFileName \n";
				$recode = 0;
			}elsif( $INTF::LoadTableDef{"$capword"}->{flag} == 2 ){
				print "[RECEIVE]: $SRCFILE_DIR$DIRDELI$SrcDataFileName \n";
				unless( INTF::WriteDataLog($dbh,2,$DayOfData,$SUBSYS,$capword)){
					print "ERROR -- Fail to write log \n";
				}
				print "[ERROR]: - File Definition MisMatch -$SrcDataFileName \n";
				$recode = 0;
			}elsif( $INTF::LoadTableDef{"$capword"}->{flag} == -1 ){
				print "[RECEIVE]: $SRCFILE_DIR$DIRDELI$SrcDataFileName \n";
				unless( INTF::WriteDataLog($dbh,-1,$DayOfData,$SUBSYS,$capword)){
					print "ERROR -- Fail to write log \n";
				}
				print "[ERROR]: - File Not Deliver -$SrcDataFileName \n";
				$recode = 0;
			}elsif( $INTF::LoadTableDef{"$capword"}->{flag} == -2 ){
#				print "[RECEIVE]: $SRCFILE_DIR$DIRDELI$SrcDataFileName \n";
				print "[NO RECEIVE]: $capword \n";
				unless( INTF::WriteDataLog($dbh,-2,$DayOfData,$SUBSYS,$capword)){
					print "ERROR -- Fail to write log \n";
				}
				if ( $INTF::LoadTableDef{"$capword"}->{ChkFlag} == 1 ){
					print "[ERROR]: - File Miss - $SrcDataFileName \n";
					$recode = 0;
				}else{
					print "[Message]: - Generate Empty File ControlFile - $capword \n";
					$SrcListFileName=uc($SUBSYS).'_'.uc($SUBSYS).'_'.$capword ."_".$DayOfData.".lst";
					$SrcCtlFileName=lc($SUBSYS).'_' . $capword."$DayOfData";
			        unless ( INTF::GenListFile("$FLG_RCV", "$SRCFILE_DIR$DIRDELI$SrcDataFileName", "$SrcListFileName","N")){
						print "ERROR - Failed to generate list file for $SrcDataFileName ! \n";
						next;
					}
					unless ( INTF::GenSingleCtlFile("$FLG_RCV", "$SrcListFileName", "$SrcCtlFileName")){
						print "ERROR - Failed to generate Control File of $SrcCtlFileName !\n";
						next;
					}
				   my ($sec,$min,$hour,$mday,$mon,$year1,$wday,$yday,$isdst) = localtime(time());
				   my $current = "";
				   
				   $hour = sprintf("%02d", $hour);
				   $min  = sprintf("%02d", $min);
				   $sec  = sprintf("%02d", $sec);
				   $year1 = $year1 + 1900;
				   $current = "$year1-$mon-$mday ${hour}:${min}:${sec}";
					print S01LOG_H "$current $capword 0	$DayOfData \n";
				}
			}
			next;
		}		
		#$DataFileFormat{$capword}Ϊͨ�������ļ��ҵ���Ӧ�ı�
		$SrcListFileName=uc($SUBSYS).'_'.uc($SUBSYS).'_'.$capword ."_".$DayOfData.".lst";
		$SrcCtlFileName=lc($SUBSYS).'_' . $capword."$DayOfData";
        unless ( INTF::GenListFile("$FLG_RCV", "$SRCFILE_DIR$DIRDELI$SrcDataFileName", "$SrcListFileName","Y")){
			print "ERROR - Failed to generate list file for $SrcDataFileName !\n";
			next;
		}
		unless ( INTF::GenSingleCtlFile("$FLG_RCV", "$SrcListFileName", "$SrcCtlFileName")){
			print "ERROR - Failed to generate Control File of $SrcCtlFileName !\n";
			next;
		}
		print "[RECEIVE]: $SRCFILE_DIR$DIRDELI$SrcDataFileName \n";
		unless( INTF::WriteDataLog($dbh,0,$DayOfData,$SUBSYS,$capword)){
			print "ERROR -- Fail to write log \n";
		}
		
	   my ($sec,$min,$hour,$mday,$mon,$year1,$wday,$yday,$isdst) = localtime(time());
	   my $current = "";
	   
	   $hour = sprintf("%02d", $hour);
	   $min  = sprintf("%02d", $min);
	   $sec  = sprintf("%02d", $sec);
	   $year1 = $year1 + 1900;
	   $current = "$year1-$mon-$mday ${hour}:${min}:${sec}";
		print S01LOG_H "$current $capword ".$INTF::LoadTableDef{"$capword"}->{RecNum}."		$DayOfData \n";
	}
	close(S01LOG_H);
	return $recode;		
}

sub LOG_Filter
{
#	my ( %INTF::LoadTableDef) = @_;
	my $logfile = $INTF::LoadTableDef{LogFileName};
	unless (open(LOG_H,"$logfile")){
		ETL::showTime();print "ERROR-- Failed to open $logfile \n";
		return %INTF::LoadTableDef;
	}
	my ($TBName);
	while ( my $tabstr = <LOG_H> ){
		$TBName = INTF::trimstr((split(' ',$tabstr))[2]);
		#�ļ�������flag=-1,-2 next
		unless ( defined $INTF::LoadTableDef{"$TBName"} ){next;}
		#�ļ�����flag=1,2,3 next
		unless ( defined $INTF::LoadTableDef{"$TBName"}->{flag} ){next;}
		#�ļ�����flag=0,�޸�flag=3
		$INTF::LoadTableDef{"$TBName"}->{flag}=3;
	}
#	return %INTF::LoadTableDef;
}

#####################################################################################################
# function GenListFile

sub GenListFile
{
    my ($srcdir,$fulldatafile, $listfile, $Flag) = @_;

    unless(open(OUTFILE, ">$srcdir$DIRDELI$listfile")) {
            ETL::showTime();print STDOUT "Can not create file: $srcdir$DIRDELI$listfile \n";
            return ${ETL::FALSE};
    }

	if ($Flag eq "Y"){
		print OUTFILE "$fulldatafile\n";		
	}
	else{
		print OUTFILE "";	
	}
	
	close(OUTFILE);	 
	return ${ETL::TRUE};
}

######################################################################################################
## function GenSingleCTLFile
sub GenSingleCtlFile
{
	my ($srcdir, $listfilename, $filename) = @_;	

	my @filename = split /\s+/, $filename ;

	my (@fields,$basefn);
	@fields = split(/\./, $filename[0]);
	$basefn = $fields[0];
	$basefn =~ tr [A-Z][a-z];

	my $FileTempPrefix="tmp.dir.";
	my $FilePrefix="dir.";
	my $ListFileSize = (stat("$srcdir$DIRDELI$listfilename"))[7];
	
	unless(open(OUTFILE, ">$srcdir$DIRDELI$FileTempPrefix$basefn")) {
		ETL::showTime();print STDOUT "Can not open file $srcdir$DIRDELI$FileTempPrefix$basefn\n";
		return ${ETL::FALSE};
	}
	print OUTFILE "$listfilename  $ListFileSize \n";
	close(OUTFILE);
			
	unless( rename "$srcdir$DIRDELI$FileTempPrefix$basefn","$srcdir$DIRDELI$FilePrefix$basefn" ){
		ETL::showTime();print "ERROR--Generate control file: dir.$basefn fail! \n";
		return ${ETL::FALSE};
	}
	 
	return ${ETL::TRUE};

}

#####################################################################################################
#Get The Table List which should be loaded into Teradata and the format type of source data
sub GetLoadTableList
{
	my ($dbh,$SUBSYS)=@_;
	
	unless(defined ($dbh)){
		ETL::showTime(); print "****[INTF::GetLoadTableList]Error:can not connect the etl repository****\n";
		return 0;
	}

	my $sqlText = "SELECT Trim(Source_Table_Name),Trim(DataFileFormat),cast(Check_Flag as char(1)) FROM ${ETL_DB}BOCN_CTL_TABLE_DEF \
	       WHERE Source_System='${SUBSYS}' ORDER BY 1";
	my $sth = $dbh->prepare($sqlText) or return 0;

	my $ret = $sth->execute();

	my ($SrcTabName,$DatFileFormat,$ChkFlag);
	
	my $n=0;

	while (($SrcTabName,$DatFileFormat,$ChkFlag) = $sth->fetchrow()) {
	       	$INTF::LoadTableDef{$SrcTabName}->{flag} = -2;
	       	$INTF::LoadTableDef{$SrcTabName}->{format} = "$DatFileFormat"; 
	       	$INTF::LoadTableDef{$SrcTabName}->{ChkFlag} = "$ChkFlag"; 
	       	$INTF::SrcTableDef{$DatFileFormat}->{tartable} = "$SrcTabName";
	       	$n +=1;
#	       	print "==> $SUBSYS == $n $SrcTabName,$DatFileFormat,$ChkFlag  \n";
	}
	
	$sth->finish();
	
	if ( $n == 0 ){
		return 0;
	}
	return 1;
}

sub WriteDataLog
{
	my ($dbh,$flag,$DayOfData,$SUBSYS,$TableName) = @_;
	unless(defined ($dbh)){
		ETL::showTime(); print "****[CheckDataFile]Error:can not connect the etl repository****\n";
		return undef;
	}

	my $sth;
	my $sqlRET;

	my $istSQL;
	my $Header = "$TableName" . "_" . "$DayOfData" . ".dat"; 
	$Header =~ tr [a-z][A-Z];

#	ETL::showTime(); print "Delete the log row:[$SUBSYS][$DayOfData]...";
	my $delSQL = "DELETE FROM ${ETL_DB}BOCN_CTL_LOAD_LOG ".
					"WHERE TXDATE = CAST('${DayOfData}' AS DATE FORMAT 'YYYYMMDD') ".
					"AND Source_System = '$SUBSYS'".
					"AND Source_Table_Name = '$TableName'";
	
	#print "$delSQL \n";
	$sth = $dbh->prepare($delSQL) or return undef;
	$sqlRET = $sth->execute();
	$sth->finish();
#	print "[OK] \n";	

	my $datafile = $INTF::LoadTableDef{$TableName}->{FileList};
#	$datafile=~ tr [a-z][A-Z];
#	if ( uc($SUBSYS) ne 'S01' ) {
#		$datafile = $datafile . "_" . $DayOfData . ".dat";
#	}
	my $DataFileSize = $INTF::LoadTableDef{$TableName}->{FileSize};
	my $DataFileRowCnt = $INTF::LoadTableDef{$TableName}->{RecNum};
	if ( $flag == 0 ){
		$istSQL = "INSERT INTO  ${ETL_DB}BOCN_CTL_LOAD_LOG(             ".
						    "TXDATE                                     ".
						    ",Source_System                             ".
						    ",Source_Table_Name                         ".
						    ",Data_File                                 ".
						    ",Data_File_Size                            ".
						    ",Data_FIle_Row_CNT)                        ".
						    " VALUES ('${DayOfData}'                    ".
						    "         ,'${SUBSYS}'                      ".
						    "         ,'${TableName}'					".
						    "         ,'${datafile}'                  	 ".
						    "         ,$DataFileSize	                 ".
						    "         ,$DataFileRowCnt	               )";        
		#print "$istSQL \t";
		print "[Result]\tһ��\n";
	}elsif( $flag == 1 ){
		$istSQL = "INSERT INTO  ${ETL_DB}BOCN_CTL_LOAD_LOG(              ".
						    "TXDATE                                      ".
						    ",Source_System                              ".
						    ",Source_Table_Name                          ".
						    ",Data_File)                                 ".
						    " VALUES ('${DayOfData}'                     ".
						    "         ,'${SUBSYS}'                       ".
						    "         ,'${TableName}'					".
						    "         ,'${datafile} size wrong'         )";
				print "[Result]\t�ļ����Ȳ�һ��\n";
	}elsif( $flag == 2 ){
		$istSQL = "INSERT INTO  ${ETL_DB}BOCN_CTL_LOAD_LOG(              ".
						    "TXDATE                                      ".
						    ",Source_System                              ".
						    ",Source_Table_Name                          ".
						    ",Data_File)                                 ".
						    " VALUES ('${DayOfData}'                     ".
						    "         ,'${SUBSYS}'                       ".
						    "         ,'${TableName}'					".
						    "         ,'${datafile} definition wrong'   )";
				print "[Result]\t�ļ����岻һ��\n";
	}elsif( $flag == -1 ){
		$istSQL = "INSERT INTO  ${ETL_DB}BOCN_CTL_LOAD_LOG(                  ".
							    "TXDATE                                      ".
							    ",Source_System                              ".
							    ",Source_Table_Name                          ".
							    ",Data_File)                                 ".
							    " VALUES ('${DayOfData}'                     ".
							    "         ,'${SUBSYS}'                       ".
							    "         ,'${TableName}'					".
							    "         ,'data file in list but not have' )";
		print "[Result]\t��List�ļ��д��ڣ����������ļ�--[$TableName]: \n";
	}elsif( $flag == -2 ){
		$istSQL = "INSERT INTO  ${ETL_DB}BOCN_CTL_LOAD_LOG(                      ".
								    "TXDATE                                      ".
								    ",Source_System                              ".
								    ",Source_Table_Name                          ".
								    ",Data_File)                                 ".
								    " VALUES ('${DayOfData}'                     ".
								    "         ,'${SUBSYS}'                       ".
								    "         ,'${TableName}'					".
								    "         ,'No data file in list'          ) ";
		print "[Result]\t��LIST�в�����--[$TableName] \n";
	}
	$sth = $dbh->prepare($istSQL) or return 0;
	$sqlRET = $sth->execute();
	#print " $sqlRET\n";
	$sth->finish() or return 0;
	return 1;
}
sub filecnt
{
	my ( $file ) = @_;
	my $count=0;
	open(FILE, "<", $file) or die "can't open $file: $!";
	$count++ while <FILE>;
	# $count now holds the number of lines read
	return $count;
}


# Don't remove the below line, otherwise, the other perl program
# which require this file will be terminated,
# it has to be true value at the last line.
1;

__END__
