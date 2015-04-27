#!/usr/bin/perl
###############################################################################
# Program  : etlslave_unix.pl
# Argument : (ControlFile)
#            This program need one argument.
#	Modified : 2009-3-25 15:18 ER1 export with execute-code 
#					   ExportMultiloadET & ExportFastLoadER1
###############################################################################

use strict;
use DBI;

my $VERSION = "v2.6.1";

my $home = $ENV{"AUTO_HOME"};

unshift(@INC, "$home/bin");

require etl_unix;

my $LOGDIR;
my $LOGFILE;
my $TODAY;

my $TRUE  = 1;
my $FALSE = 0;

my $STOP_FLAG = 0;
my $LOCK_FILE;

my $ControlFile;
my ($Sys, $Job, $AutoOff, $JobType, $TxDate, $HeadJobGroup, $AutoOnChild);
my $JobSessionID;
my $AppPath;
my $AppBINPath;
my $AppDDLPath;

my $SyntaxMode = 0;
my $RunDownstreamFlag = 1;
my $RunGroupFlag = 1;

my @dirFileList;
my $dbCon;

my @dataFileList;

my $value;

$value = $ENV{"AUTO_DATA_RECEIVE"};
if ( defined($value) ) {
   $ETL::ETL_RECEIVE = $value;
}

$value = $ENV{"AUTO_DATA_QUEUE"};
if ( defined($value) ) {
   $ETL::ETL_QUEUE = $value;
}

$value = $ENV{"AUTO_DATA_PROCESS"};
if ( defined($value) ) {
   $ETL::ETL_PROCESS = $value;
}

$value = $ENV{"AUTO_DATA_COMPLETE"};
if ( defined($value) ) {
   $ETL::ETL_COMPLETE = $value;
}

$value = $ENV{"AUTO_DATA_FAIL"};
if ( defined($value) ) {
   $ETL::ETL_FAIL = $value;
}

$value = $ENV{"AUTO_DATA_ERROR"};
if ( defined($value) ) {
   $ETL::ETL_ERROR = $value;
}

# Create log file for this program
sub createLogFile
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   
   $year  += 1900;
   $mon    = sprintf("%02d", $mon + 1);
   $mday   = sprintf("%02d", $mday);
   $TODAY  = "${year}${mon}${mday}";

   # To check the LOG directory is exist or not
   # If it is not exist then create the directory   
   if ( ! -d ${ETL::ETL_LOG} ) {
      ETL::createDirectory($ETL::ETL_LOG);
   }

   $LOGDIR = "${ETL::ETL_LOG}/${Sys}";
   # To check the system's log directory is exist or not
   # If it is not exist then create the directory
   if ( ! -d ${LOGDIR} ) {
      ETL::createDirectory($LOGDIR);
   }

   $LOGDIR = "${LOGDIR}/${TODAY}";
   # To check the system today log directory is exist or not
   # If it is not exist then create the directory
   if ( ! -d ${LOGDIR} ) {
      ETL::createDirectory($LOGDIR);
   }

   my $confile = substr($ControlFile, 0, length($ControlFile) - 4);
   $LOGFILE = "${LOGDIR}/${confile}.${JobSessionID}.log";
   
   unless ( open(LOGF_H, ">>${LOGFILE}") ) {
      return $FALSE;
   }

   select (LOGF_H);
   $| = 1;

   return $TRUE;
}

sub moveDataFileToError
{
   my ($oldpath, $newpath, $syspath);
   my $file;

   ETL::showPrefixSpace(); print "Move data file(s) to error directory...\n";

   $oldpath = "${ETL::ETL_PROCESS}";
   $syspath = "${ETL::ETL_ERROR}/${Sys}";
   $newpath = "${syspath}/${TODAY}";

   unless ( -d $syspath ) {
      ETL::createDirectory($syspath);   	
   }

   unless ( -d $newpath ) {
      ETL::createDirectory($newpath);
   }

   my $count = $#dataFileList;

   for (my $n=0; $n <= $count; $n++) {
      $file = $dataFileList[$n];

      $file = ETL::cutLeadingTrailSpace($file);
      if ( "$file" eq "" ) { next; }

      ETL::showPrefixSpace(); print "Move '$file' to error directory...\n";
      ETL::moveFile("$oldpath/$file", "$newpath/$file");
      ETL::updateFileLocation($dbCon, $Sys, $Job, $file, $newpath);
   }

   return $TRUE;
}

sub moveControlFileToError
{
   my ($controlFile) = @_;
   my ($oldpath, $newpath);
   
   $oldpath = "${ETL::ETL_PROCESS}/${controlFile}";
   $newpath = "${ETL::ETL_ERROR}/${Sys}/${TODAY}/${controlFile}";
   
   ETL::showPrefixSpace(); print "Move '$controlFile' to error directory.\n";
   ETL::moveFile($oldpath, $newpath);
}

sub moveDataFileToComplete
{
   my ($oldpath, $newpath, $syspath);
   my $file;

   ETL::showPrefixSpace(); print "Move data file(s) to complete directory...\n";

   $oldpath = "${ETL::ETL_PROCESS}";
   $syspath = "${ETL::ETL_COMPLETE}/${Sys}";
   $newpath = "${syspath}/${TODAY}";

   unless ( -d $syspath ) {
      ETL::createDirectory($syspath);   	
   }

   unless ( -d $newpath ) {
      ETL::createDirectory($newpath);
   }

   my $count = $#dataFileList;

   for (my $n=0; $n <= $count; $n++) {
      $file = $dataFileList[$n];

      $file = ETL::cutLeadingTrailSpace($file);
      if ( "$file" eq "" ) { next; }

      ETL::showPrefixSpace(); print "Move '$file' to complete directory...\n";
      ETL::moveFile("$oldpath/$file", "$newpath/$file");     
      ETL::updateFileLocation($dbCon, $Sys, $Job, $file, $newpath);
   }

   return $TRUE;
}

sub moveControlFileToComplete
{
   my ($controlFile) = @_;
   my ($oldpath, $newpath);
   
   $oldpath = "${ETL::ETL_PROCESS}/${controlFile}";
   $newpath = "${ETL::ETL_COMPLETE}/${Sys}/${TODAY}/${controlFile}";
   
   ETL::showPrefixSpace(); print "Move '$controlFile' to complete directory.\n";
   ETL::moveFile($oldpath, $newpath);
}

sub parseLogFile
{
   my ($logFile) = @_;
   my ($num_ins, $num_dup, $num_upd, $num_del, $num_out);
   my ($num_er1, $num_er2, $num_et, $num_uv);
   my $type = "";

   $num_ins = 0; $num_dup = 0; $num_upd = 0; $num_del = 0, $num_out = 0;
   $num_er1 = 0; $num_er2 = 0; $num_et  = 0; $num_uv  = 0;
 
   unless ( open(MRESULT, "$logFile") ) {
      return ($type,$num_ins,$num_dup,$num_upd,$num_del,$num_out,$num_et,$num_uv,$num_er1,$num_er2);
   }

   my $line;
   my $typeflag = 0;

   while ( $line = <MRESULT> ) {
      if ( $typeflag == 0 ) {
         my $buf = $line;
         $buf =~ tr/[a-z]/[A-Z]/;

         if ( index($buf, "UTILITY") > 0)
         {
            $type = (split(' ', $buf))[1];
            $typeflag = 1;
            next;
         }
      }

      if ( $type eq "FASTLOAD" ) {
         if ( index($line, "Total Inserts Applied") > 0 ) {
            $num_ins = (split(' ', $line))[4];
         }
         elsif ( index($line, "Total Duplicate Rows") > 0 ) {
            $num_dup = (split(' ', $line))[4];
         }
         elsif ( index($line, "Total Error Table 1") > 0 ) {
            $num_er1 = (split(' ', $line))[5];
         }
         elsif ( index($line, "Total Error Table 2") > 0 ) {
            $num_er2 = (split(' ', $line))[5];
         }
      }
      elsif ( $type eq "MULTILOAD" ) {
         if ( substr($line, 0, 16) eq "        Inserts:" ) {
            $num_ins = (split(' ', $line))[1];
         } 
         elsif ( substr($line, 0, 16) eq "        Updates:" ) {
            $num_upd = (split(' ', $line))[1];
         }
         elsif ( substr($line, 0, 16) eq "        Deletes:" ) {
            $num_del = (split(' ', $line))[1];
         }
         elsif ( substr($line, 0, 37) eq "     Number of Rows  Error Table Name" ) {
            my $buf1 = <MRESULT>;
            my $etline = <MRESULT>;
            $num_et = (split(' ', $etline))[0];
            my $uvline = <MRESULT>;
            $num_uv = (split(' ', $uvline))[0];
         }
      }
   }
   close(MRESULT);

   return ($type,$num_ins,$num_dup,$num_upd,$num_del,$num_out,$num_et,$num_uv,$num_er1,$num_er2);
}

sub exportFastloadE1
{
   my ($logFile) = @_;
   my $er1_table = "";

   ETL::showTime(); print "ER1 TABLE has record, we export it to file\n";

   unless ( open(MRESULT, "$logFile") ) {
      ETL::showPrefixSpace(); print "ERROR - Unable to open the log file '$logFile'\n";
      return $FALSE;
   }

   my $line;
   my $buf;
   
   while ( $line = <MRESULT> ) {
      $buf = $line;
      $buf =~ tr/[a-z]/[A-Z]/;

      if ( index($buf, "ERRORFILES") > 0) {
         $buf = (split(' ', $buf))[1];
         $er1_table = (split(',', $buf))[0];
      }
   }
   close(MRESULT);

   ETL::showPrefixSpace(); print "ER1 TABLE is $er1_table\n";
   
   if ( "$er1_table" eq "" ) {
      ETL::showPrefixSpace(); print "ERROR - Unable to file ER1 table name\n";
      return $FALSE;
   }
   
   my $cmd;
   my $outfile;
   
   if ( ! -d "${ETL::ETL_ERROR}/${Sys}" ) {
      ETL::createDirectory("${ETL::ETL_ERROR}/${Sys}");
   }
   
   if ( ! -d "${ETL::ETL_ERROR}/${Sys}/${TODAY}" ) {
      ETL::createDirectory("${ETL::ETL_ERROR}/${Sys}/${TODAY}");
   }
   
   
   $outfile = "${ETL::ETL_ERROR}/${Sys}/${TODAY}/${Job}.${JobSessionID}.ER1";
   
   $cmd = "${ETL::ETL_BIN}/ExportFastLoadER1 \"${er1_table}\" \"${outfile}\"";

   ETL::showPrefixSpace(); print "$cmd\n";
   
   system("$cmd");

   return $TRUE;
}

sub exportMultiloadET
{
   my ($logFile) = @_;
   my $et_table = "";

   ETL::showTime(); print "Multiload E1 TABLE has record, we export it to file\n";

   unless ( open(MRESULT, "$logFile") ) {
      ETL::showPrefixSpace(); print "ERROR - Unable to open the log file '$logFile'\n";
      return $FALSE;
   }

   my $line;
   my $buf;
   
   while ( $line = <MRESULT> ) {
      $buf = $line;
      $buf =~ tr/[a-z]/[A-Z]/;

      if ( index($buf, "ERRORTABLES") > 0) {
         $buf = (split(' ', $buf))[1];
         $et_table = (split(',', $buf))[0];
      }
   }
   close(MRESULT);

   ETL::showPrefixSpace(); print "ET TABLE is $et_table\n";
   
   if ( "$et_table" eq "" ) {
      ETL::showPrefixSpace(); print "ERROR - Unable to file E1 table name\n";
      return $FALSE;
   }
   
   my $cmd;
   my $outfile;
   
   if ( ! -d "${ETL::ETL_ERROR}/${Sys}" ) {
      ETL::createDirectory("${ETL::ETL_ERROR}/${Sys}");
   }
   
   if ( ! -d "${ETL::ETL_ERROR}/${Sys}/${TODAY}" ) {
      ETL::createDirectory("${ETL::ETL_ERROR}/${Sys}/${TODAY}");
   }
   
   
   $outfile = "${ETL::ETL_ERROR}/${Sys}/${TODAY}/${Job}.${JobSessionID}.E1";

   $cmd = "${ETL::ETL_BIN}/ExportMultiloadET \"${et_table}\" \"${outfile}\"";

   ETL::showPrefixSpace(); print "$cmd\n";
 
   system("$cmd");

   return $TRUE;
}

sub insertRecordLog
{
   my ($dbh, $sys, $job, $sessionid, $num_ins, $num_upd, $num_del,
       $num_dup, $num_out, $num_et, $num_uv, $num_er1, $num_er2) = @_;

   my $curtime = ETL::getCurrentDateTime();
   
   my $sqlText = "INSERT INTO ${ETL::ETLDB}ETL_Record_Log" .
                 "       (ETL_System, ETL_Job, JobSessionID, RecordTime," .
                 "        InsertedRecord, UpdatedRecord, DeletedRecord," .
                 "        DuplicateRecord, OutputRecord, ETRecord, UVRecord," .
                 "        ER1Record, ER2Record)" .
                 "  VALUES ( '$sys', '$job', $sessionid, '$curtime', " .
                 "       $num_ins, $num_upd, $num_del, $num_dup, $num_out," .
                 "       $num_et, $num_uv, $num_er1, $num_er2)";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub clearRecordLog
{
   my ($dbh, $sys, $job, $sessionid) = @_;
   
   my $sqlText = "DELETE FROM ${ETL::ETLDB}ETL_Record_Log" .
                 " WHERE ETL_System = '$sys'" .
                 "   AND ETL_Job = '$job'" .
                 "   AND JobSessionID = $sessionid";

   my $sth = $dbh->prepare($sqlText) or return $FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub updateRecordLog
{
   my ($logFile, $status, $retcode, $checkflag) =  @_;
   my $txdate = ETL::formatTXDate($TxDate);

   ETL::showPrefixSpace(); print "Parse loading log file to find the record information\n";

   my ($loadtype, $num_ins, $num_dup, $num_upd, $num_del, $num_out,
       $num_et, $num_uv, $num_er1, $num_er2) = parseLogFile($logFile);

   ETL::showPrefixSpace(); print "Insert the record information into repository\n";

   clearRecordLog($dbCon, $Sys, $Job, $JobSessionID);
   insertRecordLog($dbCon, $Sys, $Job, $JobSessionID, $num_ins, $num_upd, $num_del,
                   $num_dup, $num_out, $num_et, $num_uv, $num_er1, $num_er2);

   # If the error table 1 has record,
   # we export the data into file for later checking               
   if ( $num_er1 > 0 ) {
      exportFastloadE1($logFile);
   }

   # If the multiload error table has record,
   # we export the data into file for later checking   
   if ( $num_et > 0 ) {
      exportMultiloadET($logFile);	
   }

   # If there is any error record count greater than 0
   # we send the message notification to notify user
   if ( $num_dup > 0 || $num_et > 0 || $num_uv > 0 ||
        $num_er1 > 0 || $num_er2 > 0 ) {
      # generate message file
      my $content;
      $content = "Duplicate Record = $num_dup\n" .
                 "Multiload ET record = $num_et\n" .
                 "Multiload UV record = $num_uv\n" .
                 "Fastload ER1 record = $num_er1\n" .
                 "Fastload ER2 record = $num_er2\n" ;
      
      writeMessageNotification("RecordError",
                            "Job [$Sys,$Job] has error record",
                            $content,
                            "");      
   }
}

sub writeMessageNotification
{
   my ($type, $subject, $content, $jobLogFile) = @_;
   my $msgControlFile;
   my $current;
   
   $current = ETL::getCurrentDateTime1();
   
   $msgControlFile = "${current}_$$.msg";

   ETL::showPrefixSpace(); print "write a message control file ${msgControlFile}\n";
      
   unless ( open(MSGCONTROL, ">${ETL::ETL_MESSAGE}/${msgControlFile}") ) {
      ETL::showPrefixSpace(); print "[ERROR] cannot open message control file\n";
      return $FALSE;
   }
   
   my $txdate = ETL::formatTXDate($TxDate);

   print MSGCONTROL "Automation Message Notification\n";
   print MSGCONTROL "SYSTEM: ${Sys}\n";
   print MSGCONTROL "JOB: ${Job}\n";  
   print MSGCONTROL "TXDATE: ${txdate}\n";    
   print MSGCONTROL "TYPE: ${type}\n";
   print MSGCONTROL "ATT: ${jobLogFile}\n";
   print MSGCONTROL "SUBJECT: ${subject}\n";
   print MSGCONTROL "CONTENT: ${content}\n";
   
   close(MSGCONTROL);
      
   return $TRUE;
}

sub getJobRunningServer
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;
      
   my $sqlText = "SELECT ETL_Server FROM ${ETL::ETLDB}ETL_Job" .
                " WHERE ETL_System = '$sys'" .
                "   AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) { return ""; }

   $sth->execute();
    
   @tabrow = $sth->fetchrow();

   $sth->finish();

   return $tabrow[0];
}

sub putJobIntoQueue
{
   my ($dbh, $server, $sys, $job, $txdate) = @_;
   my ($seqid, $reqtime);
   my $sqlText;
   my $ret;
   my @tabrow;

   $txdate = ETL::formatTXDate($txdate);

   $sqlText = "SELECT MAX(SeqID) FROM ${ETL::ETLDB}ETL_Job_Queue";
   
   my $sth = $dbh->prepare($sqlText);

   unless ($sth) { return $FALSE; }

   $sth->execute();
   @tabrow = $sth->fetchrow();
   $sth->finish();
   
   $seqid = $tabrow[0];
   if ("$seqid" eq "") {
      $seqid = 0;
   }
      
   $seqid++;
   $reqtime = ETL::getCurrentDateTime();

   $sqlText = "INSERT INTO ${ETL::ETLDB}ETL_Job_Queue" .
              "       (ETL_Server, SeqID, ETL_System, ETL_Job, TXDate, RequestTime)" .
              "  VALUES ( '$server', $seqid, '$sys', '$job', '$txdate', '$reqtime')";
                 
   $sth = $dbh->prepare($sqlText) or return $FALSE;
   $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub invokeJobPerl
{
   my ($scriptFile, $controlFile) = @_;
   my $ret;
   my $job = "";
   my $dataFile;

   my $fileseq = substr($scriptFile, length($scriptFile) - 7, 4);

   my $txdate = ETL::formatTXDate($TxDate);

   my $logfile = "${LOGDIR}/${scriptFile}.${JobSessionID}.log";
   
   $job = "perl ${AppBINPath}/${scriptFile} $controlFile > $logfile";

   ETL::showPrefixSpace(); print "${job}\n";

   $ret = system($job);
   
   $ret = $ret >> 8;

   ETL::showPrefixSpace(); print "Return Code is $ret\n";

   if ( $fileseq eq "0100" ) {
      if ( $ret == 0 ) {
         updateRecordLog($logfile, "Done", $ret, "Y");
      }
   }

   return $ret;
}

sub invokeJobShell
{
   my ($shellFile) = @_;
   my $ret;
   my $job = "";  
   my $dataFile;

   my $fileseq = substr($shellFile, length($shellFile) - 7, 4);

   for (my $n=0; $n <= $#dataFileList; $n++) {
      $dataFile = $dataFileList[$n];
      chomp($dataFile);

      $job = $job . $dataFile;
      $job = $job . " ";
   }

   my $txdate = ETL::formatTXDate($TxDate);
   my $logfile = "${LOGDIR}/${shellFile}.${JobSessionID}.log";
   
   if ( $fileseq eq "0100" ) {
      $job = "${AppBINPath}/${shellFile} " . $job . " >$logfile 2>&1";
   }
   else {
      $job = "${AppBINPath}/${shellFile} " . "$txdate " . $job . " >$logfile 2>&1";
   }

   $ret = system($job);

   $ret = $ret >> 8;
   
   ETL::showPrefixSpace(); print "Return Code is $ret\n";

   if ( $fileseq eq "0100" ) {
      if ( $ret == 0 ) {
         updateRecordLog($logfile, "Done", $ret, "Y");
      }
   }

   return $ret;
}

sub getDataFileList
{
   my ($controlFile) = @_;
   my @fields;

   unless ( open(CTRLFILEH, "${ETL::ETL_PROCESS}/$controlFile") ) {
      ETL::showPrefixSpace(); print "ERROR - Can not open control file\n";
      return $FALSE;
   }

   my $file;
   my $n = 0;
   while($file=<CTRLFILEH>) {
      $file = ETL::cutLeadingTrailSpace($file);
      if ("$file" eq "") { next; }
      
      @fields = split(/\s+/, $file);
      $dataFileList[$n++] = $fields[0];
   }
   
   close(CTRLFILEH);

   return $TRUE;
}

sub runPrefixScript
{
   my $job;
   my $ret;

   # To solve the too many data file problem,
   # If there is merge_file.sh exist under the application's bin directory
   # we run it first!
   if ( -s "${AppBINPath}/merge_file.sh" ) {
      $job = "${AppBINPath}/merge_file.sh $ControlFile >>$LOGFILE 2>&1";

      ETL::showPrefixSpace(); print ("Run merge_file.sh first!\n");

      $ret = system($job);
   }
}

# This function is particularly for trigger a virtual downstream job.
# For a virtual job, we don't write a control file into receive directory.
# Instead, we invoke it directly by running another etlslave program.
sub triggerVirtualJob
{
   my ($dbh, $txdate, $sys, $job) = @_;
   my @Tabrow;

   my $sqlText = "SELECT Conv_File_Head FROM ${ETL::ETLDB}ETL_Job_Source" .
                 "   WHERE ETL_System = '$sys'" .
                 "     AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return -1;
   }

   $sth->execute();
   @Tabrow = $sth->fetchrow();
   $sth->finish();

   unless( @Tabrow ) {
      return $FALSE;
   }

   my $controlfile;
   $controlfile = $sys . "_" . $Tabrow[0] . "_" . $txdate . ".dir";
   
   ETL::invokeJob($controlfile);
}

sub updateJobTraceStart
{
   my ($dbh, $sys, $job, $txdate, $starttime) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job_Trace" .
                 " SET JobStatus = 'Running', StartTime = '$starttime'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "     AND TXDate = '$txdate'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub updateJobTraceEnd
{
   my ($dbh, $sys, $job, $txdate, $status, $endtime) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job_Trace" .
                 " SET JobStatus = '$status', EndTime = '$endtime'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "     AND TXDate = '$txdate'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub updateGroupChildCheckFlag
{
   my ($dbh, $sys, $job, $checkflag, $txdate) = @_;

   my $sqltext = "UPDATE ${ETL::ETLDB}ETL_Job_GroupChild" .
                 " SET CheckFlag = '$checkflag', TxDate = '$txdate'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqltext) or return $FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub uncheckGroupChildJob
{
   my ($dbh, $group, $sys, $job) = @_;

   my $sqltext = "UPDATE ${ETL::ETLDB}ETL_Job_GroupChild SET CheckFlag = 'N'" .
                 "   WHERE GroupName = '$group'" .
                 "     AND ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqltext) or return $FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub isAllGroupChildWasDone
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;
   
   my $sqltext = "SELECT count(*) FROM ${ETL::ETLDB}ETL_Job_GroupChild" .
                 "  WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "    AND Enable = '1'" .
                 "    AND TurnOnFlag = 'Y'" .
                 "    AND CheckFlag = 'Y'";

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return $FALSE;
   }

   $sth->execute();
   @tabrow = $sth->fetchrow();
   $sth->finish();

   unless( @tabrow ) {
      return $FALSE;
   }

   if ($tabrow[0] == 0) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub getBelongGroup
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;
   my @belongGroup;
   
   my $sqltext = "SELECT GroupName" .
                 "  FROM ${ETL::ETLDB}ETL_Job_GroupChild" .
                 " WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 " ORDER BY GroupName";
                 
   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return @belongGroup;
   }

   $sth->execute();

   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $belongGroup[$n++] = $tabrow[0];
   }

   $sth->finish();

   return @belongGroup;
}

# The main function for executing real job
sub mainRealJob
{
   $AppPath    = "${ETL::ETL_APP}/${Sys}/${Job}";
   $AppBINPath = "${AppPath}/bin";
   $AppDDLPath = "${AppPath}/ddl";

   unless ( -d $AppPath ) {
      ETL::showPrefixSpace(); print ("Error: $AppPath does not exist, job terminated!\n");
      return $FALSE;
   }

   unless ( -d $AppBINPath ) {
      ETL::showPrefixSpace(); print ("Error: bin directory does not exist, job terminated!\n");
      return $FALSE;
   }
  
   unless ( -d $AppDDLPath ) {
      ETL::showPrefixSpace(); print ("Warnning: ddl directory does not exist, might cause error!\n");
   }

   # Open the application's bin directory for processing
   unless ( opendir(BIN_DIR, $AppBINPath) ) {
      ETL::showTime(); print ("Unable to open $AppBINPath!\n");
      return $FALSE;
   }
   
   my $n = 0;
   my $filename;

   my $jobscript = $Job;
   $jobscript =~ tr/[A-Z]/[a-z]/;

   while ($filename = readdir(BIN_DIR))
   {
      if ( $STOP_FLAG ) { last; }

      if ( "$filename" eq "." || "$filename" eq ".." ) {
      	 next;
      }
      
      # If the file is directory then skip it
      if ( -d "${AppBINPath}/${filename}" ) { next; }
      
      # To check if it is a shell script file
      if ( substr($filename, 0, length($Job)) eq $jobscript ) {
         if ( substr($filename, length($filename) - 3, 3) eq ".sh" ||
              substr($filename, length($filename) - 3, 3) eq ".pl") {      
            $dirFileList[$n++] = $filename;
         }
      }
   }

   # Close the application's bin directory
   closedir(BIN_DIR);

   my $eventDesc;
   my $cur_time;
   my $txdate = ETL::formatTXDate($TxDate);

   # If there no shell or perl script file existing, we write a log message into log file
   # then return from this funtion
   if ($n == 0) {
      ETL::showTime(); print ("No shell or perl script file existing!\n");

      $eventDesc = "[$Sys], [$Job] no any job script exiting";
      ETL::insertEventLog($dbCon, "SLV", "H", "$eventDesc");

      $cur_time = ETL::getCurrentDateTime();
      ETL::updateJobEndTime($dbCon, $Sys, $Job, $cur_time);
      ETL::updateJobStatusWithCheckFlag($dbCon, $Sys, $Job, "Failed", "N");

      return $FALSE;
   }

   ETL::showTime(); print ("Clear Job Script Log for session id '$JobSessionID'!\n");
   ETL::clearJobLog($dbCon, $Sys, $Job, $JobSessionID);

   runPrefixScript();

   # Set the group child job check flag to 'N'
   updateGroupChildCheckFlag($dbCon, $Sys, $Job, "N", $txdate);

   # Sort the script file list
   my @sortList = sort(@dirFileList);
   my $ret;
   my $failflag = 0;

   getDataFileList($ControlFile);

   $cur_time = ETL::getCurrentDateTime();
   ETL::updateJobStartTime($dbCon, $Sys, $Job, $cur_time);

   # Update the job trace record
   updateJobTraceStart($dbCon, $Sys, $Job, $txdate, $cur_time);

   # Processing the shell script file file one by one.
   for (my $i=0; $i < $n; $i++)
   {
      if ( $STOP_FLAG ) { last; }

      $filename = $sortList[$i];

      ETL::showTime(); print "Invoke $filename\n";

      $cur_time = ETL::getCurrentDateTime();
      ETL::insertJobLog($dbCon, $Sys, $Job, $txdate, $filename, $cur_time, $JobSessionID);

      ETL::updateJobRunningScript($dbCon, $Sys, $Job, $filename);

      my $ext = substr($filename, length($filename) - 3, 3);
      
      if ( "$ext" eq ".sh" ) {
         # Call the function to execute the Shell script file
         $ret = invokeJobShell($filename);
      } else {
         # Call the function to execute the Perl script file
         $ret = invokeJobPerl($filename, $ControlFile);      	
      }
      
      $cur_time = ETL::getCurrentDateTime();
      ETL::updateJobLog($dbCon, $Sys, $Job, $txdate, $filename, $cur_time, $ret, $JobSessionID);
      
      # If the job failed
      if ( $ret != 0 ) {
         ETL::showPrefixSpace(); print "Invoke failed!\n";
         $failflag = 1;

         $eventDesc = "[$Sys], [$Job] invoke job script $filename failed";
         ETL::insertEventLog($dbCon, "SLV", "H", "$eventDesc");

         writeMessageNotification("Failed",
                                  "Job [$Sys,$Job] invoke job script $filename failed",
                                  "",
                                  "${LOGDIR}/${filename}.${JobSessionID}.log");
         last;
      }
      ETL::showPrefixSpace(); print "Invoke succeed!\n";
   }

   my ($status, $checkflag);

   if ( $failflag == 1 ) {
      $status = "Failed";
      $checkflag = "N";
   }
   else {
      $status = "Done";
      $checkflag = "Y";
      # The job has done, we write a message file for done job
      writeMessageNotification("Done",
                               "Job [$Sys,$Job] was done, TXDate=${txdate}",
                               "",
                               "${LOGDIR}/${filename}.${JobSessionID}.log");
   }

   if ( "$status" eq "Done" && "$AutoOff" eq "Y" ) {
      ETL::showTime(); print "Job's auto off flag is on, so turn off job!\n";
      ETL::turnOffJob($dbCon, $Sys, $Job);
   }

   ETL::showTime(); print "Update Job status <$status>\n";

   $cur_time = ETL::getCurrentDateTime();
   ETL::updateJobEndTime($dbCon, $Sys, $Job, $cur_time);
   ETL::updateJobStatusWithCheckFlag($dbCon, $Sys, $Job, $status, $checkflag);

   # Set the group child job check flag for 'Y' if job was done
   # Otherwise, set it to 'N'
   updateGroupChildCheckFlag($dbCon, $Sys, $Job, $checkflag, $txdate);

   # Update the job trace record 
   updateJobTraceEnd($dbCon, $Sys, $Job, $txdate, $status, $cur_time);

   ETL::showPrefixSpace(); print "Log Job status\n";
   ETL::clearJobStatusLog($dbCon, $Sys, $Job, $txdate, $JobSessionID);
   ETL::insertJobStatusLog($dbCon, $Sys, $Job, $txdate, $JobSessionID);

   if ( $failflag == 1 ) {
      moveDataFileToError();
      moveControlFileToError($ControlFile);
   }
   else {
      moveDataFileToComplete();
      moveControlFileToComplete($ControlFile);
   }

   my $server;
   
   if ( "$status" eq "Done" && $RunDownstreamFlag == 1 ) {
      # Invoke down stream job
      my @jobstream = ETL::getJobStream($dbCon, $Sys, $Job);

      ETL::showTime(); print "Invoke downstream job\n";
      
      for (my $i=0; $i <= $#jobstream; $i += 3) {
      	 # We only invoke down stream job while its enable status is 1
      	 if ( "$jobstream[$i+2]" eq "1" ) { 
            ETL::showPrefixSpace(); print "Trigger job stream for $jobstream[$i], $jobstream[$i + 1]\n";
            # If the down stream job is a virtual job, we invoke it right away
            if ( ETL::getJobType($dbCon, $jobstream[$i], $jobstream[$i+1]) eq "V") {
               triggerVirtualJob($dbCon, $TxDate, $jobstream[$i], $jobstream[$i + 1]);
            }
            else {	
               $server = getJobRunningServer($dbCon, $jobstream[$i], $jobstream[$i +1]);
               ETL::showPrefixSpace(); print "Job running server is at '$server'\n";
               if ("$server" eq "${ETL::ETL_SERVER}") {
                  ETL::triggerJob($dbCon, $TxDate, $jobstream[$i], $jobstream[$i + 1]);
               } else {
                  ETL::showPrefixSpace(); print "Down stream job is not at same server, we put it into job queue\n";
                  if (putJobIntoQueue($dbCon, $server, $jobstream[$i], $jobstream[$i + 1], $TxDate)
                      == $FALSE) {
                     ETL::showPrefixSpace(); print "ERROR - Put into job queue failed!\n";
                  }
               }
            }
         }
      } # end of for
   }

   # If this job is belong to one job group,
   # we have to check all childs of job group have been set or not.
   # If they have been set then we trigger the head job, which associated with job group.
   my @belongGroup = getBelongGroup($dbCon, $Sys, $Job);
   my $gn = 0;
   my $groupname;
   for ($gn=0; $gn <= $#belongGroup; $gn++) {
      $groupname = $belongGroup[$gn];

      ETL::showTime(); print "This job is belong to job group [$groupname]\n";

      #my $groupOK = ETL::isGroupChildOK($dbCon, $groupname);
      my $groupOK = ETL::isGroupChildOK1($dbCon, $groupname);

      if ( $groupOK == $TRUE) {
         ETL::showPrefixSpace(); print "All group child job(s) are ready\n";
         my ($gsys, $gjob) = ETL::getGroupJob($dbCon, $groupname);
         ETL::showPrefixSpace(); print "Group Head Job is [$gsys], [$gjob]\n";
         ETL::showPrefixSpace(); print "Trigger job for $gsys, $gjob\n";

         if ( ETL::isTriggerByTime($dbCon, $gsys, $gjob) == $TRUE ) {
            ETL::showPrefixSpace(); print "The head job is trigger by time, we skip it.\n";
            next;
         }

         # If the group head job is a virtual job, we invoke it right away
         if ( ETL::getJobType($dbCon, $gsys, $gjob) eq "V") {
            triggerVirtualJob($dbCon, $TxDate, $gsys, $gjob);
         }
         else {
            $server = getJobRunningServer($dbCon, $gsys, $gjob);
            ETL::showPrefixSpace(); print "Job running server is at '$server'\n";
            if ("$server" eq "${ETL::ETL_SERVER}") {
               ETL::triggerJob($dbCon, $TxDate, $gsys, $gjob);
            } else {
               ETL::showPrefixSpace(); print "Group head job is not at same server, we put it into job queue\n";
               if (putJobIntoQueue($dbCon, $server, $TxDate, $gsys, $gjob)==$FALSE) {
                  ETL::showPrefixSpace(); print "ERROR - Put into job queue failed!\n";
               }
            }
         }
      }
      else {
         ETL::showPrefixSpace(); print "There are some other group child job(s) not ready, skip it!\n";
      }
   }

   # If this job is a group's head job, we need to do some clean up task when job was done
   if ( "$HeadJobGroup" ne "" && "$status" eq "Done" ) {
      ETL::showTime(); print "This job is group [$HeadJobGroup]'s head job\n";
      ETL::showPrefixSpace(); print "Do some clean up task...\n";

      my @childjob = ETL::getGroupChildJob($dbCon, $HeadJobGroup);
      my ($gc_sys, $gc_job);

      for (my $i=0; $i <= $#childjob; $i += 2) {
      	 $gc_sys = $childjob[$i];
      	 $gc_job = $childjob[$i + 1];
      	 
         ETL::showPrefixSpace(); print "Reset group child job [$gc_sys, $gc_job] flag\n";

         #ETL::uncheckJobFlag($dbCon, $childjob[$i], $childjob[$i + 1]);
         uncheckGroupChildJob($dbCon, $HeadJobGroup, $gc_sys, $gc_job);
         
         # We only turn on child job when group head job was done
         if ( "$AutoOnChild" eq "Y" && "$status" eq "Done" ) {
            if (isAllGroupChildWasDone($dbCon, $gc_sys, $gc_job)==$TRUE) {
               ETL::showPrefixSpace(); print "Turn on child job [$gc_sys, $gc_job] flag\n";
               ETL::turnOnJob($dbCon, $gc_sys, $gc_job);
            }
         }
      }
   }

   return $TRUE;
}

# The main function for executing virtual job
sub mainVirtualJob
{
   my $cur_time;
   my $txdate = ETL::formatTXDate($TxDate);

   ETL::showTime(); print ("Clear Job Script Log for $txdate!\n");
   ETL::clearJobLog($dbCon, $Sys, $Job, $JobSessionID);

   # Set the group child job check flag to 'N'
   updateGroupChildCheckFlag($dbCon, $Sys, $Job, "N", $txdate);
   
   $cur_time = ETL::getCurrentDateTime();
   ETL::updateJobStartTime($dbCon, $Sys, $Job, $cur_time);

   $cur_time = ETL::getCurrentDateTime();
   ETL::insertJobLog($dbCon, $Sys, $Job, $txdate, "virtual", $cur_time, $JobSessionID);
   ETL::updateJobLog($dbCon, $Sys, $Job, $txdate, "virtual", $cur_time, 0, $JobSessionID);

   ETL::updateJobEndTime($dbCon, $Sys, $Job, $cur_time);
   ETL::updateJobStatusWithCheckFlag($dbCon, $Sys, $Job, "Done", "Y");

   ETL::clearJobStatusLog($dbCon, $Sys, $Job, $txdate, $JobSessionID);
   ETL::insertJobStatusLog($dbCon, $Sys, $Job, $txdate, $JobSessionID);

   # Set the group child job check flag to 'Y'
   updateGroupChildCheckFlag($dbCon, $Sys, $Job, "Y", $txdate);

   writeMessageNotification("Done",
                            "Job [$Sys,$Job] was done, TXDate=${txdate}",
                            "",
                            "");

   # If there is a control file existing for this virtual job
   # we delete the control file from process directory
   if ( -f "${ETL::ETL_PROCESS}/${ControlFile}" ) {
      unlink("${ETL::ETL_PROCESS}/${ControlFile}");
   }

   my $server;

   if ( $RunDownstreamFlag == 1 ) {
      # Invoke down stream job   
      my @jobstream = ETL::getJobStream($dbCon, $Sys, $Job);

      ETL::showTime(); print "Invoke downstream job\n";
      
      for (my $i=0; $i <= $#jobstream; $i += 3) {
         # We only invoke down stream job while its enable status is 1
         if ( "$jobstream[$i+2]" eq "1" ) {
            ETL::showPrefixSpace(); print "Trigger job stream for $jobstream[$i], $jobstream[$i + 1]\n";
            # If the down stream job is a virtual job, we invoke it right away
            if ( ETL::getJobType($dbCon, $jobstream[$i], $jobstream[$i+1]) eq "V") {
               triggerVirtualJob($dbCon, $TxDate, $jobstream[$i], $jobstream[$i + 1]);
            }
            else {	
               $server = getJobRunningServer($dbCon, $jobstream[$i], $jobstream[$i +1]);
               ETL::showPrefixSpace(); print "Job running server is at '$server'\n";
               if ("$server" eq "${ETL::ETL_SERVER}") {
                  ETL::triggerJob($dbCon, $TxDate, $jobstream[$i], $jobstream[$i + 1]);
               } else {
                  ETL::showPrefixSpace(); print "Down stream job is not at same server, we put it into job queue\n";
                  if (putJobIntoQueue($dbCon, $server, $jobstream[$i], $jobstream[$i + 1], $TxDate)
                      == $FALSE) {
                     ETL::showPrefixSpace(); print "ERROR - Put into job queue failed!\n";
                  }
               }
            }
         }
      } # end of for
   }

   # If this job is belong to one job group,
   # we have to check all childs of job group have been set or not.
   # If they have been set then we trigger the head job, which associated with job group.
   my @belongGroup = getBelongGroup($dbCon, $Sys, $Job);
   my $gn = 0;
   my $groupname;
   for ($gn=0; $gn <= $#belongGroup; $gn++) {
      $groupname = $belongGroup[$gn];

      ETL::showTime(); print "This job is belong to job group [$groupname]\n";

      #my $groupOK = ETL::isGroupChildOK($dbCon, $groupname);
      my $groupOK = ETL::isGroupChildOK1($dbCon, $groupname);
      if ( $groupOK == $TRUE) {
         ETL::showPrefixSpace(); print "All group child job(s) are ready\n";
         my ($gsys, $gjob) = ETL::getGroupJob($dbCon, $groupname);
         ETL::showPrefixSpace(); print "Group Head Job is [$gsys], [$gjob]\n";
         ETL::showPrefixSpace(); print "Trigger job for $gsys, $gjob\n";

         if ( ETL::isTriggerByTime($dbCon, $gsys, $gjob) == $TRUE ) {
            ETL::showPrefixSpace(); print "The head job is trigger by time, we skip it.\n";
            next;
         }

         # If the group head job is a virtual job, we invoke it right away
         if ( ETL::getJobType($dbCon, $gsys, $gjob) eq "V") {
            triggerVirtualJob($dbCon, $TxDate, $gsys, $gjob);
         }
         else {
            $server = getJobRunningServer($dbCon, $gsys, $gjob);
            ETL::showPrefixSpace(); print "Job running server is at '$server'\n";
            if ("$server" eq "${ETL::ETL_SERVER}") {
               ETL::triggerJob($dbCon, $TxDate, $gsys, $gjob);
            } else {
               ETL::showPrefixSpace(); print "Group head job is not at same server, we put it into job queue\n";
               if (putJobIntoQueue($dbCon, $server, $TxDate, $gsys, $gjob)==$FALSE) {
                  ETL::showPrefixSpace(); print "ERROR - Put into job queue failed!\n";
               }
            }
         }
      }
      else {
         ETL::showPrefixSpace(); print "There are some other group child job(s) not ready, skip it!\n";
      }
   }

   # If this job is a group's head job, we need to do some clean up task
   if ( $HeadJobGroup ne "" ) {
      ETL::showTime(); print "This job is group [$HeadJobGroup]'s head job\n";
      ETL::showPrefixSpace(); print "Do some clean up task...\n";

      my @childjob = ETL::getGroupChildJob($dbCon, $HeadJobGroup);
      my ($gc_sys, $gc_job);

      for (my $i=0; $i <= $#childjob; $i += 2) {
      	 $gc_sys = $childjob[$i];
      	 $gc_job = $childjob[$i + 1];
      	 
         ETL::showPrefixSpace(); print "Reset group child job [$gc_sys, $gc_job] flag\n";

         #ETL::uncheckJobFlag($dbCon, $childjob[$i], $childjob[$i + 1]);
         uncheckGroupChildJob($dbCon, $HeadJobGroup, $gc_sys, $gc_job);

         if ( $AutoOnChild eq "Y" ) {
            if (isAllGroupChildWasDone($dbCon, $gc_sys, $gc_job)==$TRUE) {
               ETL::showPrefixSpace(); print "Turn on child job [$gc_sys, $gc_job] flag\n";
               ETL::turnOnJob($dbCon, $gc_sys, $gc_job);
            }
         }
      }
   }

   return $TRUE;
}

# This function is to see if there is another instance of program running.
# Only one instance of program allow to run at any given time.
# If there is another instance of program is running, we stop the new one.
sub check_instance()
{
   my $count = 1;
   my $LK_FILE_H;

   ETL::showTime(); print "Check for instance...\n";

   until (ETL::getMasterLock($ETL::ETL_LOCK)) {
      if ($count++ == 10) {
         ETL::showPrefixSpace(); print "Unable to get master lock for five times, program terminated!\n";
         return $FALSE;
      }
      sleep(1);
   }

   if ( -f $LOCK_FILE ) {
      ETL::releaseMasterLock($ETL::ETL_LOCK);
      ETL::showPrefixSpace(); print "Only one instance of etlslave.pl allow to run, program terminated!\n";

      return $FALSE;
   }  

   unless ( open(LK_FILE_H, ">$LOCK_FILE") ) {
      ETL::releaseMasterLock($ETL::ETL_LOCK);

      ETL::showPrefixSpace(); print "Unable to create lock file, program terminated!\n";
      return $FALSE;
   }

   print LK_FILE_H ("$$\n");

   close(LK_FILE_H);

   ETL::releaseMasterLock($ETL::ETL_LOCK);

   return $TRUE;
}

# To remove the lock file create by this program
sub removeLock
{
   my $count = 1;

   until (ETL::getMasterLock($ETL::ETL_LOCK)) {
      if ($count++ == 5) {
         unlink($LOCK_FILE);
         return;
      }
      sleep(1);
   }

   unlink($LOCK_FILE);

   ETL::releaseMasterLock($ETL::ETL_LOCK);
}

# This function will be called when the program has catched some signal
# The purpose of this program is to some clean up task when the program terminate.
sub cleanUp
{
   my ($signal) = @_;
   my $count = 1;

   until (ETL::getMasterLock($ETL::ETL_LOCK)) {
      if ($count++ == 5) {
         unlink($LOCK_FILE);
         return;
      }
      sleep(1);
   }

   unlink($LOCK_FILE);

   ETL::releaseMasterLock($ETL::ETL_LOCK);

   print LOGF_H "Stop by signal '${signal}'\n";

   $STOP_FLAG = 1;

   exit(0);
}

sub printVersionInfo
{
   print "\n";
   ETL::showTime(); print "*********************************************************************\n";
   ETL::showTime(); print "* ETL Automation Slave Program for Unix ${VERSION}, NCR 2001 Copyright. *\n";
   ETL::showTime(); print "*********************************************************************\n";
   print "\n";
}

sub updateJobStatusRunning
{
   my ($dbh, $sys, $job, $txdate) = @_;

   $txdate = ETL::formatTXDate($txdate);

   ETL::showTime(); print "Reset [$sys, $job] status to running for '$txdate'.\n";
   
   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET Last_JobStatus = 'Running'," .
                 "       Last_TXDate = '$txdate', RunningScript = ''" . 
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub printSyntax
{
   print STDERR "etlslave_unix.pl: Automation Job Script Runner, v2.5.1\n";
   print STDERR "Syntax 1: etlslave_unix.pl <control file>\n";
   print STDERR "          <control file> the control file name for executing job.\n";
   print STDERR "Syntax 2: etlslave_unix.pl <sys> <job> <txdate> [-nd] [-ng]\n";
   print STDERR "          <sys>, the system name of executing job.\n";
   print STDERR "          <job>, the job name of executing job.\n";
   print STDERR "          <txdate>, the data date for the executing job.\n";
   print STDERR "          -nd, disable the down stream job execution, it's optional.\n";
   print STDERR "          -ng, disable the job group execution, it's optional.\n"; 
}

###############################################################################

if ( $#ARGV == 0 ) {
   # Get the argument of control file name
   $ControlFile = $ARGV[0];

   # Check the argument is presented or not
   unless ( defined($ControlFile) ) {
      print STDERR ("ERROR: Unknow job control file\n");
      exit(1);
   }

   # Connect to ETL Automation repository
   $dbCon = ETL::connectETL();

   unless ( defined($dbCon) ) {
      # Can not connect to database
      print STDERR "Unable to connect database\n";
      exit(2);
   }

   ($Sys, $Job, $AutoOff, $TxDate, $JobType) = ETL::getJobRunInfo($dbCon, $ControlFile);
   if ( "$Sys" eq "" || "$Job" eq "" ) {
      print STDERR "Unable to find job information, terminate this program\n";
      exit(3);
   }
   
   $SyntaxMode = 1;   
} elsif ( $#ARGV >= 2 ) {
   $Sys = $ARGV[0];
   $Job = $ARGV[1];
   $TxDate = $ARGV[2];

   # Connect to ETL Automation repository
   $dbCon = ETL::connectETL();

   unless ( defined($dbCon) ) {
      # Can not connect to database
      print STDERR "Unable to connect database\n";
      exit(2);
   }
   
   ($AutoOff, $JobType) = ETL::getJobRunInfo1($dbCon, $Sys, $Job);
   if ( "$AutoOff" eq "" ) {
      print STDERR "Unable to find job information, terminate this program\n";
      exit(3);
   }

   $ControlFile = "${Sys}_${Job}_${TxDate}.dir";

   $SyntaxMode = 2;
   
   for (my $i=3; $i <= $#ARGV; $i++) {
      if ("$ARGV[$i]" eq "-nd") {
         $RunDownstreamFlag = 0;
      }
      elsif ("$ARGV[$i]" eq "-ng") {
      	 $RunGroupFlag = 0;
      }
   }   
} else {
   printSyntax();
   exit(1);	
}

$JobSessionID = ETL::getJobSessionID($dbCon, $Sys, $Job);

($HeadJobGroup, $AutoOnChild) = ETL::isGroupHeadJob($dbCon, $Sys, $Job);

unless ( createLogFile() ) {
   print STDERR "Unable to create log file, terminate this program\n";
   exit(4);
}

printVersionInfo();

ETL::showTime(); print "System:[$Sys] Job:[$Job] AutoOff:[$AutoOff] TXDate:[$TxDate]\n";
ETL::showPrefixSpace(); print "Type:[$JobType] Head Job Group:[$HeadJobGroup]\n";

# If the job is not a virtual job, we need to check the control file is existing or not
# If the job is a virtaul job, we don't care the control file
if ($JobType ne "V" && $SyntaxMode == 1) {
   if ( ! -f "${ETL::ETL_PROCESS}/${ControlFile}" ) {
      ETL::showTime(); print "ERROR - Control file '$ControlFile' is not existing\n";

      ETL::showTime(); print "Disconnect from ETL DB...\n";
      ETL::disconnectETL($dbCon);

      close(LOGF_H);
      exit(1);
   }
}

$LOCK_FILE = "${ETL::ETL_LOCK}/etlslave_${Sys}_${Job}.lock";
unless ( check_instance() ) {
   ETL::showTime(); print "Instance check failed, disconnect from ETL DB...\n";
   ETL::disconnectETL($dbCon);

   close(LOGF_H);

   exit(1);
}

$SIG{'INT'}  = 'cleanUp';
$SIG{'QUIT'} = 'cleanUp';
$SIG{'TERM'} = 'cleanUp';

open(STDERR, ">&STDOUT");


updateJobStatusRunning($dbCon, $Sys, $Job, $TxDate);

if ($JobType eq "V") {
   mainVirtualJob();	
}
else {
   mainRealJob();
}

ETL::increaseJobSessionID($dbCon, $Sys, $Job);

removeLock();

ETL::showTime(); print "Disconnect from ETL DB...\n";
ETL::disconnectETL($dbCon);

close(LOGF_H);

exit(0);

__END__
