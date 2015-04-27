#!/usr/bin/perl
###############################################################################
# Program  : etlmaster.pl
# Argument : none
###############################################################################

use strict;
use DBI;

my $VERSION = "v2.5.2";

my $home = $ENV{"AUTO_HOME"};
my $os   = $^O;

$os =~ tr [A-Z][a-z];

if ( $os eq "svr4" ) {
   unshift(@INC, "$home/bin");
   require etl_unix;
}
elsif ( $os eq "mswin32" ) {
   unshift(@INC, "$home\\bin");
   require etl_nt;
}
else {
   print STDERR "<ERROR> --- Unknown OS setting!\n";
   exit(1);
}

my $DIRDELI;

if ( $os eq "svr4" ) {
   $DIRDELI = "/";
}
elsif ( $os eq "mswin32" ) {
   $DIRDELI = "\\";
}

my $LOGDIR;
my ($LOGFILE, $LASTLOGFILE);
my $TODAY;

my $TRUE  = 1;
my $FALSE = 0;

my $STOP_FLAG = 0;
my $LOCK_FILE;

my $LOG_STAT = 0;

my $dbCon;
my @dirFileList;

my %currentJobStatus;

my $dataFileCount = 0;

my $maxJobCount = 5;

my $MutexObj;

my $value;

$value = $ENV{"AUTO_DATA_QUEUE"};
if ( defined($value) ) {
   $ETL::ETL_QUEUE = $value;
}

$value = $ENV{"AUTO_DATA_PROCESS"};
if ( defined($value) ) {
   $ETL::ETL_PROCESS = $value;
}

$value = $ENV{"AUTO_JOB_COUNT"};
if ( defined($value) ) {
   $maxJobCount = $value;
} 

my $PRINT_VERSION_FLAG = 0;

# Create log file for this program
sub createLogFile
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   
   $year  += 1900;
   $mon    = sprintf("%02d", $mon + 1);
   $mday   = sprintf("%02d", $mday);
   $TODAY  = "${year}${mon}${mday}";
   $LOGDIR = "${ETL::ETL_LOG}${DIRDELI}${TODAY}"; 

   # To check the LOG directory is exist or not
   # If it is not exist then create the directory   
   if ( ! -d $ETL::ETL_LOG) {
      ETL::createDirectory($ETL::ETL_LOG);
   }

   # To check the today's LOG directoyr is exist or not
   # If it is not exist then create the directory
   #if ( ! -d $LOGDIR) {
   #   ETL::createDirectory($LOGDIR);
   #}

   # Composite the log file name
   $LOGFILE = "${ETL::ETL_LOG}${DIRDELI}etlmas_${TODAY}.log";

   if ("$LOGFILE" eq "$LASTLOGFILE") {
      return $TRUE;	
   }

   # If log file was open before, close it first   
   if ( $LOG_STAT == 1 ) {
      close(LOGF_H);
      $LOG_STAT = 0;
   }

   # Try to open the log file   
   unless ( open(LOGF_H, ">>${LOGFILE}") ) {
      return $FALSE;
   }

   $LASTLOGFILE = $LOGFILE;
   $LOG_STAT = 1;
   
   # Set the buffer of log file to be 1,
   # which means the log message will be wrriten out immediately
   select (LOGF_H);
   $| = 1;

   return $TRUE;
}

sub isControlFile
{
   my($filename) = @_;
   
   if ( substr($filename, length($filename)-4, 4) eq ".dir" ) {
      return $TRUE;
   }
   else  {
      return $FALSE;
   }
}

sub moveToProcess
{
   my ($controlFile, $sys, $job) = @_;
   my ($oldpath, $newpath);
   my $CTRLFILEH;
   my $file;
   my @fields;

   ETL::showPrefixSpace(); print "Move data file(s) to process directory...\n";
   
   $oldpath = "${ETL::ETL_QUEUE}";
   $newpath = "${ETL::ETL_PROCESS}";
   
   unless ( open(CTRLFILEH, "${ETL::ETL_QUEUE}${DIRDELI}$controlFile") ) { return $FALSE; }

   my @list = <CTRLFILEH>;
   close(CTRLFILEH);

   my $n = 0;

   for ($n = 0; $n <= $#list ; $n++) {
      chomp($list[$n]);
      @fields = split(/\s+/, $list[$n]);
 
      $file = $fields[0];

      ETL::showPrefixSpace(); print "Move source data '$file' to process directory.\n";

      ETL::moveFile("$oldpath${DIRDELI}$file", "$newpath${DIRDELI}$file");
      ETL::updateFileLocation($dbCon, $sys, $job, $file, $newpath);
   }

   $dataFileCount = $n;

   ETL::showPrefixSpace(); print "Move control file '$controlFile' to process directory.\n";
   rename("$oldpath${DIRDELI}$controlFile", "$newpath${DIRDELI}$controlFile");

   return $TRUE;
}

sub moveBackToQueue
{
   my ($controlFile, $sys, $job) = @_;
   my ($oldpath, $newpath);
   my $CTRLFILEH;
   my $file;
   my @fields;
   
   ETL::showPrefixSpace(); print "Move data file(s) back to queue directory...\n";
   
   $oldpath = "${ETL::ETL_PROCESS}";
   $newpath = "${ETL::ETL_QUEUE}";
   
   unless ( open(CTRLFILEH, "${ETL::ETL_PROCESS}${DIRDELI}$controlFile") ) { return $FALSE; }

   my @list = <CTRLFILEH>;
   close(CTRLFILEH);

   my $n = 0;

   for ($n = 0; $n <= $#list ; $n++) {
      chomp($list[$n]);
      @fields = split(/\s+/, $list[$n]);
 
      $file = $fields[0];

      ETL::showPrefixSpace(); print "Move source data '$file' back to queue directory.\n";

      ETL::moveFile("$oldpath${DIRDELI}$file", "$newpath${DIRDELI}$file");
      ETL::updateFileLocation($dbCon, $sys, $job, $file, $newpath);
   }

   ETL::showPrefixSpace(); print "Move control file '$controlFile' back to queue directory.\n";
   rename("$oldpath${DIRDELI}$controlFile", "$newpath${DIRDELI}$controlFile");

   return $TRUE;
}

sub isJobLockFile
{
   my ($file) = @_;

   if (substr($file, 0, 8) eq "etlslave") {
      if (substr($file, length($file) - 5, 5) eq ".lock") {
         return $TRUE;
      }
   }

   return $FALSE;
}

sub checkCurrentJobCount
{
   my $filename;
   my $count = 1;

   until (ETL::getMasterLock($ETL::ETL_LOCK)) {
      if ($count++ == 5) {
         return $FALSE;
      }
      sleep(1);
   }

   unless ( opendir(LOCK_DIR, "${ETL::ETL_LOCK}") ) {
      ETL::releaseMasterLock($ETL::ETL_LOCK);
      ETL::showPrefixSpace(); print ("Unable to open '${ETL::ETL_LOCK}'!");
      return $FALSE;
   }

   my $n = 0;

   while ($filename = readdir(LOCK_DIR))
   {
      if ( $STOP_FLAG ) { last; }

      if ( $filename eq "." || $filename eq ".." ) { next; }
      
      # If the file is directory then skip it
      if ( -d "${ETL::ETL_LOCK}${DIRDELI}${filename}" ) { next; }

      if ( isJobLockFile($filename) == $TRUE ) {      
         $n++;
      }
   }
   # Close the locking directory
   closedir(LOCK_DIR);

   ETL::releaseMasterLock(${ETL::ETL_LOCK});

   if ($n >= $maxJobCount) {   # Current running job has reached the max job count
      return $FALSE;
   }
   else {
      return $TRUE;
   }
}

sub getCurrentJobCount
{
   my $filename;
   my $count = 1;

   until (ETL::getMasterLock($ETL::ETL_LOCK)) {
      if ($count++ == 5) {
         return 0;
      }
      sleep(1);
   }

   unless ( opendir(LOCK_DIR, "${ETL::ETL_LOCK}") ) {
      ETL::releaseMasterLock($ETL::ETL_LOCK);
      ETL::showPrefixSpace(); print ("Unable to open '${ETL::ETL_LOCK}'!");
      return 0;
   }

   my $n = 0;

   while ($filename = readdir(LOCK_DIR))
   {
      if ( $STOP_FLAG ) { last; }

      if ( $filename eq "." || $filename eq ".." ) { next; }
      
      # If the file is directory then skip it
      if ( -d "${ETL::ETL_LOCK}${DIRDELI}${filename}" ) { next; }

      if ( isJobLockFile($filename) == $TRUE ) {      
         $n++;
      }
   }
   # Close the locking directory
   closedir(LOCK_DIR);

   ETL::releaseMasterLock(${ETL::ETL_LOCK});

   return $n;
}

sub checkJobPath
{
   my ($sys, $job) = @_;

   my $AppPath    = "${ETL::ETL_APP}${DIRDELI}${sys}${DIRDELI}${job}";
   my $AppBinPath = "${AppPath}${DIRDELI}bin";

   unless ( -d $AppPath ) { return $FALSE; }

   unless ( -d $AppBinPath ) { return $FALSE; }

   return $TRUE;
}

sub updateJobStatus
{
   my ($controlFile, $sys, $job, $txdate, $status) = @_;

   $txdate = ETL::formatTXDate($txdate);
   
   my $ret;

   $ret = ETL::updateJobStatus($dbCon, $sys, $job, $status);

   return $TRUE;
}

sub invokeJob
{
   my ($controlFile) = @_;
   my $eventDesc;
   
   $dataFileCount = 0;

   ETL::showTime(); print "Invoke job for $controlFile\n";

   my ($sys, $job, $txdate) = ETL::getJobInfo($dbCon, $controlFile);

   moveToProcess($controlFile, $sys, $job);

   updateJobStatus($controlFile, $sys, $job, $txdate, "Running");

   my $ret = ETL::invokeJob($controlFile);

   if ( $ret == 1 ) {
      ETL::showPrefixSpace(); print "Invoke Job OK.\n";
   }
   else {
      ETL::showPrefixSpace(); print "Unable to invoke job for $controlFile\n";
      moveBackToQueue($controlFile, $sys, $job);

      updateJobStatus($controlFile, $sys, $job, $txdate, "Invoke Failed");

      $eventDesc = "[$sys], [$job] Invoke job failed";
      ETL::insertEventLog($dbCon, "MAS", "H", "$eventDesc");
      
      return $FALSE;
   }

   return $TRUE;
}

sub isJobAlreadyHasEvent
{
   my ($sys, $job, $status) = @_;
   my $key = "${sys}_${job}";
   my $value;
   
   if ( exists($currentJobStatus{$key}) ) {
      $value = $currentJobStatus{$key};
      if ( "$value" eq "$status" ) {
      	 return $TRUE;
      } else {
      	$currentJobStatus{$key} = $status;
      	return $FALSE;
      }
   } else {
      $currentJobStatus{$key} = $status;
      return $FALSE;
   }
}

sub removeJobEventRecord
{
   my ($sys, $job) = @_;
   my $key = "${sys}_${job}";

   delete $currentJobStatus{$key};
}

sub compareCurrentTxDate
{
   my ($dbh, $sys, $job, $txdate) = @_;
   my @tabrow;
   
   $txdate = ETL::formatTXDate($txdate);
   
   my $sqltext = "SELECT Last_TxDate " . 
                 "   FROM ${ETL::ETLDB}ETL_Job" . 
                 "      WHERE ETL_System = '$sys'" .
                 "        AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqltext);

   unless ($sth) {
      return $FALSE;
   }

   $sth->execute();
   @tabrow = $sth->fetchrow();
   $sth->finish();
   
   if ( !defined($tabrow[0]) || $tabrow[0] eq "" ) {
      return $TRUE;
   }

   my $curtxdate = substr($tabrow[0], 0, 10);
   
   if ( "$curtxdate" gt "$txdate" ) {
      return $FALSE;
   }
   
   return $TRUE;
}

sub processControlFile
{
   my ($controlFile) = @_;
   my ($sys, $job, $txdate) = ETL::getJobInfo($dbCon, $controlFile);
   my $jobtype;
   my $eventDesc;
   
   ETL::showTime(); print "Processing control file '$controlFile'\n";
   ETL::showPrefixSpace(); print "System:[$sys] Job:[$job], TxDate:[$txdate]\n";

   my $enable = ETL::checkJobEnable($dbCon, $sys, $job);
   if ( $enable == -1 ) {
      ETL::showPrefixSpace(); print "Database connection error!\n";
      return $FALSE;
   }
   elsif ( $enable == 0 ) {
      ETL::showPrefixSpace(); print "Job is not enabled, wait for next time!\n";
      return $FALSE;
   }

   # If the job need to check last status before execution, then we check the status
   # Otherwise, we don't check it
   if ( ETL::isJobCheckLastStatus($dbCon, $sys, $job) == $TRUE ) {
      # Get the job current status
      ETL::showPrefixSpace(); print "Check Job Current Status\n";
      my $currentStatus = ETL::getJobStatus($dbCon, $sys, $job);
      if ( $currentStatus ne "Pending" ) {
         # The current job status is not 'Pending',
         # we refuse to execute this job at the moment
         ETL::showPrefixSpace(); print "WARNING - The job is in $currentStatus mode, we will process this file next time.\n";

         if ( isJobAlreadyHasEvent($sys, $job, "Status Mismatch") ) {
            return $FALSE;
         }

         $eventDesc = "[$sys], [$job] is $currentStatus but has received another file $controlFile";
         ETL::insertEventLog($dbCon, "MAS", "M", "$eventDesc");
         return $FALSE;	
      }
   }

   if ( compareCurrentTxDate($dbCon, $sys, $job, $txdate) == $FALSE ) {
      ETL::showTime(); print ("ERROR - The TxDate '$txdate' is less then job current TxDate.\n");

      if ( isJobAlreadyHasEvent($sys, $job, "Less TxDate") ) {
          return $FALSE;
      }

      $eventDesc = "[$sys], [$job] job txdate is less then the current txdate";
      ETL::insertEventLog($dbCon, "MAS", "M", "$eventDesc");
      
      return $FALSE;	
   }

   ETL::showPrefixSpace(); print "Check Job Dependency\n";
   my ($dep, $depsys, $depjob) = ETL::checkJobDependency($dbCon, $sys, $job, $txdate);
   if ( $dep == -1 ) {
      ETL::showPrefixSpace(); print "Database connection error!\n";
      return $FALSE;
   }
   elsif ( $dep == 0 ) {
      ETL::showPrefixSpace(); print "Dependent job [$depsys, $depjob] does not finish yet, wait for next time!\n";

      if ( isJobAlreadyHasEvent($sys, $job, "Dependent Job") ) {
          return $FALSE;
      }

      $eventDesc = "[$sys], [$job] Dependent job [$depsys, $depjob] does not finish yet, wait for next time!";
      ETL::insertEventLog($dbCon, "MAS", "L", "$eventDesc");

      return $FALSE;
   }

   ETL::showPrefixSpace(); print "Check Job Time Window\n";
   my $timewin = ETL::checkJobTimeWindow($dbCon, $sys, $job);
   if ($timewin == $FALSE) {
      ETL::showPrefixSpace(); print "The current hour does not match the job time window, wait for next time!\n";

      if ( isJobAlreadyHasEvent($sys, $job, "Time Window") ) {
          return $FALSE;
      }

      $eventDesc = "[$sys], [$job] The current hour does not match the job time window, wait for next time!";
      ETL::insertEventLog($dbCon, "MAS", "L", "$eventDesc");

      return $FALSE;
   }

   $jobtype = ETL::getJobType($dbCon, $sys, $job);

   if ( "$jobtype" ne "V" && checkCurrentJobCount() == $FALSE ) {
      ETL::showPrefixSpace(); print "Current running jobs reached the limitation, wait for next time!\n";

      if ( isJobAlreadyHasEvent($sys, $job, "Max Job Count") ) {
          return $FALSE;
      }

      $eventDesc = "[$sys], [$job] Current running jobs has reached the limitation, wait for next time!";
      ETL::insertEventLog($dbCon, "MAS", "M", "$eventDesc");

      return $FALSE;
   }

   if ( "$jobtype" ne "V" ) { 
      unless ( checkJobPath($sys, $job) ) {
         ETL::showPrefixSpace(); print "Can not find the mapping job path, wait for next time!\n";

         if ( isJobAlreadyHasEvent($sys, $job, "No Job Path") ) {
            return $FALSE;
         }

         $eventDesc = "[$sys], [$job] Can not find the mapping job path, wait for next time!";
         ETL::insertEventLog($dbCon, "MAS", "H", "$eventDesc");

         return $FALSE;
      }
   }

   invokeJob($controlFile);

   removeJobEventRecord($sys, $job);
   
   return $TRUE;
}

# Sort the control file by data date and job name
sub sortControlFile
{
   my $job1 =  substr($a, 0, (length($a) - 13));
   my $job2 =  substr($b, 0, (length($b) - 13));
   
   my $date1 = substr($a, (length($a) - 12), 8);
   my $date2 = substr($b, (length($b) - 12), 8);
   
   if ( "$date1" eq "$date2" ) {
      $job1 cmp $job2;
   } else {
      $date1 cmp $date2;
   }
}

# This function is to check the queue directory to see if there is any
# control file need to be processed.
sub checkQueueDir
{
   my $filename;
   my $QUE_DIR;

   ETL::showTime(); print "Checking queue directory '$ETL::ETL_QUEUE'...\n";

   # Open the queue directory for processing
   unless ( opendir(QUE_DIR, $ETL::ETL_QUEUE) ) {
      ETL::showTime(); print "ERROR - Unable to open ${ETL::ETL_QUEUE}!";
      return $FALSE;
   }

   my $n = 0;
   @dirFileList = ();
   my @tempList;
   
   while ($filename = readdir(QUE_DIR))
   {
      if ( $STOP_FLAG ) { last; }

      if ( $filename eq "." || $filename eq ".." ) { next; }
      
      # If the file is directory then skip it
      if ( -d "${ETL::ETL_QUEUE}${DIRDELI}${filename}" ) { next; }

      if ( isControlFile($filename) ) {
         $tempList[$n++] = $filename;
      }
   }
   # Close the queue directory
   closedir(QUE_DIR);

   # If there no control file existing, we write a log message into log file
   # then return from this funtion
   if ($n == 0) {
      return $FALSE;
   }

   # Sorting the control file list
   @dirFileList = sort sortControlFile @tempList;

   # There is some control file need to be processed
   # but we have to connect to the ETL database first.
   ETL::showTime(); print "Connect to ETL DB...\n";
   $dbCon = ETL::connectETL();

   unless ( defined($dbCon) ) {
      ETL::showTime(); print "ERROR - Unable to connect to ETL database!\n";
      my $errstr = $DBI::errstr;
      ETL::showTime(); print "$errstr\n";
      return $FALSE;
   }

   my $currentJob = getCurrentJobCount();

   # Processing the control file one by one.
   for (my $i=0; $i < $n; $i++)
   {
      if ( $STOP_FLAG ) { last; }

      if ($currentJob >= $maxJobCount) {
         last;
      }

      $filename = $dirFileList[$i];

      # Call the function to process the control file
      my $ret = processControlFile($filename);
      if ($ret == $TRUE) {
      	 $currentJob++;
      }
      
      unless ( $dbCon->ping() ) {
         ETL::showTime(); print "ERROR - Lost database connection.\n";

      	 return $FALSE;
      }

   }

   # We have finished all control file, so we disconnect from ETL database.
   ETL::showTime(); print "Disconnect from ETL DB...\n";
   ETL::disconnectETL($dbCon);

   ETL::showTime(); print "Check queue directory '${ETL::ETL_QUEUE}' done.\n";

   return $TRUE;
}

sub deleteJobFromQueue
{
   my ($dbh, $server, $seqid) = @_;

   my $sqlText = "DELETE FROM ${ETL::ETLDB}ETL_Job_Queue" .
                 "   WHERE ETL_Server = '$server' AND SeqID = $seqid";

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

sub getJobInQueue
{
   my @tabrow;
   my @queueList;
   my $server;
   
   # Get the current automation server name
   $server = ${ETL::ETL_SERVER};

   ETL::showTime(); print "Get job in queue for server '$server'...\n";

   ETL::showTime(); print "Connect to ETL DB...\n";
   $dbCon = ETL::connectETL();

   unless ( defined($dbCon) ) {
      ETL::showTime(); print "ERROR - Unable to connect to ETL database!\n";
      my $errstr = $DBI::errstr;
      ETL::showTime(); print "$errstr\n";
      return $FALSE;
   }

   my $sqlText = "SELECT SeqID, ETL_System, ETL_Job, TXDate" .
                 "   FROM ${ETL::ETLDB}ETL_Job_Queue" .
                 "   WHERE ETL_Server = '$server'" .
                 "   ORDER BY SeqID";

   my $sth = $dbCon->prepare($sqlText);
   unless ($sth) {
      ETL::showTime(); print "Disconnect from ETL DB...\n";

      unless( ETL::disconnectETL($dbCon) ) {
         ETL::showTime(); print "ERROR - Disconnect failed!\n";
      }

      return $FALSE;
   }

   $sth->execute();

   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $queueList[$n++] = $tabrow[0];
      $queueList[$n++] = $tabrow[1];
      $queueList[$n++] = $tabrow[2];
      $queueList[$n++] = $tabrow[3];
   }

   $sth->finish();

   if ( $n == 0 ) {
      ETL::showTime(); print "There is no job in queue for server '$server'.\n";
   	
      ETL::showTime(); print "Disconnect from ETL DB...\n";

      unless( ETL::disconnectETL($dbCon) ) {
         ETL::showTime(); print "ERROR - Disconnect failed!\n";
      }

      return $FALSE;
   }
   
   my ($seqid, $sys, $job, $txdate, $fhead);
   my $ctrlFile;
   my $txdate1;
   my $i;
   my $currentStatus;
   
   for ($i=0; $i <$n; $i = $i+4) {
       $seqid  = $queueList[$i];
       $sys    = $queueList[$i+1];
       $job    = $queueList[$i+2];
       $txdate = $queueList[$i+3];

       ETL::showPrefixSpace(); print "SeqID='$seqid', System='$sys', Job='$job', TXDate='$txdate'\n";

       $fhead = ETL::getConvFileHead($dbCon, $sys, $job);
       if ($fhead eq "") {
          ETL::showPrefixSpace(); print "ERROR - We can not get the converted file head for this job, delete it from queue in repository!\n";
       	  deleteJobFromQueue($dbCon, $server, $seqid);
       	  next;
       }

       $currentStatus = ETL::getJobStatus($dbCon, $sys, $job);
       if ( !($currentStatus eq "" || $currentStatus eq "Ready" || $currentStatus eq "Done" ||
          $currentStatus eq "Frequency Mismatch" || $currentStatus eq "Calendar Mismatch" ||
          $currentStatus eq "Waiting Related Job") ) {
          ETL::showPrefixSpace(); print "The current status is '$currentStatus', we skip it until the next time!\n";
          next;
       }

       ETL::showPrefixSpace(); print "Update the job current status to 'Pending'\n";
       ETL::clearJobStatus($dbCon, $sys, $job, $txdate, 0, "Pending");

       $txdate1 = substr($txdate, 0, 4) . substr($txdate, 5, 2) . substr($txdate, 8, 2);
       $ctrlFile = "${sys}_${fhead}_${txdate1}.dir";

       ETL::showPrefixSpace(); print "Generate a control file '$ctrlFile'\n";

       # Generate control file
       open(CTRL_FILEH, ">${ETL::ETL_QUEUE}${DIRDELI}${ctrlFile}");
       close(CTRL_FILEH);

       ETL::showPrefixSpace(); print "Delete job from queue in repository\n";
       deleteJobFromQueue($dbCon, $server, $seqid);      
   }

   ETL::showTime(); print "Disconnect from ETL DB...\n";

   unless( ETL::disconnectETL($dbCon) ) {
      ETL::showTime(); print "ERROR - Disconnect failed!\n";
   }

   return $TRUE;
}

# The main function of this program
sub main
{
   while ($TRUE)
   {
      if ( $STOP_FLAG ) { last; }

      # Try to create the log file
      unless ( createLogFile() ) {
      	 print STDERR "ERROR - Unable to create log file!\n";
      }

      if ($PRINT_VERSION_FLAG != 1) {
      	 printVersionInfo();
      }

      # Check the queue directory
      checkQueueDir();

      # Check the job queue in repository and try to get the job
      getJobInQueue();
      
      if ( $STOP_FLAG ) { last; }

      # Go to sleep for a while
      sleep($ETL::SLEEP_TIME);
   }

   removeLock();
}

# This function is to see if there is another instance of program running.
# Only one instance of program allow to run at any given time.
# If there is another instance of program is running, we stop the new one.
sub check_instance()
{
   my $count = 1;
   my $LK_FILE_H;

   # For windows platform, we use mutex to control the single instance of program
   if ( $os eq "mswin32" ) {
      $MutexObj = ETL::CreateMutex("ETLMAS_MUTEX");
      if ( ! defined($MutexObj) ) {
         print STDERR "Only one instance of etlmaster.pl allow to run, program terminated!\n";
      	 return $FALSE;
      }

      until (ETL::getMasterLock($ETL::ETL_LOCK)) {
         if ($count++ == 5) {
            #print STDERR "Unable to get master lock for five times, program terminated!\n";
            last;
         }
         sleep(1);
      }
   
      unless ( open(LK_FILE_H, ">$LOCK_FILE") ) {
         ETL::releaseMasterLock($ETL::ETL_LOCK);
         print STDERR "Unable to create lock file!\n";
         return $FALSE;
      }
   
      print LK_FILE_H ("$$\n");
   
      close(LK_FILE_H);
   
      ETL::releaseMasterLock($ETL::ETL_LOCK);
   
      return $TRUE;
   }
   else {
      until (ETL::getMasterLock($ETL::ETL_LOCK)) {
         if ($count++ == 5) {
            print STDERR "Unable to get master lock for five times, program terminated!\n";
            return $FALSE;
         }
         sleep(1);
      }
   
      if ( -f $LOCK_FILE ) {
         ETL::releaseMasterLock($ETL::ETL_LOCK);
         print STDERR "Only one instance of etlmaster.pl allow to run, program terminated!\n";
   
         return $FALSE;
      }  
   
      unless ( open(LK_FILE_H, ">$LOCK_FILE") ) {
         ETL::releaseMasterLock($ETL::ETL_LOCK);
   
         print STDERR "Unable to create lock file, program terminated!\n";
         return $FALSE;
      }
   
      print LK_FILE_H ("$$\n");
   
      close(LK_FILE_H);
   
      ETL::releaseMasterLock($ETL::ETL_LOCK);
   
      return $TRUE;
   }
}

# To remove the lock file create by this program
sub removeLock
{
   my $count = 1;

   # For windows platform, we use mutex to control single instance of program
   # We have to release the mutex before program terminated
   if ( $os eq "mswin32" ) {
      ETL::ReleaseMutex($MutexObj); 

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
   else {
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
}

# This function will be called when the program has catched some signal
# The purpose of this program is to some clean up task when the program terminate.
sub cleanUp
{
   my ($signal) = @_;
   my $count = 1;

   until (ETL::getMasterLock($ETL::ETL_LOCK)) {
      if ($count++ == 5) {
         return;
      }
      sleep(1);
   }

   unlink($LOCK_FILE);

   ETL::releaseMasterLock($ETL::ETL_LOCK);

#   print STDOUT ("Clean Up by $signal\n");

   if ( $LOG_STAT == 1 ) {
      print LOGF_H "Stop by signal '${signal}'\n";
   }

   $STOP_FLAG = 1;

   # If the system is Unix, we exit the program when receive the singnal
   if ( $os eq "svr4" ) {
      exit(0);
   }
}

sub printVersionInfo
{
   print "\n";
   ETL::showTime(); print "************************************************************\n";
   ETL::showTime(); print "* ETL Automation Master Program {$VERSION}, NCR 2001 Copyright. *\n";
   ETL::showTime(); print "************************************************************\n";
   print "\n";
   $PRINT_VERSION_FLAG = 1;
}

###############################################################################
# Program Section

$LOCK_FILE = "${ETL::ETL_LOCK}${DIRDELI}etlmaster.lock";
$LASTLOGFILE = "";

unless ( check_instance() ) { exit(1); }

$SIG{'INT'}  = 'cleanUp';
$SIG{'QUIT'} = 'cleanUp';
$SIG{'TERM'} = 'cleanUp';

open(STDERR, ">&STDOUT");

main();

exit(0);

__END__

