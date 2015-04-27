#!/usr/bin/perl -w
#####################################################################
# Program: etlclean.pl
# Purpose: This program is doing the housekeeping job
#####################################################################

use strict;
use DBI;

my $VERSION = "v2.5.3";

my $home = $ENV{"AUTO_HOME"};
my $os   = $^O;

$os =~ tr [A-Z][a-z];
my $DIRDELI;
if ( $os eq "mswin32" ) {
   unshift(@INC, "$home\\bin");
   require etl_nt;
   $DIRDELI = "\\";
}
else {
   unshift(@INC, "$home/bin");
   require etl_unix;
   $DIRDELI = "/";
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
my @SystemList;

my $MutexObj;

my $SLEEP_TIME = 60;
my $KEEP_PERIOD = 30;
my $PRIMARY_SERVER = 0;

my $value;

$value = $ENV{"AUTO_DATA_COMPLETE"};
if ( defined($value) ) {
   $ETL::ETL_COMPLETE = $value;
}

$value = $ENV{"AUTO_DATA_CORRUPT"};
if ( defined($value) ) {
   $ETL::ETL_CORRUPT = $value;
}

$value = $ENV{"AUTO_DATA_DUPLICATE"};
if ( defined($value) ) {
   $ETL::ETL_DUPLICATE = $value;
}

$value = $ENV{"AUTO_DATA_ERROR"};
if ( defined($value) ) {
   $ETL::ETL_ERROR = $value;
}

$value = $ENV{"AUTO_DATA_BYPASS"};
if ( defined($value) ) {
   $ETL::ETL_BYPASS = $value;
}

$value = $ENV{"AUTO_DATA_UNKNOWN"};
if ( defined($value) ) {
   $ETL::ETL_UNKNOWN = $value;
} 

$value = $ENV{"AUTO_KEEP_PERIOD"};
if ( defined($value) ) {
   $KEEP_PERIOD = $value;
} 
else {
   $KEEP_PERIOD = 30;
}

$value = $ENV{"AUTO_PRIMARY_SERVER"};
if ( defined($value) ) {
   $PRIMARY_SERVER = $value;
} 
else {
   $PRIMARY_SERVER = 0;
}

my $PRINT_VERSION_FLAG = 0;

#####################################################################
# Function section

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
   $LOGFILE = "${ETL::ETL_LOG}${DIRDELI}etlclean_${TODAY}.log";

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

###################################################################
# Database related function
sub getSystemList
{
   my ($dbh) = @_;
   my @tabrow;

   my $sqlText = "SELECT ETL_System FROM ${ETL::ETLDB}ETL_Sys" .
                 " ORDER BY ETL_System";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return -1;
   }

   $sth->execute();

   @SystemList = ();
   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $SystemList[$n++] = $tabrow[0];
   }

   $sth->finish();

   return $n;
}

sub getKeepPeriod
{
   my ($dbh, $sys) = @_;
   my @tabrow;

   my $sqlText = "SELECT DataKeepPeriod, LogKeepPeriod, RecordKeepPeriod" .
                 "  FROM ${ETL::ETLDB}ETL_Sys" .
                 " WHERE ETL_System = '$sys'";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return "";
   }

   $sth->execute();

   @tabrow = $sth->fetchrow();
   $sth->finish();

   unless ( @tabrow ) {
      return undef;
   }

   return ($tabrow[0], $tabrow[1], $tabrow[2]);
}

# End of database related function
###################################################################

sub removeSubDirectory
{
   my ($dirname) = @_;
   my $fname;
   
   unless ( opendir(SUB_DIR, "${dirname}") ) {
      ETL::showTime(); print ("Unable to open '${dirname}'\n");
      return $FALSE;
   }

   while ($fname = readdir(SUB_DIR))
   {
      if ( $fname eq "." || $fname eq ".." ) { next; }
      if ( -f "${dirname}${DIRDELI}${fname}" ) {
      	 unlink("${dirname}${DIRDELI}${fname}");
      }
   }

   closedir(SUB_DIR);

   my $ret = rmdir("${dirname}");

   if (! $ret) {
      ETL::showTime(); print "ERROR: $!\n";
   }
   
   return $TRUE;
}

sub removeDirectory
{
   my ($dirname) = @_;
   my $fname;

   ETL::showPrefixSpace(); print "Removing directory '${dirname}'...\n";

   if ( $os ne "mswin32" ) {
      `/usr/bin/rm -r "${dirname}"`;
      return $TRUE;
   }
  
   unless ( opendir(REM_DIR, "${dirname}") ) {
      ETL::showTime(); print ("Unable to open '${dirname}'\n");
      return $FALSE;
   }

   
   while ($fname = readdir(REM_DIR))
   {
      if ( $fname eq "." || $fname eq ".." ) { next; }
      
      if ( -f "${dirname}${DIRDELI}${fname}" ) {
      	 unlink("${dirname}${DIRDELI}${fname}");
      }
      elsif ( -d "${dirname}${DIRDELI}${fname}" ) {
      	 removeSubDirectory("${dirname}${DIRDELI}${fname}");
      }
   }

   closedir(REM_DIR);

   my $ret = rmdir("${dirname}");
   
   if (! $ret) {
      ETL::showTime(); print "ERROR: $!\n";
   }
   
   return $TRUE;
}

sub cleanDataComplete
{
   my ($sys, $expireDate) = @_;
   my $filename;
   my $dataDir;

   $dataDir = "${ETL::ETL_COMPLETE}${DIRDELI}${sys}";
   if ( ! -d $dataDir ) {
      return;
   }
      
   ETL::showTime(); print "Clean complete directory for system '$sys' before '$expireDate'\n";

   # Open the directory for processing
   unless ( opendir(COMPLETE_DIR, $dataDir) ) {
      ETL::showTime(); print "Unable to open ${dataDir}\n";
      return $FALSE;
   }

   while ($filename = readdir(COMPLETE_DIR))
   {
      if ( $filename eq "." || $filename eq ".." ) { next; }

      if ( -d "${dataDir}${DIRDELI}${filename}" ) {
         if ( $filename < $expireDate ) {
            ETL::showPrefixSpace(); print "Complete Data Directory ${dataDir}${DIRDELI}${filename} is expired, remove it!\n";
            removeDirectory("${dataDir}${DIRDELI}${filename}");
         }
      }
   }
   # Close the directory
   closedir(COMPLETE_DIR);
}

sub cleanDataFailCorrupt
{
   my ($sys, $expireDate) = @_;
   my $filename;
   my $dataDir;

   $dataDir = "${ETL::ETL_CORRUPT}${DIRDELI}${sys}";
   if ( ! -d $dataDir ) {
      return;
   }

   ETL::showTime(); print "Clean corrupt directory for system '$sys' before '$expireDate'\n";

   # Open the directory for processing
   unless ( opendir(CORRUPT_DIR, $dataDir) ) {
      ETL::showTime(); print ("Unable to open ${dataDir}\n");
      return $FALSE;
   }

   while ($filename = readdir(CORRUPT_DIR))
   {
      if ( $filename eq "." || $filename eq ".." ) { next; }

      if ( -d "${dataDir}${DIRDELI}${filename}" ) {
         if ( $filename < $expireDate ) {
            ETL::showPrefixSpace(); print "Corrupt Directory ${dataDir}${DIRDELI}${filename} is expired, remove it!\n";
            removeDirectory("${dataDir}${DIRDELI}${filename}");
         }
      }
   }
   # Close the directory
   closedir(CORRUPT_DIR);
}

sub cleanDataFailDuplicate
{
   my ($sys, $expireDate) = @_;
   my $filename;
   my $dataDir;

   $dataDir = "${ETL::ETL_DUPLICATE}${DIRDELI}${sys}";
   if ( ! -d $dataDir ) {
      return;
   }

   ETL::showTime(); print "Clean duplicate directory for system '$sys' before '$expireDate'\n";

   # Open the directory for processing
   unless ( opendir(DUPLICATE_DIR, $dataDir) ) {
      ETL::showTime(); print ("Unable to open ${dataDir}\n");
      return $FALSE;
   }

   while ($filename = readdir(DUPLICATE_DIR))
   {
      if ( $filename eq "." || $filename eq ".." ) { next; }

      if ( -d "${dataDir}${DIRDELI}${filename}" ) {
         if ( $filename < $expireDate ) {
            ETL::showPrefixSpace(); print "Duplicate Directory ${dataDir}${DIRDELI}${filename} is expired, remove it!\n";
            removeDirectory("${dataDir}${DIRDELI}${filename}");
         }
      }
   }
   # Close the directory
   closedir(DUPLICATE_DIR);
}

sub cleanDataFailError
{
   my ($sys, $expireDate) = @_;
   my $filename;
   my $dataDir;

   $dataDir = "${ETL::ETL_ERROR}${DIRDELI}${sys}";
   if ( ! -d $dataDir ) {
      return;
   }

   ETL::showTime(); print "Clean error directory for system '$sys' before '$expireDate'\n";

   # Open the directory for processing
   unless ( opendir(ERROR_DIR, $dataDir) ) {
      ETL::showTime(); print ("Unable to open ${dataDir}!\n");
      return $FALSE;
   }

   while ($filename = readdir(ERROR_DIR))
   {
      if ( $filename eq "." || $filename eq ".." ) { next; }

      if ( -d "${dataDir}${DIRDELI}${filename}" ) {
         if ( $filename < $expireDate ) {
            ETL::showPrefixSpace(); print "Error Directory ${dataDir}${DIRDELI}${filename} is expired, remove it!\n";
            removeDirectory("${dataDir}${DIRDELI}${filename}");
         }
      }
   }
   # Close the directory
   closedir(ERROR_DIR);
}

sub cleanDataFailBypass
{
   my ($sys, $expireDate) = @_;
   my $filename;
   my $dataDir;

   $dataDir = "${ETL::ETL_BYPASS}${DIRDELI}${sys}";
   if ( ! -d $dataDir ) {
      return;
   }

   ETL::showTime(); print "Clean bypass directory for system '$sys' before '$expireDate'\n";

   # Open the directory for processing
   unless ( opendir(BYPASS_DIR, $dataDir) ) {
      ETL::showTime(); print ("Unable to open ${dataDir}!\n");
      return $FALSE;
   }

   while ($filename = readdir(BYPASS_DIR))
   {
      if ( $filename eq "." || $filename eq ".." ) { next; }

      if ( -d "${dataDir}${DIRDELI}${filename}" ) {
         if ( $filename < $expireDate ) {
            ETL::showPrefixSpace(); print "Bypass Directory ${dataDir}${DIRDELI}${filename} is expired, remove it!\n";
            removeDirectory("${dataDir}${DIRDELI}${filename}");
         }
      }
   }
   # Close the directory
   closedir(BYPASS_DIR);
}

sub cleanDataFailUnknown
{
   my ($expireDate) = @_;
   
   my $filename;

   # Open the directory for processing
   unless ( opendir(UNKNOWN_DIR, ${ETL::ETL_UNKNOWN}) ) {
      ETL::showTime(); print ("Unable to open ${ETL::ETL_UNKNOWN}\n");
      return $FALSE;
   }

   while ($filename = readdir(UNKNOWN_DIR))
   {
      if ( $filename eq "." || $filename eq ".." ) { next; }

      if ( -d "${ETL::ETL_UNKNOWN}${DIRDELI}${filename}" ) {
         if ( $filename < $expireDate ) {
            ETL::showPrefixSpace(); print "Unknown Directory $filename is expired, remove it!\n";
            removeDirectory("${ETL::ETL_UNKNOWN}${DIRDELI}${filename}");
         }
      }
   }
   # Close the directory
   closedir(UNKNOWN_DIR);
}

sub cleanSystemLog
{
   my ($expireDate) = @_;   
   my $filename;

   # Open the directory for processing
   unless ( opendir(SYSLOG_DIR, ${ETL::ETL_LOG}) ) {
      ETL::showTime(); print ("Unable to open ${ETL::ETL_LOG}\n");
      return $FALSE;
   }

   ETL::showTime(); print "Clean up system log file...\n";
   
   while ($filename = readdir(SYSLOG_DIR))
   {
      if ( $filename eq "." || $filename eq ".." ) { next; }

      if ( -d "${ETL::ETL_LOG}${DIRDELI}${filename}" ) {
      	 next;
      }
      elsif ( -f "${ETL::ETL_LOG}${DIRDELI}${filename}" ) {
         if (substr($filename, length($filename) - 4, 4) eq ".log") {
            my $date = substr($filename, length($filename) - 12, 8);
            if ( $date < $expireDate ) {
               ETL::showPrefixSpace(); print "Log File $filename is expired, remove it!\n";
               unlink("${ETL::ETL_LOG}${DIRDELI}${filename}");
            }
         }
      }
   }
   # Close the directory
   closedir(SYSLOG_DIR);
}

sub calculateExpiredDate
{
   my ($period) = @_;
   my $expiredDate;
   my ($year, $month, $day);
   
   if ( $period == 0 ) { return ""; }
   
   $year  = substr($TODAY, 0, 4);
   $month = substr($TODAY, 4, 2);
   $day   = substr($TODAY, 6, 2);

   # Convert string to number in order to cut the prefix zero
   $year += 0;
   $month += 0;
   $day += 0;

   while ($period > 0) {
      if ($day == 1) {
      	$month--;
      	if ($month==0) {
      	   $year--;
      	   $month = 12;
      	}
      	if ($month==1||$month==3||$month==5||$month==7||$month==8||
      	    $month==10||$month==12) {
            $day = 31;      	    	
      	}
      	elsif ($month==4||$month==6||$month==9||$month==11) {
      	    $day = 30;
      	}
      	elsif ($month==2 && ((($year%4)==0 && ($year%100)!=0) || ($year%400)==0)) {
      	    $day = 29;
      	}
      	elsif ($month==2) {
      	    $day = 28;
      	}
      } else {
      	$day--;
      }
      
      $period--;
   }

   if ($month < 10 && substr($month, 0, 1) ne "0") {
      $month = "0${month}";
   }

   if ($day < 10 && substr($day, 0, 1) ne "0") {
      $day = "0${day}";
   }
   
   $expiredDate = "${year}${month}${day}";   

   return $expiredDate;
}

sub calculateExpiredDateTime
{
   my ($period) = @_;
   my $expiredDateTime;
   my ($year, $month, $day);

   if ( $period == 0 ) { return ""; }
      
   $year  = substr($TODAY, 0, 4);
   $month = substr($TODAY, 4, 2);
   $day   = substr($TODAY, 6, 2);

   while ($period > 0) {
      if ($day == 1) {
      	$month--;
      	if ($month==0) {
      	   $year--;
      	   $month = 12;
      	}
      	if ($month==1||$month==3||$month==5||$month==7||$month==8||
      	    $month==10||$month==12) {
            $day = 31;      	    	
      	}
      	elsif ($month==4||$month==6||$month==9||$month==11) {
      	    $day = 30;
      	}
      	elsif ($month==2 && ((($year%4)==0 && ($year%100)!=0) || ($year%400)==0)) {
      	    $day = 29;
      	}
      	elsif ($month==2) {
      	    $day = 28;
      	}
      } else {
      	$day--;
      }
      
      $period--;
   }

   if ($month < 10 && substr($month, 0, 1) ne "0") {
      $month = "0${month}";
   }

   if ($day < 10 && substr($day, 0, 1) ne "0") {
      $day = "0${day}";
   }

   $expiredDateTime = "${year}-${month}-${day} 23:59:59";   

   return $expiredDateTime;
}

sub cleanJobReceivedFileLog
{
   my ($dbh, $sys, $expireDateTime) = @_;
   my $perioddate;
   
   ETL::showTime(); print "Clean ETL_Received_File for system '$sys' before '$expireDateTime'\n";
   
   my $sqltext = "DELETE FROM ${ETL::ETLDB}ETL_Received_File" .
                 " WHERE ETL_System = '$sys'" .
                 "   AND ReceivedTime <= '${expireDateTime}'";
   
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

sub cleanJobRecordLog
{
   my ($dbh, $sys, $expireDateTime) = @_;
   my $perioddate;
   
   ETL::showTime(); print "Clean ETL_Record_Log for system '$sys' before '$expireDateTime'\n";

   my $sqltext = "DELETE FROM ${ETL::ETLDB}ETL_Record_Log" .
                 " WHERE ETL_System = '$sys'" .
                 "   AND RecordTime <= '${expireDateTime}'";

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

sub cleanJobStatus
{
   my ($dbh, $sys, $expireDateTime) = @_;
   my $perioddate;
   
   ETL::showTime(); print "Clean ETL_Job_Status for system '$sys' before '$expireDateTime'\n";

   my $sqltext = "DELETE FROM ${ETL::ETLDB}ETL_Job_Status" .
                 " WHERE ETL_System = '$sys'" .
                 "   AND StartTime <= '${expireDateTime}'";

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

sub cleanJobDetailLog
{
   my ($dbh, $sys, $expireDateTime) = @_;
   my $perioddate;
   
   ETL::showTime(); print "Clean ETL_Job_Log for system '$sys' before '$expireDateTime'\n";

   my $sqltext = "DELETE FROM ${ETL::ETLDB}ETL_Job_Log" .
                 " WHERE ETL_System = '$sys'" .
                 "   AND StartTime <= '${expireDateTime}'";

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


sub cleanRecordLogForSys
{
   my ($dbh, $sys, $expireDateTime) = @_;
   
   ETL::showTime(); print "Clean repository record log for system '$sys'\n";
   
   cleanJobReceivedFileLog($dbh, $sys, $expireDateTime);
   cleanJobRecordLog($dbh, $sys, $expireDateTime);
   cleanJobStatus($dbh, $sys, $expireDateTime);
   cleanJobDetailLog($dbh, $sys, $expireDateTime);
}

sub cleanLocalLogFile
{
   my ($sys, $expireDate) = @_;
   my $filename;
   my $logDir;
   
   $logDir = "${ETL::ETL_LOG}${DIRDELI}${sys}";
   if ( ! -d $logDir ) {
      return;
   }
   
   ETL::showTime(); print "Clean local log file for system '$sys' before '$expireDate'\n";
      
   # Open the directory for processing
   unless ( opendir(LOG_DIR, ${logDir}) ) {
      ETL::showTime(); print ("Unable to open ${logDir}!\n");
      return $FALSE;
   }

   my $count = 0;
   my @tempList;
   while ($filename = readdir(LOG_DIR))
   {
      if ( $filename eq "." || $filename eq ".." ) { next; }

      if ( -d "${logDir}${DIRDELI}${filename}" ) {
         if ( $filename < $expireDate ) {
            ETL::showPrefixSpace(); print "Local log directory ${logDir}${DIRDELI}${filename} is expired, remove it!\n";
            removeDirectory("${logDir}${DIRDELI}${filename}");
         }
      }      
   }
   # Close the directory
   closedir(LOG_DIR);
}

sub cleanSystem
{
   my ($dbh, $sys, $dataPeriod, $logPeriod, $recordPeriod) = @_;

   ETL::showTime(); print "Do house keeping for system '$sys'\n";
   ETL::showPrefixSpace(); print "Data Period=$dataPeriod, Log Period=$logPeriod, Record Period=$recordPeriod\n";
   
   my $expireDate = calculateExpiredDate($dataPeriod);
   if ( $expireDate ne "" ) {
      cleanDataComplete($sys, $expireDate);
      cleanDataFailCorrupt($sys, $expireDate);
      cleanDataFailDuplicate($sys, $expireDate);
      cleanDataFailError($sys, $expireDate);
      cleanDataFailBypass($sys, $expireDate);
   }
   
   $expireDate = calculateExpiredDate($logPeriod);
   if ( $expireDate ne "" ) {
      cleanLocalLogFile($sys, $expireDate);
   }
   
   if ( $PRIMARY_SERVER == 1 ) {
      my $expireDateTime = calculateExpiredDateTime($recordPeriod);
      if ( $expireDateTime ne "" ) {
         cleanRecordLogForSys($dbh, $sys, $expireDateTime);
      }
   }
}

sub houseKeeping
{
   my ($dataPeriod, $logPeriod, $recordPeriod);
   my $sysname;
   
   ETL::showTime(); print "Connect to ETL DB...\n";
   $dbCon = ETL::connectETL();

   unless ( defined($dbCon) ) {
      ETL::showTime(); print "ERROR - Unable to connect to ETL database!\n";
      my $errstr = DBI::errstr;
      ETL::showTime(); print "$errstr\n";
      return $FALSE;
   }

   ETL::showTime(); print "Get the system list from repository...\n";
   my $count = getSystemList($dbCon);
   
   for (my $i = 0; $i < $count; $i++) {
       $sysname = $SystemList[$i];
       ($dataPeriod, $logPeriod, $recordPeriod) = getKeepPeriod($dbCon, $sysname);
       
       cleanSystem($dbCon, $sysname, $dataPeriod, $logPeriod, $recordPeriod);
   }
   
   my $expireDate = calculateExpiredDate($KEEP_PERIOD);
   if ( $expireDate ne "" ) {
      cleanDataFailUnknown($expireDate);
      cleanSystemLog($expireDate);
   }

   ETL::showTime(); print "Disconnect from ETL DB...\n";
   ETL::disconnectETL($dbCon);
}

sub checkDataCalendar
{
   my ($dbh, $sys, $job, $year, $month, $day) = @_;
   my @tabrow;
   my ($dateSeq, $checkFlag);

   # To see if the data date is in data calendar
   my $sqltext = "SELECT SeqNum" .
                 "  FROM ${ETL::ETLDB}DataCalendar" .
                 "    WHERE etl_system = '$sys' AND etl_job = '$job'" .
                 "      AND calendarYear = $year AND calendarMonth = $month" .
                 "      AND calendarDay = $day";

   my $sth = $dbh->prepare($sqltext);

   unless ($sth) {
      return $FALSE;
   }

   $sth->execute();

   if (@tabrow = $sth->fetchrow()) {
      $dateSeq = $tabrow[0];
   } else {
      $dateSeq = -1;
   }

   $sth->finish();

   # This data date is not in data calendar
   if ($checkFlag == -1) {
      return $FALSE;
   }
   
   return $TRUE;
}

sub checkFrequency
{
   my ($frequency, $txdate) = @_;
   my $frequency_chk = 0;
   
   my @mLIST = split(/,/,$frequency) ;
   my $myDate;

   foreach(@mLIST)
   {
      $myDate=$_;

      if ($myDate == 0) {   # Everyday
         $frequency_chk = 1;
         last;
      }

      if ($myDate == -1) { # End of Month 
         my $nextDay = ETL::getNextDayNumber($txdate);

         if ( $nextDay == 1 ) {
            $frequency_chk = 1;
            last;
         }
      }

      if (($myDate >= 1) and ($myDate <= 31)) { # Monthly
         my $monthDay = ETL::getMonthDayNumber($txdate);

         if ($myDate == $monthDay) {
            $frequency_chk = 1;
            last;
         } 
      }

      if (($myDate >= 41) and ($myDate <= 47)) { # Weekly
         my $weekDay = $myDate - 40;
         if ($weekDay == 7) { $weekDay = 0 };

         my $wday = ETL::getWeekDayNumber($txdate);

         if ( $weekDay == $wday ) {
            $frequency_chk = 1;
         }
      }
   }   # end of foreach

   return $frequency_chk;
}

sub main
{
   my $lastday = "";
   my $count = 0;
   
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

      if ( "$lastday" ne "$TODAY" ) {
         # Do the house keeping job
         if ( houseKeeping() == $FALSE ) {
            sleep($SLEEP_TIME);
            next;
         }
         
         $lastday = $TODAY;
      }

      if ( $STOP_FLAG ) { last; }

      # Go to sleep for a while
      sleep($SLEEP_TIME);
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
      $MutexObj = ETL::CreateMutex("ETLCLN_MUTEX");
      if ( ! defined($MutexObj) ) {
         print STDERR "Only one instance of etlclean.pl allow to run, program terminated!\n";
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
         print STDERR "Only one instance of etlclean.pl allow to run, program terminated!\n";
   
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
   exit(0);
}

sub printVersionInfo
{
   print "\n";
   ETL::showTime(); print "*******************************************************************\n";
   ETL::showTime(); print "* ETL Automation House Keeping Program ${VERSION}, NCR 2001 Copyright. *\n";
   ETL::showTime(); print "*******************************************************************\n";
   print "\n";
   $PRINT_VERSION_FLAG = 1;
}

#####################################################################

$LOCK_FILE = "${ETL::ETL_LOCK}${DIRDELI}etlclean.lock";
$LASTLOGFILE = "";

unless ( check_instance() ) { exit(1); }

$SIG{'INT'}  = 'cleanUp';
$SIG{'QUIT'} = 'cleanUp';
$SIG{'TERM'} = 'cleanUp';

open(STDERR, ">&STDOUT");

main();

exit(0);

__END__

