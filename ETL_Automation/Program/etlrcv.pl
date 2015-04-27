#!/usr/bin/perl
###############################################################################
# Program  : etlrcv.pl
# Argument : none
###############################################################################

package PerlSvc;

our $Name = 'TestSvc';
our $DisplayName = 'Test Service Display Name';

sub Startup {
    # here's where your startup code will go
    while (ContinueRun()) {
        sleep(1);
    }
}

sub Install {
    # add your additional install messages or functions here
    print "\nAdditional install notes\n";
}

sub Remove {
    # add your additional remove messages or functions here
    print "\nAdditional remove notes\n";
}

sub Help {
    # add your additional help messages or functions here
    print "\nAdditional help notes\n";
}

package main;

use strict;
use DBI;

my $VERSION = "v2.50";

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
my @dataFileList;

my %currentJobStatus;

my $MutexObj;

my $LONG_NAME_MODE = 0;
my $SHORT_NAME_MODE = 1;
my $EVA_NAME_MODE = 2;

# Get the control file name mode
# If the value is 0, using long name mode, control file name is start with dir.
# If the value is 1, using short name mode, control file name is start with D
my $NameMode = $ENV{"AUTO_NAME"};
my $ConvertSourceName = $ENV{"AUTO_CONVERT"};
my $ConvertControlFlag = $ENV{"AUTO_CONVERT_CONTROL"};

if ( $NameMode eq "" ) {
   $NameMode = 0;
}

if ( $ConvertSourceName eq "" ) {
   $ConvertSourceName = 0;
}

if ( $ConvertControlFlag eq "" ) {
   $ConvertControlFlag = 0;
}

$ConvertSourceName = 0;  # Set the variable to be 0 in order to avoid do source file name convert

my $value;

$value = $ENV{"AUTO_DATA_RECEIVE"};
if ( defined($value) ) {
   $ETL::ETL_RECEIVE = $value;
}

$value = $ENV{"AUTO_DATA_QUEUE"};
if ( defined($value) ) {
   $ETL::ETL_QUEUE = $value;
}

$value = $ENV{"AUTO_DATA_COMPLETE"};
if ( defined($value) ) {
   $ETL::ETL_COMPLETE = $value;
}

$value = $ENV{"AUTO_DATA_FAIL"};
if ( defined($value) ) {
   $ETL::ETL_FAIL = $value;
}

$value = $ENV{"AUTO_DATA_CORRUPT"};
if ( defined($value) ) {
   $ETL::ETL_CORRUPT = $value;
}

$value = $ENV{"AUTO_DATA_BYPASS"};
if ( defined($value) ) {
   $ETL::ETL_BYPASS = $value;
}

$value = $ENV{"AUTO_DATA_DUPLICATE"};
if ( defined($value) ) {
   $ETL::ETL_DUPLICATE = $value;
}

$value = $ENV{"AUTO_DATA_ERROR"};
if ( defined($value) ) {
   $ETL::ETL_ERROR = $value;
}

$value = $ENV{"AUTO_DATA_UNKNOWN"};
if ( defined($value) ) {
   $ETL::ETL_UNKNOWN = $value;
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
   $LOGFILE = "${ETL::ETL_LOG}${DIRDELI}etlrcv_${TODAY}.log";

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

   if ( $NameMode == $SHORT_NAME_MODE ) {
      if ( substr($filename, 0, 1) eq "D" ) {
         return $TRUE;
      }
      else  {
         return $FALSE;
      }
   }
   elsif ( $NameMode == $LONG_NAME_MODE ) {
      if ( substr($filename, 0, 4) eq "dir." ) {
         return $TRUE;
      }
      else  {
         return $FALSE;
      }
   }
   elsif ( $NameMode == $EVA_NAME_MODE ) {
      if ( substr($filename, 0, 4) eq "dir." ||
           substr($filename, 0, 4) eq "DIR." ) {
         return $TRUE;
      }
      else  {
         return $FALSE;
      }
   }
}

sub checkDataFileSize
{
   my $count = $#dataFileList;
   my ($file, $size, $newsize);
   my $ret = $TRUE;
   my @fields;

   ETL::showPrefixSpace(); print "Check data file size...\n";

   for (my $n=0; $n <= $count; $n++)
   {
      @fields = split(/\s+/, $dataFileList[$n]);

      $file = $fields[0];

      unless ( -f "${ETL::ETL_RECEIVE}${DIRDELI}${file}" ) {
         ETL::showPrefixSpace(); print "Error: Data file '$file' is not existing\n";
         $ret = $FALSE;
         next;
      }

      $size = (stat("${ETL::ETL_RECEIVE}${DIRDELI}${file}"))[7];
      
      # If the size is less than 0, which means the real size is greater than 2GB
      # We convert it to real size. However, we only accept a file which size is
      # less than 4GB only in order to compare the size in control file
      if ( $size < 0 ) {
         $newsize = 4294967296 + $size;
         $size = $newsize;
      }
      
      if ( $size != $fields[1] ) {
         ETL::showPrefixSpace(); print "Error: Data file '$file' size didn't match\n";
         $ret = $FALSE;
      }
   }

   return $ret;
}

sub checkDataFileDuplicate
{
   my ($sys, $job) = @_;
   my $count = $#dataFileList;
   my $file;
   my $ret = $TRUE;
   my @fields;
   my $eventDesc;

   ETL::showPrefixSpace(); print "Check data file duplicate...\n";

   for (my $n=0; $n <= $count; $n++)
   {
      @fields = split(/\s+/, $dataFileList[$n]);

      $file = $fields[0];

      if ( ETL::checkSourceFileDuplicate($dbCon, $sys, $job, $file) ) {
         ETL::showPrefixSpace(); print "The source file '$file' is already received!\n";
         $ret = $FALSE;
         
         $eventDesc = "[$sys], [$job] has received duplicate file $file";
         ETL::insertEventLog($dbCon, "RCV", "M", "$eventDesc");
      }
   }

   return $ret;
}

sub moveDataFileToCorrupt
{
   my($controlFile, $sys) = @_;
   my ($oldpath, $newpath, $syspath);
   my @fields;
   my $file;

   ETL::showPrefixSpace(); print "Move data file(s) to corrupt directory...\n";

   $oldpath = "${ETL::ETL_RECEIVE}";
   $syspath = "${ETL::ETL_CORRUPT}${DIRDELI}${sys}";
   $newpath = "${syspath}${DIRDELI}${TODAY}";

   unless ( -d $syspath ) {
      ETL::createDirectory($syspath);   	
   }
   
   unless ( -d $newpath ) {
      ETL::createDirectory($newpath);
   }

   my $count = $#dataFileList;

   for (my $n=0; $n <= $count; $n++)
   {
      @fields = split(/\s+/, $dataFileList[$n]);

      $file = $fields[0];

      ETL::moveFile("${oldpath}${DIRDELI}${file}", "${newpath}${DIRDELI}${file}");
   }

   return $TRUE;
}

sub moveDataFileToByPass
{
   my($controlFile, $sys) = @_;
   my ($oldpath, $newpath, $syspath);
   my @fields;
   my $file;

   ETL::showPrefixSpace(); print "Move data file(s) to bypass directory...\n";

   $oldpath = "${ETL::ETL_RECEIVE}";
   $syspath = "${ETL::ETL_BYPASS}${DIRDELI}${sys}";
   $newpath = "${syspath}${DIRDELI}${TODAY}";

   unless ( -d $syspath ) {
      ETL::createDirectory($syspath);   	
   }

   unless ( -d $newpath ) {
      ETL::createDirectory($newpath);
   }

   my $count = $#dataFileList;

   for (my $n=0; $n <= $count; $n++)
   {
      @fields = split(/\s+/, $dataFileList[$n]);

      $file = $fields[0];

      ETL::moveFile("${oldpath}${DIRDELI}${file}", "${newpath}${DIRDELI}${file}");
   }

   return $TRUE;
}

sub moveDataFileToDuplicate
{
   my($controlFile, $sys) = @_;
   my ($oldpath, $newpath, $syspath);
   my @fields;
   my $file;

   ETL::showPrefixSpace(); print "Move data file(s) to duplicate directory...\n";

   $oldpath = "${ETL::ETL_RECEIVE}";
   $syspath = "${ETL::ETL_DUPLICATE}${DIRDELI}${sys}";
   $newpath = "${syspath}${DIRDELI}${TODAY}";

   unless ( -d $syspath ) {
      ETL::createDirectory($syspath);   	
   }

   unless ( -d $newpath ) {
      ETL::createDirectory($newpath);
   }

   my $count = $#dataFileList;

   for (my $n=0; $n <= $count; $n++)
   {
      @fields = split(/\s+/, $dataFileList[$n]);

      $file = $fields[0];

      ETL::moveFile("${oldpath}${DIRDELI}${file}", "${newpath}${DIRDELI}${file}");
   }

   return $TRUE;
}

sub moveDataFileToUnknown
{
   my ($controlFile) = @_;
   my ($oldpath, $newpath);
   my @fields;
   my $file;

   ETL::showPrefixSpace(); print "Move data file(s) to unknown directory...\n";
   
   $oldpath = "${ETL::ETL_RECEIVE}";
   $newpath = "${ETL::ETL_UNKNOWN}${DIRDELI}${TODAY}";

   unless ( -d $newpath ) {
      ETL::createDirectory($newpath);
   }

   my $count = $#dataFileList;

   for (my $n=0; $n <= $count; $n++)
   {
      @fields = split(/\s+/, $dataFileList[$n]);

      $file = $fields[0];

      ETL::moveFile("${oldpath}${DIRDELI}${file}", "${newpath}${DIRDELI}${file}");
   }

   return $TRUE;
}

sub moveDataFileToQueue
{
   my ($controlFile) = @_;
   my ($oldpath, $newpath);
   my @fields;
   my $file;

   ETL::showPrefixSpace(); print "Move data file(s) to queue directory...\n";
   
   $oldpath = "${ETL::ETL_RECEIVE}";
   $newpath = "${ETL::ETL_QUEUE}";
   
   my $rc;

   my $count = $#dataFileList;

   for (my $n=0; $n <= $count; $n++)
   {
      @fields = split(/\s+/, $dataFileList[$n]);

      $file = $fields[0];

      ETL::showPrefixSpace(); print "Move source data '$file' to queue directory.\n";

      if ( $os eq "svr4" ) {  # Unix
         $rc = ETL::moveFile("$oldpath/$file", "$newpath/$file");

         if ( $rc != 0 ) {
            ETL::showPrefixSpace(); print "Move source data '$file' to queue failed!!!\n"; 
         }
      }
      elsif ( $os eq "mswin32" ) {  # NT/2000
         $rc = ETL::moveFile("$oldpath\\$file", "$newpath\\$file");

         unless ( $rc ) {
            ETL::showPrefixSpace(); print "Move source data '$file' to queue failed!!!\n"; 
         }
      }
   }

   return $TRUE;
}

sub moveControlFileToCorrupt
{
   my ($controlFile, $sys) = @_;
   my ($oldpath, $newpath);
   
   $oldpath = "${ETL::ETL_RECEIVE}";
   $newpath = "${ETL::ETL_CORRUPT}${DIRDELI}${sys}${DIRDELI}${TODAY}";
   
   ETL::showPrefixSpace(); print "Move '$controlFile' to corrupt directory.\n";
   ETL::moveFile("${oldpath}${DIRDELI}${controlFile}", "${newpath}${DIRDELI}${controlFile}");
}

sub moveControlFileToByPass
{
   my ($controlFile, $sys) = @_;
   my ($oldpath, $newpath);
   
   $oldpath = "${ETL::ETL_RECEIVE}";
   $newpath = "${ETL::ETL_BYPASS}${DIRDELI}${sys}${DIRDELI}${TODAY}";
   
   ETL::showPrefixSpace(); print "Move '$controlFile' to bypass directory.\n";
   ETL::moveFile("${oldpath}${DIRDELI}${controlFile}", "${newpath}${DIRDELI}${controlFile}");
}

sub moveControlFileToDuplicate
{
   my ($controlFile, $sys) = @_;
   my ($oldpath, $newpath);
   
   $oldpath = "${ETL::ETL_RECEIVE}";
   $newpath = "${ETL::ETL_DUPLICATE}${DIRDELI}${sys}${DIRDELI}${TODAY}";
   
   ETL::showPrefixSpace(); print "Move '$controlFile' to duplicate directory.\n";
   ETL::moveFile("${oldpath}${DIRDELI}${controlFile}", "${newpath}${DIRDELI}${controlFile}");
}

sub moveControlFileToQueue
{
   my ($controlFile) = @_;
   my ($oldpath, $newpath);
   
   $oldpath = "${ETL::ETL_TMP}${DIRDELI}${controlFile}";
   $newpath = "${ETL::ETL_QUEUE}${DIRDELI}${controlFile}";
   
   ETL::showPrefixSpace(); print "Move '$controlFile' to queue directory.\n";

   open(OLDFILE, "$oldpath");
   open(NEWFILE, ">$newpath");

   my @ctrlFileList = <OLDFILE>;
   print NEWFILE @ctrlFileList;

   close(NEWFILE);
   close(OLDFILE);

   unlink($oldpath);
}

sub moveControlFileToUnknown
{
   my ($controlFile) = @_;
   my ($oldpath, $newpath);
   
   $oldpath = "${ETL::ETL_RECEIVE}";
   $newpath = "${ETL::ETL_UNKNOWN}${DIRDELI}${TODAY}";
   
   ETL::showPrefixSpace(); print "Move '$controlFile' to unknown directory.\n";
   if ( $os eq "svr4" ) {  # Unix
      ETL::moveFile("$oldpath/$controlFile", "$newpath/$controlFile");
   }
   elsif ( $os eq "mswin32" ) {  # NT/2000
      ETL::moveFile("$oldpath\\$controlFile", "$newpath\\$controlFile");
   }
}

sub checkJobFrequency
{
   my ($sys, $job, $txdate) = @_;
   my $TXDate = ETL::formatTXDate($txdate);

   ETL::showPrefixSpace(); print "Check job frequency $sys, $job\n";

   my $freq = ETL::checkJobFrequency($dbCon, $sys, $job, $txdate);
   if ( $freq == -1 ) {
      return $FALSE;
   }
   elsif ( $freq == 0 ) {
      ETL::showPrefixSpace(); print "Job frequency is not matched, skip it!\n";
      return $FALSE;
   }

   return $TRUE;
}

sub checkDataCalendar
{
   my ($dbh, $sys, $job, $txdate) = @_;
   my $TXDate = ETL::formatTXDate($txdate);

   my $year  = substr($txdate, 0, 4);
   my $month = substr($txdate, 4, 2);
   my $day   = substr($txdate, 6, 2);

   # Convert string to number in order to cut the prefix zero
   $year += 0;
   $month += 0;
   $day += 0;
   
   ETL::showPrefixSpace(); print "Check data calendar $sys, $job, $year, $month, $day.\n";

   if (ETL::isDataDateOK($dbh, $sys, $job, $year, $month, $day) == $TRUE) {
      ETL::showPrefixSpace(); print "Data calendar check OK.\n";

      return $TRUE;
   } else {
      ETL::showPrefixSpace(); print "Data calendar mismatch.\n";
      return $FALSE;
   }
}

sub markDataDate
{
   my ($sys, $job, $txdate) = @_;

   my $year  = substr($txdate, 0, 4);
   my $month = substr($txdate, 4, 2);
   my $day   = substr($txdate, 6, 2);

   # Convert string to number in order to cut the prefix zero
   $year += 0;
   $month += 0;
   $day += 0;

   ETL::showPrefixSpace(); print "Mark data date $sys, $job, $year, $month, $day.\n";

   ETL::markDataDate($dbCon, $sys, $job, $year, $month, $day);
}

sub updateJobExpectedRecord
{
   my ($dbh, $sys, $job, $rec) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET " .
                 "       ExpectedRecord = $rec" .
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

# This function will convert the orignal control file to a new name.
# The new control file name is $SYS_$CONVERTFILE_$TXDATE.dir.
# In this function, we keep same information from orignal control file,
# which include <file name> <file size> <expected record>.
sub convertControlFile
{
   my ($controlFile, $source, $txdate, $sys, $job, $filehead, $sid) = @_;
   my $newControlFile;
   my @fields;
   my $count = $#dataFileList;
   my $totalExpectedRecord = 0;

   my $TXDate = ETL::formatTXDate($txdate);

   ETL::showPrefixSpace(); print "Convert control file $controlFile.\n";

   if ( ($sys eq "") || ($job eq "") ) {
      ETL::showTime(); print "Unknown control file $controlFile\n";
      return undef;
   }

   $newControlFile = "${sys}_${filehead}_${txdate}.dir";

   unless ( open(NEWCTRLFILEH, ">${ETL::ETL_TMP}${DIRDELI}${newControlFile}") ) {
      ETL::showPrefixSpace(); print "ERROR - Can not open converted control file.\n";
      return "";
   }

   my $loc = 0;
   my ($file, $size, $rec, $conv_file);
   my $ret;

   my @filestat;
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);
   my $arrival;
   
   for (my $n = 0; $n <= $count ; $n++) {
      @fields = split(/\s+/, $dataFileList[$n]);
      
      $file = $fields[0];
      $size = $fields[1];
      if (!defined($fields[2])) {
      	 $rec = 0;
      } else {
         $rec  = $fields[2];
      }
      $conv_file = $file;
      $totalExpectedRecord = $totalExpectedRecord + $rec;
      
      # We keep same information in the new control file
      print NEWCTRLFILEH ("$dataFileList[$n]\n");

      @filestat = stat("${ETL::ETL_RECEIVE}${DIRDELI}${file}");
      ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($filestat[9]);
   
      $year += 1900;
      $mon = sprintf("%02d", $mon + 1);
      $mday = sprintf("%02d", $mday);
      $hour = sprintf("%02d", $hour);
      $min  = sprintf("%02d", $min);
      $sec  = sprintf("%02d", $sec);

      $arrival = "${year}-${mon}-${mday} ${hour}:${min}:${sec}";
   
      $ret = ETL::insertReceivedFileLog($dbCon, $sys, $job, $sid, $file, $size, $rec, $arrival, "${ETL::ETL_QUEUE}");
      
      unless ( $ret ) {
         ETL::showPrefixSpace(); print "Error - insert source '$file' into log failed!\n";
         ETL::showPrefixSpace(); print "[$sys, $job], SessionID=$sid, Size=$size, Rec=$rec, Arrival=$arrival\n";
      }
   }

   close(NEWCTRLFILEH);

   ETL::showPrefixSpace(); print "Update job status to 'Pending' for $sys, $job, $TXDate\n";
   ETL::clearJobStatus($dbCon, $sys, $job, $TXDate, $count + 1, "Pending");
   updateJobExpectedRecord($dbCon, $sys, $job, $totalExpectedRecord);

   return $newControlFile;
}

# This function will convert the orignal control file to a new name.
# The new control file name is $SYS_$CONVERTFILE_$TXDATE.dir.
# In this function, we only keep file name in the new control file,
# the file size and expected record will be removed.
sub convertControlFile1
{
   my ($controlFile, $source, $txdate, $sys, $job, $filehead, $sid) = @_;
   my $newControlFile;
   my @fields;
   my $count = $#dataFileList;
   my $totalExpectedRecord = 0;

   my $TXDate = ETL::formatTXDate($txdate);

   ETL::showPrefixSpace(); print "Convert control file $controlFile.\n";

   if ( ($sys eq "") || ($job eq "") ) {
      ETL::showTime(); print "Unknown control file $controlFile\n";
      return undef;
   }

   $newControlFile = "${sys}_${filehead}_${txdate}.dir";

   unless ( open(NEWCTRLFILEH, ">${ETL::ETL_TMP}${DIRDELI}$newControlFile") ) {
      ETL::showPrefixSpace(); print "ERROR - Can not open converted control file.\n";
      return "";
   }

   my $loc = 0;
   my ($file, $size, $rec, $conv_file);
   my $ret;

   my @filestat;
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);
   my $arrival;

   for (my $n = 0; $n <= $count ; $n++) {
      @fields = split(/\s+/, $dataFileList[$n]);

      $file = $fields[0];
      $size = $fields[1];
      if (!defined($fields[2])) {
      	 $rec = 0;
      } else {
         $rec  = $fields[2];
      }
      $totalExpectedRecord = $totalExpectedRecord + $rec;

      # If $ConvertSourceName == 1, we convert the source file name
      # However, we don't do it anymore. The $ConvertSourceName will always
      # set to 0 in the beginning of script
      if ( $ConvertSourceName == 1 ) {
         $loc = index($fields[0], $source);
         substr($fields[0], $loc, length($source)) = "${sys}_${filehead}_";
         $conv_file = $fields[0];
         print NEWCTRLFILEH ("$conv_file\n");
         if ( $os eq "svr4" ) {  # Unix
            ETL::moveFile("${ETL::ETL_RECEIVE}/$file", "${ETL::ETL_RECEIVE}/$conv_file");
         }
         elsif ( $os eq "mswin32" ) {  # NT/2000
            ETL::moveFile("${ETL::ETL_RECEIVE}\\$file", "${ETL::ETL_RECEIVE}\\$conv_file");
         }
      }
      else {
         $conv_file = $file;
         # We only keep file name in new control file
         print NEWCTRLFILEH ("$conv_file\n");
      }

      @filestat = stat("${ETL::ETL_RECEIVE}${DIRDELI}${conv_file}");
      ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($filestat[9]);
   
      $year += 1900;
      $mon = sprintf("%02d", $mon + 1);
      $mday = sprintf("%02d", $mday);
      $hour = sprintf("%02d", $hour);
      $min  = sprintf("%02d", $min);
      $sec  = sprintf("%02d", $sec);

      $arrival = "${year}-${mon}-${mday} ${hour}:${min}:${sec}";

      ETL::insertReceivedFileLog($dbCon, $sys, $job, $sid, $conv_file, $size, $rec, $arrival, "${ETL::ETL_QUEUE}");

      unless ( $ret ) {
         ETL::showPrefixSpace(); print "Error: insert source '$file' into log failed!\n";
      }
   }

   close(NEWCTRLFILEH);

   ETL::clearJobStatus($dbCon, $sys, $job, $TXDate, $count + 1, "Pending");
   updateJobExpectedRecord($dbCon, $sys, $job, $totalExpectedRecord);

   return $newControlFile;
}

sub getDataFileList
{
   my ($controlFile) = @_;
   my $line;

   unless ( open(CTRLFILEH, "${ETL::ETL_RECEIVE}${DIRDELI}${controlFile}") ) {
      return $FALSE;
   }

   my @list = <CTRLFILEH>;
   close(CTRLFILEH);

   my $n = 0;
   my $i = 0;

   @dataFileList = ();

   for ($n = 0; $n <= $#list ; $n++) {
      $line = ETL::cutLeadingTrailSpace($list[$n]);
      if ($line ne "") {
         $dataFileList[$i++] = $line;
      }
   }

   return $TRUE;
}

sub checkRelatedJobForDaily
{
   my ($sys, $job, $txdate) = @_;

   ETL::showTime(); print "Check related daily job status...\n";

   my $daily_txdate = ETL::getDoneJobTxDate($dbCon, $sys, $job);
   
   if ( "$daily_txdate" eq "" ) {
      ETL::showPrefixSpace(); print "No TxDate for related daily job\n";
      return $TRUE;
   } else {
      ETL::showPrefixSpace(); print "TxDate for related daily job is '$daily_txdate\n";

      my $dailymm = substr($daily_txdate, 5, 2);
      my $dailydd = substr($daily_txdate, 8, 2);
      my $jobmm = substr($txdate, 4, 2);
      my $jobdd = substr($txdate, 6, 2);
      
      my $offset;
      
      ETL::showPrefixSpace(); print "JobMM = '$jobmm', JobDD = '$jobdd'\n";
      ETL::showPrefixSpace(); print "DailyMM = '$dailymm', DailyDD = '$dailydd'\n";
      
      if ( $jobmm != $dailymm ) {
      	 $offset = $jobdd;
      	 if ($dailymm == "1" || $dailymm == "3" || $dailymm == "5" || $dailymm == "7" ||
      	     $dailymm == "8" || $dailymm == "10" || $dailymm == "12") {
      	     $offset += (31 - $dailydd);
      	 }
      	 else {
      	     if ($dailymm == 2) {
                if ($dailydd < 28) {
                   $offset += (28 - $dailydd);
                }
      	     }
      	     else {
      	        $offset += (30 - $dailydd);
      	     }	
      	 }
      }
      else {
         $offset = $jobdd - $dailydd;
      }
      
      if ( $offset <= 1 ) {
      	 ETL::showPrefixSpace(); print "Related daily job date offset is in range\n";
      	 return $TRUE;
      } else {
      	 ETL::showPrefixSpace(); print "Related daily job date offset is over range\n";
      	 return $FALSE;
      }
   }
}

sub checkRelatedJobForWeekly
{
   my ($sys, $job, $txdate) = @_;

   ETL::showTime(); print "Check related job weekly job status...\n";

   my $weekly_txdate = ETL::getDoneJobTxDate($dbCon, $sys, $job);
   
   if ( "$weekly_txdate" eq "" ) {
      ETL::showPrefixSpace(); print "No TxDate for related weekly job\n";
      return $TRUE;
   } else {
      ETL::showPrefixSpace(); print "TxDate for related weekly job is '$weekly_txdate\n";

      my $weeklymm = substr($weekly_txdate, 5, 2);
      my $weeklydd = substr($weekly_txdate, 8, 2);
      my $jobmm = substr($txdate, 4, 2);
      my $jobdd = substr($txdate, 6, 2);
      
      my $offset;
      
      ETL::showPrefixSpace(); print "JobMM = '$jobmm', JobDD = '$jobdd'\n";
      ETL::showPrefixSpace(); print "WeeklyMM = '$weeklymm', WeeklyDD = '$weeklydd'\n";
      
      if ( $jobmm != $weeklymm ) {
      	 $offset = $jobdd;
      	 if ($weeklymm == "1" || $weeklymm == "3" || $weeklymm == "5" || $weeklymm == "7" ||
      	     $weeklymm == "8" || $weeklymm == "10" || $weeklymm == "12") {
      	     $offset += (31 - $weeklydd);
      	 }
      	 else {
      	     $offset += (30 - $weeklydd);
      	     if ($weeklymm == "2") {
      	     	$offset -= 1;
      	     }	
      	 }
      }
      else {
         $offset = $jobdd - $weeklydd;
      }
      
      if ( $offset <= 7 ) {
      	 ETL::showPrefixSpace(); print "Related weekly job date offset is in range\n";
      	 return $TRUE;
      } else {
      	 ETL::showPrefixSpace(); print "Related weekly job date offset is over range\n";
      	 return $FALSE;
      }
   }
}

sub checkRelatedJobForMonthly
{
   my ($sys, $job, $txdate) = @_;

   ETL::showTime(); print "Check related job monthly job status...\n";
   
   my $monthly_txdate = ETL::getDoneJobTxDate($dbCon, $sys, $job);
   
   if ( "$monthly_txdate" eq "" ) {
      ETL::showPrefixSpace(); print "No TxDate for related monthly job\n";

      return $TRUE;
   } else {
      ETL::showPrefixSpace(); print "TxDate for related monthly job is '$monthly_txdate\n";
      
      my $monthlymm = substr($monthly_txdate, 5, 2);
      my $jobmm = substr($txdate, 4, 2);
      
      ETL::showPrefixSpace(); print "Related job month is '$monthlymm', job month is '$jobmm'\n";
      
      if ( $monthlymm == 12 && $jobmm == 1 ) {
         ETL::showPrefixSpace(); print "Related job date is matched\n";
      	 return $TRUE;
      } else {
      	 if ( $monthlymm == ($jobmm - 1) ) {
            ETL::showPrefixSpace(); print "Related job job date is matched\n";
      	    return $TRUE;
      	 } else {
            ETL::showPrefixSpace(); print "Related job job date is not matched\n";
      	    return $FALSE;
      	 }
      }
   }
}

sub getPriorCalendarDate
{
   my ($dbh, $sys, $job, $checkmode, $year, $month, $day) = @_;
   my ($retYear, $retMonth, $retDay);
   my @tabrow;
   my $sqlText;
  
   ETL::showPrefixSpace(); print "Get prior calendar date, $sys, $job, $checkmode, $year, $month, $day\n";
   if ( $checkmode eq "0") {
      $sqlText = "SELECT max(SeqNum)" .
                 "  FROM ${ETL::ETLDB}DataCalendar" .
                 "    WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "      AND CalendarYear <= $year AND CalendarMonth <= $month" .
                 "      AND CalendarDay <= $day";
   } else {
      $sqlText = "SELECT max(SeqNum)" .
                 "  FROM ${ETL::ETLDB}DataCalendar" .
                 "    WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "      AND CalendarYear <= $year AND CalendarMonth <= $month" .
                 "      AND CalendarDay < $day";
   }

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return (0, 0, 0);
   }	

   $sth->execute();
   my $maxSeq;
   
   if (@tabrow = $sth->fetchrow()) {
      $maxSeq  = $tabrow[0];
   } else {
      $maxSeq = 0;
   }

   $sth->finish();
   
   if ( $maxSeq == 0 ) {
      return (0, 0, 0);
   }

   $sqlText = "SELECT CalendarYear, CalendarMonth, CalendarDay" .
              "  FROM ${ETL::ETLDB}DataCalendar" .
              "    WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
              "      AND SeqNum = $maxSeq";
   
   $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return (0, 0, 0);
   }	

   $sth->execute();

   $retYear  = 0;
   $retMonth = 0;
   $retDay   = 0;
      
   if (@tabrow = $sth->fetchrow()) {
      $retYear  = $tabrow[0];
      $retMonth = $tabrow[1];
      $retDay   = $tabrow[2];
   }

   $sth->finish();

   return ($retYear, $retMonth, $retDay);
}

sub getPriorCalendarDateByMonth
{
   my ($dbh, $sys, $job, $year, $month) = @_;
   my ($retYear, $retMonth, $retDay);
   my @tabrow;
   my $sqlText;

   ETL::showPrefixSpace(); print "Get prior calendar date by month, $sys, $job, $year, $month\n";

   $sqlText = "SELECT max(SeqNum)" .
              "  FROM ${ETL::ETLDB}DataCalendar" .
              "    WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
              "      AND CalendarYear <= $year AND CalendarMonth < $month";
   
   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return (0, 0, 0);
   }	

   $sth->execute();
   my $maxSeq;
   
   if (@tabrow = $sth->fetchrow()) {
      $maxSeq  = $tabrow[0];
   } else {
      $maxSeq = 0;
   }

   $sth->finish();
   
   if ( $maxSeq == 0 ) {
      return (0, 0, 0);
   }

   $sqlText = "SELECT CalendarYear, CalendarMonth, CalendarDay" .
             "  FROM ${ETL::ETLDB}DataCalendar" .
             "    WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
             "      AND SeqNum = $maxSeq";
   
   $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return (0, 0, 0);
   }	

   $sth->execute();

   $retYear  = 0;
   $retMonth = 0;
   $retDay   = 0;
      
   if (@tabrow = $sth->fetchrow()) {
      $retYear  = $tabrow[0];
      $retMonth = $tabrow[1];
      $retDay   = $tabrow[2];
   }

   $sth->finish();

   return ($retYear, $retMonth, $retDay);
}

sub getPriorCalendarDateByYear
{
   my ($dbh, $sys, $job, $year) = @_;
   my ($retYear, $retMonth, $retDay);
   my @tabrow;
   my $sqlText;

   ETL::showPrefixSpace(); print "Get prior calendar date by year, $sys, $job, $year\n";

   $sqlText = "SELECT max(SeqNum)" .
              "  FROM ${ETL::ETLDB}DataCalendar" .
              "    WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
              "      AND CalendarYear < $year";
   
   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return (0, 0, 0);
   }	

   $sth->execute();
   my $maxSeq;
   
   if (@tabrow = $sth->fetchrow()) {
      $maxSeq  = $tabrow[0];
   } else {
      $maxSeq = 0;
   }

   $sth->finish();
   
   if ( $maxSeq == 0 ) {
      return (0, 0, 0);
   }

   $sqlText = "SELECT CalendarYear, CalendarMonth, CalendarDay" .
             "  FROM ${ETL::ETLDB}DataCalendar" .
             "    WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
             "      AND SeqNum = $maxSeq";
   
   $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return (0, 0, 0);
   }	

   $sth->execute();

   $retYear  = 0;
   $retMonth = 0;
   $retDay   = 0;
      
   if (@tabrow = $sth->fetchrow()) {
      $retYear  = $tabrow[0];
      $retMonth = $tabrow[1];
      $retDay   = $tabrow[2];
   }

   $sth->finish();

   return ($retYear, $retMonth, $retDay);
}

# Check job's prior date status is ok or not base on job's data calendar
sub checkJobPriorStatus
{
   my ($dbh, $sys, $job, $txdate, $checkmode) = @_;
   
   my $year  = substr($txdate, 0, 4);
   my $month = substr($txdate, 4, 2);
   my $day   = substr($txdate, 6, 2);
  
   my ($pyear, $pmonth, $pday) = getPriorCalendarDate($dbh, $sys, $job, $checkmode, $year, $month, $day);
   
   # If it can not get prior date in the same month and day
   # We get it again only by year and month
   if ( "$pyear" eq "0" ) {
      ($pyear, $pmonth, $pday) = getPriorCalendarDateByMonth($dbh, $sys, $job, $year, $month);
   }

   if ( "$pyear" eq "0" ) {
      ($pyear, $pmonth, $pday) = getPriorCalendarDateByYear($dbh, $sys, $job, $year);
   }

   ETL::showPrefixSpace(); print "Related job date should be '${pyear}-${pmonth}-${pday}'\n";

   if ($pyear == 0 && $pmonth == 0 && $pday == 0) {
      return $TRUE;
   }

   if ( $pmonth < 10 ) {
      $pmonth = "0${pmonth}";
   }
   
   if ( $pday < 10 ) {
      $pday = "0${pday}";
   }
   
   my $expectedDate = "${pyear}-${pmonth}-${pday}";
   
   my $lastDoneDate = ETL::getDoneJobTxDate($dbh, $sys, $job);
   
   if ("$expectedDate" eq "$lastDoneDate") {
      return $TRUE;
   } 
   else {
      return $FALSE;
   }
}

sub checkRelatedJobStatus
{
   my ($dbh, $sys, $job, $txdate) = @_;
   my $TXDate = ETL::formatTXDate($txdate);

   my @tabrow;
   my @relatedJob;

   my $sqlText = "SELECT RelatedSystem, RelatedJob, CheckMode" .
                 "   FROM ${ETL::ETLDB}ETL_RelatedJob" .
                 "   WHERE ETL_System = '$sys'" .
                 "     AND ETL_Job = '$job'" .
                 "   ORDER BY RelatedSystem, RelatedJob";


   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return $TRUE;
   }

   $sth->execute();

   my $count = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $relatedJob[$count++] = $tabrow[0];
      $relatedJob[$count++] = $tabrow[1];
      $relatedJob[$count++] = $tabrow[2];
   }

   $sth->finish();

   ETL::showTime(); print "Check related job status...\n";

   my $jobtype;
   my $relatedFlag = $TRUE;
   my $n;
   
   my ($relSys, $relJob, $checkMode);
   
   for ($n=0; $n < $count; $n+=3) {
      $relSys    = $relatedJob[$n];
      $relJob    = $relatedJob[$n+1];
      $checkMode = $relatedJob[$n+2];
      
      ETL::showPrefixSpace(); print "Check related job status for $relSys,$relJob\n";
   	
      $jobtype = ETL::getJobType($dbh, $relSys, $relJob);
      ETL::showPrefixSpace(); print "Job type for related job is '$jobtype'\n";

      # The related job has been setting to check data calendar
      # We check the prior job status at data calendar
      if ( ETL::isJobCheckDataDate($dbh, $relSys, $relJob) == $TRUE) {
         ETL::showPrefixSpace(); print "Check related job by data calendar\n";
      	 $relatedFlag = checkJobPriorStatus($dbh, $relSys, $relJob, $txdate, $checkMode);
      	 if ( $relatedFlag == $FALSE ) {
      	    last;
      	 }
      	
      	 next;
      }
      
      if ($jobtype eq "D") {    # When related job is a daily job
      	 $relatedFlag = checkRelatedJobForDaily($relSys, $relJob, $txdate);      
      }
      elsif ($jobtype eq "W") { # When related job is a weekly job
      	 $relatedFlag = checkRelatedJobForWeekly($relSys, $relJob, $txdate);
      }
      elsif ($jobtype eq "M") { # When related job is a monthly job
      	 $relatedFlag = checkRelatedJobForMonthly($relSys, $relJob, $txdate);      	
      }

      if ( $relatedFlag == $FALSE ) {
      	 last;
      }
   }

   my $current;
   my $eventDesc;

   $current = ETL::getCurrentDateTime();

   if ( $relatedFlag == $FALSE ) {
      ETL::showPrefixSpace(); print "Related job status is not ready.\n";
      #ETL::clearJobStatus($dbh, $sys, $job, $TXDate, 0, "Waiting Related Job");
      return $FALSE;
   } else {
      ETL::showPrefixSpace(); print "Related job status OK.\n";
      return $TRUE;
   }
}

sub updateSourceLastCount
{
   my ($dbh, $source, $lastCount) = @_;
   
   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job_Source " .
                 "   SET LastCount = $lastCount" .
                 "   WHERE Source = '$source'";

   my $sth = $dbh->prepare($sqlText) or return $FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret == 1) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub calculateCheckingMissingDate
{
   my ($today, $offset) = @_;
   my $checkingDate;
   my ($year, $month, $day);
   
   if ( $offset == 0 ) { return $today; }
   
   $year  = substr($today, 0, 4);
   $month = substr($today, 4, 2);
   $day   = substr($today, 6, 2);

   # Convert string to number in order to cut the prefix zero
   $year += 0;
   $month += 0;
   $day += 0;

   while ($offset > 0) {
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
      	elsif ($month==2 && ($year/4)==0) {
      	    $day = 29;
      	}
      	elsif ($month==2 && ($year/4)!=0) {
      	    $day = 28;
      	}
      } else {
      	$day--;
      }
      
      $offset--;
   }

   if ($month < 10 && substr($month, 0, 1) ne "0") {
      $month = "0${month}";
   }

   if ($day < 10 && substr($day, 0, 1) ne "0") {
      $day = "0${day}";
   }
   
   $checkingDate = "${year}${month}${day}";   

   return $checkingDate;
}

sub checkMissingWithDataCalendar
{
   my ($dbh, $sys, $job, $checkingday) = @_;
   my @tabrow;
   my ($checkFlag);   

   my ($year, $month, $day);

   $year  = substr($checkingday, 0, 4);
   $month = substr($checkingday, 4, 2);
   $day   = substr($checkingday, 6, 2);

   # Convert string to number in order to cut the prefix zero
   $year += 0;
   $month += 0;
   $day += 0;

   ETL::showPrefixSpace(); print "Check job data calendar with year='$year', month='$month', day='$day'\n";
   
   # To see if the data date is in data calendar
   my $sqlText = "SELECT CheckFlag" .
                 "  FROM ${ETL::ETLDB}DataCalendar" .
                 "    WHERE etl_system = '$sys' AND etl_job = '$job'" .
                 "      AND calendarYear = $year AND calendarMonth = $month" .
                 "      AND calendarDay = $day";

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return $ETL::FALSE;
   }

   $sth->execute();

   if (@tabrow = $sth->fetchrow()) {
      $checkFlag = $tabrow[0];
   }
   else {
      $checkFlag = "X";
   }

   $sth->finish();

   # This checking date is not in data calendar
   if ($checkFlag eq "X") {
      return $FALSE;
   }
   
   return $TRUE;
}


sub checkMissingWithFrequency
{
   my ($dbh, $sys, $job, $checkingday) = @_;
   
   ETL::showPrefixSpace(); print "Check job frequency with '$checkingday'\n";

   my $freq = ETL::checkJobFrequency($dbh, $sys, $job, $checkingday);
   
   if ( $freq == -1 ) {
      return $FALSE;
   }
   elsif ( $freq == 0 ) {
      return $FALSE;
   }

   return $TRUE;
}

sub writeMessageNotification
{
   my ($sys, $convfile, $txdate) = @_;
   my $msgControlFile;
   
   $msgControlFile = "${sys}_${convfile}_${txdate}.msg";

   ETL::showPrefixSpace(); print "write a message control file ${msgControlFile}\n";
      
   unless ( open(MSGCONTROL, ">${ETL::ETL_MESSAGE}${DIRDELI}${msgControlFile}") ) {
      ETL::showPrefixSpace(); print "[ERROR] cannot open message control file\n";
      return $FALSE;
   }
   
   print MSGCONTROL "Missing\n";
   print MSGCONTROL "\n";

   close(MSGCONTROL);
      
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

# Processing the control file
sub processControlFile
{
   my($controlFile) = @_;
   my $newControlFile;
   my ($jobSource, $txDate);
   my ($sysName, $jobName, $convHead, $jobSessionID);
   my $eventDesc;
   
   ETL::showTime(); print "Processing control file '$controlFile'...\n";

   # Get the job source name and data extrac date from control file name
   ($jobSource, $txDate) = ETL::getControlFileInfo($controlFile);
   ETL::showPrefixSpace(); print "Job Source = '$jobSource', TxDate='$txDate'\n";

   # Get the mapping job information
   ($sysName, $jobName, $convHead)  = ETL::getMappingJobInfo($dbCon, $jobSource);
   ETL::showPrefixSpace(); print "System='$sysName', Job='$jobName', ConvHead='$convHead'\n";

   # Get the data file list from control file
   getDataFileList($controlFile);

   # Unknown Job
   if ( $sysName eq "" || $jobName eq "" ) {
      ETL::showTime(); print "Unknown control file '$controlFile'\n";

      $eventDesc = "Unknown control file $controlFile";
      ETL::insertEventLog($dbCon, "RCV", "L", "$eventDesc");

      moveDataFileToUnknown($controlFile);
      moveControlFileToUnknown($controlFile);

      return $FALSE;  
   }

   my $nowday = (localtime(time()))[3];
   updateSourceLastCount($dbCon, $jobSource, $nowday);

   my $enable = ETL::checkJobEnable($dbCon, $sysName, $jobName);
   if ( $enable == 0 ) {
      ETL::showPrefixSpace(); print "WARNING - The job is not enabled, we will process this file next time.\n";
      return $FALSE;	
   }

   $jobSessionID = ETL::getJobSessionID($dbCon, $sysName, $jobName);

   # Get the job current status
   my $currentStatus = ETL::getJobStatus($dbCon, $sysName, $jobName);
   if ( !($currentStatus eq "" || $currentStatus eq "Ready" ||
         $currentStatus eq "Done" )) {
      # The current job status is not 'Done' or 'Ready',
      # we don't allow to receiving file at the moment
      ETL::showPrefixSpace(); print "WARNING - The job is in $currentStatus, we will process this file next time.\n";
      
      if ( isJobAlreadyHasEvent($sysName, $jobName, "Status Mismatch") ) {
          return $FALSE;
      }
      
      $eventDesc = "[$sysName], [$jobName] still in <$currentStatus> but has received another file $controlFile";
      ETL::insertEventLog($dbCon, "RCV", "M", "$eventDesc");
      return $FALSE;	
   }

   # Check the received file size is correct or not   
   unless ( checkDataFileSize($controlFile) ) {
      moveDataFileToCorrupt($controlFile, $sysName);
      moveControlFileToCorrupt($controlFile, $sysName);

      $eventDesc = "[$sysName], [$jobName] has corrupt data still in control file $controlFile";
      ETL::insertEventLog($dbCon, "RCV", "H", "$eventDesc");

      removeJobEventRecord($sysName, $jobName);
      return $FALSE;
   }
   
   # Check the received file is duplicate or not
   unless ( checkDataFileDuplicate($sysName, $jobName) ) {
      moveDataFileToDuplicate($controlFile, $sysName);
      moveControlFileToDuplicate($controlFile, $sysName);

      removeJobEventRecord($sysName, $jobName); 
      return $FALSE;
   }

   unless ( checkJobFrequency($sysName, $jobName, $txDate) ) {
      moveDataFileToByPass($controlFile, $sysName);
      moveControlFileToByPass($controlFile, $sysName);

      $eventDesc = "[$sysName], [$jobName] has received a control file $controlFile did not match the frequency";
      ETL::insertEventLog($dbCon, "RCV", "L", "$eventDesc");
      
      removeJobEventRecord($sysName, $jobName);
      return $FALSE;   	
   }

   # Check the received control file is matched TXDate in data calendar
   unless ( checkDataCalendar($dbCon, $sysName, $jobName, $txDate) ) {
      moveDataFileToByPass($controlFile, $sysName);
      moveControlFileToByPass($controlFile, $sysName);

      $eventDesc = "[$sysName], [$jobName] has received a control file $controlFile did not match the data calendar";
      ETL::insertEventLog($dbCon, "RCV", "H", "$eventDesc");

      removeJobEventRecord($sysName, $jobName);
      return $FALSE;
   }

   # If the related job status is not ready, then we skip this control file
   # and process it next time
   unless ( checkRelatedJobStatus($dbCon, $sysName, $jobName, $txDate) ) {   	
      ETL::showTime(); print ("Check job related job failed!\n");

      if ( isJobAlreadyHasEvent($sysName, $jobName, "Related Job") ) {
          return $FALSE;
      }

      $eventDesc = "[$sysName], [$jobName] has related job did not finish yet, wait to next time";
      ETL::insertEventLog($dbCon, "RCV", "M", "$eventDesc");

      return $FALSE;
   }
   
   if ( $ConvertControlFlag == 0 ) {
      $newControlFile = convertControlFile($controlFile, $jobSource, $txDate,
                                           $sysName, $jobName, $convHead, $jobSessionID);
   }
   else {
      $newControlFile = convertControlFile1($controlFile, $jobSource, $txDate,
                                            $sysName, $jobName, $convHead, $jobSessionID);
   }

   # Check if error occured
   if ( $newControlFile eq "" ) {
      return $FALSE;
   }

   markDataDate($sysName, $jobName, $txDate);

   moveDataFileToQueue($newControlFile);
   moveControlFileToQueue($newControlFile);

   removeJobEventRecord($sysName, $jobName);

   # We will move the original control file to complete directory
   my $newpath = "${ETL::ETL_COMPLETE}${DIRDELI}${sysName}";
   unless ( -d $newpath ) {
      ETL::createDirectory($newpath);
   }
   
   $newpath = "${newpath}${DIRDELI}${TODAY}";
   unless ( -d $newpath ) {
      ETL::createDirectory($newpath);
   }

   ETL::showPrefixSpace(); print "Move ${controlFile} to ${newpath}${DIRDELI}${controlFile}\n";
   ETL::moveFile("${ETL::ETL_RECEIVE}${DIRDELI}${controlFile}", "${newpath}${DIRDELI}${controlFile}");

   return $TRUE;
}

# Sort the control file by data date
sub sortControlFile
{
   my ($date1, $date2);
   my ($year, $mon);
   
   if ( $ETL::NameMode == $ETL::SHORT_NAME_MODE ) {
      ($mon,$year) = (localtime(time()))[4,5];
      $year += 1900;
      $mon  += 1;

      my $d1 = substr($a, 6, 4);
      if ( $mon == 1 || $mon == 2 ) {
         if ( substr($d1, 0, 2) eq "12" ) {
            $year -= 1;
         }
      }
      $date1 = "${year}${d1}";
      
      my $d2 = substr($b, 6, 4);
      if ( $mon == 1 || $mon == 2 ) {
         if ( substr($d2, 0, 2) eq "12" ) {
            $year -= 1;
         }
      }
      $date2 = "${year}${d2}";
   }
   elsif ( $ETL::NameMode == $ETL::LONG_NAME_MODE ) {
      $date1 = substr($a, (length($a) - 8), 8);
      $date2 = substr($b, (length($b) - 8), 8);
   }
   elsif ( $ETL::NameMode == $ETL::EVA_NAME_MODE ) {
      $date1 = substr($a, (length($a) - 8), 8);
      $date2 = substr($b, (length($b) - 8), 8);
   }

   $date1 cmp $date2;
}

# Check the receiving directory to see if there is any control file
# here need to be processed
sub checkReceiveDir
{
   my $filename;
   my $RCV_DIR;
   my $j;
   
   ETL::showTime(); print "Checking receiving directory '${ETL::ETL_RECEIVE}'...\n";

   # Open the receiving directory for processing
   unless ( opendir(RCV_DIR, "${ETL::ETL_RECEIVE}") ) {
      ET::showTime(); print "ERROR - Unable to open '${ETL::ETL_RECEIVE}'\n";
      return $FALSE;
   }
 
   my $n = 0;
   @dirFileList = ();
   my @tempList;
   
   while ($filename = readdir(RCV_DIR))
   {
      if ( $STOP_FLAG ) { last; }

      if ( $filename eq "." || $filename eq ".." ) { next; }

      # If the file is directory then skip it
      if ( -d "${ETL::ETL_RECEIVE}${DIRDELI}${filename}" ) { next; }

      if ( isControlFile($filename) ) {
         $tempList[$n++] = $filename;
      }
   }
   # Close the receiving directory
   closedir(RCV_DIR);

   if ($n == 0) {
      #ETL::showTime(); print "No control file existing!\n";
      return $FALSE;
   }

   ETL::showTime(); print "Sorting the control file list...\n";

   # Sorting the control file list
   @dirFileList = sort @tempList; # Order by filename Modified by Ralph Niu @dirFileList = sort sortControlFile @tempList;
   
   ETL::showTime(); print "Connect to ETL Automation repository...\n";
   $dbCon = ETL::connectETL();

   unless ( defined($dbCon) ) {
      ETL::showTime(); print "ERROR - Unable to connect to ETL Automation repository!\n";
      return $FALSE;
   }

   for (my $i=0; $i < $n; $i++)
   {
      if ( $STOP_FLAG ) { last; }

      $filename = $dirFileList[$i];
    
      if ( ETL::isSizeStable("${ETL::ETL_RECEIVE}${DIRDELI}${filename}") ) {
         processControlFile($filename);
         my $filename1;
         my $filename2;
         my $j;
         $filename1 = substr($filename, 0, length($filename)-8);
         for ($j=$i+1; $j < $n; $j++) {
         	$filename2 = substr($dirFileList[$j], 0, length($dirFileList[$j])-8);
         	if ($filename1 ne $filename2) {
         		$i=$j-1;
         		last;	
         	}
         } 
         if ($j == $n) { last; }
      }
   }

   ETL::showTime(); print "Disconnect from ETL Automation repository...\n";
   unless( ETL::disconnectETL($dbCon) ) {
      ETL::showTime(); print "ERROR - Disconnect failed!\n";
   }
   
   ETL::showTime(); print "Check receiving directory '$ETL::ETL_RECEIVE' done.\n";

   return $TRUE;
}

# Check the missing job source if the alert flag has been set
sub checkMissingSource
{
   my @tabrow;
   my @sourceList;
   
   ETL::showTime(); print "Checking the missing job source...\n";

   my ($sec,$nowmin,$nowhour,$nowday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $nowtime = ($nowhour * 100) + $nowmin;
   ETL::showTime(); print "Now day='$nowday', hour='$nowhour', min='$nowmin'\n";
   
   ETL::showTime(); print "Connect to ETL Automation repository...\n";
   $dbCon = ETL::connectETL();

   unless ( defined($dbCon) ) {
      ETL::showTime(); print "ERROR - Unable to connect to ETL Automation repository!\n";
      return $FALSE;
   }

   # Get the missing job source from repository
   my $sqlText = "SELECT Source, ETL_System, ETL_Job, Conv_File_Head, OffsetDay" .
                 "   FROM ${ETL::ETLDB}ETL_Job_Source" .
                 "   WHERE Alert = '1'" .
                 "     AND LastCount <> $nowday" .
                 "     AND (BeforeHour*100 + BeforeMin) < $nowtime";

   my $sth = $dbCon->prepare($sqlText);
   unless ($sth) {
      ETL::showTime(); print "Disconnect from ETL Automation repository...\n";

      unless( ETL::disconnectETL($dbCon) ) {
         ETL::showTime(); print "ERROR - Disconnect failed!\n";
      }

      return $FALSE;
   }

   $sth->execute();

   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $sourceList[$n++] = $tabrow[0];
      $sourceList[$n++] = $tabrow[1];
      $sourceList[$n++] = $tabrow[2];
      $sourceList[$n++] = $tabrow[3];
      $sourceList[$n++] = $tabrow[4];
   }

   $sth->finish();

   if ( $n == 0 ) {
      ETL::showTime(); print "There is no missing source at the moment.\n";
   	
      ETL::showTime(); print "Disconnect from ETL Automation repository...\n";

      unless( ETL::disconnectETL($dbCon) ) {
         ETL::showTime(); print "ERROR - Disconnect failed!\n";
      }

      return $FALSE;
   }

   my $i;
   my ($source, $sys, $job, $convfile, $offsetDay, $txdate, $checkingday);
   my $ret;
   
   for ($i=0; $i <$n; $i = $i+5) {
       $source    = $sourceList[$i];
       $sys       = $sourceList[$i+1];
       $job       = $sourceList[$i+2];
       $convfile  = $sourceList[$i+3];
       $offsetDay = $sourceList[$i+4];

       ETL::showPrefixSpace(); print "System='$sys', Job='$job', OffsetDay='$offsetDay'\n";

       # If the job is not enabled, we do not send source missing alert message
       if ( ETL::checkJobEnable($dbCon, $sys, $job) != 1 ) {
          ETL::showTime(); print "The job is disabled, update source '$source' last count to '$nowday'\n";
       	  updateSourceLastCount($dbCon, $source, $nowday);
          next;	
       }

       $txdate = ETL::getCurrentFullDate();
       $checkingday = calculateCheckingMissingDate($txdate, $offsetDay);

       # If the job is set to check with data calendar, we check the missing source
       # with job's data calendar, otherwise, we check with job's frequency
       if ( ETL::isJobCheckDataDate($dbCon, $sys, $job) == $ETL::TRUE ) {
          ETL::showTime(); print "Check missing source date with job data calendar.\n";
          $ret = checkMissingWithDataCalendar($dbCon, $sys, $job, $checkingday);
          if ( $ret == $FALSE ) {  # Today does not match with data calendar criteria
             ETL::showPrefixSpace(); print "Missing source date does not match job data calendar.\n";
             ETL::showPrefixSpace(); print "Update source '$source' last count to '$nowday'\n";
       	     updateSourceLastCount($dbCon, $source, $nowday);
       	     next;
          }
          ETL::showPrefixSpace(); print "Missing source date match job data calendar.\n";
       } else {
       	  ETL::showTime(); print "Check missing source date with job frequency.\n";
       	  $ret = checkMissingWithFrequency($dbCon, $sys, $job, $checkingday);
          if ( $ret == $FALSE ) {  # Today does not match with frequency
             ETL::showPrefixSpace(); print "Missing source date does not match job frequency.\n";
             ETL::showPrefixSpace(); print "Update source '$source' last count to '$nowday'\n";
       	     updateSourceLastCount($dbCon, $source, $nowday);
       	     next;
          }
          ETL::showPrefixSpace(); print "Missing source date match job frequency.\n";
       }

       ETL::showTime(); print "Generate source missing message...\n";
       writeMessageNotification($sys, $convfile, $txdate);

       ETL::showPrefixSpace(); print "Update source '$source' last count to '$nowday'\n";       
       updateSourceLastCount($dbCon, $source, $nowday);
   }

   ETL::showTime(); print "Disconnect from ETL Automation repository...\n";

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
      #if ( $STOP_FLAG ) { last; }

      unless ( createLogFile() ) {
      	 print STDERR "ERROR - Unable to create log file!\n";
      }

      if ($PRINT_VERSION_FLAG != 1) {
      	 printVersionInfo();
      }

      # Check the receive directory
      checkReceiveDir();

      # Check the missing job source
      checkMissingSource();
      
      #if ( $STOP_FLAG ) { last; }

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
      $MutexObj = ETL::CreateMutex("ETLRCV_MUTEX");
      if ( ! defined($MutexObj) ) {
         print STDERR "Only one instance of etlrcv.pl allow to run, program terminated!\n";
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
         print STDERR "Only one instance of etlrcv.pl allow to run, program terminated!\n";
   
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

#  print STDOUT ("Clean Up by $signal\n");

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
   ETL::showTime(); print "**************************************************************\n";
   ETL::showTime(); print "* ETL Automation Receiver Program ${VERSION}, NCR 2001 Copyright. *\n";
   ETL::showTime(); print "**************************************************************\n";
   print "\n";
   $PRINT_VERSION_FLAG = 1;
}

###############################################################################

$LOCK_FILE = "${ETL::ETL_LOCK}${DIRDELI}etlrcv.lock";
$LASTLOGFILE = "";

unless ( check_instance() ) { exit(1); }

$SIG{'INT'}  = 'cleanUp';
$SIG{'QUIT'} = 'cleanUp';
$SIG{'TERM'} = 'cleanUp';

open(STDERR, ">&STDOUT");

main();

exit(0);

__END__
