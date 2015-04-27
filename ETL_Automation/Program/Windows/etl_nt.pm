###############################################################################
# Program: etl_nt.pm

use strict;

use DBI;
use Time::Local;
use Win32::Process;
use Win32::Mutex;

package ETL;

###############################################################################
# variable section
###############################################################################

my $OS;
my $ETLDIR;
my $ETL_DSN;
my $ETL_SERVER;

my $ETL_ETC;
my $ETL_BIN;
my $ETL_TMP;
my $ETL_LOCK;
my $ETL_LOG;
my $ETL_APP;

my $ETL_RECEIVE;
my $ETL_QUEUE;
my $ETL_MESSAGE;
my $ETL_PROCESS;
my $ETL_COMPLETE;
my $ETL_FAIL;
my $ETL_CORRUPT;
my $ETL_DUPLICATE;
my $ETL_ERROR;
my $ETL_BYPASS;
my $ETL_UNKNOWN;

my $PERLPATH;
my $PERL;
my $INVOKER;
my $ETLDB;
my $DATADB;
my $TEMPDB;
my $STAGEDB;
my $LOGDB;

my $SLEEP_TIME;
my $FALSE;
my $TRUE;

my $LONG_NAME_MODE;
my $SHORT_NAME_MODE;
my $EVA_NAME_MODE;

my $EVENT_COUNT;
$ETL::EVENT_COUNT = 0;

$ETL::OS = "NT";

$ETL::ETLDIR = $ENV{"AUTO_HOME"};
if ( !defined($ETL::ETLDIR) ) {
   $ETL::ETLDIR = "D:\\ETL";
}

$ETL::ETL_SERVER = $ENV{"AUTO_SERVER"};
if ( !defined($ETL::ETL_SERVER) ) {
   $ETL::ETL_SERVER = "";
}
 
$ETL::ETL_DSN = $ENV{"AUTO_DSN"};
if ( !defined($ETL::ETL_DSN) ) {
   $ETL::ETL_DSN = "ETL";
}

$ETL::ETL_ETC  = "${ETL::ETLDIR}\\etc";
$ETL::ETL_BIN  = "${ETL::ETLDIR}\\bin";
$ETL::ETL_TMP  = "${ETL::ETLDIR}\\tmp";
$ETL::ETL_LOCK = "${ETL::ETLDIR}\\lock";
$ETL::ETL_LOG  = "${ETL::ETLDIR}\\LOG";
$ETL::ETL_APP  = "${ETL::ETLDIR}\\APP";

$ETL::ETL_RECEIVE   = "${ETL::ETLDIR}\\DATA\\receive";
$ETL::ETL_QUEUE     = "${ETL::ETLDIR}\\DATA\\queue";
$ETL::ETL_MESSAGE   = "${ETL::ETLDIR}\\DATA\\message";
$ETL::ETL_PROCESS   = "${ETL::ETLDIR}\\DATA\\process";
$ETL::ETL_COMPLETE  = "${ETL::ETLDIR}\\DATA\\complete";
$ETL::ETL_FAIL      = "${ETL::ETLDIR}\\DATA\\fail";
$ETL::ETL_CORRUPT   = "${ETL::ETLDIR}\\DATA\\fail\\corrupt";
$ETL::ETL_BYPASS    = "${ETL::ETLDIR}\\DATA\\fail\\bypass";
$ETL::ETL_DUPLICATE = "${ETL::ETLDIR}\\DATA\\fail\\duplicate";
$ETL::ETL_ERROR     = "${ETL::ETLDIR}\\DATA\\fail\\error";
$ETL::ETL_BYPASS    = "${ETL::ETLDIR}\\DATA\\fail\\bypass";
$ETL::ETL_UNKNOWN   = "${ETL::ETLDIR}\\DATA\\fail\\unknown";

$ETL::PERLPATH = $ENV{"AUTO_PERL"};

$ETL::PERL  = "wperl.exe";

$ETL::INVOKER = "${ETL::ETL_BIN}\\etlslave_nt.pl";

$ETL::ETLDB = $ENV{"AUTO_DB"};
if ( !defined($ETL::ETLDB) ) {
   $ETL::ETLDB = "ETL.";
}
else {
   if (substr($ETL::ETLDB, length($ETL::ETLDB)-1, 1) ne ".") {
      $ETL::ETLDB = $ETL::ETLDB . ".";
   }
}

$ETL::DATADB  = "DP_MCIF";
$ETL::TEMPDB  = "DP_MCIF_TEMP";
$ETL::STAGEDB = "pstage";
$ETL::LOGDB   = "DP_MCIF_LOAD_ERROR";

$ETL::LONG_NAME_MODE  = 0;
$ETL::SHORT_NAME_MODE = 1;
$ETL::EVA_NAME_MODE   = 2;

$ETL::SLEEP_TIME = $ENV{"AUTO_SLEEP"};
if ( !defined($ETL::SLEEP_TIME) ) {
   # If the sleep time does not set, we set the sleep time to be 60 seconds
   $ETL::SLEEP_TIME = 60;
}

$ETL::FALSE = 0;
$ETL::TRUE  = 1;

my $NameMode = $ETL::LONG_NAME_MODE;

$ETL::NameMode = $ENV{"AUTO_NAME"};
if ( !defined($ETL::NameMode) ) {
   $ETL::NameMode = $ETL::LONG_NAME_MODE;
}

###############################################################################
# function section
###############################################################################

sub getCurrentDate
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   
   $current = "${year}-${mon}-${mday}";

   return $current;
}

sub getCurrentDateTime
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   
   $current = "${year}-${mon}-${mday} ${hour}:${min}:${sec}";

   return $current;
}

sub getCurrentDateTime1
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   
   $current = "${year}${mon}${mday}${hour}${min}${sec}";

   return $current;
}

sub getCurrentShortDate
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   
   $current = "${mon}${mday}";

   return $current;
}

sub getCurrentFullDate
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   
   $current = "${year}${mon}${mday}";

   return $current;
}

sub formatTXDate
{
   my ($txdate) = @_;

   my $formatTXDate;

   $formatTXDate = substr($txdate, 0, 4) . "-" . substr($txdate, 4, 2) . "-" .
                   substr($txdate, 6, 2);

   return $formatTXDate; 
}

sub showDateTime
{
   my ($output) = @_;

   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   
   $current = "${year}-${mon}-${mday} ${hour}:${min}:${sec}";

   if ( defined($output) ) {
      print $output ("[$current] ");
   }
   else {
      print "[$current] ";
   }
}

sub showTime
{
   my ($output) = @_;

   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   
   $current = "${hour}:${min}:${sec}";

   if ( defined($output) ) {
      print $output ("[$current] ");
   }
   else {
      print "[$current] ";
   }
}

sub showPrefixFullSpace
{
   my ($output) = @_;

   if ( defined($output) ) {
      print $output ("                      ");
   }
   else {
      print "                      ";
   }
}

sub showPrefixSpace
{
   my ($output) = @_;

   if ( defined($output) ) {
      print $output ("           ");
   }
   else {
      print "           ";
   }
}

sub isSizeStable
{
   my ($file) = @_;
   my ($firstSize, $secondSize);

   # Get the first time file size
   $firstSize = (stat($file))[7];

   sleep(2);

   # Get the second time file size
   $secondSize = (stat($file))[7];

   # Compare the file size of two times, to see if they are same
   if ( $firstSize == $secondSize ) {
      # File sizes are same
      return $ETL::TRUE;
   }
   else {
      # File sizes are not same, means it still in transferring data
      return $ETL::FALSE;
   }
}

sub checkJobControl
{
   my ($controlDir, $jobSys) = @_;
   my $controlAll;
   my $controlSys;
   my $controlFlag;

   unless ( open($controlAll, "$controlDir\\CTRL_ALL") ) {
      return $ETL::FALSE;
   }

   $controlFlag = <$controlAll>;
   close($controlAll);

   unless ( (defined($controlFlag) && $controlFlag == 1) ) {
      return $ETL::FALSE;
   }

   unless ( open($controlSys, "$controlDir\\CTRL_${jobSys}") ) {
      return $ETL::FALSE;
   }

   $controlFlag = <$controlSys>;
   close($controlSys);

   unless ( (defined($controlFlag) && $controlFlag == 1) ) {
      return $ETL::FALSE;
   }
   
   return $ETL::TRUE;
}

sub checkSourceFileDuplicate
{
   my ($dbh, $sys, $job, $source) = @_;
   my $sqlText;
   my @tabrow;
   
   $sqlText = "SELECT ReceivedFile " . 
              "  FROM ${ETL::ETLDB}ETL_Received_File" . 
              " WHERE ETL_System = '$sys'" .
              "   AND ETL_Job = '$job'" .
              "   AND ReceivedFile = '$source'";

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) { return $ETL::FALSE; }

   $sth->execute();
    
   @tabrow = $sth->fetchrow();

   $sth->finish();

   if ( $tabrow[0] eq "$source" ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub insertReceivedFileLog
{
   my ($dbh, $sys, $job, $sessionid, $file, $size, $rec, $arrival, $location) = @_;

   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   
   $current = "${year}-${mon}-${mday} ${hour}:${min}:${sec}";

   my $sqlText = "INSERT INTO ${ETL::ETLDB}ETL_Received_File (ETL_System," .
                 "            ETL_Job, JobSessionID, ReceivedFile, FileSize," .
                 "            ExpectedRecord, ArrivalTime, ReceivedTime, Location, Status)" .
                 "   VALUES ('$sys', '$job', $sessionid, '$file', $size, $rec," .
                 "           '$arrival', '$current', '$location', '1')";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( defined($ret) ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub getJobSourceInfo
{
   my ($dbh, $source) = @_;
   my @Tabrow;
   
   my $sqlText = "SELECT AutoFilter FROM ${ETL::ETLDB}ETL_Job_Source" .
                 "   WHERE Source = '$source'";

   my $sth = $dbh->prepare($sqlText) or return "";

   unless ($sth) { return "" }

   my $ret = $sth->execute();

   @Tabrow = $sth->fetchrow();

   $sth->finish();

   return $Tabrow[0];
}

sub getJobStatus
{
   my ($dbh, $sys, $job) = @_;
   my @Tabrow;
   
   my $sqlText = "SELECT Last_JobStatus FROM ${ETL::ETLDB}ETL_Job" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return "";

   unless ($sth) { return "" }

   my $ret = $sth->execute();

   @Tabrow = $sth->fetchrow();

   $sth->finish();

   return $Tabrow[0];
}

sub updateFileLocation
{
   my ($dbh, $sys, $job, $rcvfile, $location) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Received_File " .
                 "   SET Location = '$location'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "     AND ReceivedFile = '$rcvfile'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }	
}

sub updateJobStatus
{
   my ($dbh, $sys, $job, $status) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job " .
                 "   SET Last_JobStatus = '$status'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub updateJobStartTime
{
   my ($dbh, $sys, $job, $starttime) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET Last_StartTime = '$starttime'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub updateJobEndTime
{
   my ($dbh, $sys, $job, $endtime) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET Last_EndTime = '$endtime'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub updateJobStatusWithCheckFlag
{
   my ($dbh, $sys, $job, $status, $checkflag) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET Last_JobStatus = '$status'," .
                 "       CheckFlag = '$checkflag'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub updateJobRunningScript
{
   my ($dbh, $sys, $job, $script) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET RunningScript = '$script'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub clearJobStatus
{
   my ($dbh, $sys, $job, $txdate, $filecnt, $status) = @_;
   my $curtime = ETL::getCurrentDateTime();
   
   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET Last_StartTime = '$curtime'," .
                 "       Last_EndTime = null, Last_JobStatus = '$status'," .
                 "       Last_TXDate = '$txdate', Last_FileCnt= $filecnt," .
                 "       Last_CubeStatus = null" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub updateJobStatusError
{
   my ($dbh, $sys, $job, $txdate, $current, $status) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET Last_StartTime = '$current'," .
                 "       Last_EndTime = null, Last_JobStatus = '$status'," .
                 "       Last_TXDate = '$txdate', Last_FileCnt= 0," .
                 "       Last_CubeStatus = null" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub checkJobDependency
{
   my ($dbh, $sys, $job, $txdate) = @_;
   my @Tabrow;
   my @deptab;
   my $dependency_chk = 1;

   $txdate = formatTXDate($txdate);

   my $sqlText = "SELECT Dependency_System, Dependency_Job" .
                 "   FROM ${ETL::ETLDB}ETL_Job_Dependency" .
                 "   WHERE ETL_System = '$sys'" .
                 "     AND ETL_Job = '$job'" .
                 "     AND Enable = '1'" ;

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return -1;
   }

   $sth->execute();

   my $n = 0;

   while ( @Tabrow = $sth->fetchrow() ) {
      $deptab[$n++] = $Tabrow[0];
      $deptab[$n++] = $Tabrow[1];
   }
   $sth->finish();

   if ( $n == 0 ) {
       return ($dependency_chk, "", "");
   }
   
   my $depsys;
   my $depjob;
   my $sqlText1;
   my $sth1;
      
   for (my $i=0; $i < $n; $i = $i + 2) { 
      $depsys = $deptab[$i];
      $depjob = $deptab[$i + 1];

      $sqlText1 = "SELECT Last_JobStatus FROM ${ETL::ETLDB}ETL_Job" .
                     "   WHERE ETL_System = '$depsys'" .
                     "     AND ETL_Job = '$depjob'" .
                     "     AND Last_JobStatus = 'Done'" .
                     "     AND Last_TXDate >= '$txdate'";

      $sth1 = $dbh->prepare($sqlText1);

      $sth1->execute();
      unless ($sth1) {
         return -1;
      }

      my @status = $sth1->fetchrow();

      $sth1->finish();

      if ( $status[0] ne "Done" ) {
         $dependency_chk = 0;
         last;
      }
   }

   return ($dependency_chk, $depsys, $depjob);
}

sub getNextDayNumber
{
   my ($txdate) = @_;
   my ($year, $month, $mday, $wday);

   $year  = substr($txdate, 0, 4);
   $month = substr($txdate, 4, 2);
   $mday  = substr($txdate, 6, 2);

   $month -= 1;

   my $timenum = Time::Local::timelocal(0, 0, 0, $mday, $month, $year);
   $timenum += 86400;  # plus one day second number

   my @timestr = localtime($timenum);

   $year  = $timestr[5] + 1900;
   $month = $timestr[4] + 1;
   $mday  = $timestr[3];
   $wday  = $timestr[6];

   return $mday;
}

sub getMonthDayNumber
{
   my ($txdate) = @_;
   my ($year, $month, $mday);

   $year  = substr($txdate, 0, 4);
   $month = substr($txdate, 4, 2);
   $mday  = substr($txdate, 6, 2);

   $month -= 1;

   my $timenum = Time::Local::timelocal(0, 0, 0, $mday, $month, $year);

   my @timestr = localtime($timenum);

   $mday  = $timestr[3];

   return $mday;
}

sub getWeekDayNumber
{
   my ($txdate) = @_;
   my ($year, $month, $mday, $wday);

   $year  = substr($txdate, 0, 4);
   $month = substr($txdate, 4, 2);
   $mday  = substr($txdate, 6, 2);

   $month -= 1;

   my $timenum = Time::Local::timelocal(0, 0, 0, $mday, $month, $year);

   my @timestr = localtime($timenum);

   $wday  = $timestr[6];

   return $wday;
}

sub isTimeWindowOK
{
   my ($type, $bhour, $ehour, $current) = @_;
   my @hours;
   my $flag;

   if ($bhour < 0 || $bhour > 23 || $ehour < 0 || $ehour > 23) {
      return $ETL::FALSE;
   }

   if ( $type eq "Y" ) {
      $flag = 0;
   }
   else {
      $flag = 1;
   }

   for (my $i=0; $i<=23; $i++) {
      $hours[$i] = $flag;
   }

   if ( $type eq "Y" ) {
      $flag = 1;
   }
   else {
      $flag = 0;
   }

   if ($bhour == $ehour) {
      $hours[$bhour] = $flag;
   }
   elsif ($bhour < $ehour) {
      for (my $i=$bhour; $i<=23; $i++) {
         if ($i > $ehour) {
            last;
         }
         $hours[$i] = $flag;
      }
   }
   else {  # $bhour > $ehour
      for (my $i=$bhour; $i<=23; $i++) {
         $hours[$i] = $flag;
      }
      for (my $i=0; $i<=$ehour; $i++) {
         $hours[$i] = $flag;
      }
   }

   if ($hours[$current] == 1) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub getJobTimeWindow
{
   my ($dbh, $sys, $job) = @_;
   my @Tabrow;

   my $sqlText = "SELECT Allow, BeginHour, EndHour" .
                 "   FROM ${ETL::ETLDB}ETL_Job_TimeWindow" .
                 "      WHERE ETL_System = '$sys'" .
                 "        AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return undef;
   }

   $sth->execute();
   @Tabrow = $sth->fetchrow();
   $sth->finish();

   unless( @Tabrow ) {
      return undef;
   }

   return @Tabrow;
}

sub checkJobTimeWindow
{
   my ($dbh, $sys, $job) = @_;
   my $current;
   my ($allow, $bhour, $ehour);

   my @ret = getJobTimeWindow($dbh, $sys, $job);
   
   unless (@ret) {
      return $ETL::FALSE;
   }

   $current = (localtime(time()))[2];
   my $ok = isTimeWindowOK($ret[0], $ret[1], $ret[2], $current);

   return $ok;
}

sub checkJobFrequency
{
   my ($dbh, $sys, $job, $txdate) = @_;
   my @Tabrow;
   my $frequency_chk = 0;

   my $sqlText = "SELECT Frequency FROM ${ETL::ETLDB}ETL_Job" .
                 "   WHERE ETL_System = '$sys'" .
                 "     AND ETL_Job = '$job'" ;

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return -1;
   }

   $sth->execute();

   @Tabrow = $sth->fetchrow();

   $sth->finish();

   unless (@Tabrow) {
      return -1;
   }

   my @mLIST=split(/,/,$Tabrow[0]) ;
   my $myDate;

   foreach(@mLIST)
   {
      $myDate=$_;

      if ($myDate == 0) {   # Everyday
         $frequency_chk = 1;
         last;
      }

      if ($myDate == -1) { # End of Month 
         my $nextDay = getNextDayNumber($txdate);

         if ( $nextDay == 1 ) {
            $frequency_chk = 1;
            last;
         }
      }

      if (($myDate >= 1) and ($myDate <= 31)) { # Monthly
         my $monthDay = getMonthDayNumber($txdate);

         if ($myDate == $monthDay) {
            $frequency_chk = 1;
            last;
         } 
      }

      if (($myDate >= 41) and ($myDate <= 47)) { # Weekly
         my $weekDay = $myDate - 40;
         if ($weekDay == 7) { $weekDay = 0 };

         my $wday = getWeekDayNumber($txdate);

         if ( $weekDay == $wday ) {
            $frequency_chk = 1;
         }
      }
   }   # foreach

   return $frequency_chk;
}

sub checkJobEnable
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;

   my $sqlText = "SELECT Enable FROM ${ETL::ETLDB}ETL_Job" .
                 "   WHERE ETL_System = '$sys'" .
                 "     AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return -1;
   }

   $sth->execute();

   @tabrow = $sth->fetchrow();

   $sth->finish();

   unless (@tabrow) {
      return 0;
   }

   if ( "$tabrow[0]" ne "1" ) {
      return 0;
   }

   return 1;
}

sub getConvFileHead
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;
   
   my $sqlText = "SELECT Conv_File_Head " . 
                 "   FROM ${ETL::ETLDB}ETL_Job_Source" . 
                 "      WHERE ETL_System = '$sys'" .
                 "        AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return "";
   }

   $sth->execute();

   if (@tabrow = $sth->fetchrow()) {
      $tabrow[0] =~ tr/[a-z]/[A-Z]/;
   }
   else {
      $tabrow[0] = "";
   }

   $sth->finish();

   return $tabrow[0];
}

sub getMappingJobInfo
{
   my ($dbh, $source) = @_;
   my @Tabrow;
   my ($sys, $job, $filehead);

   $source =~ tr/[a-z]/[A-Z]/;

   my $sqlText = "SELECT ETL_System, ETL_Job, Conv_File_Head " . 
                 "   FROM ${ETL::ETLDB}ETL_Job_Source" . 
                 "      WHERE source = '$source'";

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      $Tabrow[0] = "";
      $Tabrow[1] = "";
      $Tabrow[2] = "";

      return ($Tabrow[0], $Tabrow[1], $Tabrow[2]);
   }

   $sth->execute();

   if (@Tabrow = $sth->fetchrow()) {
      $Tabrow[0] =~ tr/[a-z]/[A-Z]/;
      $Tabrow[1] =~ tr/[a-z]/[A-Z]/;
      $Tabrow[2] =~ tr/[a-z]/[A-Z]/;
   }
   else {
      $Tabrow[0] = "";
      $Tabrow[1] = "";
      $Tabrow[2] = "";
   }

   $sth->finish();

   return ($Tabrow[0], $Tabrow[1], $Tabrow[2]);
}

sub isJobCheckDataDate
{
   my ($dbh, $sys, $job) = @_;
   my @Tabrow;
   my $checkCalendar = "N";

   # To see if this job needs to check data calendar
   my $sqlText = "SELECT CheckCalendar " .
                 "  FROM ${ETL::ETLDB}ETL_Job" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return $ETL::FALSE;
   }

   $sth->execute();

   if (@Tabrow = $sth->fetchrow()) {
      $checkCalendar = $Tabrow[0];
   }

   $sth->finish();

   # This job does not need to check data calendar
   if ($checkCalendar eq "Y") {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub isJobCheckLastStatus
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;
   my $checkLastStatus = "Y";

   # To see if this job needs to check last status before execution
   my $sqltext = "SELECT CheckLastStatus " .
                 "  FROM ${ETL::ETLDB}ETL_Job" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqltext);

   unless ($sth) {
      return $ETL::TRUE;
   }

   $sth->execute();

   if (@tabrow = $sth->fetchrow()) {
      $checkLastStatus = $tabrow[0];
   }

   $sth->finish();

   if ($checkLastStatus eq "N") {
      return $ETL::FALSE;
   }
   else {
      return $ETL::TRUE;
   }
}

sub isDataDateOK
{
   my ($dbh, $sys, $job, $year, $month, $day) = @_;
   my @Tabrow;
   my ($dateSeq, $checkFlag);

   if ( ETL::isJobCheckDataDate($dbh, $sys, $job) == $ETL::FALSE ) {
      return $ETL::TRUE;
   }

   # To see if the data date is in data calendar
   my $sqlText = "SELECT SeqNum, CheckFlag" .
                 "  FROM ${ETL::ETLDB}DataCalendar" .
                 "    WHERE etl_system = '$sys' AND etl_job = '$job'" .
                 "      AND calendarYear = $year AND calendarMonth = $month" .
                 "      AND calendarDay = $day";

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return $ETL::FALSE;
   }

   $sth->execute();

   if (@Tabrow = $sth->fetchrow()) {
      $dateSeq   = $Tabrow[0];
      $checkFlag = $Tabrow[1];
   }
   else {
      $checkFlag = "X";
   }

   $sth->finish();

   # This data date is not in data calendar
   if ($checkFlag eq "X") {
      return $ETL::FALSE;
   }

   # If this date is the first day in calendar, then just return ture
   if ($dateSeq == 1) {
      return $ETL::TRUE;
   }

   # To see if the previous day in data calendar is set or not
   $sqlText = "SELECT SeqNum, CheckFlag" .
              "  FROM ${ETL::ETLDB}DataCalendar" .
              "    WHERE etl_system = '$sys' AND etl_job = '$job'" .
              "      AND calendarYear = '$year' AND seqNum < $dateSeq" .
              "    ORDER BY SeqNum DESC";

   $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return $ETL::FALSE;
   }

   $sth->execute();

   if (@Tabrow = $sth->fetchrow()) {
      $dateSeq   = $Tabrow[0];
      $checkFlag = $Tabrow[1];
   }
   else {
      $checkFlag = "X";
   }

   $sth->finish();

   if ($checkFlag eq "X" || $checkFlag eq "N") {
      return $ETL::FALSE;
   }

   return $ETL::TRUE;
}

sub markDataDate
{
   my ($dbh, $sys, $job, $year, $month, $day) = @_;
   my @Tabrow;
   my $checkCalendar;

   # To see if this job needs to check data calendar
   my $sqlText = "SELECT CheckCalendar " .
                 "  FROM ${ETL::ETLDB}ETL_Job" .
                 "   WHERE etl_system = '$sys' AND etl_job = '$job'";

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      return $ETL::FALSE;
   }

   $sth->execute();

   if (@Tabrow = $sth->fetchrow()) {
      $checkCalendar = $Tabrow[0];
   }
   else {
      $checkCalendar = "N";
   }

   $sth->finish();

   # This job does not need to check data calendar
   if ($checkCalendar ne "Y") {
      return $ETL::TRUE;
   }

   $sqlText = "UPDATE ${ETL::ETLDB}DataCalendar SET CheckFlag = 'Y'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "     AND CalendarYear = $year AND CalendarMonth = $month" .
                 "     AND CalendarDay = $day";

   $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub getJobInfo
{
   my ($dbh, $file) = @_;
   my @Tabrow;
   my ($etlsys, $conv_head, $txdate);

   $etlsys    = substr($file, 0, 3);
   $conv_head = substr($file, 4, (length($file) - 17));
   $txdate    = substr($file, (length($file) - 12), 8);
   
   my $sqlText = "SELECT ETL_System, ETL_Job " . 
                 "   FROM ${ETL::ETLDB}ETL_Job_Source" . 
                 "      WHERE ETL_system = '$etlsys'" .
                 "        AND Conv_File_Head = '$conv_head'";

   my $sth = $dbh->prepare($sqlText);

   unless ($sth) {
      $Tabrow[0] = $etlsys;
      $Tabrow[1] = "";
      $txdate = "";
      return ($Tabrow[0], $Tabrow[1], $txdate);
   }

   $sth->execute();

   if (@Tabrow = $sth->fetchrow()) {
      $Tabrow[0] =~ tr/[a-z]/[A-Z]/;
      $Tabrow[1] =~ tr/[a-z]/[A-Z]/;
   }
   else {
      $Tabrow[0] = $etlsys;
      $Tabrow[1] = "";
      $txdate = "";
   }

   $sth->finish();

   return ($Tabrow[0], $Tabrow[1], $txdate);
}

sub getJobRunInfo
{
   my ($dbh, $file) = @_;
   my @Tabrow;
   my ($etlsys, $conv_head, $txdate);
   my ($etljob, $autoOff, $jobType, $jobGroup);
   my $sqltext;

   $etlsys    = substr($file, 0, 3);
   $conv_head = substr($file, 4, (length($file) - 17));
   $txdate    = substr($file, (length($file) - 12), 8);

   $sqltext = "SELECT ETL_Job" . 
                 "   FROM ${ETL::ETLDB}ETL_Job_Source" . 
                 "      WHERE ETL_system = '$etlsys'" .
                 "        AND Conv_File_Head = '$conv_head'";

   my $sth = $dbh->prepare($sqltext);

   unless ($sth) {
      $etljob  = "";
      $autoOff = "";
      $jobGroup = "";
      $txdate = "";
      return ($etlsys, $etljob, $autoOff, $txdate, $jobGroup);
   }

   $sth->execute();

   if (@Tabrow = $sth->fetchrow()) {
      $Tabrow[0] =~ tr/[a-z]/[A-Z]/;
      $etljob = $Tabrow[0];
   }
   else {
      $etljob  = "";
      $autoOff = "";
      $jobGroup = "";
      $txdate = "";
      $sth->finish();
      return ($etlsys, $etljob, $autoOff, $txdate, $jobGroup);
   }
   $sth->finish();

   # get the auto off flag of job
   $sqltext = "SELECT AutoOff, JobType FROM ${ETL::ETLDB}ETL_Job" .
              " WHERE ETL_System = '$etlsys' AND ETL_Job = '$etljob'";

   $sth = $dbh->prepare($sqltext);
   $sth->execute();

   @Tabrow = $sth->fetchrow();

   $autoOff = $Tabrow[0];
   $jobType = $Tabrow[1];

   $sth->finish();

   # get the job group of job
   $sqltext = "SELECT GroupName FROM ${ETL::ETLDB}ETL_Job_GroupChild" .
              " WHERE ETL_System = '$etlsys' AND ETL_Job = '$etljob'";

   $sth = $dbh->prepare($sqltext);
   $sth->execute();

   @Tabrow = $sth->fetchrow();

   $jobGroup = $Tabrow[0];

   $sth->finish();

   return ($etlsys, $etljob, $autoOff, $txdate, $jobGroup, $jobType);
}

sub getJobRunInfo1
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;
   my ($autoOff, $jobType, $jobGroup);
   my $sqltext;
   my $sth;
   
   # get the auto off flag of job
   $sqltext = "SELECT AutoOff, JobType FROM ${ETL::ETLDB}ETL_Job" .
              " WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   $sth = $dbh->prepare($sqltext);
   $sth->execute();

   if (@tabrow = $sth->fetchrow()) {
      $autoOff = $tabrow[0];
      $jobType = $tabrow[1];
   } else {
      $sth->finish();
      return ("", "", "");	
   }
   
   $sth->finish();

   # get the job group of job
   $sqltext = "SELECT GroupName FROM ${ETL::ETLDB}ETL_Job_GroupChild" .
              " WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   $sth = $dbh->prepare($sqltext);
   $sth->execute();

   @tabrow = $sth->fetchrow();

   $jobGroup = $tabrow[0];

   $sth->finish();

   return ($autoOff, $jobGroup, $jobType);
}

sub getJobStream
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;
   my @jobstream;

   my $sqltext = "SELECT Stream_System, Stream_Job, Enable" .
                 "   FROM ${ETL::ETLDB}ETL_Job_Stream" .
                 "   WHERE ETL_System = '$sys'" .
                 "     AND ETL_Job = '$job'" .
                 "   ORDER BY Stream_System, Stream_Job";

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return -1;
   }

   $sth->execute();

   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $jobstream[$n++] = $tabrow[0];
      $jobstream[$n++] = $tabrow[1];
      $jobstream[$n++] = $tabrow[2];
   }

   $sth->finish();

   return @jobstream;
}

sub getGroupChildJob
{
   my ($dbh, $group) = @_;
   my @Tabrow;
   my @ChildJob;

   my $sqlText = "SELECT ETL_System, ETL_Job" .
                 "   FROM ${ETL::ETLDB}ETL_Job_GroupChild" .
                 "   WHERE GroupName = '$group'" .
                 "   ORDER BY ETL_System, ETL_Job";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return -1;
   }

   $sth->execute();

   my $n = 0;
   while ( @Tabrow = $sth->fetchrow() ) {
      $ChildJob[$n++] = $Tabrow[0];
      $ChildJob[$n++] = $Tabrow[1];
   }

   $sth->finish();

   return @ChildJob;
}

sub uncheckJobFlag
{
   my ($dbh, $sys, $job) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET CheckFlag = 'N'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub clearJobStatusLog
{
   my ($dbh, $sys, $job, $sessionid) = @_;

   my $sqlText = "DELETE ${ETL::ETLDB}ETL_Job_Status" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "     AND JobSessionID = $sessionid";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub insertJobStatusLog
{
   my ($dbh, $sys, $job, $txdate, $sessionid) = @_;

   my $sqlText = "INSERT INTO ${ETL::ETLDB}ETL_Job_Status" .
                 "         (ETL_System, ETL_Job, JobSessionID, TXDate, StartTime, EndTime," .
                 "          JobStatus, FileCnt, CubeStatus, ExpectedRecord)" .
                 "   SELECT '$sys', '$job', $sessionid, '$txdate', Last_StartTime," .
                 "          Last_EndTime, Last_JobStatus, Last_FileCnt," .
                 "          Last_CubeStatus, ExpectedRecord" .
                 "      FROM ${ETL::ETLDB}ETL_Job" .
                 "         WHERE ETL_System = '$sys' AND ETL_Job = '$job'";
   
   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub clearJobLog
{
   my ($dbh, $sys, $job, $sessionid) = @_;

   my $sqlText = "DELETE ${ETL::ETLDB}ETL_Job_Log" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "     AND JobSessionID = $sessionid";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub insertJobLog
{
   my ($dbh, $sys, $job, $txdate, $script, $starttime, $sessionid) = @_;

   my $sqlText = "INSERT INTO ${ETL::ETLDB}ETL_Job_Log" .
                 "       (ETL_System, ETL_Job, JobSessionID, ScriptFile, TxDate, StartTime," .
                 "        EndTime, ReturnCode)".
                 "   VALUES ('$sys', '$job', $sessionid, '$script', '$txdate', '$starttime'," .
                 "           null, null)";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub updateJobLog
{
   my ($dbh, $sys, $job, $txdate, $script, $endtime, $retcode, $sessionid) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job_Log SET" .
                 "       EndTime = '$endtime', ReturnCode = $retcode" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "     AND JobSessionID = $sessionid AND ScriptFile = '$script'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub turnOffJob
{
   my ($dbh, $sys, $job) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET Enable = '0'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub turnOnJob
{
   my ($dbh, $sys, $job) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET Enable = '1'" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub triggerJob
{
   my ($dbh, $txdate, $sys, $job) = @_;
   my @Tabrow;

   my $sqlText = "SELECT Source FROM ${ETL::ETLDB}ETL_Job_Source" .
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
      return $ETL::FALSE;
   }

   my $controlfile;

   if ( $ETL::NameMode == $ETL::SHORT_NAME_MODE ) {
      my $shortdate = substr($txdate, 4, 4);
      $controlfile = "D" . $Tabrow[0] . $shortdate;
   }
   elsif ( $ETL::NameMode == $ETL::LONG_NAME_MODE ){
      $controlfile = "dir." . $Tabrow[0] . $txdate;
   }
   elsif ( $ETL::NameMode == $ETL::EVA_NAME_MODE ){
      $controlfile = "dir." . $Tabrow[0] . "." . $txdate;
   }

   unless ( open(TRIGGERCONTROL, ">${ETL::ETL_RECEIVE}\\$controlfile") ) {
      return $ETL::FALSE;
   }

   close(TRIGGERCONTROL);

   return $ETL::TRUE;
}

sub isGroupChildOK
{
   my ($dbh, $groupName) = @_;
   my @tabrow;

   my $sqltext = "SELECT count(*) FROM ${ETL::ETLDB}ETL_Job_GroupChild A, ${ETL::ETLDB}ETL_Job B" .
                 "   WHERE A.GroupName = '$groupName'" .
                 "     AND A.Enable = '1'" .
                 "     AND (A.ETL_System = B.ETL_System AND A.ETL_Job = B.ETL_Job)" .
                 "     AND (B.CheckFlag <> 'Y' OR B.CheckFlag is null)";

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return $ETL::FALSE;
   }

   $sth->execute();
   @tabrow = $sth->fetchrow();
   $sth->finish();

   unless( @tabrow ) {
      return $ETL::FALSE;
   }

   if ($tabrow[0] == 0) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub getGroupJob
{
   my ($dbh, $groupName) = @_;
   my @Tabrow;

   my $sqlText = "SELECT etl_system, etl_job FROM ${ETL::ETLDB}ETL_Job_Group" .
                 "   WHERE groupname = '$groupName'";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      $Tabrow[0] = "";
      $Tabrow[1] = "";

      return ($Tabrow[0], $Tabrow[1]);
   }

   $sth->execute();
   @Tabrow = $sth->fetchrow();
   $sth->finish();

   return ($Tabrow[0], $Tabrow[1]);
}

sub isGroupHeadJob
{
   my ($dbh, $etlsys, $etljob) = @_;
   my @Tabrow;

   my $sqlText = "SELECT GroupName, AutoOnChild FROM ${ETL::ETLDB}ETL_Job_Group" .
                 "   WHERE ETL_System = '$etlsys'" .
                 "     AND ETL_Job = '$etljob'";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      $Tabrow[0] = "";
      $Tabrow[1] = "";

      return ($Tabrow[0], $Tabrow[1]);
   }

   $sth->execute();
   @Tabrow = $sth->fetchrow();
   $sth->finish();

   return ($Tabrow[0], $Tabrow[1]);
}

sub getJobType
{
   my ($dbh, $sys, $job) = @_;
   my @Tabrow;

   my $sqlText = "SELECT JobType FROM ${ETL::ETLDB}ETL_Job" .
                 "   WHERE ETL_System = '$sys'" .
                 "     AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return "";
   }

   $sth->execute();
   @Tabrow = $sth->fetchrow();
   $sth->finish();

   return $Tabrow[0];
}

sub getDoneJobTxDate
{
   my ($dbh, $sys, $job) = @_;
   my @Tabrow;

   my $sqlText = "SELECT Last_TXDate FROM ${ETL::ETLDB}ETL_Job" .
                 "   WHERE ETL_System = '$sys'" .
                 "     AND ETL_Job = '$job'" .
                 "     AND Last_JobStatus = 'Done'";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return undef;
   }

   $sth->execute();
   @Tabrow = $sth->fetchrow();
   $sth->finish();
   
   return $Tabrow[0];
}

sub insertEventLog
{
   my ($dbh, $prg, $severity, $desc ) = @_;
   my ($eventid, $logtime);

   $logtime = ETL::getCurrentDateTime();
   my $curtime = ETL::getCurrentDateTime1();
   
   $eventid = "${curtime}${prg}${ETL::EVENT_COUNT}";
   
   $ETL::EVENT_COUNT++;
   if ($ETL::EVENT_COUNT == 10) {
      $ETL::EVENT_COUNT = 0;
   }

   my $sqlText = "INSERT INTO ${ETL::ETLDB}ETL_Event" .
                 "         (EventID, EventStatus, Severity, Description," .
                 "          LogTime, CloseTime)" .
                 "   VALUES ('$eventid', 'O', '$severity', '$desc'," .
                 "           '$logtime', null)";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub getRunningJobCount
{
   my ($dbh) = @_;
   my @Tabrow;

   my $sqlText = "SELECT count(*) FROM ${ETL::ETLDB}ETL_JOB" .
                 "   WHERE Last_JobStatus = 'Running'";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return $ETL::FALSE;
   }

   $sth->execute();
   @Tabrow = $sth->fetchrow();
   $sth->finish();

   return $Tabrow[0];
}

sub getJobSessionID
{
   my ($dbh, $sys, $job) = @_;
   my @Tabrow;

   my $sqlText = "SELECT JobSessionID FROM ${ETL::ETLDB}ETL_JOB" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return $ETL::FALSE;
   }

   $sth->execute();
   @Tabrow = $sth->fetchrow();
   $sth->finish();

   return $Tabrow[0];
}

sub increaseJobSessionID
{
   my ($dbh, $sys, $job) = @_;

   my $sqlText = "UPDATE ${ETL::ETLDB}ETL_Job SET JobSessionID = JobSessionID + 1" .
                 "   WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqlText) or return $ETL::FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub cutLeadingSpace
{
   my ($string) = @_;
   my $ch;
   my $i = 0;

   for ($i = 0; $i < length($string); $i++) {
      $ch = substr($string, $i, 1);
      unless (($ch eq " ") || ($ch eq "\t") || ($ch eq "\r") || ($ch eq "\n")) {
         last;
      }
   }

   unless ($i == 0) {
      substr($string, 0, $i) = "";
   }

   return $string;
}

sub cutTrailSpace
{
   my ($string) = @_;
   my $ch;
   my $i = 0;
   my $n = 0;

   for ($i = (length($string) - 1); $i >= 0; $i--) {
      $ch = substr($string, $i, 1);
      unless (($ch eq " ") || ($ch eq "\t") || ($ch eq "\r") || ($ch eq "\n")) {
         last;
      }
      $n++;
   }

   unless ($i == (length($string) - 1)) {
      substr($string, ($i + 1), $n) = "";
   }

   return $string;
}

sub cutLeadingTrailSpace
{
   my ($string) = @_;

   $string = cutLeadingSpace($string);
   $string = cutTrailSpace($string);

   return $string;
}

sub getMasterLock
{
   my ($lockDir) = @_;
   my $masterLock = "$lockDir\\master.lock";

   unless ( defined($lockDir) ) { return $ETL::FALSE; }

   if ( mkdir($masterLock, 0700) ) {
      return $ETL::TRUE;
   }
   else {
      return $ETL::FALSE;
   }
}

sub releaseMasterLock
{
   my ($lockDir) = @_;
   my $masterLock = "$lockDir\\master.lock";

   unless ( defined($lockDir) ) { return $ETL::FALSE; }

   rmdir($masterLock);

   return $ETL::TRUE;
}

###############################################################################
# OS dependent function section
###############################################################################

sub moveFile
{
   my ($sourceFile, $targetFile) = @_;
   my $ret;
   
   $ret = rename($sourceFile, $targetFile);
   
   return $ret;	
}

sub CreateMutex
{
   my ($mutexName) = @_;

   my $mutex = Win32::Mutex->new(1, $mutexName);

   my $ret = $mutex->wait(1);
   
   if ($ret == 1) {
      return $mutex;
   } else {
      return undef;	
   }
}

sub ReleaseMutex
{
   my ($mutex) = @_;
   
   $mutex->release();
}

sub createDirectory
{
   my ($dir) = @_;

   unless (mkdir($dir)) { return $ETL::FALSE; }

   return $ETL::TRUE;
}

sub invokeJob
{
   my ($param) = @_;
   my $job;
   my $processObj;

   $job = "${ETL::PERL} ${ETL::INVOKER} ${param}";

   unless ( Win32::Process::Create($processObj,
                                   "${ETL::PERLPATH}\\bin\\${ETL::PERL}",
                                   $job, 0,
                                   Win32::Process::NORMAL_PRIORITY_CLASS,
                                   $ETL::ETLDIR) )
   {
      return 0;
   }

   return 1;
}

sub connectETL
{
   open(LOGONF, "${ETL::ETL_ETC}\\ETL_LOGON");
   my $logon =<LOGONF>;
   close(LOGONF);

   $logon =~ s/([\n\.\;])//g;
   $logon =~ s/([^ ]*) *([^ ]*)/$2/;
   my ($user , $passwd) = split(',' , $logon);
   
   # We decode the password in ICE algorithm
   my $decodepass = `${ETL::ETL_BIN}\\IceCode.exe -d "$passwd" "$user"`;

   DBI->trace(0);

   my $dbh = DBI->connect("dbi:ODBC:${ETL::ETL_DSN}", $user, $decodepass,
                          { AutoCommit => 1, PrintError => 0, RaiseError => 0 } ) ;

   unless ( defined($dbh) ) { return undef; }
   
   return $dbh;
}

sub disconnectETL
{
   my ($dbh) = @_;

   unless ( $dbh->disconnect() ) { return $ETL::FALSE; }

   return $ETL::TRUE;
}

# Don't remove the below line, otherwise, the other perl program
# which require this file will be terminated,
# it has to be true value at the last line.
1;

__END__

