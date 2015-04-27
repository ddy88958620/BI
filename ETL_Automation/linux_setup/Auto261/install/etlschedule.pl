#!/usr/bin/perl -w
#####################################################################
# Program: etlschedule.pl
# Purpose: This program is doing job scheduling
#####################################################################

use strict;
use DBI;
use Time::Local;

my $DEBUG = 0;

my $VERSION = "v2.5.3";

my $home = $ENV{"AUTO_HOME"};
my $os   = $^O;

$os =~ tr [A-Z][a-z];
my $DIRDELI;
if ( $os eq "mswin32" ) {
   $DIRDELI = "\\";
   unshift(@INC, "$home\\bin");
   require etl_nt;
} else {
   $DIRDELI = "/";
   unshift(@INC, "$home/bin");
   require etl_unix;
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

my @JobList;
my @AllJobList;
my ($LastHour, $LastMin);

my $MutexObj;

my $SLEEP_TIME = 10;
my $PRIMARY_SERVER = 0;

my $value;

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
   $LOGFILE = "${ETL::ETL_LOG}${DIRDELI}etlschedule_${TODAY}.log";

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

sub isLastDayInMonth
{
   my ($year, $month, $day) = @_;
   
   if ( $month == 1 || $month == 3 || $month == 5 || $month == 7 ||
        $month == 8 || $month == 10 || $month == 12 ) {
      if ( $day == 31 ) {
      	 return $TRUE;
      } else {
      	 return $FALSE;
      } 	
   } elsif ( $month == 4 || $month == 6 || $month == 9 || $month == 11 ) {
      if ( $day == 30 ) {
      	 return $TRUE;
      } else {
         return $FALSE;
      }
   } else {
      # Check if it is leap year
      if ( (($year%4)==0 && ($year%100)!=0) || ($year%400)==0 ) {
      	 if ( $day == 29 ) {
      	    return $TRUE;
      	 } else {
      	    return $FALSE;
      	 }
      } else {
      	 if ( $day == 28 ) {
      	    return $TRUE;
      	 } else {
      	    return $FALSE;
      	 }      	
      }
   }
}

sub addOneDay
{
   my ($date) = @_;
   my ($year, $month, $day);
   
   $year  = int($date/10000);
   $month = int(($date%10000)/100);
   $day   = int($date % 100);

   if ( $month == 1 || $month == 3 || $month == 5 || $month == 7 ||
        $month == 8 || $month == 10 || $month == 12 ) {
      if ( $day == 31 ) {
      	 $month++;
      	 $day = 1;
         if ($month==13) {
            $month=1;
            $year++;
         }
      } else {
      	 $day++;
      }
   } elsif ( $month == 4 || $month == 6 || $month == 9 || $month == 11 ) {
      if ( $day == 30 ) {
      	 $month++;
      	 $day = 1;
      } else {
      	 $day++;
      }
   } else {
      # Check if it is leap year
      if ( (($year%4)==0 && ($year%100)!=0) || ($year%400)==0 ) {
      	 if ( $day == 29 ) {
            $month++;
            $day = 1;
         } else {
            $day++;
         }
      } else {
      	 if ( $day == 28 ) {
            $month++;
            $day = 1;
         } else {
            $day++;
         }
      }
   }
   
   $date = ($year*10000) + ($month*100) + $day;
   
   return $date;
}

sub calculateTxDate
{
   my ($txdate, $period) = @_;
   my ($year, $month, $day);
   
   if ($period == 0) { return $txdate; }
   
   $year  = substr($txdate, 0, 4);
   $month = substr($txdate, 4, 2);
   $day   = substr($txdate, 6, 2);

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
   
   $txdate = "${year}${month}${day}";   

   return $txdate;
}


# Get the current moment, by year, month, day, hour, minute
sub getNow
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon   = $mon + 1;
   
   return ($year, $mon, $mday, $wday, $hour, $min);
}

sub getWeekDayNumber
{
   my ($year, $month, $day) = @_;
   my $wday;

   $month -= 1;

   my $timenum = Time::Local::timelocal(0, 0, 0, $day, $month, $year);
   my @timestr = localtime($timenum);

   $wday  = $timestr[6];

   return $wday;
}

# Get the job list for which is trigger by time
sub getJobList
{
   my ($dbh) = @_;
   my @tabrow;

   my $sqltext = "SELECT ETL_Server, ETL_System, ETL_Job, JobType" .
                 "  FROM ${ETL::ETLDB}ETL_Job" .
                 " WHERE TimeTrigger = 'Y' AND JobType <> 'V'" .
                 "   AND Enable = '1'" .
                 " ORDER BY ETL_System, ETL_Job";

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return undef;
   }

   $sth->execute();

   @JobList = ();
   my $n = 0;
   my $count = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      if (!defined($tabrow[0])) { $tabrow[0] = ""; }
      
      $JobList[$n++] = $tabrow[0];
      $JobList[$n++] = $tabrow[1];
      $JobList[$n++] = $tabrow[2];
      $JobList[$n++] = $tabrow[3];

      $count++;
   }

   $sth->finish();

   return $count;
}

sub getTimeTrigger
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;
   my ($starthour, $startmin, $offset);
   my ($lastdate, $lasttime);

   my $sqltext = "SELECT StartHour, StartMin, OffsetDay," .
                 "       LastRunDate, LastRunTime" .
                 "  FROM ${ETL::ETLDB}ETL_TimeTrigger" .
                 " WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return undef;
   }

   $sth->execute();

   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $starthour = $tabrow[0];
      $startmin  = $tabrow[1];
      $offset    = $tabrow[2];
      $lastdate  = $tabrow[3];
      $lasttime  = $tabrow[4];
   }
   
   $sth->finish();
   
   if (!defined($offset)) { $offset = 0; }
   if (!defined($lastdate)) { $lastdate = -1; }
   if (!defined($lasttime)) { $lasttime = -1; }
   
   return ($starthour, $startmin, $offset, $lastdate, $lasttime);
}

sub getTimeTriggerWeekly
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;
   my ($timing);

   my $sqltext = "SELECT Timing" .
                 "  FROM ${ETL::ETLDB}ETL_TimeTrigger_Weekly" .
                 " WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return undef;
   }

   $sth->execute();

   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $timing = $tabrow[0];
   }

   $sth->finish();
   
   if (!defined($timing)) { $timing = "NNNNNNN"; }

   return ($timing);	
}

sub getTimeTriggerMonthly
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;
   my ($timing, $eom);

   my $sqltext = "SELECT Timing, EndOfMonth" .
                 "  FROM ${ETL::ETLDB}ETL_TimeTrigger_Monthly" .
                 " WHERE ETL_System = '$sys' AND ETL_Job = '$job'";

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return undef;
   }

   $sth->execute();

   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $timing = $tabrow[0];
      $eom = $tabrow[1];
   }

   $sth->finish();

   if (!defined($timing)) { $timing = "NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN"; }
   if (!defined($eom)) { $eom = "N"; }
   
   return ($timing, $eom);
}

sub updateJobLastRun
{
   my ($dbh, $sys, $job, $lastdate, $lasttime) = @_;
   my $sqltext;

   $sqltext = "UPDATE ${ETL::ETLDB}ETL_TimeTrigger" .
              "   SET LastRunDate = $lastdate, LastRunTime = $lasttime" .
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

sub isInTimeTriggerCalendar
{
   my ($dbh, $sys, $job, $year, $month, $day) = @_;
   my @tabrow;
   my $existing = 0;

   my $sqltext = "SELECT 1" .
                 "  FROM ${ETL::ETLDB}ETL_TimeTrigger_Calendar" .
                 " WHERE ETL_System = '$sys' AND ETL_Job = '$job'" .
                 "   AND YearNum = $year AND MonthNum = $month AND DayNum = $day";

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return undef;
   }

   $sth->execute();

   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $existing = $tabrow[0];
   }

   $sth->finish();

   if (!defined($existing)) { $existing = "0"; }

   if ( "$existing" eq "1" ) {
      return $TRUE;
   } else {
      return $FALSE;
   }
}

sub invokeJob
{
   my ($dbh, $server, $sys, $job, $txdate) = @_;
   my ($seqid, $reqtime);
   my ($sqltext, $sth);
   my $ret;
   my @tabrow;
 
   $txdate = ETL::formatTXDate($txdate);

   insertJobTraceRecord($dbh, $sys, $job, $txdate);

   ETL::showTime(); print "Generate job [$sys, $job] into job queue for '$txdate'\n";
   if ($DEBUG == 1) { print STDOUT "Invoke job [$sys, $job] for TxDate:$txdate\n";	}
   
   $sqltext = "SELECT MAX(SeqID) FROM ${ETL::ETLDB}ETL_Job_Queue";
   $sth = $dbh->prepare($sqltext);

   unless ($sth) { return $FALSE; }

   $sth->execute();
   @tabrow = $sth->fetchrow();
   $sth->finish();
   
   $seqid = $tabrow[0];
   if (!defined($seqid) || "$seqid" eq "") { $seqid = 0; }

   $seqid++;
   $reqtime = ETL::getCurrentDateTime();

   $sqltext = "INSERT INTO ${ETL::ETLDB}ETL_Job_Queue" .
              "       (ETL_Server, SeqID, ETL_System, ETL_Job, TXDate, RequestTime)" .
              "  VALUES ('$server', $seqid, '$sys', '$job', '$txdate', '$reqtime')";
   
   $sth = $dbh->prepare($sqltext) or return $FALSE;
   $ret = $sth->execute();

   $sth->finish();

   if ($ret) {
      return $TRUE;
   }
   else {
      return $FALSE;
   }
}

sub checkDailyJob
{
   my ($dbh, $server, $sys, $job, $year, $month, $day, $hour, $min) = @_;
   my ($starthour, $startmin, $offsetday, $txdate);
   my ($lastdate, $lasttime);
   my ($currentdate, $currenttime, $starttime);

   if ( $DEBUG == 1 ) { print STDOUT "Checking for daily job...\n"; }
   
   ($starthour, $startmin, $offsetday, $lastdate, $lasttime) = getTimeTrigger($dbh, $sys, $job);   

   $currentdate = ($year * 10000) + ($month * 100) + $day;
   $currenttime = ($hour * 100) + $min;
   
   $starttime = ($starthour * 100) + $startmin;
   
   if ( $DEBUG == 1 ) {
      print STDOUT "StartHour=$starthour, StartMin=$startmin, OffsetDay=$offsetday\n";
      print STDOUT "LastRunDate=$lastdate, LastRunTime=$lasttime\n";
      print STDOUT "CurrentDate=$currentdate, CurrentTime=$currenttime, StartTime=$starttime\n";
   }

   my $checkingdate;

   if ( $lastdate == -1 ) {
      $checkingdate = $currentdate;
   } else {
      $checkingdate = $lastdate;
   }

   my $ret;
   my ($runflag, $resetflag);

   $resetflag = $TRUE;
   
   while ( $TRUE ) {
      if ( $DEBUG == 1 ) { print STDOUT "Checking date is '$checkingdate'\n"; }

      $year  = int($checkingdate/10000);
      $month = int(($checkingdate%10000)/100);
      $day   = int($checkingdate%100);

      $runflag = $FALSE;

      if ($checkingdate == $lastdate) {
         if ($lasttime < $starttime && $currenttime >= $starttime) {
            $runflag = $TRUE;
         }
      } elsif ($checkingdate > $lastdate && $checkingdate != $currentdate) {
         $runflag = $TRUE;
      } elsif ($checkingdate > $lastdate && $checkingdate == $currentdate) {
         if ($lasttime <= $starttime && $currenttime >= $starttime) {
            $runflag = $TRUE;
         }      	
      }

      if ( $runflag == $TRUE ) {
      	 $txdate = calculateTxDate("$checkingdate", $offsetday);
      	 
         $ret = invokeJob($dbh, $server, $sys, $job, $txdate);
         if ($ret != $TRUE) { return $FALSE; }
         
         $lastdate = $checkingdate;
         
         if ($checkingdate == $currentdate) {
            $lasttime = $currenttime;
         } else {
            $lasttime = $starttime;
         }

         ETL::showTime(); print "Update job's last running date and time to '$lastdate $lasttime'.\n";
         if ( $DEBUG == 1 ) { print STDOUT "Update job's last running date and time to '$lastdate $lasttime'.\n"; }
         updateJobLastRun($dbh, $sys, $job, $lastdate, $lasttime);

         $resetflag = $FALSE;
      } else {
         if ( $DEBUG == 1 ) { print STDOUT "This demand job is no need to invoke\n"; }
      }
      
      if ( $checkingdate == $currentdate ) { last; }

      # Add one day to checking date variable
      $checkingdate = addOneDay($checkingdate);
      $lasttime = 0;
   }

   if ( $resetflag == $TRUE ) {
      if ( $DEBUG == 1 ) { print STDOUT "Reset job's last running date and time to '$currentdate $currenttime'.\n"; }

      updateJobLastRun($dbh, $sys, $job, $currentdate, $currenttime);
   }

   return $TRUE;   
}

sub checkWeeklyJob
{
   my ($dbh, $server, $sys, $job, $year, $month, $day, $hour, $min) = @_;
   my ($starthour, $startmin, $offsetday, $txdate);
   my ($lastdate, $lasttime);
   my $timing;
   my $wday;
   my ($currentdate, $currenttime, $starttime);

   if ( $DEBUG == 1 ) { print STDOUT "Checking for weekly job...\n"; }

   ($starthour, $startmin, $offsetday, $lastdate, $lasttime) = getTimeTrigger($dbh, $sys, $job);   
   $timing = getTimeTriggerWeekly($dbh, $sys, $job);

   $currentdate = ($year * 10000) + ($month * 100) + $day;
   $currenttime = ($hour * 100) + $min;
   
   $starttime = ($starthour * 100) + $startmin;
   
   if ( $DEBUG == 1 ) {
      print STDOUT "StartHour=$starthour, StartMin=$startmin, OffsetDay=$offsetday\n";
      print STDOUT "LastRunDate=$lastdate, LastRunTime=$lasttime\n";
      print STDOUT "CurrentDate=$currentdate, CurrentTime=$currenttime, StartTime=$starttime\n";
      print STDOUT "Timing=$timing\n";
   }

   my $checkingdate;

   if ( $lastdate == -1 ) {
      $checkingdate = $currentdate;
   } else {
      $checkingdate = $lastdate;
   }

   my $ret;
   my ($runflag, $resetflag, $wdayflag);
   
   $resetflag = $TRUE;
   
   while ( $TRUE ) {
      if ( $DEBUG == 1 ) { print STDOUT "Checking date is '$checkingdate'\n"; }

      $year  = int($checkingdate/10000);
      $month = int(($checkingdate%10000)/100);
      $day   = int($checkingdate%100);

      $wday = getWeekDayNumber($year, $month, $day);
      
      $runflag = $FALSE;

      $wdayflag = substr($timing, $wday, 1);

      if ( "$wdayflag" eq "Y" ) { # Today is in weekday setting
         if ($checkingdate == $lastdate) {
            if ($lasttime < $starttime && $currenttime >= $starttime) {
               $runflag = $TRUE;
            }
         } elsif ($checkingdate > $lastdate && $checkingdate != $currentdate) {
            $runflag = $TRUE;
         } elsif ($checkingdate > $lastdate && $checkingdate == $currentdate) {
            if ($lasttime <= $starttime && $currenttime >= $starttime) {
               $runflag = $TRUE;
            }      	
         }
      }

      if ( $runflag == $TRUE ) {
      	 $txdate = calculateTxDate("$checkingdate", $offsetday);

         $ret = invokeJob($dbh, $server, $sys, $job, $txdate);
         if ($ret != $TRUE) { return $FALSE; }

         $lastdate = $checkingdate;

         if ($checkingdate == $currentdate) {
            $lasttime = $currenttime;
         } else {
            $lasttime = $starttime;
         }

         ETL::showTime(); print "Update job's last running date and time to '$lastdate $lasttime'.\n";
         if ( $DEBUG == 1 ) { print STDOUT "Update job's last running date and time to '$lastdate $lasttime'.\n"; }
         updateJobLastRun($dbh, $sys, $job, $lastdate, $lasttime);
         
         $resetflag = $FALSE;
      } else {
         if ( $DEBUG == 1 ) { print STDOUT "This demand job is no need to invoke\n"; }
      }
      
      if ( $checkingdate == $currentdate ) { last; }

      # Add one day to checking date variable
      $checkingdate = addOneDay($checkingdate);
      $lasttime = 0;
   }
   
   if ( $resetflag == $TRUE ) {
      if ( $DEBUG == 1 ) { print STDOUT "Reset job's last running date and time to '$currentdate $currenttime'.\n"; }

      updateJobLastRun($dbh, $sys, $job, $currentdate, $currenttime);
   }

   return $TRUE;      
}

sub checkMonthlyJob
{
   my ($dbh, $server, $sys, $job, $year, $month, $day, $hour, $min) = @_;
   my ($starthour, $startmin, $offsetday, $txdate);
   my ($lastdate, $lasttime);
   my ($timing, $eom);
   my ($currentdate, $currenttime, $starttime);

   if ( $DEBUG == 1 ) { print STDOUT "Checking for monthly job...\n"; }

   ($starthour, $startmin, $offsetday, $lastdate, $lasttime) = getTimeTrigger($dbh, $sys, $job);   
   ($timing, $eom) = getTimeTriggerMonthly($dbh, $sys, $job);

   $currentdate = ($year * 10000) + ($month * 100) + $day;
   $currenttime = ($hour * 100) + $min;

   $starttime = ($starthour * 100) + $startmin;

   if ( $DEBUG == 1 ) {
      print STDOUT "StartHour=$starthour, StartMin=$startmin, OffsetDay=$offsetday\n";
      print STDOUT "LastRunDate=$lastdate, LastRunTime=$lasttime\n";
      print STDOUT "CurrentDate=$currentdate, CurrentTime=$currenttime, StartTime=$starttime\n";
      print STDOUT "Timing=$timing, EOM=$eom\n";
   }

   my $checkingdate;

   if ( $lastdate == -1 ) {
      $checkingdate = $currentdate;
   } else {
      $checkingdate = $lastdate;
   }

   my $ret;
   my ($runflag, $resetflag, $mdayflag);

   $resetflag = $TRUE;
   
   while ( $TRUE ) {
      if ( $DEBUG == 1 ) { print STDOUT "Checking date is '$checkingdate'\n"; }

      $year  = int($checkingdate/10000);
      $month = int(($checkingdate%10000)/100);
      $day   = int($checkingdate%100);

      $runflag = $FALSE;

      if ( "$eom" eq "Y" ) {
         if ( isLastDayInMonth($year, $month, $day) == $TRUE ) {
            if ($checkingdate == $lastdate) {
               if ($lasttime < $starttime && $currenttime >= $starttime) {
                  $runflag = $TRUE;
               }
            } elsif ($checkingdate > $lastdate && $checkingdate != $currentdate) {
               $runflag = $TRUE;
            } elsif ($checkingdate > $lastdate && $checkingdate == $currentdate) {
               if ($lasttime <= $starttime && $currenttime >= $starttime) {
                  $runflag = $TRUE;
               }
            }
         }
      }
      
      $mdayflag = substr($timing, $day-1, 1);
      if ( "$mdayflag" eq "Y" ) { # Today is in month setting
         if ($checkingdate == $lastdate) {
            if ($lasttime < $starttime && $currenttime >= $starttime) {
               $runflag = $TRUE;
            }
         } elsif ($checkingdate > $lastdate) {
            if ($lasttime <= $starttime && $currenttime >= $starttime) {
               $runflag = $TRUE;
            }
         }
      }

      if ( $runflag == $TRUE ) {
      	 $txdate = calculateTxDate("$checkingdate", $offsetday);

         $ret = invokeJob($dbh, $server, $sys, $job, $txdate);
         if ($ret != $TRUE) { return $FALSE; }

         $lastdate = $checkingdate;
         if ($checkingdate == $currentdate) {
            $lasttime = $currenttime;
         } else {
            $lasttime = $starttime;
         }

         ETL::showTime(); print "Update job's last running date and time to '$lastdate $lasttime'.\n";      
         if ( $DEBUG == 1 ) { print STDOUT "Update job's last running date and time to '$lastdate $lasttime'.\n"; }
         updateJobLastRun($dbh, $sys, $job, $lastdate, $lasttime);

         $resetflag = $FALSE;
      } else {
         if ( $DEBUG == 1 ) { print STDOUT "This demand job is no need to invoke\n"; }
      }
      
      if ( $checkingdate == $currentdate ) { last; }

      # Add one day to checking date variable
      $checkingdate = addOneDay($checkingdate);      
      $lasttime = 0;
   }

   if ( $resetflag == $TRUE ) {
      if ( $DEBUG == 1 ) { print STDOUT "Reset job's last running date and time to '$currentdate $currenttime'.\n"; }

      updateJobLastRun($dbh, $sys, $job, $currentdate, $currenttime);
   }

   return $TRUE;
}

sub checkDemandJob
{
   my ($dbh, $server, $sys, $job, $year, $month, $day, $hour, $min) = @_;
   my ($starthour, $startmin, $offsetday, $txdate);
   my ($lastdate, $lasttime);
   my ($currentdate, $currenttime, $starttime);

   if ( $DEBUG == 1 ) { print STDOUT "Checking for demand job...\n"; }

   ($starthour, $startmin, $offsetday, $lastdate, $lasttime) = getTimeTrigger($dbh, $sys, $job);   

   $currentdate = ($year * 10000) + ($month * 100) + $day;
   $currenttime = ($hour * 100) + $min;
   
   $starttime = ($starthour * 100) + $startmin;
   
   if ( $DEBUG == 1 ) {
      print STDOUT "StartHour=$starthour, StartMin=$startmin, OffsetDay=$offsetday\n";
      print STDOUT "LastRunDate=$lastdate, LastRunTime=$lasttime\n";
      print STDOUT "CurrentDate=$currentdate, CurrentTime=$currenttime, StartTime=$starttime\n";
   }

   my $checkingdate;

   if ( $lastdate == -1 ) {
      $checkingdate = $currentdate;
   } else {
      $checkingdate = $lastdate;
   }

   my $ret;
   my ($runflag, $resetflag);
   
   $resetflag = $TRUE;
   
   while ( $TRUE ) {
      if ( $DEBUG == 1 ) { print STDOUT "Checking date is '$checkingdate'\n"; }

      $year  = int($checkingdate/10000);
      $month = int(($checkingdate%10000)/100);
      $day   = int($checkingdate%100);

      $runflag = $FALSE;
      
      $ret = isInTimeTriggerCalendar($dbh, $sys, $job, $year, $month, $day);

      if ( $ret == $TRUE ) {
      	 if ( $DEBUG == 1 ) { print STDOUT "'$checkingdate' is in time calendar\n"; }
      	 
         if ($checkingdate == $lastdate) {
            if ($lasttime < $starttime && $currenttime >= $starttime) {
               $runflag = $TRUE;
            }
         } elsif ($checkingdate > $lastdate && $checkingdate != $currentdate) {
            $runflag = $TRUE;
         } elsif ($checkingdate > $lastdate && $checkingdate == $currentdate) {
            if ($lasttime <= $starttime && $currenttime >= $starttime) {
               $runflag = $TRUE;
            }      	
         }
      }
       	 
      if ($runflag == $TRUE) {
      	 $txdate = calculateTxDate("$checkingdate", $offsetday);

         $ret = invokeJob($dbh, $server, $sys, $job, $txdate);
         if ($ret != $TRUE) { return $FALSE; }

         $lastdate = $checkingdate;
         if ($checkingdate == $currentdate) {
            $lasttime = $currenttime;
         } else {
            $lasttime = $starttime;
         }

         ETL::showTime(); print "Update job's last running date and time to '$lastdate $lasttime'.\n";      
         if ( $DEBUG == 1 ) { print STDOUT "Update job's last running date and time to '$lastdate $lasttime'.\n"; }
         updateJobLastRun($dbh, $sys, $job, $lastdate, $lasttime);

         $resetflag = $FALSE;
      } else {
         if ( $DEBUG == 1 ) { print STDOUT "This demand job is no need to invoke\n"; }
      }
      
      if ( $checkingdate == $currentdate ) { last; }

      # Add one day to checking date variable
      $checkingdate = addOneDay($checkingdate);
      $lasttime = 0;
   }

   if ( $resetflag == $TRUE ) {
      if ( $DEBUG == 1 ) { print STDOUT "Reset job's last running date and time to '$currentdate $currenttime'.\n"; }

      updateJobLastRun($dbh, $sys, $job, $currentdate, $currenttime);
   }
   
   return $TRUE;
}

sub doSchedule
{
   my ($server, $sys, $job, $type);
   my ($year, $month, $day, $wday, $hour, $min);
   my ($starthour, $startmin, $rerun, $sleeptime, $retrytime);

   ($year, $month, $day, $wday, $hour, $min) = getNow();
   if ( $DEBUG == 1 ) { print STDOUT "Year=$year, Month=$month, Day=$day, Weekday=$wday, Hour=$hour, Min=$min\n"; }	

   if ($hour == $LastHour && $min == $LastMin) { return $FALSE;	}
   
   ETL::showTime(); print "Connect to ETL DB...\n";
   if ( $DEBUG == 1 ) { print STDOUT "Connect to ETL DB...\n"; }
   $dbCon = ETL::connectETL();

   unless ( defined($dbCon) ) {
      ETL::showTime(); print "ERROR - Unable to connect to ETL database!\n";
      if ( $DEBUG == 1 ) { print STDOUT "ERROR - Unable to connect to ETL database!\n"; }
      
      my $errstr = DBI::errstr;
      ETL::showTime(); print "$errstr\n";
      if ( $DEBUG == 1 ) { print STDOUT "$errstr\n"; }
      return $FALSE;
   }

   ETL::showTime(); print "Get the job list from repository...\n";
   if ( $DEBUG == 1 ) { print STDOUT "Get the job list from repository...\n"; }
   my $count = getJobList($dbCon);

   my ($i, $n);
   for ($i=0, $n=0; $i<$count; $i++) {
      unless ( $dbCon->ping() ) {
         ETL::showTime(); print "ERROR - Lost database connection.\n";
         if ( $DEBUG == 1 ) { print STDOUT "ERROR - Lost database connection.\n"; }
         last;
      }

      $server = $JobList[$n++];
      $sys    = $JobList[$n++];
      $job    = $JobList[$n++];
      $type   = $JobList[$n++];

      if ( $DEBUG == 1 ) {
         print STDOUT "**********************************************************************\n";
      	 print STDOUT "Job:[$sys, $job] Type:[$type]\n";
      }
     
      if ( $type eq "D" ) { # It is a daily job
      	 checkDailyJob($dbCon, $server, $sys, $job, $year, $month, $day, $hour, $min);
      } elsif ( $type eq "W" ) { # It is a weekly job
      	 checkWeeklyJob($dbCon, $server, $sys, $job, $year, $month, $day, $hour, $min);
      } elsif ( $type eq "M" ) { # It is a monthly job
         checkMonthlyJob($dbCon, $server, $sys, $job, $year, $month, $day, $hour, $min);	
      } elsif ( $type eq "9" ) { # It is a demand job
      	 checkDemandJob($dbCon, $server, $sys, $job, $year, $month, $day, $hour, $min);
      }

      if ( $DEBUG == 1 ) {
         print STDOUT "**********************************************************************\n";
      }
   }

   ETL::showTime(); print "Disconnect from ETL DB...\n";
   if ( $DEBUG == 1 ) { print STDOUT "Disconnect from ETL DB...\n"; }

   ETL::disconnectETL($dbCon);

   $LastHour = $hour; $LastMin  = $min;
   
   return $TRUE;
}

sub insertJobTraceRecord
{
   my ($dbh, $sys, $job, $txdate) = @_;
   
   ETL::showPrefixSpace(); print "Insert job trace record for '$sys', '$job', '$txdate'...\n";
   if ($DEBUG == 1) { print STDOUT "Insert job trace record for '$sys', '$job', '$txdate'...\n"; }
   
   my $sqlext = "INSERT INTO ${ETL::ETLDB}ETL_Job_Trace" .
                 "       (ETL_System, ETL_Job, TXDate, JobStatus," .
                 "        StartTime, EndTime)" .
                 "  VALUES ('$sys', '$job', '$txdate', 'Waiting'," .
                 "       NULL, NULL)";

   my $sth = $dbh->prepare($sqlext) or return $FALSE;
   my $ret = $sth->execute();

   $sth->finish();

   if ( $ret ) {
      return $TRUE;
   }
   else {
      ETL::showTime(); print "ERROR - Unable to insert job trace record.\n";  
      if ($DEBUG == 1) { print STDOUT "ERROR - Unable to insert job trace record.\n"; }
      return $FALSE;
   }
}

sub getAllJobList
{
   my ($dbh) = @_;
   my @tabrow;

   my $sqltext = "SELECT ETL_System, ETL_Job, JobType, Frequency,".
                 "       CheckCalendar, TimeTrigger" .
                 "  FROM ${ETL::ETLDB}ETL_Job" .
                 " ORDER BY ETL_System, ETL_Job";

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return -1;
   }

   $sth->execute();

   @AllJobList = ();
   my $n = 0;
   my $count = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $AllJobList[$n++] = $tabrow[0];
      $AllJobList[$n++] = $tabrow[1];
      $AllJobList[$n++] = $tabrow[2];
      $AllJobList[$n++] = $tabrow[3];
      $AllJobList[$n++] = $tabrow[4];
      if (!defined($tabrow[5])) {
      	 $AllJobList[$n++] = "N";
      } else {
         $AllJobList[$n++] = $tabrow[5];
      }
      $count++;
   }

   $sth->finish();

   return $count;
}

sub generateJobTrace
{
   my ($today) = @_;

   if ($DEBUG == 1) { print STDOUT "Generate job trace record.\n"; }
   
   ETL::showTime(); print "Connect to ETL DB...\n";
   if ($DEBUG == 1) { print STDOUT "Connect to ETL DB...\n"; }
   $dbCon = ETL::connectETL();

   unless ( defined($dbCon) ) {
      ETL::showTime(); print "ERROR - Unable to connect to ETL database!\n";
      if ($DEBUG == 1) { print STDOUT "ERROR - Unable to connect to ETL database!\n"; }

      my $errstr = DBI::errstr;
      ETL::showTime(); print "$errstr\n";
      return $FALSE;
   }

   ETL::showTime(); print "Get the all job list from repository...\n";   
   if ($DEBUG == 1) { print STDOUT "Get the all job list from repository...\n"; }
   my $jobCount = getAllJobList($dbCon);

   my $n;
   my $p;
   my ($sys, $job, $jtype, $freq, $chkcal, $timetrigger);
   my $txdate = ETL::formatTXDate($today);

   my ($year, $month, $day);
      
   for ($n=0; $n<$jobCount; $n++) {
      $p = $n * 6;
      $sys = $AllJobList[$p];
      $job = $AllJobList[$p+1];
      $jtype = $AllJobList[$p+2];
      $freq = $AllJobList[$p+3];
      $chkcal = $AllJobList[$p+4];
      $timetrigger = $AllJobList[$p+5];

      if ( "$jtype" ne "Y" ) { next; } # This job is triggered by time, we skip it
      
      if ( "$jtype" eq "V" || "$jtype" eq "9" ) {
         ETL::showPrefixSpace(); print "It is a virtual or demand job, no need to put into trace record.\n";
         next;
      }

      ETL::showTime(); print "Check job '$sys', '$job'...\n";   
      if ( $DEBUG == 1 ) { print STDOUT "Check job '$sys', '$job'...\n"; }
      
      if ( "$chkcal" eq "Y" ) {
      	 # This job need to check with data calendar
         $year  = substr($today, 0, 4);
         $month = substr($today, 4, 2);
         $day   = substr($today, 6, 2);

         # Convert string to number in order to cut the prefix zero
         $year += 0;
         $month += 0;
         $day += 0;
      	 
      	 if (checkDataCalendar($dbCon, $sys, $job, $year, $month, $day) == $FALSE) {
            ETL::showPrefixSpace(); print "Job data calendar is not match, no need to put into trace record.\n";
            next;      	 	
      	 }
      } else {
         # This job need to check with frequency
        
         if (checkFrequency($freq, $today) != 1) {
            ETL::showPrefixSpace(); print "Job frequency is not match, no need to put into trace record.\n";
            next;
         }
      }
      
      ETL::showPrefixSpace(); print "Put job into trace record.\n";
      if ( $DEBUG == 1 ) { print STDOUT "Put job into trace record.\n"; }      
      insertJobTraceRecord($dbCon, $sys, $job, $txdate);
   }

   ETL::showTime(); print "Disconnect from ETL DB...\n";
   if ( $DEBUG == 1 ) { print STDOUT "Disconnect from ETL DB...\n"; }
   ETL::disconnectETL($dbCon);
   
   return $TRUE;
}

sub main
{
   my $lastday = "";
   my $count = 0;

   $LastHour = -1;
   $LastMin  = -1;
      
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
      
      if ( $PRIMARY_SERVER != 1 ) {
         ETL::showTime(); print "This is not the primary server, terminate this program!\n";
      	 last;
      }

      if ( "$lastday" ne "$TODAY" ) {
         # Do generate job trace data
         if ( generateJobTrace("$TODAY") == $FALSE ) {
            sleep($SLEEP_TIME);
            next;
         }
         $lastday = $TODAY;
      }
      
      doSchedule();
       
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
      $MutexObj = ETL::CreateMutex("ETLSCH_MUTEX");
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

   if ( $LOG_STAT == 1 ) {
      print LOGF_H "Stop by signal '${signal}'\n";
   }

   $STOP_FLAG = 1;

   # we exit the program when receive the singnal
   exit(0);
}

sub printVersionInfo
{
   print "\n";
   ETL::showTime(); print "*******************************************************************\n";
   ETL::showTime(); print "* ETL Automation Job Schedule Program ${VERSION}, NCR 2002 Copyright. *\n";
   ETL::showTime(); print "*******************************************************************\n";
   print "\n";
   $PRINT_VERSION_FLAG = 1;
}

#####################################################################

$LOCK_FILE = "${ETL::ETL_LOCK}${DIRDELI}etlschedule.lock";
$LASTLOGFILE = "";

unless ( check_instance() ) { exit(1); }

$SIG{'INT'}  = 'cleanUp';
$SIG{'QUIT'} = 'cleanUp';
$SIG{'TERM'} = 'cleanUp';

open(STDERR, ">&STDOUT");

main();

exit(0);

__END__

