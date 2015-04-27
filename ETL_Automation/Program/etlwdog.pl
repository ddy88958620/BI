#!/usr/bin/perl
#
###############################################################################
# Program     : etlwdog.pl
# Argument    : none
# Description :
###############################################################################

use strict;
use Socket;

my $DEBUG = 0;

my $VERSION = "v2.5.2";

my $home = $ENV{"AUTO_HOME"};
my $os   = $^O;
my $DIRDELI;

$os =~ tr [A-Z][a-z];

if ( $os eq "svr4" ) {
   unshift(@INC, "$home/bin");
   require etl_unix;
   $DIRDELI = "/";
}
elsif ( $os eq "mswin32" ) {
   unshift(@INC, "$home\\bin");
   require etl_nt;
   $DIRDELI = "\\";
}

my $TRUE  = 1;
my $FALSE = 0;

my $LiveCount = 30;

my $LOG_STAT = 0;
my $LOCK_FILE;

my $AUTO_HOME = "";
my $AUTO_LOG  = "";
my $AUTO_LOCK = "";
my $AUTO_SERVER_IP = "";
my $AUTO_WDOG_PORT = "";
my $PRIMARY_SERVER = 1;
my $AUTO_PRI_SERVER_IP = "";
my $AUTO_PRI_SERVER_PORT = "";

my $value;

$AUTO_HOME = $ENV{"AUTO_HOME"};
if ( !defined($AUTO_HOME) ) {
   print STDERR "Could not get AUTO_HOME variable, terminate this program!\n";
   exit(1);
}

$AUTO_LOG = $ENV{"AUTO_LOG"};
if ( !defined($AUTO_LOG) ) {
   $AUTO_LOG = "${AUTO_HOME}${DIRDELI}LOG";
}

$AUTO_LOCK = "${AUTO_HOME}${DIRDELI}lock";

$AUTO_SERVER_IP = $ENV{"AUTO_SERVER_IP"};
if ( !defined($AUTO_SERVER_IP) ) {
   $AUTO_SERVER_IP = "localhost";
}

$AUTO_WDOG_PORT = $ENV{"AUTO_WDOG_PORT"};
#If the port number is not setting at variable, we assign it a default port, 6346
if ( !defined($AUTO_WDOG_PORT) ) {
   $AUTO_WDOG_PORT = 6346;
}

$PRIMARY_SERVER = $ENV{"AUTO_PRIMARY_SERVER"};
if ( !defined($PRIMARY_SERVER) ) {
   $PRIMARY_SERVER = 0;
}

$AUTO_PRI_SERVER_IP = $ENV{"AUTO_PRIMARY_SERVER_IP"};
if ( !defined($AUTO_PRI_SERVER_IP) ) {
   $AUTO_PRI_SERVER_IP = "";
} 

$AUTO_PRI_SERVER_PORT = $ENV{"AUTO_PRIMARY_SERVER_PORT"};
if ( !defined($AUTO_PRI_SERVER_PORT) ) {
   $AUTO_PRI_SERVER_PORT = 6346;
} 

my $LOGDIR;
my $LOGFILE;
my $LASTLOGFILE = "";
my $TODAY;

my @ServerList;

my $MutexObj;

###############################################################################
# Function Section

sub createLogFile
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   
   $year  += 1900;
   $mon    = sprintf("%02d", $mon + 1);
   $mday   = sprintf("%02d", $mday);
   $TODAY  = "${year}${mon}${mday}";

   $LOGFILE = "${AUTO_LOG}${DIRDELI}etlwdog_${TODAY}.log";

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

sub getServerList
{
   my ($dbh, $primaryServer) = @_;
   my @tabrow;

   my $sqltext = "SELECT ETL_Server FROM ${ETL::ETLDB}ETL_Server" .
                 "  WHERE ETL_Server <> '$primaryServer'"; 

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return -1;
   }

   $sth->execute();

   @ServerList = ();
   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $ServerList[$n++] = $tabrow[0];
   }

   $sth->finish();

   return $n;
}

sub resetServerLiveCount
{
   my ($dbh, $server) = @_;

   my $sqltext = "UPDATE ${ETL::ETLDB}ETL_Server SET LiveCount = 0" .
                 "   WHERE ETL_Server = '$server'";

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

sub increaseServerLiveCount
{
   my ($dbh, $server) = @_;

   my $sqltext = "UPDATE ${ETL::ETLDB}ETL_Server SET LiveCount = LiveCount + 1" .
                 "   WHERE ETL_Server = '$server'";

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

sub initPrimaryServer
{
   my ($d1, $d2, $d3, $d4);
   my $prototype;
   my $host;
   my $ipaddr;
   my $rawserver;
   my $serverip;
   
   ETL::showTime(); print "Init the ETLWDog for primary server at port ${AUTO_WDOG_PORT}...\n";
   
   if ( $DEBUG == 1 ) {
       print STDOUT "Init the ETLWDog for primary server at port ${AUTO_WDOG_PORT}...\n";
   }
   
   ($d1, $d2, $prototype) = getprotobyname("udp");
     
   $host = $AUTO_SERVER_IP;

   ($d1, $d2, $d3, $d4, $rawserver) = gethostbyname($host);
   
   unless ( socket(WDogSocket, PF_INET, SOCK_DGRAM, $prototype) ) {
      ETL::showTime(); print "socket function error: $!\n";
      if ( $DEBUG == 1 ) {
          print STDOUT "socket function error: $!\n";
      }

      return $FALSE;
   }
   
   $serverip = sockaddr_in(${AUTO_WDOG_PORT}, ${rawserver});
   $ipaddr = inet_ntoa(${rawserver});
   ETL::showTime(); print "Binding socket to IP $ipaddr\n";
   if ( $DEBUG == 1 ) {
       print STDOUT "Binding socket to IP $ipaddr\n";
   }

   unless ( bind(WDogSocket, $serverip) ) {
      ETL::showTime(); print "bind function error: $!\n";
      if ( $DEBUG == 1 ) {
         print STDOUT "bind function error: $!\n";
      }

      return $FALSE;
   }

   ETL::showTime(); print "Start to listen socket\n";
   if ( $DEBUG == 1 ) {
       print STDOUT "Start to listen socket\n";
   }

   return $TRUE;
}

sub initSecondaryServer
{
   my ($d1, $d2, $d3, $d4);
   my $prototype;
   my $host;
   my $ipaddr;
   my $rawserver;
   my $serverip;
   
   ETL::showTime(); print "Init the ETLWDog for secondary server at port ${AUTO_WDOG_PORT}...\n";
   
   if ( $DEBUG == 1 ) {
       print STDOUT "Init the ETLWDog for secondary server at port ${AUTO_WDOG_PORT}...\n";
   }
   
   ($d1, $d2, $prototype) = getprotobyname("udp");
     
   $host = $AUTO_SERVER_IP;

   ($d1, $d2, $d3, $d4, $rawserver) = gethostbyname($host);
   
   unless ( socket(WDogSocket, PF_INET, SOCK_DGRAM, $prototype) ) {
      ETL::showTime(); print "socket function error: $!\n";
      if ( $DEBUG == 1 ) {
          print STDOUT "socket function error: $!\n";
      }

      return $FALSE;
   }
   
  
   $serverip = sockaddr_in(${AUTO_WDOG_PORT}, ${rawserver});
   $ipaddr = inet_ntoa(${rawserver});
   ETL::showTime(); print "Binding socket to IP $ipaddr\n";
   if ( $DEBUG == 1 ) {
       print STDOUT "Binding socket to IP $ipaddr\n";
   }

   unless ( bind(WDogSocket, $serverip) ) {
      ETL::showTime(); print "bind function error: $!\n";
      if ( $DEBUG == 1 ) {
         print STDOUT "bind function error: $!\n";
      }

      return $FALSE;
   }

   ETL::showTime(); print "Start to send heartbeat packet\n";
   if ( $DEBUG == 1 ) {
       print STDOUT "Start to listen socket\n";
   }

   return $TRUE;
}

sub touchServer
{
   my ($dbh, $liveServer) = @_;
   my $server;
   my $i;
   
   my $count = $#ServerList;
   
   for ($i=0; $i <= $count; $i++) {
      $server = $ServerList[$i];

      if ( "$server" eq "$liveServer" ) {
      	 next;
      }

      if ( $DEBUG == 1 ) {
         print STDOUT "Increase server live count for '$server'\n";
      }

      increaseServerLiveCount($dbh, $server);
   }
}

sub getDeadServer
{
   my ($dbh) = @_;
   my @deadServerList;   
   my @tabrow;

   my $sqltext = "SELECT ETL_Server FROM ${ETL::ETLDB}ETL_Server" .
                 "  WHERE LiveCount = 10"; 

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return undef;
   }

   $sth->execute();

   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $deadServerList[$n++] = $tabrow[0];
   }

   $sth->finish();
   
   return @deadServerList;
}

sub getRunningJob
{
   my ($dbh, $server) = @_;
   my @runningJobList;   
   my @tabrow;

   my $sqltext = "SELECT ETL_System, ETL_Job FROM ${ETL::ETLDB}ETL_Job" .
                 "  WHERE ETL_Server = '$server'" .
                 "    AND Last_JobStatus = 'Running'"; 

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return undef;
   }

   $sth->execute();

   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $runningJobList[$n++] = $tabrow[0];
      $runningJobList[$n++] = $tabrow[1];
   }

   $sth->finish();
   
   return @runningJobList;
}

sub updateRunningJobToFailed
{
   my ($dbh, $sys, $job) = @_;

   my $sqltext = "UPDATE ${ETL::ETLDB}ETL_Job SET Last_JobStatus = 'Failed'" .
                 "   WHERE ETL_System = '$sys'" .
                 "     AND ETL_Job = '$job'";

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

sub doDeadServerProcedure
{
   my ($dbh) = @_;
   my ($i, $j);
   my @deadServer;
   my @runningJob;
   my ($server, $sys, $job);
   my $eventDesc;

   @deadServer = getDeadServer($dbh);
   
   unless (@deadServer) {
      return;
   }
   
   for ($i = 0; $i <= $#deadServer; $i++) {
      $server = $deadServer[$i];

      if ( $DEBUG == 1 ) {
         print STDOUT "Do dead server procedure for '$server'\n";
      }

      $eventDesc = "Server $server was down!";
      ETL::insertEventLog($dbh, "WDG", "H", "$eventDesc");

      @runningJob = getRunningJob($dbh, $server);
      
      unless (@runningJob) {
      	 next;
      }
      
      for ($j = 0; $j < $#runningJob; $j = $j + 2) {
      	 $sys = $runningJob[$j];
      	 $job = $runningJob[$j+1];
      	 
         if ( $DEBUG == 1 ) {
            print STDOUT "update $sys, $job to 'Failed'\n";
         }
      
         $eventDesc = "Update [$sys], [$job] to Failed because server $server was down!";
         ETL::insertEventLog($dbh, "WDG", "H", "$eventDesc");

         updateRunningJobToFailed($dbh, $sys, $job);
      }
   }
}

sub waitingHeartBeat
{
   my $sendAddress;
   my $packet;
   my ($rin, $rout);
   my $count;
   my ($port, $sendiadd, $ipaddr);
   my $dbh;
   my $errstr;
   
   while ($TRUE) {
      ETL::showTime(); print "Connect to ETL DB...\n";
      if ( $DEBUG == 1 ) {
         print STDOUT "Connect to ETL DB...\n";
      }

      $dbh = ETL::connectETL();

      if ( ! defined($dbh) ) {
         ETL::showTime(); print "ERROR - Unable to connect to ETL database!\n";
         if ( $DEBUG == 1 ) {
            print STDOUT "ERROR - Unable to connect to ETL database!\n";
         }

         $errstr = $DBI::errstr;
         ETL::showTime(); print "$errstr\n";
         if ( $DEBUG == 1 ) {
            print STDOUT "$errstr\n";
         }

         sleep(180);
         next;
      } else {
         if ( $DEBUG == 1 ) {
            print STDOUT "Connection established.\n";
         }
      	
      	 last;
      }
   }
   
   $count = 0;
   while ($TRUE)
   {
      unless ($dbh->ping()) {
         ETL::showTime(); print "ERROR - Lost database connection.\n";
         if ( $DEBUG == 1 ) {
            print STDOUT "ERROR - Lost database connection.\n";
         }
      	
      	 # try to reconnect database
         while ($TRUE) {
            ETL::showTime(); print "Reconnect to ETL DB...\n";
            if ( $DEBUG == 1 ) {
               print STDOUT "Reconnect to ETL DB...\n";
            }

            $dbh = ETL::connectETL();

            if ( !defined($dbh) ) {
               ETL::showTime(); print "ERROR - Unable to connect to ETL database!\n";
               if ( $DEBUG == 1 ) {
                  print STDOUT "ERROR - Unable to connect to ETL database!\n";
               }

               $errstr = $DBI::errstr;
               ETL::showTime(); print "$errstr\n";
               if ( $DEBUG == 1 ) {
                  print STDOUT "$errstr\n";
               }

               sleep(60);
               next;
            } else {
               if ( $DEBUG == 1 ) {
                  print STDOUT "Connection established.\n";
               }
               $count = 0;
               last;
            }
         } # end of while
      } # end of unless

      if ( $count == 0 ) {
      	 createLogFile();
         ETL::showTime(); print "Get the server list from repository...\n";
         getServerList($dbh, ${ETL::ETL_SERVER});
         resetServerLiveCount($dbh, ${ETL::ETL_SERVER});
      }
      
      if ( $DEBUG == 1 ) {
         print STDOUT "Waiting heartbeat from secondary server, $count...\n";
      }

      $rin = '';
      vec($rin, fileno(WDogSocket), 1) = 1;

      $packet = undef;
      $sendAddress = undef;
      
      # timeout after 1.0 seconds
      if (select($rout = $rin, undef, undef, 1.0)) {
         $sendAddress = recv(WDogSocket, $packet, 10, 0);
      }

      $count++;
            
      if ( defined($sendAddress) ) {
         ($port, $sendiadd) = sockaddr_in($sendAddress);
         $ipaddr = inet_ntoa($sendiadd);
    
         if ( $DEBUG == 1 ) {
            print STDOUT "Sender address is $ipaddr\n";
            print STDOUT "The received data is '$packet'\n";
         }

         if ( $DEBUG == 1 ) {
            print STDOUT "Reset server live count for '$packet'\n";
         }
         
         resetServerLiveCount($dbh, $packet);
         if ($count == $LiveCount) {
            touchServer($dbh, $packet);
            $count = 0;
            doDeadServerProcedure($dbh);
         }
      }
      else {
      	 if ($count == $LiveCount) {
      	    touchServer($dbh, "");
      	    $count = 0;
            doDeadServerProcedure($dbh);
      	 }
      }
   } # end of while
}

sub sendingHeartBeat
{
   my ($primaryiaddr, $primarypaddr);
   my $packet = ${ETL::ETL_SERVER};
   my $ret;
   
   $primaryiaddr = inet_aton(${AUTO_PRI_SERVER_IP});
   $primarypaddr = sockaddr_in(${AUTO_PRI_SERVER_PORT}, $primaryiaddr);
   
   while ($TRUE)
   {
      if ( $DEBUG == 1 ) {
         print STDOUT "Sending heart beat to primary server ${AUTO_PRI_SERVER_IP}...\n";
      }
      
      $ret = send(WDogSocket, $packet, 0, $primarypaddr);
      
      if ( defined($ret) ) {
         if ( $DEBUG == 1 ) {
            print STDOUT "Sent out $ret byte(s)\n";
         }
      }
      
      sleep(10);
   }
}

sub main
{
   createLogFile();

   printVersionInfo();

   if ( $PRIMARY_SERVER == 1 ) {  
      unless ( initPrimaryServer() ) {
         ETL::showTime(); print "Unable to init primary server\n";
         if ( $DEBUG == 1 ) {
            print STDOUT print "Unable to init primary server\n";
         }

         return $FALSE;
      }
      
      waitingHeartBeat();
      
   } else {
      unless ( initSecondaryServer() ) {
         ETL::showTime(); print "Unable to init secondary server\n";
         if ( $DEBUG == 1 ) {
            print STDOUT print "Unable to init secondary server\n";
         }

         return $FALSE;
      }
   	
      sendingHeartBeat();
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
      $MutexObj = ETL::CreateMutex("ETLWDOG_MUTEX");
      if ( ! defined($MutexObj) ) {
         print STDERR "Only one instance of etlwdog.pl allow to run, program terminated!\n";
      	 return $FALSE;
      }

      until (ETL::getMasterLock($AUTO_LOCK)) {
         if ($count++ == 5) {
            last;
         }
         sleep(1);
      }
   
      unless ( open(LK_FILE_H, ">$LOCK_FILE") ) {
         ETL::releaseMasterLock($AUTO_LOCK);
         print STDERR "Unable to create lock file!\n";
         return $FALSE;
      }
   
      print LK_FILE_H ("$$\n");
   
      close(LK_FILE_H);
   
      ETL::releaseMasterLock($AUTO_LOCK);
   
      return $TRUE;
   }
   else {
      until (ETL::getMasterLock($AUTO_LOCK)) {
         if ($count++ == 5) {
            last;
         }
         sleep(1);
      }
   
      if ( -f $LOCK_FILE ) {
         ETL::releaseMasterLock($AUTO_LOCK);
         print STDERR "Only one instance of etlwdog.pl allow to run, program terminated!\n";
   
         return $FALSE;
      }  
   
      unless ( open(LK_FILE_H, ">$LOCK_FILE") ) {
         ETL::releaseMasterLock($AUTO_LOCK);
   
         print STDERR "Unable to create lock file, program terminated!\n";
         return $FALSE;
      }
   
      print LK_FILE_H ("$$\n");
   
      close(LK_FILE_H);
   
      ETL::releaseMasterLock($AUTO_LOCK);
   
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

      until (ETL::getMasterLock($AUTO_LOCK)) {
         if ($count++ == 5) {
            unlink($LOCK_FILE);
            return;
         }
         sleep(1);
      }
   
      unlink($LOCK_FILE);
   
      ETL::releaseMasterLock($AUTO_LOCK);
   }
   else {
      until (ETL::getMasterLock($AUTO_LOCK)) {
         if ($count++ == 5) {
            unlink($LOCK_FILE);
            return;
         }
         sleep(1);
      }
   
      unlink($LOCK_FILE);
   
      ETL::releaseMasterLock($AUTO_LOCK);
   }
}

sub cleanUp
{
   my ($signal) = @_;
   my $count = 1;

   until (ETL::getMasterLock($AUTO_LOCK)) {
      if ($count++ == 5) {
         return;
      }
      sleep(1);
   }

   unlink($LOCK_FILE);

   ETL::releaseMasterLock($AUTO_LOCK);
   
   if ( $LOG_STAT == 1 ) {
      print LOGF_H "Stop by signal '${signal}'\n";
   }

   # If the system is Unix, we exit the program when receive the singnal
   if ( $os eq "svr4" ) {
      exit(0);
   }
}

sub printVersionInfo
{
   print "\n";
   ETL::showTime(); print "****************************************************************\n";
   ETL::showTime(); print "* ETL Automation Watch Dog Program ${VERSION}, NCR 2001 Copyright. *\n";
   ETL::showTime(); print "****************************************************************\n";
   print "\n";
}

###############################################################################
# Main Program Section

$LOCK_FILE = "${AUTO_LOCK}${DIRDELI}etlwdog.lock";

unless ( check_instance() ) { exit(1); }

$SIG{'INT'}  = 'cleanUp';
$SIG{'QUIT'} = 'cleanUp';
$SIG{'TERM'} = 'cleanUp';

open(STDERR, ">&STDOUT");

main();

exit 0;

__END__

