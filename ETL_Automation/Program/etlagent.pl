#!/usr/bin/perl -w
###############################################################################
# Program  : etlagent.pl
# Argument : none
###############################################################################

use strict;
use Socket;

my $DEBUG_FLAG = 0;

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

my $OK    = "+OK";
my $ERROR = "-ERROR";

my $NEWLINE = "\015\012";

my $TRUE  = 1;
my $FALSE = 0;

my $LOG_STAT = 0;
my $LOCK_FILE;

my $AUTO_HOME = "";
my $AUTO_LOG  = "";
my $AUTO_LOCK = "";
my $AUTO_PROCESS = "";
my $AUTO_QUEUE = "";
my $AUTO_APP = "";
my $AUTO_SERVER_IP = "";
my $AUTO_AGENT_PORT = "";

$AUTO_HOME = $ENV{"AUTO_HOME"};
if ( !defined($AUTO_HOME) ) {
   print STDERR "Could not get AUTO_HOME variable, terminate this program!\n";
   exit(1);
}

$AUTO_APP = "${AUTO_HOME}${DIRDELI}APP";

$AUTO_PROCESS = $ENV{"AUTO_DATA_PROCESS"};
if ( !defined($AUTO_PROCESS) ) {
   $AUTO_PROCESS = "${AUTO_HOME}${DIRDELI}DATA${DIRDELI}process";
}

$AUTO_QUEUE = $ENV{"AUTO_DATA_QUEUE"};
if ( !defined($AUTO_QUEUE) ) {
   $AUTO_QUEUE = "${AUTO_HOME}${DIRDELI}DATA${DIRDELI}queue";
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

$AUTO_AGENT_PORT = $ENV{"AUTO_AGENT_PORT"};
#If the port number is not setting at variable, we assign it a default port, 6346
if ( !defined($AUTO_AGENT_PORT) ) {
   $AUTO_AGENT_PORT = 6346;
}

my $LOGDIR;
my $LOGFILE;
my $LASTLOGFILE = "";
my $TODAY;

my @AcceptHost;

my $MutexObj;

my $ServerSocket;

my @COMMANDS = ("INVOKE", "FORCE", "GETLOG", "GETSCR", "PUTSCR", "QUERY");
 
sub showTime
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
   print "[$current] ";
}

sub showPrefixFullSpace
{
   print "                      ";         	
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

sub createLogFile
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   
   $year  += 1900;
   $mon    = sprintf("%02d", $mon + 1);
   $mday   = sprintf("%02d", $mday);
   $TODAY  = "${year}${mon}${mday}";

   $LOGFILE = "${AUTO_LOG}${DIRDELI}etlagent_${TODAY}.log";

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

sub getAcceptHostList
{
    my $acceptList = "${AUTO_HOME}${DIRDELI}etc${DIRDELI}agent.host";
    
    @AcceptHost = ();

    if ( ! -f "$acceptList" ) {
    	return;
    }

    unless(open(AHLIST_H, "$acceptList")) {
       return;
    }
    
    my $line;
    my $n = 0;
    while($line=<AHLIST_H>) {
       $line = cutLeadingTrailSpace($line);
       if ("$line" ne "" || substr($line, 0, 1) ne "#") {
          $AcceptHost[$n++] = $line;
       }
    }
    
    close(AHLIST_H);
}

sub checkAcceptHost
{
   my ($host) = @_;
   
   my $count = $#AcceptHost;
   
   # If the accept host array is empty, we just return true
   if ($count == -1) {
      return $TRUE;
   }
   
   # Check whether the $host is in the accept list or not
   # If the host is in accept list, we return true
   # Otherwise, we return false to refuse the connection

   my $flag = $FALSE;
   for (my $i=0; $i<=$count; $i++) {
       if ( "$AcceptHost[$i]" eq substr($host, 0, length($AcceptHost[$i]))) {
       	  $flag = $TRUE;
       }	
   }
   
   return $flag;
}

sub initServer
{
   my $proto = getprotobyname('tcp');
   my ($d1, $d2, $d3, $d4);
   my $prototype;
   my $host;
   my $ipaddr;
   my $rawserver;
   my $serverip;
   
   showTime(); print "Init the ETLAgent at port ${AUTO_AGENT_PORT}...\n";
   
   if ( $DEBUG_FLAG == 1 ) {
       print STDOUT "Init the ETLAgent at port ${AUTO_AGENT_PORT}...\n";
   }
   
   ($d1, $d2, $prototype) = getprotobyname("tcp");
     
   $host = $AUTO_SERVER_IP;

   ($d1, $d2, $d3, $d4, $rawserver) = gethostbyname($host);
   
   unless ( socket(ServerSocket, PF_INET, SOCK_STREAM, $prototype) ) {
      showTime(); print "socket function error: $!\n";
      if ( $DEBUG_FLAG == 1 ) {
          print STDOUT "socket function error: $!\n";
      }

      return $FALSE;
   }
   
   $ServerSocket = *ServerSocket;
   
   setsockopt($ServerSocket, SOL_SOCKET, SO_REUSEADDR, pack("l", 1));

   $serverip = sockaddr_in(${AUTO_AGENT_PORT}, ${rawserver});
   $ipaddr = inet_ntoa(${rawserver});
   showTime(); print "Binding socket to IP $ipaddr\n";
   if ( $DEBUG_FLAG == 1 ) {
       print STDOUT "Binding socket to IP $ipaddr\n";
   }

   unless ( bind($ServerSocket, $serverip) ) {
      showTime(); print "bind function error: $!\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "bind function error: $!\n";
      }

      return $FALSE;
   }

   showTime(); print "Start to listen socket\n";
   if ( $DEBUG_FLAG == 1 ) {
       print STDOUT "Start to listen socket\n";
   }

   unless ( listen($ServerSocket, SOMAXCONN) ) {
      showTime(); print "listen function error: $!\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "listen function error: $!\n";
      }

      return $FALSE;
   }

   return $TRUE;
}    

sub shudownServer
{
   showTime(); print "Close server socket\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "Close server socket\n";
   }
   
   close(ServerSocket);	
}

sub waitForConnection
{
   my $pid;
   my $pcount = 0;
   my $clientaddr;

   my $NewSocket;
   my $client;
   
   while ($TRUE)
   {
      getAcceptHostList(); # We re-get the accept host list again

      showTime(); print "Accepting the connection...\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Accepting the connection...\n";
      }

      unless ($clientaddr = accept(NewSocket, ServerSocket)) {
         showTime(); print "Unable to accept connectiondie\n";
         if ( $DEBUG_FLAG == 1 ) {
            print STDOUT "Unable to accept connectiondie\n";
         }

         return $FALSE;
      }

      createLogFile();
   
      my ($port, $iaddr) = sockaddr_in($clientaddr);
      my $name = gethostbyaddr($iaddr, AF_INET);
      $iaddr = inet_ntoa($iaddr);

      if (!defined($name)) { $name = ""; }
      
      if (checkAcceptHost($iaddr)!=$TRUE) {
      	 # The client is not in the accept host list, we close the connection
         # Close the connection
         close(NewSocket);

         showTime(); print "Close the unauthorized connection\n";
         if ( $DEBUG_FLAG == 1 ) {
            print STDOUT "Close the unauthorized connection\n";
         }
         
         next;
      }

      showTime(); print "Connection from $name [$iaddr] at port $port\n";
      showTime(); print "Connection established\n";
      
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Connection from $name [$iaddr] at port $port\n";
         print STDOUT "Connection established\n";
      }
      $client = *NewSocket;
      serviceRoutine($client);
   }
}

sub serviceRoutine
{
   my ($Connection) = @_;
   my $buffer;
   my @cmds;
   my $cmd;
   my @result = ();

   my $oldfh = select($Connection);
   $| = 1;

   select($oldfh);

   print $Connection ("ETL Agent, Version 0.1${NEWLINE}");
   print $Connection ("Copyright 2002 NCR Taiwan${NEWLINE}");
   print $Connection ("${OK}${NEWLINE}");

   showTime(); print "Start to service\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "Start to service\n";
   }

   while (1)
   {
      $buffer = <$Connection>;
      if ( ! defined($buffer) ) { last; }

      $buffer = cutLeadingTrailSpace($buffer);

      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Get buffer '${buffer}'\n";
      }

      if ("$buffer" eq "") {
         last;
      }
      
      @cmds = split(/\s+/, $buffer);

      $cmd = $cmds[0];

      $cmd =~ tr/[a-z]/[A-Z]/;

      @result = grep(/^${cmd}$/, @COMMANDS);

      if ( defined($result[0]) && ($result[0] eq $cmd) ) {
         showTime(); print "Received Command '${cmd}'\n";
         if ( $DEBUG_FLAG == 1 ) {
            print STDOUT "Received Command '${cmd}'\n";
         }
      } else {
         showTime(); print "Unknown Command '${cmd}'\n";
         if ( $DEBUG_FLAG == 1 ) {
            print STDOUT "Unknown Command '${cmd}'\n";
         }
         last;
      }
       
      if ( $cmd eq "INVOKE" ) {
      	 cmdINVOKE($Connection, @cmds);
      }
      elsif ( $cmd eq "FORCE" ) {
      	 cmdFORCE($Connection, @cmds);
      }
      elsif ( $cmd eq "GETLOG" ) {
      	 cmdGETLOG($Connection, @cmds);
      }
      elsif ( $cmd eq "GETSCR" ) {
      	 cmdGETSCR($Connection, @cmds);
      }
      elsif ( $cmd eq "PUTSCR" ) {
      	 cmdPUTSCR($Connection, @cmds);
      }
      elsif ( $cmd eq "QUERY" ) {
      	 cmdQUERY($Connection, @cmds);
      }
      last;
   }

   # Close the connection
   close($Connection);

   showTime(); print "Client connection was terminated\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "Client connection was terminated\n";
   }
}

########################################################
# Function: cmdINVOKE(connection, parameter)
#
sub cmdINVOKE
{
   my ($con, @param) = @_;

   showTime(); print "INVOKE: @param\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "INVOKE: @param\n";
   }

   # Checking the parameters
   if ( $#param < 1 ) {
      showTime(); print "Parameter is not correct\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Parameter is not correct\n";
      }

      print $con ("${ERROR} INVOKE${NEWLINE}");
      return;
   }	

   my $ctrlFile = $param[1];
   my $fullPathFile = "${AUTO_QUEUE}${DIRDELI}${ctrlFile}";

   showTime(); print "Generate control file '$fullPathFile'\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "Generate control file '$fullPathFile'\n";
   }
   
   unless(open(CTRLFILE_H, ">${fullPathFile}")) {
      showTime(); print "Can not generate control file '${fullPathFile}'\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Can not generate control file '${fullPathFile}'\n";
      }

      print $con "${ERROR} Can not generate control file${NEWLINE}";
      return;
   }
   
   print CTRLFILE_H "\n";
   close(CTRLFILE_H);

   print $con ("${OK} INVOKE${NEWLINE}");
   print $con ("${OK}${NEWLINE}");
}

########################################################
# Function: cmdFORCE(connection, parameter)
#
sub cmdFORCE
{
   my ($con, @param) = @_;
   my $ret;
   
   showTime(); print "FORCE: @param\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "FORCE: @param\n";
   }

   # Checking the parameters
   if ( $#param < 1 ) {
      showTime(); print "Parameter is not correct\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Parameter is not correct\n";
      }

      print $con "${ERROR} Parameter is not correct${NEWLINE}";
      return;
   }

   my $ctrlFile = $param[1];
   my $fullPathFile = "${AUTO_PROCESS}${DIRDELI}${ctrlFile}";

   showTime(); print "Generate control file '$fullPathFile'\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "Generate control file '$fullPathFile'\n";
   }
   
   unless(open(CTRLFILE_H, ">${fullPathFile}")) {
      showTime(); print "Can not generate control file '${fullPathFile}'\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Can not generate control file '${fullPathFile}'\n";
      }

      print $con "${ERROR} Can not generate control file${NEWLINE}";
      return;
   }
   
   print CTRLFILE_H "\n";
   close(CTRLFILE_H);
   
   $ret = ETL::invokeJob($ctrlFile);

   if ( $ret == $TRUE ) {
      print $con ("${OK} FORCE${NEWLINE}");

      showTime(); print "Forced start job ok\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Forced start job ok\n";
      }
   } else {
      print $con ("${ERROR} FORCE${NEWLINE}");	

      showTime(); print "Forced start job failed\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Forced start job failed\n";
      }
   }
   
   print $con ("${OK}${NEWLINE}");
}

########################################################
# Function: cmdGETLOG(connection, parameter)
#
sub cmdGETLOG
{
   my ($con, @param) = @_;

   showTime(); print "GETLOG: @param\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "GETLOG: @param\n";
   }

   # Checking the parameters
   if ( $#param < 3 ) {
      print $con "${ERROR} Parameter is not correct${NEWLINE}";

      showTime(); print "${ERROR} Parameter is not correct\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "${ERROR} Parameter is not correct\n";
      }

      return;
   }

   my $sys     = $param[1];
   my $logdate = $param[2];
   my $logfile = $param[3];
   
   my $fullPathFile = "${AUTO_LOG}${DIRDELI}${sys}${DIRDELI}${logdate}${DIRDELI}${logfile}";

   showTime(); print "Get the log file '$fullPathFile'\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "Get the log file '$fullPathFile'\n";
   }
   
   if ( ! -f ${fullPathFile} ) {
      print $con "${ERROR} Log file does not exist${NEWLINE}";

      showTime(); print "${ERROR} Log file does not exist\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "${ERROR} Log file does not exist\n";
      }

      return;
   }
   
   unless( open(WORKLOG_H, "$fullPathFile") ) {
      print $con "${ERROR} Can not open log file${NEWLINE}";

      showTime(); print "${ERROR} Can not open log file\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "${ERROR} Can not open log file\n";
      }

      return;
   }
   
   my @logBuf = <WORKLOG_H>;
   close(WORKLOG_H);
   
   my $logCount = $#logBuf;

   $logCount++;
   
   print $con "${OK} GETLOG${NEWLINE}";
   print $con "$logCount${NEWLINE}";
   
   my $logLine;
 
   for (my $i=0; $i < $logCount; $i++) {
       $logLine = $logBuf[$i];
       chomp($logLine);
       print $con "${logLine}${NEWLINE}";	
   }

   print $con ("${OK}${NEWLINE}");
}

########################################################
# Function: cmdGETSRC(connection, parameter)
#
sub cmdGETSCR
{
   my ($con, @param) = @_;

   showTime(); print "GETSRC: @param\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "GETSRC: @param\n";
   }
   # Checking the parameters
   if ( $#param < 3 ) {
      showTime(); print "Parameter is not correct\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Parameter is not correct\n";
      }

      print $con "${ERROR} Parameter is not correct${NEWLINE}";
      return;
   }

   my $sys    = $param[1];
   my $job    = $param[2];
   my $script = $param[3];
   
   my $fullPathFile = "${AUTO_APP}${DIRDELI}${sys}${DIRDELI}${job}${DIRDELI}bin${DIRDELI}${script}";

   showTime(); print "Get the script file '$fullPathFile'\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "Get the script file '$fullPathFile'\n";
   }
   
   if ( ! -f ${fullPathFile} ) {
      print $con "${ERROR} Script file does not exist${NEWLINE}";
      return;
   }
   
   unless( open(SCRIPT_H, "$fullPathFile") ) {
      print $con "${ERROR} Can not open script file${NEWLINE}";
      return;
   }
   
   my @scriptBuf = <SCRIPT_H>;
   close(SCRIPT_H);
   
   my $scriptCount = $#scriptBuf;

   $scriptCount++;
   
   print $con "${OK} GETSRC${NEWLINE}";
   print $con "$scriptCount${NEWLINE}";
   
   my $scriptLine;
 
   for (my $i=0; $i < $scriptCount; $i++) {
       $scriptLine = $scriptBuf[$i];
       chomp($scriptLine);
       print $con "${scriptLine}${NEWLINE}";	
   }
   
   print $con ("${OK}${NEWLINE}");
}

########################################################
# Function: cmdPUTSRC(connection, parameter)
#
sub cmdPUTSCR
{
   my ($con, @param) = @_;

   showTime(); print "PUTSRC: @param\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "PUTSRC: @param\n";
   }
   # Checking the parameters
   if ( $#param < 4 ) {
      showTime(); print "Parameter is not correct\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "Parameter is not correct\n";
      }

      print $con "${ERROR} Parameter is not correct${NEWLINE}";
      return;
   }
   my $sys    = $param[1];
   my $job    = $param[2];
   my $script = $param[3];
   my $line   = $param[4];
   
   my $fullPathFile = "${AUTO_APP}${DIRDELI}${sys}${DIRDELI}${job}${DIRDELI}bin${DIRDELI}${script}";

   showTime(); print "Put the script file '$fullPathFile'\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "Put the script file '$fullPathFile'\n";
   }

   print $con ("${OK} PUTSRC${NEWLINE}");
   
   my @scriptBuf;
   
   my $buf;
   my $i;
   for ($i=0; $i<$line; $i++) {
      $buf = <$con>;
      if ( ! defined($buf) ) {
         showTime(); print "ERROR - Script file is not completed\n";
         if ( $DEBUG_FLAG == 1 ) {
            print STDOUT "ERROR - Script file is not completed\n";
         }
      	 return;
      }

      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "line $i: $buf";
      }
      
      $scriptBuf[$i] = $buf;   	
   }
   
   # Backup the file
   rename("${fullPathFile}", "${fullPathFile}.bak");
   
   unless( open(SCRIPT_H, ">${fullPathFile}") ) {
      print $con "${ERROR} Can not open script file${NEWLINE}";

      showTime(); print "${ERROR} Can not open script file\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT "${ERROR} Can not open script file\n";
      }

      return;
   }

   for ($i=0; $i<$line; $i++) {
      $buf = cutTrailSpace($scriptBuf[$i]);
      
      if ( $os eq "svr4" ) {  # Unix
         print SCRIPT_H "$buf\012";
      } else {  # Windows
      	 print SCRIPT_H "$buf\015\012";
      }
   }

   close(SCRIPT_H);

   if ( $os eq "svr4" ) {
      chmod(0750, "${fullPathFile}");
   }

   print $con "${OK} ${line}${NEWLINE}";
}

########################################################
# Function: cmdQUERY(connection, parameter)
#
sub cmdQUERY
{
   my ($con, @param) = @_;

   showTime(); print "QUERY: @param\n";
   if ( $DEBUG_FLAG == 1 ) {
      print STDOUT "QUERY: @param\n";
   }
   
   print $con ("${OK} QUERY${NEWLINE}");
   
   
   print $con "${NEWLINE}";
   print $con ("${OK}${NEWLINE}");
}

sub main
{
   createLogFile();

   printVersionInfo();

   getAcceptHostList();
   
   unless ( initServer() ) {
      showTime(); print "Unable to init server\n";
      if ( $DEBUG_FLAG == 1 ) {
         print STDOUT print "Unable to init server\n";
      }

      return $FALSE;
   }
   
   waitForConnection();

   shutdownServer();
   
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
      $MutexObj = ETL::CreateMutex("ETLAGT_MUTEX");
      if ( ! defined($MutexObj) ) {
         print STDERR "Only one instance of etlagent.pl allow to run, program terminated!\n";
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
            print STDERR "Unable to get master lock for five times, program terminated!\n";
            return $FALSE;
         }
         sleep(1);
      }
   
      if ( -f $LOCK_FILE ) {
         ETL::releaseMasterLock($AUTO_LOCK);
         print STDERR "Only one instance of etlagent.pl allow to run, program terminated!\n";
   
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
   
#   print STDOUT "Clean Up by $signal\n";
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
   ETL::showTime(); print "***********************************************************\n";
   ETL::showTime(); print "* ETL Automation Agent Program ${VERSION}, NCR 2001 Copyright. *\n";
   ETL::showTime(); print "***********************************************************\n";
   print "\n";
}

###############################################################################
# Program Section

$LOCK_FILE = "${AUTO_LOCK}${DIRDELI}etlagent.lock";

unless ( check_instance() ) { exit(1); }

$SIG{'INT'}  = 'cleanUp';
$SIG{'QUIT'} = 'cleanUp';
$SIG{'TERM'} = 'cleanUp';

open(STDERR, ">&STDOUT");

main();

exit 0;

__END__
