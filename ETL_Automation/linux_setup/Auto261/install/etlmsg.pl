#!/usr/bin/perl
###############################################################################
# Program  : etlmsg.pl
# Argument :
###############################################################################

use strict;
use DBI;

my $VERSION = "v2.5.3";

my $DEBUG = 0;

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

my $MutexObj;

my $SMTPSERVER = "";
my $AUTOSENDER = 'Automation@Server';
my $value;

$value = $ENV{"AUTO_SMTP_SERVER"};
if ( defined($value) ) {
   $SMTPSERVER = $value;
}

$value = $ENV{"AUTO_SENDER"};
if ( defined($value) ) {
   $AUTOSENDER = $value;
}

my $dbCon;
my @dirFileList;

my @notificationList;
my @destinationList;

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
   $LOGFILE = "${ETL::ETL_LOG}${DIRDELI}etlmsg_${TODAY}.log";

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

# Check if the file is a control file for message sender
sub isControlFile
{
   my($filename) = @_;
   
   if ( substr($filename, length($filename)-4, 4) eq ".msg" ) {
      return $TRUE;
   }
   else  {
      return $FALSE;
   }
}

sub getNotificationList
{
   my ($dbh, $sys, $job) = @_;
   my @tabrow;

   my $sqlText = "SELECT SeqID FROM ${ETL::ETLDB}ETL_Notification" .
                 " WHERE ETL_System = '$sys'" .
                 "   AND ETL_Job = '$job'" .
                 "   ORDER BY SeqID";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return -1;
   }

   $sth->execute();

   @notificationList = ();
   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $notificationList[$n++] = $tabrow[0];
   }

   $sth->finish();

   if ( $n == 0 ) {
      ETL::showTime(); print "There is no destination for this job\n";
   }
   
   return $n;
}

sub getNotificationDetail
{
   my ($dbh, $sys, $job, $seqid) = @_;
   my @tabrow;
   my ($dtype, $gname, $uname, $timing, $attach, $email, $sms, $msubject, $mcontent);

   my $sqlText = "SELECT DestType, GroupName, UserName," .
                 "       Timing, AttachLog, Email, ShortMessage," .
                 "       MessageSubject, MessageContent" .
                 "  FROM ${ETL::ETLDB}ETL_Notification" .
                 " WHERE ETL_System = '$sys'" .
                 "   AND ETL_Job = '$job'" .
                 "   AND SeqID = $seqid";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return -1;
   }

   $sth->execute();

   @tabrow = $sth->fetchrow();
   $sth->finish();

   unless ( @tabrow ) {
      return undef;
   }

   $dtype    = $tabrow[0];
   $gname    = $tabrow[1];
   $uname    = $tabrow[2];
   $timing   = $tabrow[3];
   $attach   = $tabrow[4];
   $email    = $tabrow[5];
   $sms      = $tabrow[6];
   $msubject = $tabrow[7];
   $mcontent = $tabrow[8];

   return ($dtype, $gname, $uname, $timing, $attach, $email, $sms, $msubject, $mcontent);
}

sub getGroupMemberList
{
   my ($dbh, $group) = @_;
   my @tabrow;

   my $sqlText = "SELECT UserName FROM ${ETL::ETLDB}ETL_GroupMember" .
                 " WHERE GroupName = '$group'" .
                 "   ORDER BY UserName";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return -1;
   }

   $sth->execute();

   @destinationList = ();
   my $n = 0;
   while ( @tabrow = $sth->fetchrow() ) {
      $destinationList[$n++] = $tabrow[0];
   }

   $sth->finish();

   if ( $n == 0 ) {
      ETL::showTime(); print "There is no member for this group '$group'\n";
   }
   
   return $n;
}

sub getEmailAddress
{
   my ($dbh, $user) = @_;
   my @tabrow;

   my $sqlText = "SELECT Email FROM ${ETL::ETLDB}ETL_User" .
                 " WHERE UserName = '$user'";

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

   return ($tabrow[0]);
}

sub getMobilNumber
{
   my ($dbh, $user) = @_;
   my @tabrow;

   my $sqlText = "SELECT Mobile FROM ${ETL::ETLDB}ETL_User" .
                 " WHERE UserName = '$user'";

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

   return ($tabrow[0]);
}

sub isUserEnabled
{
   my ($dbh, $user) = @_;
   my @tabrow;

   my $sqlText = "SELECT Status FROM ${ETL::ETLDB}ETL_User" .
                 " WHERE UserName = '$user'";

   my $sth = $dbh->prepare($sqlText);
   unless ($sth) {
      return $FALSE;
   }

   $sth->execute();

   @tabrow = $sth->fetchrow();
   $sth->finish();

   unless ( @tabrow ) {
      return $FALSE;
   }

   if ( $tabrow[0] eq "1" ) {
      return $TRUE;
   } else {
      return $FALSE;
   }
}

sub mailFunctionNT
{
    my ($address, $emailFile) = @_;
    my $smtp;

    open(CONTENT, "$emailFile");
    my @content = <CONTENT>;
    close(CONTENT);
    
    my $count = $#content;
    if ( $count == -1 ) {
    	return $FALSE;
    }
    
    $smtp = Net::SMTP->new("${SMTPSERVER}", Timeout => 30); # connect to an SMTP server

    if ( !defined($smtp) ) {
       ETL::showTime(); print "[Error] Unable to connect to SMTP Server!\n";
       return $FALSE;	
    }

    $smtp->mail("${AUTOSENDER}");   # use the sender's address here
    $smtp->to("$address");          # recipient's address
    $smtp->data();                  # Start the mail

    # Send the email content.
    for (my $n = 0; $n <= $count; $n++) {
       $smtp->datasend("$content[$n]");
    }

    $smtp->dataend();                   # Finish sending the mail
    $smtp->quit;                        # Close the SMTP connection

    return $TRUE;
}

sub sendMailUnix
{
   my ($dbh, $sys, $job, $txdate, $user, $result, $attachedFile, $msubject, $mcontent, $subject, $content) = @_;
   my $email;
   
   # Get email address of user
   $email = getEmailAddress($dbh, $user);

   my $messageFile = "${ETL::ETL_TMP}${DIRDELI}message.tmp";

   ETL::showTime(); print "Send message notification via email\n";
   ETL::showPrefixSpace(); print "Send to email address [$email]\n";
   
   if ( -f $attachedFile ) {
      ETL::showPrefixSpace(); print "Attached file [$attachedFile]\n";
   }
   else {
      ETL::showPrefixSpace(); print "Attached file [$attachedFile] not exist!\n";
      $attachedFile = "";	
   }
   
   ETL::showTime(); print "Generate email context file...\n";

   unless (open(MAIL, ">${messageFile}")) {
      ETL::showPrefixSpace(); print "Unable to generate email context file\n";
      return $FALSE;
   }

   if ( $DEBUG == 1 ) {
      print STDOUT "msubject is '$msubject'\n";
      print STDOUT "mcontent is '$mcontent'\n";
      print STDOUT "subject is '$subject'\n";
      print STDOUT "content is '$content'\n";      
   }

   if ( "$msubject" ne "" ) {
      print MAIL "Subject: $msubject\n";
   } else {
      if ( "$subject" ne "" ) {
      	 print MAIL "Subject: $subject\n";
      } else {
         print MAIL "Subject: Automation Message Notification\n";
      }
   }
   
   print MAIL "\n";
   
   if ( "$mcontent" ne "" ) {
      print MAIL "$mcontent\n\n";
   } else {
      print MAIL "$content\n\n";
   }

   # If there is attached file, we call uuencode to encode the attached file
   # in order to send with email
   if ( $attachedFile ne "" ) {
      ETL::showTime(); print "Generate attached file context...\n";
#      `uuencode $attachedFile "ETLAutomationAttachFileUUEncodeByJetWu" >> ${messageFile}`;

      print MAIL "============================== Job Log ==============================\n";
      print MAIL "= Log File = $attachedFile\n";
      print MAIL "\n";
      
      open(ATTFILE, $attachedFile);
      
      my @attFile = <ATTFILE>;
      
      print MAIL @attFile;
      
      close(ATTFILE);
   }

   print MAIL "\n\n";
   
   close(MAIL);

   ETL::showTime(); print "Send email out...\n";

   # Send mail to notify user
   `/usr/bin/mail $email < ${messageFile}`;

   ETL::showTime(); print "Clean up email context file...\n";

   unlink("${messageFile}");

   return $TRUE;
}

sub sendMailNT
{
   my ($dbh, $sys, $job, $txdate, $user, $result, $attachedFile, $msubject, $mcontent, $subject, $content) = @_;
   my $email;
   
   # Get email address of user
   $email = getEmailAddress($dbh, $user);

   my $messageFile = "${ETL::ETL_TMP}${DIRDELI}message.tmp";

   ETL::showTime(); print "Send message notification via email\n";
   ETL::showPrefixSpace(); print "Send to email address [$email]\n";
   ETL::showPrefixSpace(); print "Attached file [$attachedFile]\n";

   ETL::showTime(); print "Generate email context file...\n";

   unless (open(MAIL, ">${messageFile}")) {
      ETL::showPrefixSpace(); print "Unable to generate email context file\n";
      return $FALSE;
   }
   
   if ( $DEBUG == 1 ) {
      print STDOUT "msubject is '$msubject'\n";
      print STDOUT "mcontent is '$mcontent'\n";
      print STDOUT "subject is '$subject'\n";
      print STDOUT "content is '$content'\n";      
   }

   if ( "$msubject" ne "" ) {
      print MAIL "Subject: $msubject\n";
   } else {
      if ( "$subject" ne "" ) {
      	 print MAIL "Subject: $subject\n";
      } else {
         print MAIL "Subject: Automation Message Notification\n";
      }
   }
   
   print MAIL "\n";
   
   if ( "$mcontent" ne "" ) {
      print MAIL "$mcontent\n\n";
   } else {
      if ( "$content" ne "" ) {
         print MAIL "$content\n\n";
      }
   }
   
   # If there is attached file, we call uuencode to encode the attached file
   # in order to send with email
   if ( $attachedFile ne "" ) {
      ETL::showTime(); print "Generate attached file context...\n";

      print MAIL "============================== Job Log ==============================\n";
      print MAIL "\n";
      
      open(ATTFILE, $attachedFile);
      
      my @attFile = <ATTFILE>;
      
      print MAIL @attFile;
      
      close(ATTFILE);
   }

   close(MAIL);

   ETL::showTime(); print "Send email out...\n";

   # Send mail to notify user
   if ( mailFunctionNT($email, $messageFile) == $FALSE ) {
      return $FALSE; 
   }
   
   ETL::showTime(); print "Clean up email context file...\n";

   unlink("${messageFile}");

   return $TRUE;
}

sub sendSMS
{
   my ($dbh, $sys, $job, $txdate, $user, $result, $msubject, $subject) = @_;
   my $mobile;
   
   # Get the mobile phone number of user
   $mobile = getMobilNumber($dbh, $user);
 
   # To check if the smssender.pl exists in bin directory
   if ( -f "${ETL::ETL_BIN}${DIRDELI}smssender.pl" ) {
      ETL::showTime(); print "Send short message to '$mobile'...\n";

      my $smsContentFile = "${ETL::ETL_TMP}${DIRDELI}sms.content";
      open(SMSCONTENT, ">${smsContentFile}");
      print SMSCONTENT "$mobile\n";
      if ( "$msubject" eq "" ) {
      	 print SMSCONTENT "$subject\n";
      } else {
         print SMSCONTENT "$msubject\n";
      }

      close(SMSCONTENT);

      my $smscmd = "${ETL::ETL_BIN}${DIRDELI}smssender.pl ${smsContentFile}";
      ETL::showTime(); print "$smscmd\n";

      system($smscmd);
   }
   
   return $TRUE;
}

sub processControlFile
{
   my ($controlFile) = @_;
   my ($sys, $job, $txdate);
   my ($type, $subject, $content);
   my ($attachedFile, $attFile);
   my $line;
   
   ETL::showTime(); print "Processing message notification file '$controlFile'\n";
   if ( $DEBUG == 1 ) { print STDOUT "Processing message notification file '$controlFile'\n"; }

   open(CONTROL_FILE, "${ETL::ETL_MESSAGE}${DIRDELI}${controlFile}");

   $line = <CONTROL_FILE>;
   chomp($line);

   if ( "$line" ne "Automation Message Notification" ) {
      if ( $DEBUG == 1 ) { print STDOUT "Message file header error!\n"; }

      close(CONTROL_FILE);	
      return $TRUE;
   }

   $line = <CONTROL_FILE>;
   chomp($line);
   $sys = (split(' ', $line))[1];
   
   $line = <CONTROL_FILE>;
   chomp($line);
   $job = (split(' ', $line))[1];

   $line = <CONTROL_FILE>;
   chomp($line);
   $txdate = (split(' ', $line))[1];
   
   ETL::showPrefixSpace(); print "Process message notification for [$sys, $job]\n";
   if ( $DEBUG == 1 ) { print STDOUT "Process message notification for [$sys, $job]\n"; }

   # to get the notification list for this job
   my $count = getNotificationList($dbCon, $sys, $job);

   if ( $count == 0 ) {
      close(CONTROL_FILE);
      
      if ( $DEBUG == 1 ) { print STDOUT "There is no destination for message notification\n"; }
      return $TRUE;
   }

   $line = <CONTROL_FILE>;
   chomp($line);
   $type = (split(' ', $line))[1];

   unless ( defined($type) ) {
      close(CONTROL_FILE);
      return $TRUE;
   }

   if ( "$type" ne "Done" && "$type" ne "Failed" &&
        "$type" ne "Missing" && "$type" ne "Receiving" ) {
       close(CONTROL_FILE);

       ETL::showTime(); print "Unknow message type '$type'\n";
       if ( $DEBUG == 1 ) { print STDOUT "Unknow message type '$type'\n"; }
       return $TRUE;
   }

   $line = <CONTROL_FILE>;
   chomp($line);
   $attachedFile = substr($line, 5);
   unless( defined($attachedFile) ) {
      $attachedFile = "";
   }

   if ( $DEBUG == 1 ) { print STDOUT "ATT: '$attachedFile'\n"; }

   $line = <CONTROL_FILE>;
   chomp($line);
   $subject = substr($line, 9);

   if ( $DEBUG == 1 ) { print STDOUT "SUBJECT: '$subject'\n"; }

   $line = <CONTROL_FILE>;
   $content = substr($line, 9);

   while ($line = <CONTROL_FILE>) {
      $content = $content . $line;
   }

   chomp($content);

   if ( $DEBUG == 1 ) { print STDOUT "CONTENT: '$content'\n"; }
   
   close(CONTROL_FILE);

   my $n = 0;
   my $seqid;
   for ($n=0; $n <= $#notificationList; $n++) {
       $seqid = $notificationList[$n];
       my ($dtype,$gname,$uname,$timing,$attached,$email,$sms,$msubject,$mcontent) = getNotificationDetail($dbCon, $sys, $job, $seqid);
       
       $msubject = ETL::cutLeadingSpace($msubject);
       $mcontent = ETL::cutLeadingSpace($mcontent);

       # If job status is done but the timing is not set as done, we skip it.
       if ( ($type eq "Done") && ($timing ne "D") ) {
          next;
       }

       # If job status is failed but the timing is not set as failed, we skip it.
       if ( ($type eq "Failed") && ($timing ne "F") ) {
          next;
       }

       # If job status is source missing but the timing is not set as missing, we skip it.
       if ( ($type eq "Missing") && ($timing ne "M") ) {
          next;
       }
       
       # If job status is receiving error but the timing is not set as receiving, we skip it.
       if ( ($type eq "Receiving") && ($timing ne "R") ) {
          next;
       }
       
       if ( ($type eq "RecordError") && ($timing ne "E") ) {
       	  next;
       }
       
       if ( $dtype eq "U" ) {  # The destination is a user
           ETL::showPrefixSpace(); print "Send messgae to user '$uname'\n";
       	   @destinationList = ();
       	   $destinationList[0] = $uname;
       	
       } elsif ( $dtype eq "G" ) { # The destination is a group
           ETL::showPrefixSpace(); print "Send messgae to group '$gname'\n";
           getGroupMemberList($dbCon, $gname);
       }

       my $userCount = $#destinationList;

       if ( $userCount == -1 ) {
       	  next;
       }

       if ( $attached eq "N" || ($timing eq "M" || $timing eq "R")) {
          $attFile = "";
       }
       else {
          $attFile = $attachedFile;
       }

       my $up = 0;
       my $username;
       
       for ($up=0; $up<=$userCount; $up++) {
       	   $username = $destinationList[$up];
       	   
           if (isUserEnabled($dbCon, $username) != $TRUE) {
              next;	
           }

           ETL::showTime(); print "Send notification to user [$username]\n";
           ETL::showPrefixSpace(); print "Timing: $timing, Attached: $attached, Email: $email, SMS: $sms\n";
           
           if ( $email eq "Y" ) {
               # send notification via email
              if ( $DEBUG == 1 ) { print STDOUT "Send email to '$username'\n"; }
                         
              sendMailNT($dbCon, $sys, $job, $txdate, $username, $type, $attFile,
                            $msubject, $mcontent, $subject, $content);
           }
           
           if ( $sms eq "Y" ) {
              # send notification via short message
              if ( $DEBUG == 1 ) { print STDOUT "Send SMS to '$username'\n"; }

              sendSMS($dbCon, $sys, $job, $txdate, $username, $type, $msubject, $subject);
           }
       }
   }

   return $TRUE;
}

# This function is to check the message directory to see if there is any
# control file need to be processed.
sub checkMessageDir
{
   my $filename;
   my $ret;

   ETL::showTime(); print "Checking message directory '${ETL::ETL_MESSAGE}'...\n";
   if ( $DEBUG == 1 ) { print STDOUT "Checking message directory '${ETL::ETL_MESSAGE}'...\n"; }
   
   # Open the message directory for processing
   unless ( opendir(MSG_DIR, $ETL::ETL_MESSAGE) ) {
      ETL::showTime(); print "Unable to open ${ETL::ETL_MESSAGE}!\n";
      if ( $DEBUG == 1 ) { print STDOUT "Unable to open ${ETL::ETL_MESSAGE}!\n"; }
      return $FALSE;
   }

   my $n = 0;
   @dirFileList = ();
   my @tempList;
      
   while ($filename = readdir(MSG_DIR))
   {
      if ( $STOP_FLAG ) { last; }

      # If the file is directory then skip it
      if ( -d "${ETL::ETL_MESSAGE}${DIRDELI}${filename}" ) { next; }

      if ( isControlFile($filename) ) {      
         $tempList[$n++] = $filename;
      }
      
      if ( $n == 100 ) { last; } # We process 100 files one time
   }
   # Close the message directory
   closedir(MSG_DIR);

   # If there is no control file existing, we just return from this function
   if ($n == 0) {
      return $FALSE;
   }

   # Sorting the control file list
   @dirFileList = sort(@tempList);

   # There is some control file need to be processed
   # but we have to connect to the ETL database first.
   ETL::showTime(); print "Connect to ETL DB...\n";
   if ( $DEBUG == 1 ) { print STDOUT "Connect to ETL DB...\n"; }
   
   $dbCon = ETL::connectETL();

   unless ( defined($dbCon) ) {
      ETL::showTime(); print "ERROR - Unable to connect to ETL Automation repository!\n";
      my $errstr = $DBI::errstr;
      ETL::showTime(); print "$errstr\n";

      if ( $DEBUG == 1 ) { print STDOUT "$errstr\n"; }      
      return $FALSE;
   }

   # Processing the control file one by one.
   for (my $i=0; $i < $n; $i++) {
      if ( $STOP_FLAG ) { last; }

      $filename = $dirFileList[$i];

      # Call the function to process the control file
      $ret = processControlFile($filename);

      if ( $ret == $TRUE ) {
         ETL::showTime(); print "Remove message file $filename\n";
         if ( $DEBUG == 1 ) { print STDOUT "Remove message file $filename\n"; }
         unlink("${ETL::ETL_MESSAGE}${DIRDELI}${filename}");
      }

      unless ( $dbCon->ping() ) {
         ETL::showTime(); print "ERROR - Lost database connection.\n";
         if ( $DEBUG == 1 ) { print STDOUT "ERROR - Lost database connection.\n"; }
      	 return $FALSE;
      }
   }

   # We have finished all control file, so we disconnect from ETL database.
   ETL::showTime(); print "Disconnect from ETL DB...\n";
   if ( $DEBUG == 1 ) { print STDOUT "Disconnect from ETL DB...\n"; }
   ETL::disconnectETL($dbCon);

   ETL::showTime(); print "Check message directory '$ETL::ETL_MESSAGE' done.\n";

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
      checkMessageDir();

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
      $MutexObj = ETL::CreateMutex("ETLMSG_MUTEX");
      if ( ! defined($MutexObj) ) {
         print STDERR "Only one instance of etlmsg.pl allow to run, program terminated!\n";
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
         print STDERR "Only one instance of etlmsg.pl allow to run, program terminated!\n";
   
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
         last;
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
   ETL::showTime(); print "*****************************************************************\n";
   ETL::showTime(); print "* ETL Automation Messager Program ${VERSION}, NCR 2001 Copyright. *\n";
   ETL::showTime(); print "*****************************************************************\n";
   print "\n";
   $PRINT_VERSION_FLAG = 1;
}

###############################################################################

$LOCK_FILE = "${ETL::ETL_LOCK}${DIRDELI}etlmsg.lock";
$LASTLOGFILE = "";

unless ( check_instance() ) { exit(1); }

$SIG{'INT'}  = 'cleanUp';
$SIG{'QUIT'} = 'cleanUp';
$SIG{'TERM'} = 'cleanUp';

open(STDERR, ">&STDOUT");

main();

exit(0);

__END__
