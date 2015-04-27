#!/usr/bin/perl
# Program: EnPass.pl

use strict;

my $logon_file;
my $username;
my $passwd;
my $decodepass;

my $AUTO_HOME = $ENV{"AUTO_HOME"};

print STDOUT "Please input the logon file name:";
$logon_file = <>;
chomp($logon_file);

print STDOUT "Please input the user name:";
$username = <>;
chomp($username);

print STDOUT "Please input the password:";
$passwd = <>;
chomp($passwd);

$decodepass = `$AUTO_HOME/bin/IceCode.exe -e $passwd $username`;

unless (open(LOGON_FH, ">${AUTO_HOME}/etc/$logon_file")) {
   print STDOUT "ERROR - Can not open logon file.";
   exit(1);
}

print STDOUT "Output the logon file...\n";
print LOGON_FH ".LOGON $username,$decodepass;\n";
print STDOUT "Logon file was generated.\n";

close(LOGON_FH);

exit(0);
