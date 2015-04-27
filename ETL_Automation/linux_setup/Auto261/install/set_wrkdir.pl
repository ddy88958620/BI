#!/usr/bin/perl
# DW Automation, Automation 
# version 1.0
# Script File  : install.pl

use strict; # Declare using Perl strict syntax
my $os   = $^O;
$os =~ tr [A-Z][a-z];
my $DIRDELI;
my $CPCMD;
my $Syscmd;
my $AUTO_HOME = $ENV{"AUTO_HOME"};
if ( !defined($AUTO_HOME) ) {
   $AUTO_HOME = $ENV{"HOME"};
}
my $ans;

if ( $os eq "mswin32" ) {
   $DIRDELI = "\\";   
   $CPCMD="copy";
} else {
   $DIRDELI = "/";
   $CPCMD="cp";
}
while (1) {
      
   print STDOUT "We will create the all working directory under '${AUTO_HOME}',\n";
   print STDOUT "and install the ETL Automation system program.\n";
   print STDOUT "Please press the 'y' to continue...\n";

   $ans = <>;
   chomp($ans);
   $ans =~ tr [A-Z][a-z];
   
   if ("$ans" ne "y") {
      print STDOUT "You can set AUTO_HOME to your automation home directory.\n";
      print STDOUT "Installation exit, try again.\n";
      exit(0);	
   }
   
   last;
}

print STDOUT "Create the directory structure...\n";

print STDOUT "Create the 'APP' directory...\n";
mkdir("${AUTO_HOME}/APP", 0770);

print STDOUT "Create the 'DATA' directory...\n";
mkdir("${AUTO_HOME}/DATA", 0770);

print STDOUT "Create the 'DATA/complete' directory...\n";
mkdir("${AUTO_HOME}/DATA/complete", 0770);

print STDOUT "Create the 'DATA/fail' directory...\n";
mkdir("${AUTO_HOME}/DATA/fail", 0770);

print STDOUT "Create the 'DATA/fail/bypass' directory...\n";
mkdir("${AUTO_HOME}/DATA/fail/bypass", 0770);

print STDOUT "Create the 'DATA/fail/corrupt' directory...\n";
mkdir("${AUTO_HOME}/DATA/fail/corrupt", 0770);

print STDOUT "Create the 'DATA/fail/duplicate' directory...\n";
mkdir("${AUTO_HOME}/DATA/fail/duplicate", 0770);

print STDOUT "Create the 'DATA/fail/error' directory...\n";
mkdir("${AUTO_HOME}/DATA/fail/error", 0770);

print STDOUT "Create the 'DATA/fail/unknown' directory...\n";
mkdir("${AUTO_HOME}/DATA/fail/unknown", 0770);

print STDOUT "Create the 'DATA/message' directory...\n";
mkdir("${AUTO_HOME}/DATA/message", 0770);

print STDOUT "Create the 'DATA/process' directory...\n";
mkdir("${AUTO_HOME}/DATA/process", 0770);

print STDOUT "Create the 'DATA/queue' directory...\n";
mkdir("${AUTO_HOME}/DATA/queue", 0770);

print STDOUT "Create the 'DATA/receive' directory...\n";
mkdir("${AUTO_HOME}/DATA/receive", 0770);

print STDOUT "Create the 'LOG' directory...\n";
mkdir("${AUTO_HOME}/LOG", 0770);

print STDOUT "Create the 'bin' directory...\n";
mkdir("${AUTO_HOME}/bin", 0770);

print STDOUT "Create the 'etc' directory...\n";
mkdir("${AUTO_HOME}/etc", 0770);

print STDOUT "Create the 'lock' directory...\n";
mkdir("${AUTO_HOME}/lock", 0770);

print STDOUT "Create the 'tmp' directory...\n";
mkdir("${AUTO_HOME}/tmp", 0770);

print STDOUT "The working directories were created.\n";
