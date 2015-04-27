#!/usr/bin/perl
# Program: ExportMultiLoadET.pl

use strict;
use DBI;
my $home = $ENV{"AUTO_HOME"};
my $os   = $^O;

$os =~ tr [A-Z][a-z];

if ( $os eq "mswin32" ) {
   unshift(@INC, "$home\\bin");
   require etl_nt;
}
else {
   unshift(@INC, "$home/bin");
   require etl_unix;
}

my $DIRDELI;

if ( $os eq "mswin32" ) {
   $DIRDELI = "\\";
} else {
   $DIRDELI = "/";
}
my $TRUE = 1;
my $FALSE = 0;

my $AUTO_HOME = $ENV{"AUTO_HOME"};

my $ERROR_TABLE = "" ;
my $OUTPUT_FILE = "";

sub dumpET
{
   my ($dbh) = @_;
   my $sqltext;
   my @tabrow;

   $dbh->{LongReadLen} = 1000;
   $dbh->{PrintError} = 0 ;

   $sqltext = "SELECT ErrorCode,ErrorField,HostData" .
              "  FROM ${ERROR_TABLE}";

   my $sth = $dbh->prepare($sqltext);
   unless ($sth) {
      return $FALSE;
   }

   $sth->execute();

   my $n = 0;
   my $data;
   
   while ( @tabrow = $sth->fetchrow() ) {
      $data = substr($tabrow[2], 1);

      if ( $os eq "svr4" ) {
         chop($data);
         chop($data);
      } else {
      	 chop($data);
      	 chop($data);
      }
            
      print OUTFILE "[$tabrow[0]][$tabrow[1]]${data}\n";
   }

   $sth->finish();

   return $TRUE;
}

######################################################################
# main function
sub main
{
   my $ret;
   my $dbh;
   
   unlink($OUTPUT_FILE);

   ETL::showTime(); print "Connect to databse...\n";
   $dbh = ETL::connectETL();

   unless ( defined($dbh) ) {
      ETL::showTime(); print "ERROR - Unable to connect to database!\n";
      my $errstr = $DBI::errstr;
      ETL::showTime(); print "$errstr\n";
      return $FALSE;
   }

   open(OUTFILE, ">${OUTPUT_FILE}");

   ETL::showTime(); print "Dumping '${ERROR_TABLE}'...\n";
   
   # Call bteq command to load data
   $ret = dumpET($dbh);

   close(OUTFILE);

   ETL::showTime(); print "Disconnect databse...\n";
   
   unless( ETL::disconnectETL($dbh) ) {
      ETL::showTime(); print "ERROR - Disconnect failed!\n";
   }
   
   return $ret;
}

######################################################################
# program section

# To see if there are two parameters,
if ( $#ARGV < 1 ) {
   exit(1);
}

$ERROR_TABLE = ($ARGV=shift);
$OUTPUT_FILE = ($ARGV=shift);

my $ret = main();

exit($ret);
