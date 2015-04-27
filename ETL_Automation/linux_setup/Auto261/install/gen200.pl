#!/usr/bin/perl
###############################################################################
# Program : genbteqs.pl
# Writer  : Bill Wang, NCR China
# Version : 0.1, 2002/04/28
###############################################################################
use strict;
use DBI;

my $AUTO_HOME = $ENV{"AUTO_HOME"};
my $ETL_APP="${AUTO_HOME}/APP";
my $DBNAME="";
my $TABLE = "";
my $SUBSYS = "";
my $JOBDIR = "";

my $TARGETBIN  ="";
my $TARGETDDL = "";

my $OS = $^O;
$OS =~ tr [A-Z][a-z];

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

sub outputHeaderInPerl
{
   my ($utility, $table, $scriptfile) = @_;
   my $current = getCurrentDateTime();

   $table =~ tr [a-z][A-Z];

  print OUTFILE "#!/usr/bin/perl\n";
  print OUTFILE "######################################################################\n";

  print OUTFILE "# BTEQ script in Perl, generate by Script Wizard\n";
   
  print OUTFILE "# Date Time    : $current\n";
  print OUTFILE "# Table : ${DBNAME}.$table\n";
  print OUTFILE "# Script File  : $scriptfile\n";
  print OUTFILE "######################################################################\n";
  print OUTFILE "\n";
  print OUTFILE "use strict; # Declare using Perl strict syntax\n\n";
  print OUTFILE "#\n";
  print OUTFILE "# If you are using other Perl's package, declare here\n";
  print OUTFILE "#\n";
  print OUTFILE "\n";
}

sub outputVariableInPerl
{
   my ($utility, $table, $script) = @_;
   
   print OUTFILE "######################################################################\n";
   print OUTFILE "# Variable Section\n";
   print OUTFILE "my \$AUTO_HOME = \$ENV{\"AUTO_HOME\"};\n";

   print OUTFILE "my \$TARGETDB = \$ENV{\"AUTO_DATADB\"};\n";
   print OUTFILE "my \$WORKDB = \$ENV{\"AUTO_WORKDB\"};\n";
   print OUTFILE "my \$SOURCEDB = \$ENV{\"AUTO_TEMPDB\"};\n\n";

   print OUTFILE "my \$MAXDATE = \$ENV{\"AUTO_MAXDATE\"};\n";
   print OUTFILE "if ( !defined(\$MAXDATE) ) {\n";
   print OUTFILE "   \$MAXDATE = \"30001231\";\n";
   print OUTFILE "}\n";
   print OUTFILE 'my $NULLDATE = $ENV{"AUTO_NULLDATE"};'."\n";
   print OUTFILE 'if ( !defined($NULLDATE) ) {'."\n";
   print OUTFILE '    $NULLDATE = "00010101";'."\n";
   print OUTFILE "}\n";
   print OUTFILE 'my $ILLDATE = $ENV{"AUTO_ILLDATE"};'."\n";
   print OUTFILE 'if ( !defined($ILLDATE) ) {'."\n";
   print OUTFILE '    $ILLDATE = "00010102";'."\n";
   print OUTFILE "}\n";
   print OUTFILE 'my $PDDL = $ENV{"AUTO_PDDL"};'."\n";
   print OUTFILE 'if ( !defined($PDDL) ) {'."\n";
   print OUTFILE '   $PDDL = "PDDL";'."\n";
   print OUTFILE "}\n";
   print OUTFILE 'my $CUBEDB = $ENV{"AUTO_CUBEDB"};'."\n";
   print OUTFILE 'if ( !defined($CUBEDB) ) {'."\n";
   print OUTFILE '   $PDDL = "PMART";'."\n";
   print OUTFILE "}\n";

   print OUTFILE "my \$LOGON_STR;\n";
   print OUTFILE "my \$LOGON_FILE = \"\${AUTO_HOME}/etc/LOGON_${SUBSYS}\";\n";
   print OUTFILE "my \$CONTROL_FILE = \"\";\n\n";
   print OUTFILE "my \$TX_DATE = \"\";\n\n";

   print OUTFILE "my \$SCRIPT = \"${script}\";\n\n";
}

sub outputBTEQFuncInPerl
{
   my $tableName = ${TABLE};
   
   my $bteq;

   $bteq = "bteq";

   print OUTFILE "######################################################################\n";
   print OUTFILE "# BTEQ function\n";
   print OUTFILE "sub run_bteq_command\n";
   print OUTFILE "{\n";
   print OUTFILE "   my \$rc = open(BTEQ, \"| $bteq\");\n\n";
   
   print OUTFILE "   # To see if bteq command invoke ok?\n";
   print OUTFILE "   unless (\$rc) {\n";
   print OUTFILE "      print \"Could not invoke BTEQ command\\n\";\n";
   print OUTFILE "      return -1;\n";
   print OUTFILE "   }\n\n";

   print OUTFILE "   ### Below are BTEQ scripts ###\n";
   print OUTFILE "   print BTEQ <<ENDOFINPUT;\n";
   print OUTFILE "\n";
   print OUTFILE "\${LOGON_STR}\n";
   print OUTFILE ".WIDTH 253\n";
   print OUTFILE "\n";

      print OUTFILE "DELETE FROM \${TARGETDB}\.${TABLE}; \n";
	  print OUTFILE ".IF ERRORCODE <> 0 THEN .QUIT 12;\n";
      print OUTFILE "/* Add your BTEQ script here */\n\n";

      print OUTFILE "INSERT INTO \${TARGETDB}\.${TABLE}\n";
      print OUTFILE "SELECT * FROM \${WORKDB}.${TABLE};\n\n";      
	  print OUTFILE ".IF ERRORCODE <> 0 THEN .QUIT 12;\n";
   
      print OUTFILE ".LOGOFF;\n";
      print OUTFILE ".QUIT 0;\n\n";

   print OUTFILE "ENDOFINPUT\n\n";

   print OUTFILE "   ### End of BTEQ scripts ###\n";
   print OUTFILE "   close(BTEQ);\n\n";
   print OUTFILE "   my \$RET_CODE = \$? >> 8;\n\n";

   print OUTFILE "   # if the return code is 12, that means something error happen\n";
   print OUTFILE "   # so we return 1, otherwise, we return 0 means ok\n";
   print OUTFILE "   if ( \$RET_CODE == 0 ) {\n";
   print OUTFILE "      return 0;\n";
   print OUTFILE "   }\n";
   print OUTFILE "   else {\n";
   print OUTFILE "      return 1;\n";
   print OUTFILE "   }\n";
   print OUTFILE "}\n\n";
}

sub outputBTEQMainInPerl
{
   print OUTFILE "######################################################################\n";
   print OUTFILE "# main function\n";
   print OUTFILE "sub main\n";
   print OUTFILE "{\n";
   print OUTFILE "   my \$ret;\n";

   print OUTFILE "   open(LOGONFILE_H, \"\${LOGON_FILE}\");\n";
   print OUTFILE "   \$LOGON_STR = <LOGONFILE_H>;\n";
   print OUTFILE "   close(LOGONFILE_H);\n\n";

   print OUTFILE "   # Get the decoded logon string\n";
   print OUTFILE "   \$LOGON_STR = `\${AUTO_HOME}/bin/IceCode.exe \"\$LOGON_STR\"`;\n";

   print OUTFILE "   my (\$sec,\$min,\$hour,\$mday,\$mon,\$year,\$wday,\$yday,\$isdst) = localtime(time());\n";
   print OUTFILE "   \$year += 1900;\n";
   print OUTFILE "   \$mon = sprintf(\"%02d\", \$mon + 1);\n";
   print OUTFILE "   \$mday = sprintf(\"%02d\", \$mday);\n";
   print OUTFILE "   my \$today = \"\${year}\${mon}\${mday}\";\n\n";
   print OUTFILE "   # Call bteq command to load data\n";
   print OUTFILE "   \$ret = run_bteq_command();\n";
   print OUTFILE "\n";
   print OUTFILE "   print \"run_bteq_command() = \$ret\";\n";
   print OUTFILE "   return \$ret;\n";
   print OUTFILE "}\n\n";
}

sub outputBTEQProgramInPerl
{
   print OUTFILE "######################################################################\n";
   print OUTFILE "# program section\n\n";

   print OUTFILE "# To see if there is one parameter,\n";
   print OUTFILE "# if there is no parameter, exit program\n";
   print OUTFILE "if ( \$#ARGV < 0 ) {\n";
   print OUTFILE "   exit(1);\n";
   print OUTFILE "}\n\n";
   print OUTFILE "# Get the first argument\n";
   print OUTFILE "\$CONTROL_FILE = \$ARGV[0];\n\n"; 
   print OUTFILE "\$TX_DATE = substr(\${CONTROL_FILE},length(\${CONTROL_FILE})-8, 8);\n";
   print OUTFILE "if ( substr(\${CONTROL_FILE}, length(\${CONTROL_FILE})-3, 3) eq 'dir' ) {\n";
   print OUTFILE "    \$TX_DATE = substr(\${CONTROL_FILE},length(\${CONTROL_FILE})-12, 8);\n";
   print OUTFILE "};\n";

   print OUTFILE "open(STDERR, \">&STDOUT\");\n\n";

   print OUTFILE "my \$ret = main();\n\n";

   print OUTFILE "exit(\$ret);\n";
}

sub createAppPath
{
    my ($table) = @_;

    $table =~ tr[a-z][A-Z];
    $JOBDIR =~ tr[a-z][A-Z];

    my $target = "${ETL_APP}/${SUBSYS}";
    my $targetJob;
    
    $targetJob = "${target}/${JOBDIR}";
    
    my $targetBin = "${targetJob}/bin";
    my $targetDdl = "${targetJob}/ddl";
    
    if ( ! -d ${target} ) {
       print STDOUT "Createing ${target} directory...\n";
       mkdir($target, 0750);     # for Unix
    }

    if ( ! -d $targetJob ) {
       print STDOUT "Createing ${targetJob} directory...\n";
       mkdir($targetJob, 0750);  # for Unix
    }

    if ( ! -d $targetBin ) {
       print STDOUT "Createing ${targetBin} directory...\n";
       mkdir($targetBin, 0750);  # for Unix
    }

    if ( ! -d $targetDdl ) {
       print STDOUT "Createing ${targetDdl} directory...\n";
       mkdir($targetDdl, 0750);  # for Unix
    }

    $TARGETBIN = $targetBin;
    $TARGETDDL = $targetDdl;
}

sub main()
{
	my $script = "${JOBDIR}0200.pl";
	createAppPath();
	if ( -f "${TARGETBIN}/${script}" ) {
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
		$year += 1900;
		$mon = sprintf("%02d", $mon + 1);
		$mday = sprintf("%02d", $mday);
		$hour = sprintf("%02d", $hour);
		$min  = sprintf("%02d", $min);
		$sec  = sprintf("%02d", $sec);

		rename("${TARGETBIN}/${script}", "${TARGETBIN}/${script}.${mon}${mday}${hour}${min}${sec}");
		print "${script} ==> ${script}.${mon}${mday}${hour}${min}${sec}\n";
	}
	print "Creating script to  ${TARGETBIN}/${script} ...\n";

	open (OUTFILE,">${TARGETBIN}/${script}");

	outputHeaderInPerl("", $TABLE, $script);
	outputVariableInPerl("", $TABLE, $script);
	outputBTEQFuncInPerl();
	outputBTEQMainInPerl();
	outputBTEQProgramInPerl();
}

if ( $#ARGV < 3 ) {
   print "Usage: gen200.pl SUBSYS JOBDIR DBNAME TABLE\n";
   exit(1);
}

$SUBSYS = $ARGV[0];
$JOBDIR = $ARGV[1];
$DBNAME = $ARGV[2];
$TABLE = $ARGV[3];
$JOBDIR =~ tr [A-Z][a-z];


my $rc = main();

exit(0);

__END__
