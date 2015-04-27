#!/usr/bin/perl
###############################################################################
# Program : genscript.pl
# Writer  : Jet Wu, NCR Taiwan
# Version : 0.1, 2001/07/17
###############################################################################

use strict;
use DBI;

my $ETLDIR;
my $ETL_APP;

my $NOTABLE = 0;

my ($USER, $PASSWD);
my $DSOURCE = "";
my $DBNAME = "";
my $LOGDBNAME = "";
my $TABLE = "";
my $SUBSYS = "";
my $JOBDIR = "";
my $OUTFILE = "";
my $OS = "";
my $LOGON = "";
my $JOB = "fload";
my $OTYPE = "none";
my $DFORMAT = "vartext";
my $DELI = "|";
my $NEWLINE = "yes";
my $RECORD = "1";
my $HOME = ".";
my $TARGETBIN;
my $TARGETDDL;

my @TableList;
my @ColumnList;
my @IndexList;

my $TableCount = 0;
my $ColumnCount = 0;
my $IndexCount = 0;

my $VARTEXT = 1;

# get the current running operating system
$OS = $^O;
$OS =~ tr [A-Z][a-z];

my $DIRD = "";

if ( $OS eq "mswin32" ) {
   $DIRD = "\\\\";
}
else {
   $DIRD = "/";
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

# This function will find the table in the database
# It will store the table at @TableList array
# then return the count of table
sub getDBTable
{
   my ($dbh) = @_;
   my @Tabrow;
   my @row;
   my $rowcount = 0;
   my $sqlText = "";

   $dbh->{LongReadLen} = 1000;
   $dbh->{PrintError} = 0 ;

   if ( $NOTABLE == 1 ) { 
      $sqlText = "SELECT DataBaseName, TableName FROM DBC.Tables" .
                 " WHERE DATABASENAME = '$DBNAME' ORDER BY 1,2";
   }
   else {
      $sqlText = "SELECT DataBaseName, TableName FROM DBC.Tables" . 
                 " WHERE DATABASENAME = '$DBNAME' AND TableName = '$TABLE'";
   }

   my $sth = $dbh->prepare($sqlText);

   unless($sth) {
      return -1;
   }

   $TableCount = 0;

   $sth->execute();
   while (@Tabrow = $sth->fetchrow())
   {
      $TableList[$TableCount] = $Tabrow[1];
      $TableCount += 1;
   }

   $sth->finish();

   return $TableCount;
}

# This function will find out the table's scheme from DB
# It will store the table's column at @ColumnList array
# then return the count of table's column
sub getTableSchema
{
   my ($dbh, $myDBName, $myTableName) = @_;
   my @Tabrow;
   my $sqlText;

   $sqlText = "SELECT ColumnName, ColumnFormat, ColumnType," .
              "       ColumnLength, DecimalTotalDigits" .
              "  FROM DBC.Columns " .
              " WHERE DataBaseName='$myDBName' AND TableName='$myTableName'" .
              " ORDER BY ColumnID";

   my $sth = $dbh->prepare($sqlText);

   unless($sth) {
      print STDERR "*** ERROR *** - Unable to prepare statement for $sqlText\n";
      return -1;
   }

   $ColumnCount = 0;
   @ColumnList = ();

   $sth->execute();
   while (@Tabrow = $sth->fetchrow()) {
      $Tabrow[0] =~ s/ //g;
      $Tabrow[0] =~ tr [a-z][A-Z];
      $Tabrow[2] =~ s/ //g;

      $ColumnList[$ColumnCount++] = [@Tabrow];
   }

   $sth->finish();

   return $ColumnCount;
}

# This function will find out the table's index column from DB
# It will store the index column at @IndexList array
# and return the count of index column
sub getTableIndex
{
   my ($dbh, $myDBName, $myTableName) = @_;
   my @Tabrow;
   my $sqlText;

   $sqlText = "SELECT ColumnName" .
              "  FROM DBC.Indices" .
              " WHERE DatabaseName='$myDBName' AND TableName='$myTableName'" .
              "   AND IndexType='P'" .
              " ORDER BY ColumnPosition";

   my $sth = $dbh->prepare($sqlText);

   unless($sth) {
      print STDERR "*** ERROR *** - Unable to prepare statement for $sqlText\n";
      return -1;
   }

   $IndexCount = 0;
   @IndexList = ();

   $sth->execute();
   while (@Tabrow = $sth->fetchrow()) {
      $Tabrow[0] =~ s/ //g;
      $Tabrow[0] =~ tr [a-z][A-Z];
      $IndexList[$IndexCount++] = $Tabrow[0];
   }

   $sth->finish();

   return $IndexCount;
}

# Check a column whether is a index column or not
# When the column is a index column, it return 1, otherwise, it return 0
sub isIndex
{
   my ($colname) = @_;
   my $flag = 0;
   my $idxCount = $#IndexList;

   if ( $idxCount == -1 ) {
      return $flag;
   }

   for (my $i=0; $i <= $idxCount; $i++) {
      if ( $colname eq $IndexList[$i] ) {
         $flag = 1;
      } 
   }

   return $flag;
}

sub getPrimaryIndexString
{
   my $primaryIndex = "";

   if ($IndexCount == 0) {
      return $primaryIndex;
   }

   for (my $i=0; $i < $IndexCount; $i++) {
      my $idxName = $IndexList[$i];

      if ( $i > 0 ) {
         $primaryIndex = $primaryIndex . "\n        AND ";
      }
      $primaryIndex = $primaryIndex . "$idxName = :$idxName";
   }

   return $primaryIndex;
}

sub createAppPath
{
    my ($table) = @_;

    $table =~ tr[a-z][A-Z];

    my $target = "${ETL_APP}/${SUBSYS}";
    my $targetJob;

    if ( "$JOBDIR" eq "" ) {
       $JOBDIR = $table;
    } else {
       if ( $NOTABLE == 1 ) { 
          $JOBDIR = $table;
       }
    }
    
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

sub paddingSpace
{
    my ($buf, $n) = @_;

    for (my $i = 0; $i < $n; $i++) {
       $buf = $buf . " ";
    }

    $buf = $buf . "'";

    return $buf;
}

sub outputHeaderInPerl
{
   my ($utility, $table, $scriptfile) = @_;
   my $current = getCurrentDateTime();

   $table =~ tr [a-z][A-Z];

   if ( $OS ne "mswin32" ) {
      print OUTFILE "#!/usr/bin/perl\n";
   }
   print OUTFILE "######################################################################\n";

   if ( $utility eq "fastload" ) {
      print OUTFILE "# Fastload script in Perl, generate by Script Wizard\n";
   }
   elsif ( $utility eq "multiload" ) {
      print OUTFILE "# Multiload script in Perl, generate by Script Wizard\n";
   }
   elsif ( $utility eq "bteq" ) {
      print OUTFILE "# BTEQ script in Perl, generate by Script Wizard\n";
   }
   
   print OUTFILE "# Date Time    : $current\n";
   print OUTFILE "# Target Table : $DBNAME.$table\n";
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
   print OUTFILE "my \$AUTO_DATA = \"\${AUTO_HOME}${DIRD}DATA\";\n";
   print OUTFILE "my \$AUTO_LOG  = \"\${AUTO_HOME}${DIRD}LOG\";\n";
   print OUTFILE "my \$LOGDIR  = \"\";\n";
#   print OUTFILE "my \$LOGFILE = \"\";\n";

   if ( $utility eq "fastload" ) {
      print OUTFILE "my \$TARGETDB = \$ENV{\"AUTO_TEMPDB\"};\n";
      print OUTFILE "my \$TEMPDB = \"\";\n";
   }
   elsif ( $utility eq "multiload" ) {
      print OUTFILE "my \$TARGETDB = \$ENV{\"AUTO_DATADB\"};\n";
      print OUTFILE "my \$TEMPDB = \"\";\n";
   }
   elsif ( $utility eq "bteq" ) {
      print OUTFILE "my \$TARGETDB = \$ENV{\"AUTO_DATADB\"};\n";
      print OUTFILE "my \$TEMPDB = \$ENV{\"AUTO_TEMPDB\"};\n";
   }
   
   print OUTFILE "my \$LOGDB = \$ENV{\"AUTO_LOGDB\"};\n\n";

   print OUTFILE "my \$DDL = \"\${AUTO_HOME}${DIRD}APP${DIRD}${SUBSYS}${DIRD}${JOBDIR}${DIRD}ddl\";\n\n";
   print OUTFILE "my \$PDDL = \$ENV{\"AUTO_TDDL\"};\n\n";
   
   print OUTFILE "my \$LOGON_STR;\n";
   print OUTFILE "my \$LOGON_FILE = \"\${AUTO_HOME}${DIRD}etc${DIRD}LOGON_${SUBSYS}\";\n";
   print OUTFILE "my \$CONTROL_FILE;\n\n";

   print OUTFILE "my \$SCRIPT = \"${script}\";\n\n";
}


sub outputHeaderInShell
{
   my ($utility, $table, $scriptfile) = @_;
   my $current = getCurrentDateTime();

   $table =~ tr [a-z][A-Z];

   print OUTFILE "#!/usr/bin/sh\n";
   print OUTFILE "######################################################################\n";

   if ( $utility eq "fastload" ) {
      print OUTFILE "# Fastload script in Shell, generate by Script Wizard\n";
   }
   elsif ( $utility eq "multiload" ) {
      print OUTFILE "# Multi script in Shell, generate by Script Wizard\n";
   }
   elsif ( $utility eq "bteq" ) {
      print OUTFILE "# BTEQ script in Shell, generate by Script Wizard\n";
   }   	   	

   print OUTFILE "# Date Time    : $current\n";
   print OUTFILE "# Target Table : $DBNAME.$table\n";
   print OUTFILE "# Script File  : $scriptfile\n";
   print OUTFILE "######################################################################\n\n";
}

sub outputVariableInShell
{
   my ($utility, $table, $script) = @_;
   
   print OUTFILE "######################################################################\n";
   print OUTFILE "# Variable Section\n";
   print OUTFILE "set -a\n";
   print OUTFILE "PATH=\$PATH:/bin:/usr/bin\n\n";
   
   print OUTFILE "ETL_HOME=\"\${AUTO_HOME}\";\n";
   print OUTFILE "ETL_DATA=\"\${ETL_HOME}/DATA\";\n";
   print OUTFILE "ETL_LOG=\"\${ETL_HOME}/LOG\";\n"; 
   print OUTFILE "LOGDIR=\"\";\n";
#   print OUTFILE "LOGFILE=\"\";\n\n";

   if ( $utility eq "fastload" ) {
      print OUTFILE "TARGETDB=\"\${AUTO_TEMPDB}\";\n";
      print OUTFILE "LOGDB=\"\${AUTO_LOGDB}\";\n";
   }
   elsif ( $utility eq "multiload" ) {
      print OUTFILE "TARGETDB=\"\${AUTO_DATADB}\";\n";
      print OUTFILE "LOGDB=\"\${AUTO_LOGDB}\";\n";
   }
   elsif ( $utility eq "bteq" ) {
      print OUTFILE "TARGETDB=\"\${AUTO_DATADB}\";\n";
      print OUTFILE "TEMPDB=\"\${AUTO_TEMPDB}\";\n";
      print OUTFILE "LOGDB=\"\${AUTO_LOGDB}\";\n";
   }

   print OUTFILE "DDL=\"\${ETL_HOME}/APP/${SUBSYS}/${JOBDIR}/ddl\";\n\n";

   print OUTFILE "LOGON_STR=\"\";\n";
   print OUTFILE "LOGON_FILE=\"\${ETL_HOME}/etc/LOGON_${SUBSYS}\";\n\n";
   
   print OUTFILE "SCRIPT=\"$script\";\n\n";
}

sub outputFastLoadScript
{
   my ($outputType) = @_;
   
   my $tableName = ${TABLE};
   my $tableE1 = "${TABLE}_E1";
   my $tableE2 = "${TABLE}_E2";

   print OUTFILE "/* Add your fastload options here */\n";

   if ( $outputType eq "perl" ) {
      print OUTFILE "\${LOGON_STR}\n";
      print OUTFILE "DATABASE \${TARGETDB};\n\n";
#modify by zxg PBC 2004 08 19
#	print OUTFILE "drop table  \${TARGETDB}.${tableName};\n";
#	print OUTFILE "drop table  \${LOGDB}.${tableE1};\n";
#	print OUTFILE "drop table   \${LOGDB}.${tableE2};\n\n";
#	print OUTFILE "CREATE MULTISET TABLE \${TARGETDB}.${tableName} AS \$PDDL.${tableName} WITH NO DATA;\n\n";

      print OUTFILE "BEGIN LOADING     \${TARGETDB}.${tableName}\n";
      print OUTFILE "      ERRORFILES  \${LOGDB}.${tableE1},\n";
      print OUTFILE "                  \${LOGDB}.${tableE2};\n\n";   	
   }
   elsif ( $outputType eq "shell" ) {
      print OUTFILE "\${LOGON_STR}\n";
      print OUTFILE "DATABASE \${TARGETDB};\n\n";

      print OUTFILE "BEGIN LOADING     \${TARGETDB}.${tableName}\n";
      print OUTFILE "      ERRORFILES  \${LOGDB}.${tableE1},\n";
      print OUTFILE "                  \${LOGDB}.${tableE2};\n\n";
   }
   elsif ( $outputType eq "utility" ) {
      print OUTFILE "LOGON $USER,$PASSWD;\n";
      print OUTFILE "DATABASE ${DBNAME};\n\n";

      print OUTFILE "BEGIN LOADING       ${DBNAME}.${tableName}\n";
      print OUTFILE "      ERRORFILES    ${LOGDBNAME}.${tableE1},\n";
      print OUTFILE "                    ${LOGDBNAME}.${tableE2};\n";
   }


   if ( $DFORMAT eq "vartext" ) {
      print OUTFILE "SET RECORD VARTEXT \"$DELI\" DISPLAY_ERRORS NOSTOP;\n";
   }
   elsif ( $DFORMAT eq "unformatted") {
      print OUTFILE "SET RECORD UNFORMATTED;\n";
   }

   print OUTFILE "DEFINE\n";

   my $colCount = $#ColumnList;
   my ($leading, $buf);
   my $colLen;

   for (my $i=0; $i <= $colCount; $i++) {
      my $colName   = $ColumnList[$i][0];
      my $colFormat = $ColumnList[$i][1];
      my $colType   = $ColumnList[$i][2];
      my $colLen    = $ColumnList[$i][3];
      my $decDigit  = $ColumnList[$i][4];
      
      $colFormat = cutTrailSpace($colFormat);
      
      if ( $i == 0 ) {
         $leading = " ";
      } else {
         $leading = ",";
      }

      if ($colType eq "I ") { $colLen = 10; }
      elsif ($colType eq "I1") { $colLen = 3; }
      elsif ($colType eq "I2") { $colLen = 5; }
      elsif ($colType eq "DA") { $colLen = 8; }
      elsif ($colType eq "D") {
      	 if ( index($colFormat, ".") == length($colFormat) - 1 ) {
      	    $colLen = $decDigit;
      	 } else {
      	    $colLen = $decDigit + 1;
      	 }
      }
      
      if ( $DFORMAT eq "vartext" ) {
         $buf = sprintf("      %s%-30s (VARCHAR(%.3d) ",
                        $leading, $colName, $colLen);
         print OUTFILE $buf, ")\n";

      }
      elsif ( $DFORMAT eq "unformatted" ){
         $buf = sprintf("      %s%-30s (CHAR(%.3d)",
                        $leading, $colName, $colLen);
         #$buf = paddingSpace($buf, $colLen);

         print OUTFILE $buf, ")\n";
      }

   }

   if ( $NEWLINE eq "yes" && $DFORMAT eq "unformatted" ) {
      if ( $OS eq "svr4" ) {  # in unix, the newline is one char
         print OUTFILE "      ,NEWLINE (CHAR(1))\n";
      }
      elsif ( $OS eq "mswin32" ) { # in nt, the newline is two chars
         print OUTFILE "      ,NEWLINE (CHAR(2))\n";
      } else {
         print OUTFILE "      ,NEWLINE (CHAR(1))\n";
        }
    }
   }


   if ( $outputType eq "perl" ) {
      print OUTFILE "   FILE=\${AUTO_DATA}${DIRD}process${DIRD}\${datafile};\n\n";
      print OUTFILE "RECORD $RECORD;\n\n";
      print OUTFILE "INSERT INTO \${TARGETDB}.${tableName} (";
   }
   elsif ( $outputType eq "shell" ) {
      print OUTFILE "   FILE=\${ETL_DATA}/process/\${DATA_FILE};\n\n";
      print OUTFILE "RECORD $RECORD;\n\n";
      print OUTFILE "INSERT INTO \${TARGETDB}.${tableName} (";
   }
   elsif ( $outputType eq "utility" ) {
      print OUTFILE "   FILE=${tableName}.dat;\n\n";

      print OUTFILE "RECORD $RECORD;\n\n";
      print OUTFILE "INSERT INTO ${DBNAME}.${tableName} (";
   }

   for (my $i=0; $i <= $colCount; $i++) {
      my $colName   = $ColumnList[$i][0];
      my $colFormat = $ColumnList[$i][1];
      my $colType   = $ColumnList[$i][2];
      my $colLen    = $ColumnList[$i][3];

      if ( $i == 0 ) {
         print OUTFILE "\n            $colName";
      }
      else {
         print OUTFILE "\n          , $colName";
      }
   }

   print OUTFILE ")\n";
   print OUTFILE "       VALUES (";

   my $trailer;

   for (my $i=0; $i <= $colCount; $i++) {
      my $colName   = $ColumnList[$i][0];
      my $colFormat = $ColumnList[$i][1];
      my $colType   = $ColumnList[$i][2];
      my $colLen    = $ColumnList[$i][3];

      if ($colType eq "DA") { 
         $trailer = "        ";
      }
      else {
         $trailer = "";
      }    

      if ( $i == 0 ) {
         print OUTFILE "\n          :$colName", $trailer;
      }
      else {
         print OUTFILE "\n         ,:$colName", $trailer;
      }
   }

   print OUTFILE ");\n\n";

   if ( $outputType eq "perl" ) {
      print OUTFILE "\$END_LOADING_STR\n\n";   	
   }
   elsif ( $outputType eq "shell" ) {
      print OUTFILE "\$END_LOADING_STR\n\n";
   }
   elsif ( $outputType eq "utility" ) {
      print OUTFILE "END LOADING;\n\n";
   }

   print OUTFILE "LOGOFF;\n\n";
}

sub outputMultiLoadScript
{
   my ($outputType) = @_;
   
   my $tableName = ${TABLE};
   my $tableWT = "WT_${TABLE}";
   my $tableET = "ET_${TABLE}";
   my $tableUV = "UV_${TABLE}";

   print OUTFILE "/* Add your multiload options here */\n\n";

   if ( $outputType eq "perl" ) {
      print OUTFILE ".LOGTABLE \${LOGDB}.${tableName}_mlog;\n";
      print OUTFILE "\${LOGON_STR}\n\n";

      print OUTFILE ".BEGIN IMPORT MLOAD TABLES \${TARGETDB}.${tableName}\n";
      print OUTFILE "       WORKTABLES \${LOGDB}.${tableWT}\n";
      print OUTFILE "       ERRORTABLES \${LOGDB}.${tableET}\n";
      print OUTFILE "                   \${LOGDB}.${tableUV};\n\n";
   }
   elsif ( $outputType eq "shell" ) {
      print OUTFILE ".LOGTABLE \${LOGDB}.${tableName}_mlog;\n";
      print OUTFILE "\${LOGON_STR}\n\n";

      print OUTFILE ".BEGIN IMPORT MLOAD TABLES \${TARGETDB}.${tableName}\n";
      print OUTFILE "       WORKTABLES \${LOGDB}.${tableWT}\n";
      print OUTFILE "       ERRORTABLES \${LOGDB}.${tableET}\n";
      print OUTFILE "                   \${LOGDB}.${tableUV};\n\n";
   }
   elsif ( $outputType eq "utility" ) {
      print OUTFILE ".LOGTABLE ${LOGDBNAME}.${tableName}_mlog;\n\n";

      print OUTFILE ".LOGON $USER,$PASSWD;\n\n";

      print OUTFILE ".BEGIN IMPORT MLOAD TABLES ${DBNAME}.${tableName}\n";
      print OUTFILE "       WORKTABLES ${LOGDBNAME}.${tableWT}\n";
      print OUTFILE "       ERRORTABLES ${LOGDBNAME}.${tableET}\n";
      print OUTFILE "                   ${LOGDBNAME}.${tableUV};\n\n";
   }
   
   print OUTFILE ".LAYOUT ${tableName}_LAYOUT;\n";

   my $colCount = $#ColumnList;
   my $idxCount = $#IndexList;
   my ($leading, $buf);
   my $colLen;

   for (my $i=0; $i <= $colCount; $i++) {
      my $colName   = $ColumnList[$i][0];
      my $colFormat = $ColumnList[$i][1];
      my $colType   = $ColumnList[$i][2];
      my $colLen    = $ColumnList[$i][3];
      my $decDigit  = $ColumnList[$i][4];

      if ( $i == 0 ) {
         $leading = "    ";
      } else {
         $leading = "    ";
      }

      if ($colType eq "I ") { $colLen = 10; }
      elsif ($colType eq "I1") { $colLen = 3; }
      elsif ($colType eq "I2") { $colLen = 5; }
      elsif ($colType eq "DA") { $colLen = 10; }
      elsif ($colType eq "D") {
      	 if ( index($colFormat, ".") == length($colFormat) - 1 ) {
      	    $colLen = $decDigit;
      	 } else {
      	    $colLen = $decDigit + 1;
      	 }
      }

      if ( $DFORMAT eq "vartext" ) {
         $buf = sprintf("%s.FIELD  %-30s * VARCHAR(%.3d) NULLIF %s = ''",
                         $leading, $colName, $colLen, $colName);

         print OUTFILE $buf, ";\n";
      }
      elsif ( $DFORMAT eq "unformatted" ){
         $buf = sprintf("%s.FIELD  %-30s * CHAR(%.3d) NULLIF %s = ''",
                        $leading, $colName, $colLen, $colName);

         #$buf = paddingSpace($buf, $colLen);
         
         print OUTFILE $buf, ";\n";
      }
   }

   if ( $NEWLINE eq "yes" && $DFORMAT eq "unformatted" ) {
      if ( $OS eq "svr4" ) {  # in unix, the newline is one char
         print OUTFILE "    .FIELD  NEWLINE * CHAR(1);\n";
      }
      elsif ( $OS eq "mswin32" ) { # in nt, the newline is two chars
         print OUTFILE "    .FIELD  NEWLINE * CHAR(2);\n";
      }
   }

   print OUTFILE "\n";
   print OUTFILE ".DML LABEL ${tableName}\n";
   print OUTFILE "   DO INSERT FOR MISSING UPDATE ROWS;\n\n";

   if ( $outputType eq "perl" ) {
      print OUTFILE "   UPDATE \${TARGETDB}.${tableName}\n";
   }
   elsif ( $outputType eq "shell" ) {
      print OUTFILE "   UPDATE \${TARGETDB}.${tableName}\n";
   }
   elsif ( $outputType eq "utility" ) {
      print OUTFILE "   UPDATE ${DBNAME}.${tableName}\n";
   }

   print OUTFILE "         SET\n";

   my $n = 0;
   for (my $i=0; $i <= $colCount; $i++) {
      my $colName   = $ColumnList[$i][0];

      if ( isIndex($colName) == 1) {
         next;
      }

      if ( $n == 0 ) {
         print OUTFILE "         $colName = :$colName\n";
         $n = 1;
      }
      else {
         print OUTFILE "        ,$colName = :$colName\n";
      }
   }
   
   my $primaryIndex = getPrimaryIndexString();

   print OUTFILE "      WHERE $primaryIndex;\n\n";

   if ( $outputType eq "perl" ) {
      print OUTFILE "   INSERT INTO \${TARGETDB}.${tableName} (\n";
   }
   elsif ( $outputType eq "shell" ) {
      print OUTFILE "   INSERT INTO \${TARGETDB}.${tableName} (\n";
   }
   elsif ( $outputType eq "utility" ) {
      print OUTFILE "   INSERT INTO ${DBNAME}.${tableName} (\n";
   }

  $n = 0;
   for (my $i=0; $i <= $colCount; $i++) {
      my $colName   = $ColumnList[$i][0];

      if ( $n == 0 ) {
         print OUTFILE "          $colName\n";
         $n = 1;
      }
      else {
         print OUTFILE "         ,$colName\n";
      }
   }

   print OUTFILE "      )\n      VALUES ( \n";
   $n = 0;
   for (my $i=0; $i <= $colCount; $i++) {
      my $colName   = $ColumnList[$i][0];
      my $colType   = $ColumnList[$i][2];

      my $trailer = "";

      if ($colType eq "DA") {
            $trailer = "           ";
      }

      if ( $n == 0 ) {
         print OUTFILE "         :$colName",$trailer,"\n";
         $n = 1;
      }
      else {
         print OUTFILE "        ,:$colName",$trailer,"\n";
      }
   }

   print OUTFILE "   );\n\n";

   if ( $outputType eq "perl" ) {
      print OUTFILE ".IMPORT INFILE \${AUTO_DATA}${DIRD}process${DIRD}\${datafile}\n";
   }
   elsif ( $outputType eq "shell" ) {
      print OUTFILE ".IMPORT INFILE \${ETL_DATA}${DIRD}process${DIRD}\${DATA_FILE}\n";
   }
   elsif ( $outputType eq "utility" ) {
      print OUTFILE ".IMPORT INFILE ${tableName}.dat\n";
   }

   print OUTFILE "    LAYOUT ${tableName}_LAYOUT\n";
   print OUTFILE "    APPLY ${tableName}\n";

   if ( $DFORMAT eq "vartext" ) {
      print OUTFILE "    FORMAT VARTEXT \'$DELI\' DISPLAY ERRORS NOSTOP;\n";
   }
   elsif ( $DFORMAT eq "unformatted" ) {
      print OUTFILE "    FORMAT UNFORMAT;\n\n";
   }

   print OUTFILE ".END MLOAD;\n\n";
   print OUTFILE ".LOGOFF;\n";
}

###############################################################################
###############################################################################
###############################################################################
sub outputFastLoadHeaderInPerl
{
   my ($table, $scriptfile) = @_;

   outputHeaderInPerl("fastload", $table, $scriptfile);
}

sub outputFastLoadVariableInPerl
{
   my ($table, $script) = @_;
   
   $table =~ tr [a-z][A-Z];
      
   outputVariableInPerl("fastload", $table, $script);   
}

sub outputFastLoadFuncInPerl
{
   my $tableName = ${TABLE};
   my $tableE1 = "${TABLE}_E1";
   my $tableE2 = "${TABLE}_E2";

   my $fastload;

   if ( $OS eq "mswin32" ) {
      $fastload = "fastload.exe";
   }
   elsif ( $OS eq "svr4" ) {
      $fastload = "fastload";
   }

   print OUTFILE "######################################################################\n";
   print OUTFILE "# Fastload function\n";
   print OUTFILE "sub run_fastload_command\n";
   print OUTFILE "{\n";
   print OUTFILE "   my (\$datafile, \$endoffile) = \@\_;\n\n";

   print OUTFILE "   my \$END_LOADING_STR;\n\n";

   print OUTFILE "   if ( \$endoffile == 1 ) {\n";
   print OUTFILE "      \$END_LOADING_STR = \"END LOADING;\";\n";
   print OUTFILE "   } else {\n";
   print OUTFILE "      \$END_LOADING_STR = \"\";\n";
   print OUTFILE "   }\n\n";

   print OUTFILE "   # Try to invoke fastload utility through pipe\n";
   print OUTFILE "   my \$rc = open(FASTLOAD, \"| $fastload\");\n\n";

   print OUTFILE "   # To see if fastload utility invoke ok?\n";
   print OUTFILE "   unless (\$rc) {\n";
   print OUTFILE "      print \"Could not invoke fastload utility\\n\";\n";
   print OUTFILE "      return -1;\n";
   print OUTFILE "   }\n\n";

   print OUTFILE "   ### Below are the fastload scripts ###\n";
   print OUTFILE "   print FASTLOAD <<ENDOFINPUT;\n\n";

   outputFastLoadScript("perl");

   print OUTFILE "ENDOFINPUT\n\n";
   print OUTFILE "   ### End of fastload script ###\n";
   print OUTFILE "   close(FASTLOAD);\n\n";

   print OUTFILE "   my \$RET_CODE = \$? >> 8;\n\n";

   print OUTFILE "   return \$RET_CODE;\n";
   print OUTFILE "}\n\n";
}

sub outputFastLoadMainInPerl
{
   print OUTFILE "######################################################################\n";
   print OUTFILE "# main function\n";
   print OUTFILE "sub main\n";
   print OUTFILE "{\n";
   print OUTFILE "   my \$ret;\n";
   print OUTFILE "   # Open control file\n";
   print OUTFILE "   open(CTRLFILE_H, \"\${AUTO_DATA}${DIRD}process${DIRD}\${CONTROL_FILE}\");\n\n";

   print OUTFILE "   # Get all data files\n";
   print OUTFILE "   my \@fileList = <CTRLFILE_H>;\n\n";

   print OUTFILE "   # Close control file\n";
   print OUTFILE "   close(CTRLFILE_H);\n\n";

   print OUTFILE "   open(LOGONFILE_H, \"\${LOGON_FILE}\");\n";
   print OUTFILE "   \$LOGON_STR = <LOGONFILE_H>;\n";
   print OUTFILE "   close(LOGONFILE_H);\n\n";

   print OUTFILE "   # Get the decoded logon string\n";
   print OUTFILE "   \$LOGON_STR = `\${AUTO_HOME}${DIRD}bin${DIRD}IceCode.exe \"\$LOGON_STR\"`;\n";

   print OUTFILE "   my (\$sec,\$min,\$hour,\$mday,\$mon,\$year,\$wday,\$yday,\$isdst) = localtime(time());\n";
   print OUTFILE "   \$year += 1900;\n";
   print OUTFILE "   \$mon = sprintf(\"%02d\", \$mon + 1);\n";
   print OUTFILE "   \$mday = sprintf(\"%02d\", \$mday);\n";
   print OUTFILE "   my \$today = \"\${year}\${mon}\${mday}\";\n\n";
   
#  print OUTFILE "   \$LOGDIR = \"\${AUTO_LOG}${DIRD}\${today}\";\n";
#  print OUTFILE "   \$LOGFILE = \"\${LOGDIR}${DIRD}\${SCRIPT}.log\";\n\n";

#  print OUTFILE "   # If the log directory did not exist, create it!\n";
#  print OUTFILE "   if ( ! -d \$LOGDIR ) {\n";
#  print OUTFILE "      mkdir(\$LOGDIR, 0777);\n";
#  print OUTFILE "   }\n";
#  print OUTFILE "   # Clean up the log file first\n";
#  print OUTFILE "   unlink(\$LOGFILE);\n\n";

#  print OUTFILE "   # redirect output to log file\n";
#  print OUTFILE "   open(STDOUT, \">\$LOGFILE\");\n";
#  print OUTFILE "   open(STDERR, \">&STDOUT\");\n\n";

   print OUTFILE "   my \$endoffile = 0;\n";
   print OUTFILE "   my \$totalfile = \$#fileList;\n";
   print OUTFILE "   my \@fields;\n";
   print OUTFILE "   my \$datafile;\n\n";

   print OUTFILE "   # To process all of data files in control file\n";
   print OUTFILE "   for (my \$i=0; \$i <= \$totalfile; \$i++) {\n";
   print OUTFILE "      if (\$i == \$totalfile) {\n";
   print OUTFILE "         \$endoffile = 1;\n";
   print OUTFILE "      }\n\n";

   print OUTFILE "      \$datafile = \$fileList[\$i];\n";
   print OUTFILE "      chomp(\$datafile);\n";
   print OUTFILE "      \@fields = split(/\\s+/, \$datafile);\n\n";
   
   print OUTFILE "      # Call fastload command to load data\n";
   print OUTFILE "      \$ret = run_fastload_command(\$fields[0], \$endoffile);\n\n";

   print OUTFILE "      if ( \$ret == 8 || \$ret == 12 ) {\n";
   print OUTFILE "         last;\n";
   print OUTFILE "      }\n";
   print OUTFILE "   }\n\n";

   print OUTFILE "   return \$ret;\n";
   print OUTFILE "}\n\n";
}

sub outputFastLoadProgramInPerl
{
   print OUTFILE "######################################################################\n";
   print OUTFILE "# program section\n\n";

   print OUTFILE "# To see if there is one parameter,\n";
   print OUTFILE "# if there is no parameter, exit program\n";
   print OUTFILE "if ( \$#ARGV < 0 ) {\n";
   print OUTFILE "   exit(1);\n";
   print OUTFILE "}\n\n";

   print OUTFILE "# Get the first argument as control file\n";
   print OUTFILE "\$CONTROL_FILE = \$ARGV[0];\n\n";

   print OUTFILE "open(STDERR, \">&STDOUT\");\n\n";
   
   print OUTFILE "my \$ret = main();\n\n";

   print OUTFILE "exit(\$ret);\n\n";
}

###############################################################################
###############################################################################
###############################################################################
sub outputFastLoadHeaderInShell
{
   my ($table, $scriptfile) = @_;

   outputHeaderInShell("fastload", $table, $scriptfile);   
}

sub outputFastLoadVariableInShell
{
   my ($table, $script) = @_;

   $table =~ tr [a-z][A-Z];
   
   outputVariableInShell("fastload", $table, $script);
}

sub outputFastLoadFuncInShell
{
   print OUTFILE "######################################################################\n";
   print OUTFILE "# Fastload function\n";
   print OUTFILE "run_fastload_command()\n";
   print OUTFILE "{\n";
   print OUTFILE "   NEED_END_LOADING=\$1;\n";
   print OUTFILE "   DATA_FILE=\$2;\n\n";

   print OUTFILE "   if [ \$NEED_END_LOADING = \"Y\" ]\n";
   print OUTFILE "   then\n";
   print OUTFILE "      END_LOADING_STR=\"END LOADING;\";\n";
   print OUTFILE "   else\n";
   print OUTFILE "      END_LOADING_STR=\"\";\n";
   print OUTFILE "   fi\n\n";

   print OUTFILE "   LOGON_STR=`cat \${LOGON_FILE}`;\n\n";

   print OUTFILE "   cat \<\<ENDOFINPUT \| /usr/bin/fastload\n";

   outputFastLoadScript("shell");
   
   print OUTFILE "ENDOFINPUT\n\n";
   print OUTFILE "   ### End of fastload script ###\n\n";
   
   print OUTFILE "   RET_CODE=\${?};\n\n";
   
   print OUTFILE "   return \$RET_CODE;\n";
   print OUTFILE "}\n\n";
}

sub outputFastLoadProgramInShell
{
   print OUTFILE "######################################################################\n";
   print OUTFILE "# program section\n\n";

   print OUTFILE "set -a\n";
   print OUTFILE "PATH=\$PATH:/bin:/usr/bin\n\n";

   print OUTFILE "if [ \$# -lt 1 ];\n";
   print OUTFILE "then\n";
   print OUTFILE "   echo \"Usage: \$0 DataFiles\";\n";
   print OUTFILE "   exit 1;\n";
   print OUTFILE "fi\n\n";

#   print OUTFILE "LOGDIR=\"\${ETL_LOG}/`date +%Y%m%d`\";\n\n";

#   print OUTFILE "if [ ! -d \${LOGDIR} ]\n";
#   print OUTFILE "then\n";
#   print OUTFILE "   mkdir -p \${LOGDIR};\n";
#   print OUTFILE "   chmod 774 \${LOGDIR};\n";
#   print OUTFILE "fi\n\n";
 
#   print OUTFILE "LOGFILE=\${LOGDIR}/\${SCRIPT}.log;\n\n";

#   print OUTFILE "exec > \${LOGFILE};\n";
#   print OUTFILE "exec 2>> \${LOGFILE};\n\n";

   print OUTFILE "ArgC=\$#;\n";
   print OUTFILE "ArgP=1;\n\n";

   print OUTFILE "while [ \$ArgC -gt \$ArgP ]\n";
   print OUTFILE "do\n";
   print OUTFILE "  run_fastload_command \"N\" \$1;\n";
   print OUTFILE "  RET_CODE=\$?;\n\n";

   print OUTFILE "  if [ \$RET_CODE = 8 ]\n";
   print OUTFILE "  then\n";
   print OUTFILE "     exit \$RET_CODE;\n";
   print OUTFILE "  fi\n\n";
   
   print OUTFILE "  if [ \$RET_CODE = 12 ]\n";
   print OUTFILE "  then\n";
   print OUTFILE "     exit \$RET_CODE;\n";
   print OUTFILE "  fi\n\n";
   
   print OUTFILE "  ArgP=\`expr \$ArgP + 1\`;\n";
   print OUTFILE "  shift;\n";
   print OUTFILE "done\n\n";

   print OUTFILE "run_fastload_command \"Y\" \$1;\n";

   print OUTFILE "RET_CODE=\$?;\n\n";

   print OUTFILE "exit \$RET_CODE;\n\n";
}

###############################################################################
###############################################################################
###############################################################################
sub outputFastLoadHeaderInUtility
{
   my ($table) = @_;
   my $current = getCurrentDateTime();

   $table =~ tr [a-z][A-Z];

   print OUTFILE "/*******************************************************************/\n";
   print OUTFILE "/* Fastload script in Shell, generate by Script Wizard             */\n";
   print OUTFILE "/* Date Time    : $current */\n";           
   print OUTFILE "/* Target Table : $DBNAME.$table */\n";
   print OUTFILE "/*******************************************************************/\n";
}

sub outputFastLoadFuncInUtility
{
   outputFastLoadScript("utility");
}


###############################################################################
###############################################################################
###############################################################################
sub outputMultiLoadHeaderInPerl
{
   my ($table, $scriptfile) = @_;

   outputHeaderInPerl("multiload", $table, $scriptfile);
}

sub outputMultiLoadVariableInPerl
{
   my ($table, $script) = @_;

   $table =~ tr [a-z][A-Z];
   
   outputVariableInPerl("multiload", $table, $script);
}

sub outputMultiLoadFuncInPerl
{
   my $tableName = ${TABLE};
   my $tableWT = "WT_${TABLE}";
   my $tableET = "ET_${TABLE}";
   my $tableUV = "UV_${TABLE}";

   my $multiload;

   if ( $OS eq "svr4" ) {
      $multiload = "mload";
   }
   elsif ( $OS eq "mswin32" ) {
      $multiload = "mload.exe";
   }

   print OUTFILE "##################################################################\n";
   print OUTFILE "# MultiLoad function\n";
   print OUTFILE "sub run_multiload_command\n";
   print OUTFILE "{\n";
   print OUTFILE "   my (\$datafile) = \@\_;\n\n";

   print OUTFILE "   # Try to invoke multiload utility\n";
   print OUTFILE "   my \$rc = open(MULTILOAD, \"| $multiload\");\n\n";

   print OUTFILE "   # To see if multiload utility invoke ok?\n";
   print OUTFILE "   unless (\$rc) {\n";
   print OUTFILE "      print \"Could not invoke multiload utility\\n\";\n";
   print OUTFILE "      return -1;\n";
   print OUTFILE "   }\n\n";

   print OUTFILE "   ### Below are multiload scripts ###\n";
   print OUTFILE "   print MULTILOAD <<ENDOFINPUT;\n";
   print OUTFILE "\n";
   
   outputMultiLoadScript("perl");
   
   print OUTFILE "ENDOFINPUT\n\n";
   
   print OUTFILE "   ### End of multiload scripts\n";
   print OUTFILE "   close(MULTILOAD);\n\n";

   print OUTFILE "   my \$RET_CODE = \$? >> 8;\n\n";

   print OUTFILE "   return \$RET_CODE;\n";
   print OUTFILE "}\n\n";
}

sub outputMultiLoadMainInPerl
{
   print OUTFILE "######################################################################\n";
   print OUTFILE "# main function\n";
   print OUTFILE "sub main\n";
   print OUTFILE "{\n";
   print OUTFILE "   my \$ret;\n";
   print OUTFILE "   # Open control file\n";
   print OUTFILE "   open(CTRLFILE_H, \"\${AUTO_DATA}${DIRD}process${DIRD}\${CONTROL_FILE}\");\n\n";

   print OUTFILE "   # Get all data files\n";
   print OUTFILE "   my \@fileList = <CTRLFILE_H>;\n\n";

   print OUTFILE "   # Close control file\n";
   print OUTFILE "   close(CTRLFILE_H);\n\n";

   print OUTFILE "   open(LOGONFILE_H, \"\${LOGON_FILE}\");\n";
   print OUTFILE "   \$LOGON_STR = <LOGONFILE_H>;\n";
   print OUTFILE "   close(LOGONFILE_H);\n\n";

   print OUTFILE "   # Get the decoded logon string\n";
   print OUTFILE "   \$LOGON_STR = `\${AUTO_HOME}${DIRD}bin${DIRD}IceCode.exe \"\$LOGON_STR\"`;\n";

   print OUTFILE "   my (\$sec,\$min,\$hour,\$mday,\$mon,\$year,\$wday,\$yday,\$isdst) = localtime(time());\n";
   print OUTFILE "   \$year += 1900;\n";
   print OUTFILE "   \$mon = sprintf(\"%02d\", \$mon + 1);\n";
   print OUTFILE "   \$mday = sprintf(\"%02d\", \$mday);\n";
   print OUTFILE "   my \$today = \"\${year}\${mon}\${mday}\";\n\n";
   
#   print OUTFILE "   \$LOGDIR = \"\${AUTO_LOG}${DIRD}\${today}\";\n";
#   print OUTFILE "   \$LOGFILE = \"\${LOGDIR}${DIRD}\${SCRIPT}.log\";\n\n";

#   print OUTFILE "   # If the log directory did not exist, create it!\n";
#   print OUTFILE "   if ( ! -d \$LOGDIR ) {\n";
#   print OUTFILE "      mkdir(\$LOGDIR, 0777);\n";
#   print OUTFILE "   }\n";
#   print OUTFILE "   # Clean up the log file first\n";
#   print OUTFILE "   unlink(\$LOGFILE);\n\n";

#   print OUTFILE "   # redirect output to log file\n";
#   print OUTFILE "   open(STDOUT, \">\$LOGFILE\");\n";
#   print OUTFILE "   open(STDERR, \">&STDOUT\");\n\n";

   print OUTFILE "   my \$totalfile = \$#fileList;\n";
   print OUTFILE "   my \@fields;\n";
   print OUTFILE "   my \$datafile;\n\n";

   print OUTFILE "   # To process all of data files in control file\n";
   print OUTFILE "   for (my \$i=0; \$i <= \$totalfile; \$i++) {\n";

   print OUTFILE "      \$datafile = \$fileList[\$i];\n";
   print OUTFILE "      chomp(\$datafile);\n";
   print OUTFILE "      \@fields = split(/\\s+/, \$datafile);\n\n";

   print OUTFILE "      # Call multiload command to load data\n";
   print OUTFILE "      \$ret = run_multiload_command(\$fields[0]);\n";
   print OUTFILE "\n";
   print OUTFILE "      if ( \$ret == 8 || \$ret == 12 ) {\n";
   print OUTFILE "         last;\n";
   print OUTFILE "      }\n";
   print OUTFILE "   }\n\n";


   print OUTFILE "   return \$ret;\n";
   print OUTFILE "}\n\n";
}

sub outputMultiLoadProgramInPerl
{
   print OUTFILE "######################################################################\n";
   print OUTFILE "# program section\n\n";

   print OUTFILE "# To see if there is one parameter,\n";
   print OUTFILE "# if there is no parameter, exit program\n";
   print OUTFILE "if ( \$#ARGV < 0 ) {\n";
   print OUTFILE "   exit(1);\n";
   print OUTFILE "}\n\n";

   print OUTFILE "# Get the first argument as control file\n";
   print OUTFILE "\$CONTROL_FILE = \$ARGV[0];\n\n";

   print OUTFILE "open(STDERR, \">&STDOUT\");\n\n";

   print OUTFILE "my \$ret = main();\n\n";

   print OUTFILE "exit(\$ret);\n";
}


###############################################################################
###############################################################################
###############################################################################
sub outputMultiLoadHeaderInShell
{
   my ($table, $scriptfile) = @_;

   outputHeaderInShell("multiload", $table, $scriptfile);   
}

sub outputMultiLoadVariableInShell
{
   my ($table, $script) = @_;

   $table =~ tr [a-z][A-Z];
   
   outputVariableInShell("multiload", $table, $script);   
}

sub outputMultiLoadFuncInShell
{
   my $tableName = ${TABLE};
   my $tableWT = "WT_${TABLE}";
   my $tableET = "ET_${TABLE}";
   my $tableUV = "UV_${TABLE}";

   my $multiload = "/usr/bin/mload"; # shell only support at Unix

   print OUTFILE "#*********************************************************************\n";
   print OUTFILE "# MultiLoad function\n";
   print OUTFILE "run_multiload_command()\n";
   print OUTFILE "{\n";
   print OUTFILE "   DATA_FILE=\${1};\n\n";

   print OUTFILE "   LOGON_STR=`cat \${LOGON_FILE}`;\n\n";

   print OUTFILE "   cat \<\<ENDOFINPUT \| $multiload\n";
   print OUTFILE "\n";

   outputMultiLoadScript("shell");

   print OUTFILE "ENDOFINPUT\n";
   print OUTFILE "\n";
   print OUTFILE "   RET_CODE=\${?};\n\n";
   print OUTFILE "   return \$RET_CODE;\n";
   print OUTFILE "}\n\n";

   return 0;
}

sub outputMultiLoadProgramInShell
{
   print OUTFILE "######################################################################\n";
   print OUTFILE "# program section\n\n";

   print OUTFILE "set -a\n";
   print OUTFILE "PATH=\$PATH:/bin:/usr/bin\n\n";

#   print OUTFILE "LOGDIR=\"\${ETL_LOG}/`date +%Y%m%d`\";\n\n";

#   print OUTFILE "if [ ! -d \${LOGDIR} ]\n";
#   print OUTFILE "then\n";
#   print OUTFILE "   mkdir -p \${LOGDIR};\n";
#   print OUTFILE "   chmod 774 \${LOGDIR};\n";
#   print OUTFILE "fi\n\n";
 
#   print OUTFILE "LOGFILE=\${LOGDIR}/\${SCRIPT}.log;\n\n";

#   print OUTFILE "exec > \${LOGFILE};\n";
#   print OUTFILE "exec 2>> \${LOGFILE};\n\n";

   print OUTFILE "run_multiload_command \${1};\n\n";

   print OUTFILE "RET_CODE=\$?;\n\n";
   
   print OUTFILE "exit \${RET_CODE};\n";
}

###############################################################################
###############################################################################
###############################################################################
sub outputMultiLoadHeaderInUtility
{
   my ($table) = @_;
   my $current = getCurrentDateTime();

   $table =~ tr [a-z][A-Z];

   print OUTFILE "/********************************************************************/\n";
   print OUTFILE "/* Multiload script, generate by Script Wizard */\n";
   print OUTFILE "/* Date Time    : $current */\n";
   print OUTFILE "/* Target Table : $DBNAME.$table */\n";
   print OUTFILE "/********************************************************************/\n";
   print OUTFILE "\n";
}

sub outputMultiLoadFuncInUtility
{
   outputMultiLoadScript("utility");
}

###############################################################################
###############################################################################
###############################################################################
sub outputBTEQHeaderInPerl
{
   my ($table, $scriptfile) = @_;
   my $current = getCurrentDateTime();

   $table =~ tr [a-z][A-Z];

   if ( $OS eq "svr4" ) {
      print OUTFILE "#!/usr/bin/perl\n";
   }
   
   print OUTFILE "######################################################################\n";
   print OUTFILE "# BTEQ script in Perl, generate by Script Wizard\n";
   print OUTFILE "# Date Time    : $current\n";
   print OUTFILE "# Target Table : $DBNAME.$table\n";
   print OUTFILE "# Script File  : $scriptfile\n";
   print OUTFILE "######################################################################\n";
   print OUTFILE "\n";
   print OUTFILE "use strict; # Declare using Perl strict syntax\n\n";
   print OUTFILE "#\n";
   print OUTFILE "# If you are using other Perl's package, declare here\n";
   print OUTFILE "#\n";
   print OUTFILE "\n";
}

sub outputBTEQVariableInPerl
{
   my ($table, $script) = @_;
   
   $table =~ tr [a-z][A-Z];

   outputVariableInPerl("bteq", $table, $script);
}

sub outputBTEQFuncInPerl
{
   my $tableName = ${TABLE};
   my $tableE1 = "${TABLE}_E1";
   my $tableE2 = "${TABLE}_E2";
   my $tableWT = "WT_${TABLE}";
   my $tableET = "ET_${TABLE}";
   my $tableUV = "UV_${TABLE}";
   
   my $bteq;

   if ( $OS eq "svr4" ) {
      $bteq = "bteq";
   }
   elsif ( $OS eq "mswin32" ) {
      $bteq = "bteq.exe";
   }

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
   print OUTFILE "   print BTEQ <<ENDOFINPUT;\n\n";
   print OUTFILE "\${LOGON_STR}\n";
   print OUTFILE "\n";

   if ( $JOB eq "bteqf" ) {
      print OUTFILE "DROP TABLE \${LOGDB}.${tableE1};\n";
      print OUTFILE "DROP TABLE \${LOGDB}.${tableE2};\n\n";

      print OUTFILE "DROP TABLE \${TEMPDB}.${tableName};\n\n";

      print OUTFILE "DATABASE \${TEMPDB};\n\n";
      
      print OUTFILE ".RUN FILE=\${DDL}${DIRD}${tableName}.sql;\n\n";
   
      print OUTFILE ".IF ERRORCODE <> 0 THEN .QUIT 12;\n";
      print OUTFILE "/* Add your BTEQ script here */\n\n";
   
      print OUTFILE ".LOGOFF;\n";
      print OUTFILE ".QUIT 0;\n\n";
   }
   elsif ( $JOB eq "bteqm" ) {
      print OUTFILE "RELEASE MLOAD \${TARGETDB}.${tableName};\n\n";

      print OUTFILE "DROP TABLE \${LOGDB}.${tableWT};\n";
      print OUTFILE "DROP TABLE \${LOGDB}.${tableET};\n";
      print OUTFILE "DROP TABLE \${LOGDB}.${tableUV};\n\n";

      print OUTFILE "/* Add your BTEQ script here */\n\n";
      
      print OUTFILE ".LOGOFF;\n";
      print OUTFILE ".QUIT;\n\n";
   }
   elsif ( $JOB eq "bteq") {
     #modify by zxg PBC 2004 08 19
	print OUTFILE "DATABASE \${TEMPDB};\n\n";
	print OUTFILE "drop table  \${TEMPDB}.${tableName};\n";
	print OUTFILE "drop table  \${LOGDB}.${tableE1};\n";
	print OUTFILE "drop table   \${LOGDB}.${tableE2};\n\n";
    print OUTFILE "CREATE MULTISET TABLE \${TEMPDB}.${tableName} AS \$PDDL.${tableName} WITH NO DATA;\n\n";

 	
      print OUTFILE ".LOGOFF;\n";
      print OUTFILE ".QUIT;\n\n";
   }
   elsif ( $JOB eq "bteqx") {
     #modify by zxg PBC 2004 08 19
	print OUTFILE "#add your script here";
		
      print OUTFILE ".LOGOFF;\n";
      print OUTFILE ".QUIT;\n\n";
   }   
   print OUTFILE "ENDOFINPUT\n\n";

   print OUTFILE "   ### End of BTEQ scripts ###\n";
   print OUTFILE "   close(BTEQ);\n\n";
   print OUTFILE "   my \$RET_CODE = \$? >> 8;\n\n";

   print OUTFILE "   # if the return code is 12, that means something error happen\n";
   print OUTFILE "   # so we return 1, otherwise, we return 0 means ok\n";
   print OUTFILE "   if ( \$RET_CODE == 12 ) {\n";
   print OUTFILE "      return 1;\n";
   print OUTFILE "   }\n";
   print OUTFILE "   else {\n";
   print OUTFILE "      return 0;\n";
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
   print OUTFILE "   \$LOGON_STR = `\${AUTO_HOME}${DIRD}bin${DIRD}IceCode.exe \"\$LOGON_STR\"`;\n";

   print OUTFILE "   my (\$sec,\$min,\$hour,\$mday,\$mon,\$year,\$wday,\$yday,\$isdst) = localtime(time());\n";
   print OUTFILE "   \$year += 1900;\n";
   print OUTFILE "   \$mon = sprintf(\"%02d\", \$mon + 1);\n";
   print OUTFILE "   \$mday = sprintf(\"%02d\", \$mday);\n";
   print OUTFILE "   my \$today = \"\${year}\${mon}\${mday}\";\n\n";
   
#   print OUTFILE "   \$LOGDIR = \"\${AUTO_LOG}${DIRD}\${today}\";\n";
#   print OUTFILE "   \$LOGFILE = \"\${LOGDIR}${DIRD}\${SCRIPT}.log\";\n\n";

#   print OUTFILE "   # if the log directory did not exist, create it!\n";
#   print OUTFILE "   if ( ! -d \$LOGDIR ) {\n";
#   print OUTFILE "      mkdir(\$LOGDIR, 0777);\n";
#   print OUTFILE "   }\n";
#   print OUTFILE "   # clean the log file first\n";
#   print OUTFILE "   unlink(\$LOGFILE);\n\n";

#   print OUTFILE "   # redirect output to log file\n";
#   print OUTFILE "   open(STDOUT, \">\$LOGFILE\");\n";
#   print OUTFILE "   open(STDERR, \">&STDOUT\");\n";
   
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
   print OUTFILE "#if ( \$#ARGV < 0 ) {\n";
   print OUTFILE "#   exit(1);\n";
   print OUTFILE "#}\n\n";
   print OUTFILE "# Get the first argument\n";
   print OUTFILE "#\$CONTROL_FILE = \$ARGV[0];\n\n";

   print OUTFILE "open(STDERR, \">&STDOUT\");\n\n";

   print OUTFILE "my \$ret = main();\n\n";

   print OUTFILE "exit(\$ret);\n";
}

###############################################################################
###############################################################################
###############################################################################
sub outputBTEQHeaderInShell
{
   my ($table, $scriptfile) = @_;
   my $current = getCurrentDateTime();

   $table =~ tr [a-z][A-Z];

   if ( $OS eq "svr4" ) {
      print OUTFILE "#!/usr/bin/sh\n";
   }
   
   print OUTFILE "######################################################################\n";
   print OUTFILE "# BTEQ script in Shell, generate by Script Wizard\n";
   print OUTFILE "# Date Time    : $current\n";
   print OUTFILE "# Target Table : $DBNAME.$table\n";
   print OUTFILE "# Script File  : $scriptfile\n";
   print OUTFILE "######################################################################\n";
   print OUTFILE "\n";
}

sub outputBTEQVariableInShell
{
   my ($table, $script) = @_;
   
   $table =~ tr [a-z][A-Z];

   outputVariableInShell("bteq", $table, $script);
}

sub outputBTEQFuncInShell
{
   my $tableName = ${TABLE};
   my $tableE1 = "${TABLE}_E1";
   my $tableE2 = "${TABLE}_E2";
   my $tableWT = "WT_${TABLE}";
   my $tableET = "ET_${TABLE}";
   my $tableUV = "UV_${TABLE}";
   
   my $bteq;

   $tableName =~ tr [A-Z][a-z];
   
   if ( $OS eq "svr4" ) {
      $bteq = "bteq";
   }
   elsif ( $OS eq "mswin32" ) {
      $bteq = "bteq.exe";
   }

   print OUTFILE "######################################################################\n";
   print OUTFILE "# BTEQ function\n";
   print OUTFILE "run_bteq_command()\n";
   print OUTFILE "{\n";
   print OUTFILE "   LOGON_STR=`cat \${LOGON_FILE}`;\n\n";

   print OUTFILE "   ### Below are BTEQ scripts ###\n";
   print OUTFILE "   cat \<\<ENDOFINPUT \| /usr/bin/bteq\n";
   print OUTFILE "\${LOGON_STR}\n";
   print OUTFILE "\n";

   if ( $JOB eq "bteqf" ) {
      print OUTFILE "DROP TABLE \${LOGDB}.${tableE1};\n";
      print OUTFILE "DROP TABLE \${LOGDB}.${tableE2};\n\n";

      print OUTFILE "DROP TABLE \${TEMPDB}.${tableName};\n\n";

      print OUTFILE "DATABASE \${TEMPDB};\n\n";
      
      print OUTFILE ".RUN FILE=\${DDL}/${tableName}.sql;\n\n";
   
      print OUTFILE "/* Add your BTEQ script here */\n\n";
   
      print OUTFILE ".LOGOFF;\n";
      print OUTFILE ".QUIT;\n\n";
   }
   elsif ( $JOB eq "bteqm" ) {
      print OUTFILE "RELEASE MLOAD \${TARGETDB}.${tableName};\n\n";

      print OUTFILE "DROP TABLE \${LOGDB}.${tableWT};\n";
      print OUTFILE "DROP TABLE \${LOGDB}.${tableET};\n";
      print OUTFILE "DROP TABLE \${LOGDB}.${tableUV};\n\n";

      print OUTFILE "/* Add your BTEQ script here */\n\n";
      
      print OUTFILE ".LOGOFF;\n";
      print OUTFILE ".QUIT;\n\n";
   }
   elsif ( $JOB eq "bteq") {
      print OUTFILE "/* Add your BTEQ script here */\n\n";
 	
      print OUTFILE ".LOGOFF;\n";
      print OUTFILE ".QUIT;\n\n";
   }
      
   print OUTFILE "ENDOFINPUT\n\n";

   print OUTFILE "   ### End of BTEQ scripts ###\n";
   print OUTFILE "\n";
   print OUTFILE "   \RET_CODE=\${?};\n\n";

   print OUTFILE "   return \$RET_CODE;\n";
   print OUTFILE "}\n\n";
}

sub outputBTEQProgramInShell
{
   print OUTFILE "######################################################################\n";
   print OUTFILE "# program section\n\n";

#   print OUTFILE "LOGDIR=\"\${ETL_LOG}/`date +%Y%m%d`\";\n\n";

#   print OUTFILE "if [ ! -d \${LOGDIR} ]\n";
#   print OUTFILE "then\n";
#   print OUTFILE "   mkdir -p \${LOGDIR};\n";
#   print OUTFILE "   chmod 774 \${LOGDIR};\n";
#   print OUTFILE "fi\n\n";
 
#   print OUTFILE "LOGFILE=\${LOGDIR}/\${SCRIPT}.log;\n\n";

#   print OUTFILE "exec > \${LOGFILE};\n";
#   print OUTFILE "exec 2>> \${LOGFILE};\n\n";

   print OUTFILE "run_bteq_command;\n\n";

   print OUTFILE "RET_CODE=\${?};\n\n";

   print OUTFILE "if [ \$RET_CODE = 12 ]\n";
   print OUTFILE "then\n";
   print OUTFILE "   echo \"return 1\";\n";
   print OUTFILE "   exit 1;\n";
   print OUTFILE "else\n";
   print OUTFILE "   echo \"return 0\";\n";
   print OUTFILE "   exit 0;\n";
   print OUTFILE "fi\n\n";   
}

###############################################################################
###############################################################################
###############################################################################
sub generateFastLoadInPerl
{
   my ($table) = @_;

   $table =~ tr[A-Z][a-z];
   
   my $outFile;
   
   if ( $NOTABLE == 1 ) {
      $outFile = "${table}0100.pl";
   } else {
      my $jobname = $JOBDIR;
      $jobname =~ tr[A-Z][a-z];
      $outFile = "${jobname}0100.pl";
   }

   if ( -f "${TARGETBIN}/${outFile}" ) {
      print STDOUT "Renaming ${outFile} to ${outFile}.bak...\n";

      rename("${TARGETBIN}/${outFile}", "${TARGETBIN}/${outFile}.bak");
   }

   unless(open(OUTFILE, ">${TARGETBIN}/${outFile}")) {
      print STDOUT "Can not open file ${TARGETBIN}/${outFile}\n";
      return -1;
   }

   print STDOUT "Generating $outFile...\n";

   outputFastLoadHeaderInPerl($table, $outFile);
   outputFastLoadVariableInPerl($table, $outFile);
   outputFastLoadFuncInPerl();
   outputFastLoadMainInPerl();
   outputFastLoadProgramInPerl();

   close(OUTFILE);

   return 0;
}

sub generateFastLoadInShell
{
   my ($table) = @_;

   $table =~ tr[A-Z][a-z];
   
   my $outFile;
   
   if ( $NOTABLE == 1 ) {
      $outFile = "${table}0100.sh";
   } else {
      my $jobname = $JOBDIR;
      $jobname =~ tr[A-Z][a-z];
      $outFile = "${jobname}0100.sh";
   }

   if ( -f "${TARGETBIN}/${outFile}" ) {
      print STDOUT "Renaming ${outFile} to ${outFile}.bak...\n";

      rename("${TARGETBIN}/${outFile}", "${TARGETBIN}/${outFile}.bak");
   }

   unless(open(OUTFILE, ">${TARGETBIN}/${outFile}")) {
      print STDOUT "Can not open file ${TARGETBIN}/${outFile}\n";
      return -1;
   }

   print STDOUT "Generating $outFile...\n";

   outputFastLoadHeaderInShell($table, $outFile);
   outputFastLoadVariableInShell($table, $outFile);
   outputFastLoadFuncInShell($outFile);
   outputFastLoadProgramInShell();

   close(OUTFILE);

   return 0;
}

sub generateFastLoadInUtility
{
   my ($table) = @_;

   $table =~ tr[A-Z][a-z];
   my $outFile = "${table}.fld";

   if ( $OUTFILE eq "" ) {
      $outFile = "${table}.fld";
   }
   else {
      $outFile = $OUTFILE;
   }

   if ( -f "${outFile}" ) {
      print STDOUT "Renaming ${outFile} to ${outFile}.bak...\n";

      rename("${outFile}", "${outFile}.bak");
   }

   unless(open(OUTFILE, ">${outFile}")) {
      print STDOUT "Can not open file ${outFile}\n";
      return -1;
   }

   print STDOUT "Generating $outFile...\n";

   outputFastLoadHeaderInUtility($table);
   outputFastLoadFuncInUtility($table);

   close(OUTFILE);

   return 0;
}

sub generateMultiLoadInPerl
{
   my ($table) = @_;

   $table =~ tr[A-Z][a-z];
   my $outFile;
   
   if ( $NOTABLE == 1 ) {
      $outFile = "${table}0100.pl";
   } else {
      my $jobname = $JOBDIR;
      $jobname =~ tr[A-Z][a-z];
      $outFile = "${jobname}0100.pl";
   }

   if ( -f "${TARGETBIN}/${outFile}" ) {
      print STDOUT "Renaming ${outFile} to ${outFile}.bak...\n";

      rename("${TARGETBIN}/${outFile}", "${TARGETBIN}/${outFile}.bak");
   }

   unless(open(OUTFILE, ">${TARGETBIN}/${outFile}")) {
      print STDOUT "Can not open file ${TARGETBIN}/${outFile}\n";
      return -1;
   }

   print STDOUT "Generating $outFile...\n";

   outputMultiLoadHeaderInPerl($table, $outFile);
   outputMultiLoadVariableInPerl($table, $outFile);
   outputMultiLoadFuncInPerl($outFile);
   outputMultiLoadMainInPerl();
   outputMultiLoadProgramInPerl();

   close(OUTFILE);

   return 0;
}

sub generateMultiLoadInShell
{
   my ($table) = @_;

   $table =~ tr[A-Z][a-z];
   my $outFile;
   
   if ( $NOTABLE == 1 ) {
      $outFile = "${table}0100.sh";
   } else {
      my $jobname = $JOBDIR;
      $jobname =~ tr[A-Z][a-z];
      $outFile = "${jobname}0100.sh";
   }

   if ( -f "${TARGETBIN}/${outFile}" ) {
      print STDOUT "Renaming ${outFile} to ${outFile}.bak...\n";

      rename("${TARGETBIN}/${outFile}", "${TARGETBIN}/${outFile}.bak");
   }

   unless(open(OUTFILE, ">${TARGETBIN}/${outFile}")) {
      print STDOUT "Can not open file ${TARGETBIN}/${outFile}\n";
      return -1;
   }

   print STDOUT "Generating $outFile...\n";

   outputMultiLoadHeaderInShell($table, $outFile);
   outputMultiLoadVariableInShell($table, $outFile);
   outputMultiLoadFuncInShell($outFile);
   outputMultiLoadProgramInShell();

   close(OUTFILE);

   return 0;
}

sub generateMultiLoadInUtility
{
   my ($table) = @_;

   $table =~ tr[A-Z][a-z];
   my $outFile = "${table}.fld";

   if ( $OUTFILE eq "" ) {
      $outFile = "${table}.mld";
   }
   else {
      $outFile = $OUTFILE;
   }

   if ( -f "${outFile}" ) {
      print STDOUT "Renaming ${outFile} to ${outFile}.bak...\n";

      rename("${outFile}", "${outFile}.bak");
   }

   unless(open(OUTFILE, ">${outFile}")) {
      print STDOUT "Can not open file ${outFile}\n";
      return -1;
   }

   print STDOUT "Generating $outFile...\n";

   outputMultiLoadHeaderInUtility($table);
   outputMultiLoadFuncInUtility($table);

   close(OUTFILE);

   return 0;
}

sub generateBTEQInPerl
{
   my ($table) = @_;

   $table =~ tr[A-Z][a-z];
   my $outFile;
   
   if ( $NOTABLE == 1 ) {
      $outFile = "${table}0090.pl";
   } else {
      my $jobname = $JOBDIR;
      $jobname =~ tr[A-Z][a-z];
      $outFile = "${jobname}0090.pl";
   }

   if ( -f "${TARGETBIN}/${outFile}" ) {
      print STDOUT "Renaming ${outFile} to ${outFile}.bak...\n";

      rename("${TARGETBIN}/${outFile}", "${TARGETBIN}/${outFile}.bak");
   }

   unless(open(OUTFILE, ">${TARGETBIN}/${outFile}")) {
      print STDOUT "Can not open file ${TARGETBIN}/${outFile}\n";
      return -1;
   }

   print STDOUT "Generating $outFile...\n";

   outputBTEQHeaderInPerl($table, $outFile);
   outputBTEQVariableInPerl($table, $outFile);
   outputBTEQFuncInPerl($outFile);
   outputBTEQMainInPerl();
   outputBTEQProgramInPerl();

   close(OUTFILE);

   return 0;
}

sub generateBTEQInShell
{
   my ($table) = @_;

   $table =~ tr[A-Z][a-z];
   my $outFile;
   
   if ( $NOTABLE == 1 ) {
      $outFile = "${table}0090.sh";
   } else {
      my $jobname = $JOBDIR;
      $jobname =~ tr[A-Z][a-z];
      $outFile = "${jobname}0090.sh";
   }

   if ( -f "${TARGETBIN}/${outFile}" ) {
      print STDOUT "Renaming ${outFile} to ${outFile}.bak...\n";

      rename("${TARGETBIN}/${outFile}", "${TARGETBIN}/${outFile}.bak");
   }

   unless(open(OUTFILE, ">${TARGETBIN}/${outFile}")) {
      print STDOUT "Can not open file ${TARGETBIN}/${outFile}\n";
      return -1;
   }

   print STDOUT "Generating $outFile...\n";

   outputBTEQHeaderInShell($table, $outFile);
   outputBTEQVariableInShell($table, $outFile);
   outputBTEQFuncInShell();
   outputBTEQProgramInShell();

   close(OUTFILE);

   return 0;
}


sub mainFastload
{
   my ($dbh) = @_;
   my $table;
   
   my $rc = getDBTable($dbh);
   if ($rc == -1) {
      print STDEOUT "ERROR - Can not get the table definition.\n";
      return -1;
   }

   my $n;
   for ($n=0; $n < $TableCount; $n++) {
      $table = $TableList[$n];
      $table =~ s/ //g;

      print STDOUT "Generating fastload script for table '$table'...\n";
      createAppPath($table);
      $rc = getTableSchema($dbh, $DBNAME, $table);

      if ( $OTYPE eq "perl" ) {
         generateFastLoadInPerl($table);
      }
      elsif ( $OTYPE eq "shell" ) {
         generateFastLoadInShell($table);
      }
      elsif ( $OTYPE eq "none" ) {
         generateFastLoadInUtility($table);
      }
   }
}

sub mainMultiload
{
   my ($dbh) = @_;
   my $table;
   
   my $rc = getDBTable($dbh);
   if ($rc == -1) {
      return -1;
   }

   my $n;
   for ($n=0; $n < $TableCount; $n++) {
      $table = $TableList[$n];
      $table =~ s/ //g;

      print STDOUT "Generating multiload script for table '$table'...\n";
      createAppPath($table);
      $rc = getTableSchema($dbh, $DBNAME, $table);

      getTableIndex($dbh, $DBNAME, $table);

      if ( $OTYPE eq "perl" ) {
         generateMultiLoadInPerl($table);
      }
      elsif ( $OTYPE eq "shell" ) {
         generateMultiLoadInShell($table);
      }
      elsif ( $OTYPE eq "none" ) {
         generateMultiLoadInUtility($table);
      }
   }
}

sub mainBteq
{
   my ($dbh) = @_;
   my $table;
   
   my $rc = getDBTable($dbh);
   if ($rc == -1) {
      return -1;
   }

   my $n;
   for ($n=0; $n < $TableCount; $n++) {
      $table = $TableList[$n];
      $table =~ s/ //g;

      print STDOUT "Generating bteq script for table '$table'...\n";
      createAppPath($table);
      $rc = getTableSchema($dbh, $DBNAME, $table);

      if ( $OTYPE eq "perl" ) {
         generateBTEQInPerl($table);
      }
      elsif ( $OTYPE eq "shell" ) {
         generateBTEQInShell($table);
      }
   }
}

sub main
{
   my $connectString;

   if ( $OS eq "mswin32" ) {
      $connectString = "dbi:ODBC:${DSOURCE}";
   }
   elsif ( $OS eq "svr4" ) {
      $connectString = "dbi:Teradata:${DSOURCE}";
   }

   if ( $JOB eq "fload" ) {
      my $dbh = DBI->connect($connectString, $USER, $PASSWD);

      unless ($dbh) {
         print STDOUT "Connect failed: $DBI::errstr\n";
         exit(1);
      }

      print "Script Wizard is generating script...\n";

      mainFastload($dbh);
   }
   elsif ( $JOB eq "mload" ) {
      my $dbh = DBI->connect($connectString, $USER, $PASSWD);

      unless ($dbh) {
         print STDOUT "Connect failed: $DBI::errstr\n";
         exit(1);
      }

      print "Script Wizard is generating script...\n";

      mainMultiload($dbh);
   }
   elsif ( $JOB eq "bteq" || $JOB eq "bteqf" || $JOB eq "bteqm" ) {
      my $dbh = DBI->connect($connectString, $USER, $PASSWD);

      unless ($dbh) {
         print STDOUT "Connect failed: $DBI::errstr\n";
         exit(1);
      }

      print "Script Wizard is generating script...\n";

      mainBteq($dbh);
   }

   print "Finish. \n";
}

sub showArgumentError
{
   my ($count, $value) = @_;

   print STDOUT "Unknown value '$value' at position $count\n";
}

sub showUsage
{
   print STDOUT "\n";
   print STDOUT "Script Wizard v0.1, by Jet Wu, NCR Taiwan Copyright 2001\n\n";
   print STDOUT "Usage: perl scriptwz.pl -home <home> -os <os> -ds <data source>\n";
   print STDOUT "                        -logon <logon> -db <dbname> -table <table>\n";
   print STDOUT "                        -sys <system> -odir <out dir> -ofile <out file>\n";
   print STDOUT "                        -job <job> -otype <out type> -dformat <data format>\n";
   print STDOUT "                        -newline <newline> -deli <delimiter> -rec <record>\n";
   print STDOUT "                        -logdb <logdb> -dird <path deli>\n";
   print STDOUT "\n";
   print STDOUT "<home> -------- is the ETL Automation home directory\n";
   print STDOUT "<os> ---------- is the operating system type, valid options are 'unix', 'nt'\n";
   print STDOUT "<data source> - is the data source name which use to connect to Teradata\n";
   print STDOUT "<logon> ------- is the logon string use to logon into Teradata, for example,\n";
   print STDOUT "                dbc,dbc Notice that there is no space between user and password\n";
   print STDOUT "<dbname> ------ is the database name in Teradata\n";
   print STDOUT "<table> ------- is the table name in Teradata\n";
   print STDOUT "<system> ------ is the system directory name under APP\n";
   print STDOUT "<out dir> ----- is the output directory name under system directory\n";
   print STDOUT "<out file> ---- is the output script file name\n";
   print STDOUT "<job> --------- is the job type for generated script, valid options are\n";
   print STDOUT "                'fload', 'mload', 'bteq', 'bteqf', 'bteqm'.\n";
   print STDOUT "                fload is fastload, mload is multiload.\n";
   print STDOUT "                bteq is a prgram template for bteq\n";
   print STDOUT "                bteqf is bteq script for pre-fastload\n";
   print STDOUT "                bteqm is bteq script for pre-multiload\n"; 
   print STDOUT "<out type> ---- is the script type to generate, valid options are 'perl',\n";
   print STDOUT "                'shell', 'none'. 'perl' will generate the script in Perl\n";
   print STDOUT "                syntax. 'shell' will generate the script in Shell syntax,\n";
   print STDOUT "                'none' will generate script in utility format\n";
   print STDOUT "<data format> - valid options are 'vartext', 'unformatted', 'formatted'\n";
   print STDOUT "<newline> ----- valid options are 'yes', 'no'\n";
   print STDOUT "<delimiter> --- is the delimiter character when data format is vartext\n";
   print STDOUT "<record> ------ is the record number starting to load into database\n";
   print STDOUT "\n";
}

sub parseArgument
{
   my @argList = @_;

   if ( $#argList == -1 ) {
      showUsage();
      exit(1);
   }

   my $arg;
   my $count = 1;
   my ($option, $value);
   my $errflag = 0;

   while (1) {
      if ( $#argList == -1 ) {
         last;
      }

      $arg = shift(@argList);
#      print STDOUT "$count, $arg\n";

      if ( substr($arg, 0, 1) eq "-" ) {
         $option = substr($arg, 1, length($arg) - 1);
         $option =~ tr[A-Z][a-z];

         if ( $option eq "os" ) {
            $value = shift(@argList); $count++;
            $value =~ tr [A-Z][a-z];
            if ( $value eq "nt" || $value eq "unix") {
               if ( $value eq "unix" ) {
               	  $OS = "svr4";
               }
               else {
                  $OS = "mswin32";
               }
            }
            else { # unknown data format type
               showArgumentError($count, $value);
               $errflag = 1;
            }
         }
         # job is [fload, mload], no default 
         elsif ( $option eq "job" ) {
            $value = shift(@argList); $count++;
            $value =~ tr [A-Z][a-z];
            if ( $value eq "fload" || $value eq "mload" || $value eq "bteq" ||
                 $value eq "bteqf" || $value eq "bteqm") {
               $JOB = $value;
            }
            else {  # unknown job
               showArgumentError($count, $value);
               $errflag = 1;
            }
         }
         # type is [perl, shell, none], default is none
         elsif ( $option eq "otype" ) {
            $value = shift(@argList); $count++;
            $value =~ tr [A-Z][a-z];
            if ( $value eq "perl" || $value eq "shell" || $value eq "none") {
               $OTYPE = $value;
            }
            else { # unknown output script type
               showArgumentError($count, $value);
               $errflag = 1;
            }
         }
         elsif ( $option eq "ds" ) {
            $value = shift(@argList); $count++;
            $DSOURCE = $value;
         }
         elsif ( $option eq "dformat" ) {
            $value = shift(@argList); $count++;
            $value =~ tr [A-Z][a-z];
            if ( $value eq "vartext" || $value eq "unformatted" ||
                 $value eq "formatted" ) {
               $DFORMAT = $value;
            }
            else { # unknown data format type
               showArgumentError($count, $value);
               $errflag = 1;
            }
         }
         elsif ( $option eq "newline" ) {
            $value = shift(@argList); $count++;
            $value =~ tr [A-Z][a-z];
            if ( $value eq "yes" || $value eq "no") {
               $DFORMAT = $value;
            }
            else { # unknown data format type
               showArgumentError($count, $value);
               $errflag = 1;
            }
         }
         elsif ( $option eq "deli" ) {
            $value = shift(@argList); $count++;
            $DELI = $value;
         }
         # logon is the user and password use to connect to Teradata
         elsif ( $option eq "logon" ) {
            $value = shift(@argList); $count++;
            $LOGON = $value;
         }
         # db is the database name
         elsif ( $option eq "db" ) {
            $value = shift(@argList); $count++;
            $DBNAME = $value;
            if ( $LOGDBNAME eq "" ) {
               $LOGDBNAME = $DBNAME;
            }
         }
         elsif ( $option eq "logdb" ) {
            $value = shift(@argList); $count++;
            $LOGDBNAME = $value;
         }
         # table is the table name
         elsif ( $option eq "table" ) {
            $value = shift(@argList); $count++;
            $TABLE = $value;
         }
         elsif ( $option eq "sys" ) {
            $value = shift(@argList); $count++;
            $value =~ tr [a-z][A-Z];
            $SUBSYS = $value;
         }
         elsif ( $option eq "odir" ) {
            $value = shift(@argList); $count++;
            $value =~ tr [a-z][A-Z];
            $JOBDIR = $value;
         }
         elsif ( $option eq "home" ) {
            $value = shift(@argList); $count++;
            $HOME = $value;
         }
         elsif ( $option eq "rec" ) {
            $value = shift(@argList); $count++;
            $RECORD = $value;
         }
         elsif ( $option eq "dird" ) {
            $value = shift(@argList); $count++;
            $DIRD = $value;
         }	
         else {  # unknown option
            print STDOUT "Unknow option tag '$arg' at position $count\n";
            $errflag = 1;
         }
      }
      else {
#         showArgumentError($count, $arg);
#         return -1;
      }
      $count++;
   }

   if ( $errflag == 1 ) {
      showUsage();
      return -1;
   }

   return 0;
}

if ( parseArgument(@ARGV) != 0 ) {
   exit(1);
}

if ( $TABLE eq "" ) {
   $NOTABLE = 1;
}

if ( $HOME ne "" ) {
   $ETLDIR = $HOME;
   $ETL_APP = "${HOME}/APP";
}

if ( $LOGDBNAME eq "" ) {
   $LOGDBNAME = $DBNAME;
}

($USER, $PASSWD) = split(',', $LOGON);

print STDOUT "Home = $HOME, OS = $OS, DataSource = $DSOURCE\n";
print STDOUT "User = $USER, Password = $PASSWD, DBName = $DBNAME, Table = $TABLE\n";
print STDOUT "Subsys = $SUBSYS, JobDir = $JOBDIR, OutFile = $OUTFILE\n";
print STDOUT "Job = $JOB, OutType = $OTYPE\n";
print STDOUT "DataFormat = $DFORMAT, Delimiter = $DELI, Newline = $NEWLINE\n";
print STDOUT "Record = $RECORD\n";

my $rc = main();

exit(0);

__END__
