#!/usr/bin/perl

use strict;
use DBI;

if ($#ARGV < 2 )  {
    print "Usage exp_database.pl: DSN/Tpdid UserName,Passwd DataBaseName\n";
    exit 0;
}
my $DSN=($ARGV=shift);
my ($USR, $PASSWD)= split(',', $ARGV=shift);
my $DBNAME=($ARGV=shift);

DBI->trace(0);

my $os = $^O;    $os =~ tr [A-Z][a-z];
#my $DB_Connect = "dbi:Teradata:${DSN}cop1";  # W2K ODBC 
my $DB_Connect = "dbi:ODBC:$DSN";  # W2K ODBC 

unless (open(SQLCMD, ">$DBNAME.SQL") ){
	print "Create file $DBNAME.SQL failed.\n";
	exit(1);
}
my $dbh = DBI->connect($DB_Connect, $USR, $PASSWD,
                          { AutoCommit => 1, PrintError => 1, RaiseError => 1 } ) ;
unless ( defined($dbh) ) { 
	print "DB Connect failed\n";
	exit(0);
}
print "Connecting ok \n";
my @TabName;
my @TabKind;
my @TabComment;

gettablelist($dbh, $DBNAME);

my $i;
for ($i=0; $i<=$#TabName; $i++) {
	GetDefine($dbh, $DBNAME, $TabName[$i], $TabKind[$i]);
	if ($TabComment[$i] ne "") {
		print SQLCMD "COMMENT $DBNAME\.$TabName[$i] \'$TabComment[$i]\';\n\n";
	}
}
close(SQLCMD);

for ($i=0; $i<=$#TabName; $i++) { 
	
}
print "\nSuccess!\n";
exit(0);

sub gettablelist
{
	my ($hdb, $databasename) = @_;
	my $sqlstr ="SELECT TRIM(tablename), trim(tablekind), trim(CommentString)  FROM DBC.tables " .
		     "Where  DataBaseName='$databasename' " .
			 "	     ORDER BY 2, 1 ";
	my $hSQL = $hdb->prepare($sqlstr) or return undef ;
    $hSQL->execute();
	@TabName=();
	my @row;
	my $i = 0;
    while (@row = $hSQL->fetchrow())	{
         $TabName[$i] = $row[0];
		 $TabKind[$i] = $row[1];
		 $TabComment[$i++] = $row[2];
    }
  $hSQL->finish();
}

sub GetDefine
{
	my ($hdb, $databasename, $tablename, $tabkind) = @_;

	my $object_type = "TABLE";
	if ($tabkind eq "V") {
		$object_type = "VIEW";
	}
	elsif ($tabkind eq "M") {
		$object_type = "MACRO";	
	}
	elsif ($tabkind eq "P") {
		$object_type = "PROCEDURE";	
	}
	elsif ($tabkind eq "I") {
		$object_type = "JOIN INDEX";	
	}
	elsif ($tabkind eq "S") {
		$object_type = "SHOW TRIGGER ";	
	}
	my $sqlstr ="SHOW " . $object_type . " $databasename.$tablename;";
	my $hSQL = $hdb->prepare($sqlstr) or return undef ;
    $hSQL->execute();
	my $ddl = "   "; 
	$hSQL->bind_col(1, \$ddl);
    while ( $hSQL->fetchrow() ) {
		$ddl =~ tr [\x0d][\x0a];
		print SQLCMD "$ddl\n";
	}
	$hSQL->finish();
}


__END__

