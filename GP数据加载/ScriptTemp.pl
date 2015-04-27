#!/usr/bin/perl
#/*************Head Section**************************************************************************/
#/*Script Use:                                                                                      */
#/*Create Date:                                                                                     */
#/*SDM Developed By:                                                                                */
#/*SDM Developed Date:                                                                              */
#/*SDM Checked By:                                                                                  */
#/*SDM Checked Date:                                                                                */
#/*Script Developed By:                                                                             */
#/*Script Checked By:                                                                               */
#/*Source table 1:                                                                                  */
#/*Target Table:                                                                                    */
#/*Job Type:                                                                                        */
#/*ETL Frequency:                                                                                   */
#/*ETL Policy:                                                                                      */
#/*************Revision History**********************************************************************/
#Date Revised    Revised by     Revision Note
#/***************************************************************************************************/
#/* package section
#/***************************************************************************************************/
use strict;                                        #Declare using Perl strict syntax

#/***************************************************************************************************/
#/*variable section                                                                                 */
#/***************************************************************************************************/
#/*�����������                                                                                     */
my $HOME    = $ENV{"AUTO_HOME"}||'/home/etl';       #ETL����Ŀ¼
my $SCRIPT  = '';                                   #�ű���
my $LOG_DIR  = '${HOME}/LOG/TMP';                       #��־���Ŀ¼

#/***************************************************************************************************/
#/*��������                                                                                         */
my $CONTROL_FILE;                                   #�����ļ�����
my $LOGON_FILE="$HOME/etc/LOGON_EDW";               #��¼�ļ�����
my $DB_USER;                                        #���ݿ��¼�û�
my $DB_PASSWD;                                      #���ݿ��¼����
my $DB_IP=ENV{"AUTO_GREENPLUM_IP"}||'31.2.1.13';    #GreenPlum���ݿ�IP��ַ
my $DB_PORT=ENV{"AUTO_GREENPLUM_PORT"}||'5432';     #GreenPlumͨ�Ŷ˿�
my $DB_NAME=ENV{"AUTO_GREENPLUM_DB"}||'WHRCB_EDW';  #GreenPlum���ݿ�����
my $TXN_DATE;                                       #��������
my $ETL_SYS;                                        #ETLϵͳ����
my $ETL_JOB;                                        #ETL�ű�����

my $MIN_DATE=$ENV{"AUTO_MINDATE"}||'19000101';      #��С����
my $MAX_DATE=$ENV{"AUTO_MAXDATE"}||'30001231';      #�������
my $RET_CODE;                                       #������

my $SDSDATADB=$ENV{"AUTO_SDSDATADB"}||'SDSDATA';
my $BDSDATADB=$ENV{"AUTO_BDSDATADB"}||'BDSDATA';

#/***************************************************************************************************/
#/*Perl Function                                                                                    */
#/***************************************************************************************************/
#/*print message Function                                                                           */
#/***************************************************************************************************/
sub Message($)
{
	printf "[%02d:%02d:%02d] %s\n", (localtime(time()))[2,1,0], shift;
}

#/***************************************************************************************************/
#/* get db user                                                                           */
#/***************************************************************************************************/
sub GetDbUser($)
{
	my ($logon_file)=@_;
	
	unless(open(LOGON_H,"$logon_file")){
		Message(��¼�ļ���ʧ��);
		return undef;
	}
	
	my $$logon=<LOGON_H>;
	
	close(LOGON_H);
	
	$logon =~ s/([\n\.\;])//g;
   	$logon =~ s/([^ ]*) *([^ ]*)/$2/;
   	my ($user , $passwd) = split(',' , $logon);
   	
   	return $user;
}

#/***************************************************************************************************/
#/*create directory Function                                                                        */
#/***************************************************************************************************/
sub _mkdir(@)
{
	my ( $def, $split, $dir, $cnt, @dirs, @idirs );
	@dirs = @_;
	$def   = GetDirDef();
	$split = quotemeta( $def );
	
	for $dir( @dirs )
	{
		next if( -d $dir );
		@idirs = split( $split, $dir );
		my $idir;
		for ( @idirs )
		{
			next unless( $idir );
			$idir .= $_ . $def;
			unless( -d $idir )
			{
				until( mkdir($idir,0755) )
				{
					if( $cnt ++ > 5 )
					{
						Message("Ŀ¼�޷�����[$idir]");
						return 1;
					}
					sleep 1;
				}
			}
		}
	}
	return 0;
}

#/***************************************************************************************************/
#/*PSQL Function                                                                                    */
#/***************************************************************************************************/
sub run_psql_command
{
	my ($logFile) = @_ ;
	
	$logFile = $logFile?" >>$logFile 2>&1 ":''
	
	my $rc = open(PSQL, "| psql -d $DB_NAME -h $DB_IP -p $DB_PORT -U $DB_USER -a -v ON_ERROR_STOP=1 |tee ${logFile} 2>&1");
	# To see if psql command invoke ok?
	unless ($rc) 
	{
	   print "Could not invoke PSQL command\n";
	   return 1;
	}
	print PSQL <<ENDOFINPUT;

\\timing

<PutSqlString></PutSqlString>

\\q

ENDOFINPUT

	close( PSQL ) ;
	# return 0 means ok
	return $?;
}

#/***************************************************************************************************/
#/*main Function                                                                                    */
#/***************************************************************************************************/
sub main 
{
	# ��ȡ��Ϣ
	Message('��ȡԤ��Ϣ');
	if ( substr(${CONTROL_FILE}, length(${CONTROL_FILE})-3, 3) eq 'dir' ) {
    		$TXN_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 8);
	};
	
	$ETL_SYS=substr(${CONTROL_FILE},0,3);
	$ETL_JOB=substr(${CONTROL_FILE},3,length(${CONTROL_FILE})-17);
	
	$DB_USER=GetDbUser($LOGON_FILE);
	
	return 12 if( _mkdir( "$LOGDIR/$TXN_DATE" ) );
	
	my $logFile = "$LOGDIR/$TXN_DATE/${ETL_JOB}_${TXN_DATE}.log";
	
	Message("ETLϵͳ��$ETL_SYS");
	Message("ETL����$ETL_JOB");
	Message("�������ڣ�$TXN_DATE");
	Message("GREENPLUM��ַ��$DB_IP");
	Message("GREENPLUM�˿ڣ�$DB_PORT");
	Message("GREENPLUM�û���$DB_USER");
	Message("��־�ļ���$logFile");
	
	return $ret;
	
	$rec = run_psql_command( $logFile ) ;
	Message('����psqlת������,����');
	return $rec;
}

#/***************************************************************************************************/
#/*parameter selection                                                                              */
#/***************************************************************************************************/
if ( $#ARGV < 0 ) 
{
	Message('����ȷ�������');
	exit 1;
}
$CONTROL_FILE=$ARGV[0];

open(STDERR, ">&STDOUT");
$RET_CODE = main();

if( $RET_CODE )
{
	Message('�ű�����ʧ��');
	exit 1;
}else
{
	Message('�ű����гɹ�');
	exit 0;
}
