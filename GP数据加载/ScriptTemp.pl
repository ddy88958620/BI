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
#/*程序所需变量                                                                                     */
my $HOME    = $ENV{"AUTO_HOME"}||'/home/etl';       #ETL工作目录
my $SCRIPT  = '';                                   #脚本名
my $LOG_DIR  = '${HOME}/LOG/TMP';                       #日志存放目录

#/***************************************************************************************************/
#/*参数变量                                                                                         */
my $CONTROL_FILE;                                   #控制文件名称
my $LOGON_FILE="$HOME/etc/LOGON_EDW";               #登录文件名称
my $DB_USER;                                        #数据库登录用户
my $DB_PASSWD;                                      #数据库登录密码
my $DB_IP=ENV{"AUTO_GREENPLUM_IP"}||'31.2.1.13';    #GreenPlum数据库IP地址
my $DB_PORT=ENV{"AUTO_GREENPLUM_PORT"}||'5432';     #GreenPlum通信端口
my $DB_NAME=ENV{"AUTO_GREENPLUM_DB"}||'WHRCB_EDW';  #GreenPlum数据库名称
my $TXN_DATE;                                       #数据日期
my $ETL_SYS;                                        #ETL系统名称
my $ETL_JOB;                                        #ETL脚本名称

my $MIN_DATE=$ENV{"AUTO_MINDATE"}||'19000101';      #最小日期
my $MAX_DATE=$ENV{"AUTO_MAXDATE"}||'30001231';      #最大日期
my $RET_CODE;                                       #返回码

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
		Message(登录文件打开失败);
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
						Message("目录无法创建[$idir]");
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
	# 获取信息
	Message('获取预信息');
	if ( substr(${CONTROL_FILE}, length(${CONTROL_FILE})-3, 3) eq 'dir' ) {
    		$TXN_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 8);
	};
	
	$ETL_SYS=substr(${CONTROL_FILE},0,3);
	$ETL_JOB=substr(${CONTROL_FILE},3,length(${CONTROL_FILE})-17);
	
	$DB_USER=GetDbUser($LOGON_FILE);
	
	return 12 if( _mkdir( "$LOGDIR/$TXN_DATE" ) );
	
	my $logFile = "$LOGDIR/$TXN_DATE/${ETL_JOB}_${TXN_DATE}.log";
	
	Message("ETL系统：$ETL_SYS");
	Message("ETL任务：$ETL_JOB");
	Message("数据日期：$TXN_DATE");
	Message("GREENPLUM地址：$DB_IP");
	Message("GREENPLUM端口：$DB_PORT");
	Message("GREENPLUM用户：$DB_USER");
	Message("日志文件：$logFile");
	
	return $ret;
	
	$rec = run_psql_command( $logFile ) ;
	Message('运行psql转换程序,结束');
	return $rec;
}

#/***************************************************************************************************/
#/*parameter selection                                                                              */
#/***************************************************************************************************/
if ( $#ARGV < 0 ) 
{
	Message('请正确输入参数');
	exit 1;
}
$CONTROL_FILE=$ARGV[0];

open(STDERR, ">&STDOUT");
$RET_CODE = main();

if( $RET_CODE )
{
	Message('脚本运行失败');
	exit 1;
}else
{
	Message('脚本运行成功');
	exit 0;
}
