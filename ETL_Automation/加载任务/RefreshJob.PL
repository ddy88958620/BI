#!/usr/bin/perl

use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::FmtDefault;
#use Unicode::Map;
use Encode::CN;
use Encode qw(from_to);
use Time::localtime;
use Time::Local;
use DBI;
use DBD::ODBC;
use File::Basename;
use File::Copy;

#my $Map = new Unicode::Map("GB2312");
$dns="gp";
$res="petl";
$dbuser = "etluser";
$dbpass = "etluser";
$dbh = conn();
#####################################
#主函数
#####################################
sub main
{   
 
   # 解析excel
   my $excel = Spreadsheet::ParseExcel::Workbook->Parse($file) || die "无效的excel文件!";
   
   print "开始运行自动化脚本:\n";
    print "#########################################################################\n";
   
   foreach my $sheet (@{$excel->{Worksheet}})
   {
      
       if($sheet->{Name} eq "ETL_JOB" )
       {
   	      if($sheet->{MaxRow} <2)
   	      {
   	     	  next;
   
   	      }
   	      else
   	      {
   	     	   print "刷新ETL_JOB信息... ...\n";
   	      }
   	      foreach my $row ($sheet->{MinRow}+2 .. $sheet->{MaxRow})
       	  {
          		 
              		 my $cell = $sheet->{Cells}[$row][0];
              		 my $etl_system = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][1];
              		 my $etl_job = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][2];
              		 my $etl_server = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][3];
              		 my $description =$cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][4];
              		 my $frequency =$cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][5];
              		 my $jobtype =$cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][6];
              		 my $source =$cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][7];
              		 my $flag =$cell->{Val};
              		 
              		 if($flag eq "Y")
              		 {
              		    ETL_JOB($etl_system,$etl_job,$etl_server,$description,$frequency,$jobtype,$source);
              		 }
              	
       	  }
       }
       elsif($sheet->{Name} eq "ETL_JOB_STREAM")
       {
       	  if($sheet->{MaxRow} <2)
   	      {
   	     	  next;
   
   	      }
   	      else
   	      {
   	     	   print "刷新ETL_JOB_STREAM信息... ...\n";
   	      }
   	      foreach my $row ($sheet->{MinRow}+2 .. $sheet->{MaxRow})
       	  {
          		 
              		 my $cell = $sheet->{Cells}[$row][0];
              		 my $etl_system = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][1];
              		 my $etl_job = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][2];
              		 my $stream_system = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][3];
              		 my $stream_job =$cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][4];
              		 my $flag =$cell->{Val};
              		 
 
              		 if($flag eq "Y")
              		 {
              		    ETL_JOB_STREAM($etl_system,$etl_job,$stream_system,$stream_job);
              		 }
          }
       }
       elsif($sheet->{Name} eq "ETL_JOB_DEPENDENCY")
       {
       	  if($sheet->{MaxRow} <2)
   	      {
   	     	  next;
   
   	      }
   	      else
   	      {
   	     	   print "刷新ETL_JOB_DEPENDENCY信息... ...\n";
   	      }
   	      foreach my $row ($sheet->{MinRow}+2 .. $sheet->{MaxRow})
       	  {
          		 
              		 my $cell = $sheet->{Cells}[$row][0];
              		 my $etl_system = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][1];
              		 my $etl_job = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][2];
              		 my $dependency_system = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][3];
              		 my $dependency_job =$cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][4];
              		 my $flag =$cell->{Val};
              		 
 
              		 if($flag eq "Y")
              		 {
              		    ETL_JOB_DEPENDENCY($etl_system,$etl_job,$dependency_system,$dependency_job);
              		 }
          }
       }
       elsif($sheet->{Name} eq "ETL_JOB_GROUP")
       {
       	  if($sheet->{MaxRow} <2)
   	      {
   	     	  next;
   
   	      }
   	      else
   	      {
   	     	   print "刷新ETL_JOB_GROUP信息... ...\n";
   	      }
   	      foreach my $row ($sheet->{MinRow}+2 .. $sheet->{MaxRow})
       	  {
          		 
              		 my $cell = $sheet->{Cells}[$row][0];
              		 my $groupname = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][1];
              		 my $etl_system = $cell->{Val};
              		               		 
              		 $cell = $sheet->{Cells}[$row][2];
              		 my $etl_job = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][3];
              		 my $flag =$cell->{Val};
              		 
 
              		 if($flag eq "Y")
              		 {
              		    ETL_JOB_GROUP($groupname,$etl_system,$etl_job);
              		 }
          }
       }
       elsif($sheet->{Name} eq "ETL_JOB_GROUPCHILD")
       {
       	  if($sheet->{MaxRow} <2)
   	      {
   	     	  next;
   
   	      }
   	      else
   	      {
   	     	   print "刷新ETL_JOB_GROUPCHILD信息... ...\n";
   	      }
   	      foreach my $row ($sheet->{MinRow}+2 .. $sheet->{MaxRow})
       	  {
          		 
              		 my $cell = $sheet->{Cells}[$row][0];
              		 my $groupname = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][1];
              		 my $etl_system = $cell->{Val};
              		               		 
              		 $cell = $sheet->{Cells}[$row][2];
              		 my $etl_job = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][3];
              		 my $flag =$cell->{Val};
              		 
 
              		 if($flag eq "Y")
              		 {
              		    ETL_JOB_GROUPCHILD($groupname,$etl_system,$etl_job);
              		 }
          }
       }
       elsif($sheet->{Name} eq "DATACALENDAR")
       {
       	  if($sheet->{MaxRow} <2)
   	      {
   	     	  next;
   
   	      }
   	      else
   	      {
   	     	   print "刷新DATACALENDAR信息... ...\n";
   	      }
   	      foreach my $row ($sheet->{MinRow}+2 .. $sheet->{MaxRow})
       	  {
          		 
              		 my $cell = $sheet->{Cells}[$row][0];
              		 my $etl_system = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][1];
              		 my $etl_job = $cell->{Val};
              		               		 
              		 $cell = $sheet->{Cells}[$row][2];
              		 my $calendaryear = $cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][3];
              		 my $seqnum =$cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][4];
              		 my $calendarmonth =$cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][5];
              		 my $calendarday =$cell->{Val};
              		 
              		 $cell = $sheet->{Cells}[$row][6];
              		 my $flag =$cell->{Val};
              		 
 
              		 if($flag eq "Y")
              		 {
              		 	  if($seqnum == 1)
              		 	  {
              		 	    DATACALENDARYEAR($etl_system,$etl_job,$calendaryear);
              		      DATACALENDAR($etl_system,$etl_job,$calendaryear,$seqnum,$calendarmonth,$calendarday);
              		    }
              		    DATACALENDAR($etl_system,$etl_job,$calendaryear,$seqnum,$calendarmonth,$calendarday);
              		 }
          }
       }
   }    
   
   print "#########################################################################\n";
   
   close(in);
   
   print "自动化脚本运行结束!";

}

####################################
#获取数据库连接
####################################
sub conn
{
   my $dbh = DBI->connect("dbi:ODBC:$dns",$dbuser,$dbpass,
                          { AutoCommit => 1, PrintError => 0, RaiseError => 0 } ) ;

   unless ( defined($dbh) ) { return undef; }
   
   return $dbh;
}

####################################
#插入数据
####################################
sub insertData
{
	 my ($tablename,$insert,$value) = @_;
	 my @mypi = getPI($dbuser,$tablename);
	 my ($condition,$set) = getCondition($insert,$value,@mypi);
   my $sql="update $dbuser\.$tablename set $set where $condition else insert into $dbuser\.$tablename($insert) values($value)";
   print "执行SQL：--$sql\n";
   #my $sth = $dbh->prepare($sqlText) || die $dbh->errstr;
   #$sth->execute() || die "Error : $DBI::errstr\n";
   #$sth->finish();
   
}


####################################
#获取PI
####################################
sub getPI
{
	 my ($dbown,$table) = @_;
	 my @mypi;
   my $sqlText = "SELECT upper(trim(columnname)) FROM DBC.Indicesx WHERE DataBaseName='".$dbown."' AND TableName='".$table."' AND IndexType='P' ORDER BY columnPosition";
   
   my $sth = $dbh->prepare($sqlText) || die $dbh->errstr;
   unless($sth)
   {
      print  "*** ERROR *** - Unable to prepare statement for $sqlText\n";
      return 1;
   }
   $sth->execute() || die "Error : $DBI::errstr\n";

   while(@row = $sth->fetchrow())
   {
   	   push(@mypi,$row[0])
   }
   
   $sth->finish();
   
   return @mypi;
}


#####################################
#获取where条件
#######################################
sub getCondition
{
	my ($insert,$value,@mypi) = @_;
  my $num01 = 0;
  my $num02=0;
  my $condition;
  my $set;
	my @array_insert = split(",",$insert);
	my @value  = split(",",$value);
	
	my $len01 = @mypi;
	my $len02 = @array_insert;
	
	for(my $i=0;$i<$len02;$i++)
	{
			  foreach my $pi(@mypi)
	      {
				   if($pi ne $array_insert[$i])
				   {
				      if($num01 == 0)
				      {
				         $set = $set . $array_insert[$i] ." = " . $value[$i];
				         $num01++;
          
				      }
				      else
				     {
				  	     $set = $set . "," . $array_insert[$i] ." = " . $value[$i];
          
				     }
	    	   }
	    	   else
	    	   {
	    	   	  if($num02 == 0)
	            {
	                $condition = $condition . $pi ." = " . $value[$i];
	                $num02++;
	              
	            }
	            else
	            {
	         	      $condition = $condition . " AND " . $pi ." = " . $value[$i];
	         	      $num02++;
	         	     
	            }
	    	   }
				
	      }	
	}	
	
	
 return($condition,$set);
}


####################################
#处理ETL_JOB
####################################
sub ETL_JOB
{
	 my ($etl_system,$etl_job,$etl_server,$description,$frequency,$jobtype,$source) = @_;
	 my($jobsessionid,$expectedrecord,$lastcount);
	 
	 #更新表ETL_JOB
	 my $ready = "SELECT jobsessionid,expectedrecord FROM $res.ETL_JOB WHERE etl_system='$etl_system' AND etl_job='$etl_job'";
	 print "$ready\n";
	 my $sth = $dbh->prepare($ready) || die $dbh->errstr;
   $sth->execute() || die "Error : $DBI::errstr\n";
   @row = $sth->fetchrow();
   if( $#row >= 0)
   {
   	  ($jobsessionid,$expectedrecord) = ($row[0],$row[1]);
   	  $sqlstr = "UPDATE ETL_JOB SET etl_server='$etl_server',description='$description',frequency='$frequency',jobtype='$jobtype',
                 enable='1',Last_JobStatus='Ready',CubeFlag='N',AutoOff='N',CheckCalendar='N',CalendarBU='',jobsessionid='$jobsessionid',expectedrecord='$expectedrecord',CheckLastStatus='Y'
                 WHERE etl_system='$etl_system' AND etl_job='$etl_job'";# CheckCalendar ='Y' --> CheckCalendar = 'N' 2012-8-24 
   }
   else
   {
   	  ($jobsessionid,$expectedrecord) = (0,0);
   	    $sqlstr = "INSERT INTO ETL_JOB(etl_system,etl_job,etl_server,description,frequency,jobtype,enable,Last_JobStatus,CubeFlag,AutoOff,
                 CheckCalendar,CalendarBU,jobsessionid,expectedrecord,CheckLastStatus) VALUES('$etl_system','$etl_job','$etl_server','$description','$frequency','$jobtype','1','Ready','N','N','N','',
                 0,0,'Y')";# CheckCalendar ='Y' --> CheckCalendar = 'N' 2012-8-24 
   }
   
  

   print "$sqlstr\n";
   $sth = $dbh->prepare($sqlstr) || die $dbh->errstr;
   $sth->execute() || die "Error : $DBI::errstr\n";
   
   #更新表ETL_JOB_TIMEWINDOW    2013/04/18 暂时注释掉
#   $sqlstr = "UPDATE ETL_JOB_TIMEWINDOW SET Allow='Y',BeginHour=0,EndHour=23 WHERE etl_system='$etl_system' AND etl_job='$etl_job' ELSE INSERT INTO ETL_JOB_TIMEWINDOW(etl_system,etl_job,Allow,BeginHour,EndHour) VALUES('$etl_system','$etl_job','Y',0,23)";
#   print "$sqlstr\n";
#   $sth = $dbh->prepare($sqlstr) || die $dbh->errstr;
#   $sth->execute() || die "Error : $DBI::errstr\n";
   
   #更新表ETL_JOB_SOURCE
   $ready = "SELECT LastCount FROM ETL_JOB_SOURCE WHERE source='$source'";
   print "$ready\n";
	 my $sth = $dbh->prepare($ready) || die $dbh->errstr;
   $sth->execute() || die "Error : $DBI::errstr\n";
   @row = $sth->fetchrow();
   if( $#row >= 0)
   {
      $lastcount = $row[0];
      $sqlstr =  "UPDATE ETL_JOB_SOURCE SET etl_system='$etl_system',etl_job='$etl_job',Conv_File_Head='$etl_job',AutoFilter='0',Alert='0',BeforeHour=0,BeforeMin=0,OffsetDay=0,LastCount=$lastcount WHERE source='$source'";
   }
   else
   {
   	 $lastcount =0;
   	 $sqlstr ="INSERT INTO ETL_JOB_SOURCE(source,etl_system,etl_job,Conv_File_Head,AutoFilter,Alert,BeforeHour,BeforeMin,OffsetDay,LastCount) VALUES('$source','$etl_system','$etl_job','$etl_job','0','0',0,0,0,0)";
   }
   
               
   print "$sqlstr\n";
   $sth = $dbh->prepare($sqlstr) || die $dbh->errstr;
   $sth->execute() || die "Error : $DBI::errstr\n";
   
   $sth->finish();

}

####################################
#处理ETL_JOB_STREAM
####################################
sub ETL_JOB_STREAM
{
	my ($etl_system,$etl_job,$stream_system,$stream_job) = @_;
	my $sqlstr ="";
	my $ready = "select count(*) from ETL_JOB_STREAM WHERE etl_system='$etl_system' AND etl_job='$etl_job'";
	my $sth = $dbh->prepare($ready) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
  @row = $sth->fetchrow();
	if($row[0] > 0)
	{
		$sqlstr = "UPDATE ETL_JOB_STREAM SET stream_system='$stream_system',stream_job='$stream_job' WHERE etl_system='$etl_system' AND etl_job='$etl_job' ";
	}
	else
	{
		$sqlstr = "INSERT INTO ETL_JOB_STREAM(etl_system,etl_job,stream_system,stream_job,Description,Enable) VALUES('$etl_system','$etl_job','$stream_system','$stream_job','','1')";
	}
 
	print "$sqlstr\n";
	$sth = $dbh->prepare($sqlstr) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
   
  $sth->finish();
}

####################################
#处理ETL_JOB_DEPENDENCY
####################################
sub ETL_JOB_DEPENDENCY
{
	my ($etl_system,$etl_job,$dependency_system,$dependency_job) = @_;
	my $ready = "select count(*) from ETL_JOB_DEPENDENCY WHERE etl_system='$etl_system' AND etl_job='$etl_job'";
	my $sqlstr ="";
	my $sth = $dbh->prepare($ready) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
  @row = $sth->fetchrow();
	if($row[0] > 0)
	{
		$sqlstr = "UPDATE ETL_JOB_DEPENDENCY SET dependency_system='$dependency_system',dependency_job='$dependency_job' WHERE etl_system='$etl_system' AND etl_job='$etl_job' ";
	}
	else
	{
		$sqlstr = "INSERT INTO ETL_JOB_DEPENDENCY(etl_system,etl_job,dependency_system,dependency_job,Description,Enable) VALUES('$etl_system','$etl_job','$dependency_system','$dependency_job','','1')";
	}

	print "$sqlstr\n";
	$sth = $dbh->prepare($sqlstr) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
   
  $sth->finish();
}

####################################
#处理ETL_JOB_GROUP
####################################
sub ETL_JOB_GROUP
{
  my ($groupname,$etl_system,$etl_job) = @_;
	my $sqlstr = "UPDATE ETL_JOB_GROUP SET etl_system='$etl_system',etl_job='$etl_job' WHERE groupname='$groupname' \
	ELSE INSERT INTO ETL_JOB_GROUP(groupname,Description,etl_system,etl_job,AutoOnChild) VALUES('$groupname','','$etl_system','$etl_job','Y')";
	print "$sqlstr\n";
	$sth = $dbh->prepare($sqlstr) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
   
  $sth->finish();
}

####################################
#处理ETL_JOB_GROUPCHILD
####################################
sub ETL_JOB_GROUPCHILD
{
	my ($groupname,$etl_system,$etl_job) = @_;
	my $ready = "select count(*) from ETL_JOB_GROUPCHILD WHERE groupname='$groupname' AND etl_system='$etl_system' AND etl_job='$etl_job'";
	my $sqlstr ="";
	my $sth = $dbh->prepare($ready) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
  @row = $sth->fetchrow();
	if($row[0] > 0)
	{
		$sqlstr = "UPDATE ETL_JOB_GROUPCHILD SET etl_system='$etl_system',etl_job='$etl_job' WHERE groupname='$groupname' AND etl_system='$etl_system' AND etl_job='$etl_job'";
	}
	else
	{
		$sqlstr = "INSERT INTO ETL_JOB_GROUPCHILD(groupname,Description,etl_system,etl_job,Enable) VALUES('$groupname','','$etl_system','$etl_job','1')";
	}

	print "$sqlstr\n";
	$sth = $dbh->prepare($sqlstr) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
   
  $sth->finish();
}

####################################
#处理DATACALENDARYEAR
####################################
sub DATACALENDARYEAR
{
	my ($etl_system,$etl_job,$calendaryear) = @_;
	my $ready = "select count(*) from DATACALENDARYEAR WHERE calendaryear=$calendaryear AND etl_system='$etl_system' AND etl_job='$etl_job'";
	my $sqlstr ="";
	my $sth = $dbh->prepare($ready) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
  @row = $sth->fetchrow();
	if($row[0] > 0)
	{
		$sqlstr = "UPDATE DATACALENDARYEAR SET etl_system='$etl_system',etl_job='$etl_job',calendaryear=$calendaryear WHERE calendaryear=$calendaryear AND etl_system='$etl_system' AND etl_job='$etl_job'";
	}
	else
	{
		$sqlstr = "INSERT INTO DATACALENDARYEAR(etl_system,etl_job,calendaryear) VALUES('$etl_system','$etl_job',$calendaryear)";
	}
	
	$sth = $dbh->prepare($sqlstr) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
   
  $sth->finish();
}

####################################
#处理DATACALENDAR
####################################
sub DATACALENDAR
{
	my ($etl_system,$etl_job,$calendaryear,$seqnum,$calendarmonth,$calendarday) = @_;
	my $ready = "select count(*) from DATACALENDAR WHERE seqnum=$seqnum AND calendaryear=$calendaryear AND etl_system='$etl_system' AND etl_job='$etl_job'";
	my $sqlstr ="";
	my $sth = $dbh->prepare($ready) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
  @row = $sth->fetchrow();
	if($row[0] > 0)
	{
		$sqlstr = "UPDATE DATACALENDAR SET calendarmonth=$calendarmonth,calendarday=$calendarday WHERE seqnum=$seqnum AND calendaryear=$calendaryear AND etl_system='$etl_system' AND etl_job='$etl_job'";
	}
	else
	{
		$sqlstr = "INSERT INTO DATACALENDAR(etl_system,etl_job,calendaryear,seqnum,calendarmonth,calendarday,checkflag) VALUES('$etl_system','$etl_job',$calendaryear,$seqnum,$calendarmonth,$calendarday,'N')";
	}
	
	
	print "$sqlstr\n";
	
	$sth = $dbh->prepare($sqlstr) || die $dbh->errstr;
  $sth->execute() || die "Error : $DBI::errstr\n";
   
  $sth->finish();
}
################################################################################
#运行


#检验
if($#ARGV < 0)
{
	print "请输入参数,如下:\n";
	print "perl RefreshJob.pl 文件名.xls\n";
	exit;
}

#获取excel文件
$file = $ARGV[0];
   
unless( -e $file )
{
   	  print "配置文件 $file 不存在!";
   	  exit;
}

main();

$dbh->disconnect;
