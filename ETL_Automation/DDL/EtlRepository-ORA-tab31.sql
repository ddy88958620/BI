
drop table etlrep.ctl_table_log;
create table etlrep.ctl_table_log(
sysname	varchar2(3),
tablename	varchar2(30),
datafile		varchar2(30),
logname	varchar2(30),
bisdate		varchar2(8),
description	varchar2(50)
);

create unique index idx_ctl_table_log on etlrep.ctl_table_log (sysname,tablename);

comment on table etlrep.ctl_table_log is '临时区文件加载控制列表';
comment on column etlrep.ctl_table_log.sysname is '子系统代码:S01';
comment on column etlrep.ctl_table_log.tablename is '表名称:S01_MENU{mmdd}';
comment on column etlrep.ctl_table_log.datafile is '数据文件名称:S01_MENU.txt';
comment on column etlrep.ctl_table_log.logname is '就绪文件名称:S01_MENU.txt.ok';
comment on column etlrep.ctl_table_log.bisdate is '当前业务日期yyyymmdd';
comment on column etlrep.ctl_table_log.description is '备注信息';

drop table etlrep.DataCalendar;

CREATE TABLE etlrep.DataCalendar
     (
      ETL_System CHAR(3)  NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      CalendarYear INTEGER NOT NULL,
      SeqNum INTEGER NOT NULL,
      CalendarMonth INTEGER NOT NULL,
      CalendarDay INTEGER NOT NULL,
      CheckFlag CHAR(1) );
      
create index idx_DataCalendar on etlrep.DataCalendar (ETL_System ,ETL_Job ,CalendarYear ,SeqNum );

COMMENT ON COLUMN etlrep.DataCalendar.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.DataCalendar.ETL_Job IS 'ETL Job Name';

drop table etlrep.DataCalendarYear;

CREATE TABLE etlrep.DataCalendarYear
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      CalendarYear INTEGER NOT NULL);
      
create index idx_DataCalendarYear on etlrep.DataCalendarYear  (ETL_System ,ETL_Job ,CalendarYear );

COMMENT ON COLUMN etlrep.DataCalendarYear.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.DataCalendarYear.ETL_Job IS 'ETL Job Name';

drop table etlrep.ETL_Backup_Log;

CREATE TABLE etlrep.ETL_Backup_Log
     (
      Backup_Job varchar2(50)   NOT NULL,
      JobID INTEGER  NOT NULL,
      InstanceID INTEGER  NOT NULL,
      TXDate DATE  NOT NULL,
      ReturnCode INTEGER,
      start_date CHAR(19) ,
      end_date CHAR(19) ,
      sys_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6));
      
create index idx_ETL_Backup_Log on etlrep.ETL_Backup_Log  (Backup_Job ,JobID ,InstanceID );

COMMENT ON COLUMN etlrep.ETL_Backup_Log.Backup_Job IS 'BACKUP Job Name';
COMMENT ON COLUMN etlrep.ETL_Backup_Log.JobID IS 'Job ID';
COMMENT ON COLUMN etlrep.ETL_Backup_Log.InstanceID IS 'Job Instance ID';
COMMENT ON COLUMN etlrep.ETL_Backup_Log.TXDate IS 'YYYYMMDD';

drop table etlrep.ETL_Event;

CREATE TABLE etlrep.ETL_Event
     (
      EventID varchar2(20)   NOT NULL,
      EventStatus CHAR(1)   NOT NULL,
      Severity CHAR(1)   NOT NULL,
      Description varchar2(200)  ,
      LogTime CHAR(19)   NOT NULL,
      CloseTime CHAR(19)  );
      
create index idx_ETL_Event on etlrep.ETL_Event  (EventID );

COMMENT ON COLUMN etlrep.ETL_Event.EventID IS 'EventID';
COMMENT ON COLUMN etlrep.ETL_Event.EventStatus IS 'EventStatus';
COMMENT ON COLUMN etlrep.ETL_Event.Severity IS 'Severity';
COMMENT ON COLUMN etlrep.ETL_Event.Description IS 'Description';
COMMENT ON COLUMN etlrep.ETL_Event.LogTime IS 'LogTime';
COMMENT ON COLUMN etlrep.ETL_Event.CloseTime IS 'CloseTime';

drop table etlrep.ETL_GroupMember;

CREATE TABLE etlrep.ETL_GroupMember
     (
      UserName varchar2(15)   NOT NULL,
      GroupName varchar2(15)   NOT NULL);
      
create index idx_ETL_GroupMember on etlrep.ETL_GroupMember  (UserName ,GroupName );

COMMENT ON COLUMN etlrep.ETL_GroupMember.UserName IS 'UserName';
COMMENT ON COLUMN etlrep.ETL_GroupMember.GroupName IS 'GroupName';

drop table etlrep.etl_ign_bypass_jobs;

CREATE TABLE etlrep.etl_ign_bypass_jobs
     (
      etl_system CHAR(3)   NOT NULL,
      etl_job varchar2(50)   NOT NULL);
      
create index idx_etl_ign_bypass_jobs on etlrep.etl_ign_bypass_jobs  (etl_system ,etl_job );

COMMENT ON COLUMN etlrep.etl_ign_bypass_jobs.etl_system IS 'etl system name';
COMMENT ON COLUMN etlrep.etl_ign_bypass_jobs.etl_job IS 'etl job name';

drop table etlrep.ETL_Job;

CREATE TABLE etlrep.ETL_Job
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      ETL_Server varchar2(10)  ,
      Description varchar2(50) ,
      Frequency varchar2(30) ,
      JobType CHAR(1) ,
      Enable CHAR(1) ,
      Last_StartTime CHAR(19) ,
      Last_EndTime CHAR(19) ,
      Last_JobStatus varchar2(20) ,
      Last_TXDate DATE ,
      Last_FileCnt INTEGER,
      Last_CubeStatus CHAR(20) ,
      CubeFlag CHAR(1)  ,
      CheckFlag CHAR(1) ,
      AutoOff CHAR(1) ,
      CheckCalendar CHAR(1) ,
      CalendarBU varchar2(15) ,
      RunningScript varchar2(50)  ,
      JobSessionID INTEGER ,
      ExpectedRecord INTEGER ,
      CheckLastStatus CHAR(1)  ,
      TimeTrigger CHAR(1)  ,
      Job_Priority smallint default 30 );
      
create unique index idx_ETL_Job on etlrep.ETL_Job  (ETL_System ,ETL_Job );

COMMENT ON COLUMN etlrep.ETL_Job.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Job.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_Job.ETL_Server IS 'ETL Server Name';
COMMENT ON COLUMN etlrep.ETL_Job.Last_TXDate IS 'YYYY-MM-DD';
COMMENT ON COLUMN etlrep.ETL_Job.CubeFlag IS 'Last_CubeFlag';
COMMENT ON COLUMN etlrep.ETL_Job.RunningScript IS 'Running Script Name';
COMMENT ON COLUMN etlrep.ETL_Job.JobSessionID IS 'Job Session ID';
COMMENT ON COLUMN etlrep.ETL_Job.CheckLastStatus IS 'Check Last Time Status';
COMMENT ON COLUMN etlrep.ETL_Job.ExpectedRecord IS 'Expected Record';
COMMENT ON COLUMN etlrep.ETL_Job.TimeTrigger IS 'Time Trigger';
COMMENT ON COLUMN etlrep.ETL_Job.Job_Priority IS 'Job_Priority';


drop table etlrep.ETL_Job_Dependency;

CREATE TABLE etlrep.ETL_Job_Dependency
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      Dependency_System CHAR(3)   NOT NULL,
      Dependency_Job varchar2(50)   NOT NULL,
      Description varchar2(50) ,
      Enable CHAR(1) );
      
create index idx_ETL_Job_Dependency on etlrep.ETL_Job_Dependency  (ETL_System ,ETL_Job ,Dependency_System ,Dependency_Job );

COMMENT ON COLUMN etlrep.ETL_Job_Dependency.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Job_Dependency.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_Job_Dependency.Dependency_System IS 'Dependency System Name';
COMMENT ON COLUMN etlrep.ETL_Job_Dependency.Dependency_Job IS 'Dependency Job Name';

drop table etlrep.ETL_Job_Group;

CREATE TABLE etlrep.ETL_Job_Group
     (
      GroupName varchar2(50)  NOT NULL,
      Description varchar2(50) ,
      ETL_System CHAR(3)  ,
      ETL_Job varchar2(50)  ,
      AutoOnChild CHAR(1) );
      
create index idx_ETL_Job_Group on etlrep.ETL_Job_Group  ( GroupName);

COMMENT ON COLUMN etlrep.ETL_Job_Group.ETL_System IS 'Head System Name';
COMMENT ON COLUMN etlrep.ETL_Job_Group.ETL_Job IS 'Head Job Name';

drop table etlrep.ETL_Job_GroupChild;

CREATE TABLE etlrep.ETL_Job_GroupChild
     (
      GroupName varchar2(50)  NOT NULL,
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      Description varchar2(50) ,
      Enable CHAR(1) ,
      CheckFlag CHAR(1) ,
      TxDate CHAR(10) ,
      TurnOnFlag CHAR(1) );
      
create index idx_ETL_Job_GroupChild on etlrep.ETL_Job_GroupChild  (GroupName ,ETL_System ,ETL_Job );

COMMENT ON COLUMN etlrep.ETL_Job_GroupChild.ETL_System IS 'Child System Name';
COMMENT ON COLUMN etlrep.ETL_Job_GroupChild.ETL_Job IS 'Child Job Name';

drop table etlrep.ETL_Job_Log;

CREATE TABLE etlrep.ETL_Job_Log
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      JobSessionID INTEGER  NOT NULL,
      ScriptFile varchar2(60)  NOT NULL,
      TXDate DATE  NOT NULL,
      StartTime CHAR(19) ,
      EndTime CHAR(19) ,
      ReturnCode INTEGER,
      Seconds INTEGER);
      
create index idx_ETL_Job_Log on etlrep.ETL_Job_Log  ( ETL_System ,ETL_Job ,JobSessionID ,ScriptFile);

COMMENT ON COLUMN etlrep.ETL_Job_Log.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Job_Log.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_Job_Log.JobSessionID IS 'Job Session ID';
COMMENT ON COLUMN etlrep.ETL_Job_Log.TXDate IS 'YY/MM/DD';

drop table etlrep.ETL_Job_Queue;

CREATE TABLE etlrep.ETL_Job_Queue
     (
      ETL_Server varchar2(10)   NOT NULL,
      SeqID INTEGER NOT NULL,
      ETL_System CHAR(3)  NOT NULL,
      ETL_Job varchar2(50)  NOT NULL,
      TXDate DATE ,
      RequestTime varchar2(19)  );
      
create index idx_ETL_Job_Queue on etlrep.ETL_Job_Queue  (ETL_Server ,SeqID );

COMMENT ON COLUMN etlrep.ETL_Job_Queue.ETL_Server IS 'ETL Server';
COMMENT ON COLUMN etlrep.ETL_Job_Queue.TXDate IS 'YYYY-MM-DD';
COMMENT ON COLUMN etlrep.ETL_Job_Queue.RequestTime IS 'Request Time';

drop table etlrep.ETL_Job_Source;

CREATE TABLE etlrep.ETL_Job_Source
     (
      Source varchar2(36)  NOT NULL,
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      Conv_File_Head varchar2(50)   NOT NULL,
      AutoFilter CHAR(1)  ,
      Alert CHAR(1)  ,
      BeforeHour INTEGER ,
      BeforeMin INTEGER ,
      OffsetDay INTEGER ,
      LastCount INTEGER );
      
create index idx_ETL_Job_Source on etlrep.ETL_Job_Source  (  Source,ETL_System ,Conv_File_Head);

COMMENT ON COLUMN etlrep.ETL_Job_Source.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Job_Source.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_Job_Source.Conv_File_Head IS 'Convert File Header';
COMMENT ON COLUMN etlrep.ETL_Job_Source.AutoFilter IS 'Filter Out Duplicate File';
COMMENT ON COLUMN etlrep.ETL_Job_Source.Alert IS 'Alert When Missing';
COMMENT ON COLUMN etlrep.ETL_Job_Source.BeforeHour IS 'Before Hour';
COMMENT ON COLUMN etlrep.ETL_Job_Source.BeforeMin IS 'Before Min';
COMMENT ON COLUMN etlrep.ETL_Job_Source.OffsetDay IS 'Offset Day';
COMMENT ON COLUMN etlrep.ETL_Job_Source.LastCount IS 'Last Count';


drop table etlrep.ETL_Job_Status;

CREATE TABLE etlrep.ETL_Job_Status
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      JobSessionID INTEGER  NOT NULL,
      TXDate DATE  NOT NULL,
      StartTime CHAR(19) ,
      EndTime CHAR(19) ,
      JobStatus varchar2(20) ,
      FileCnt INTEGER,
      CubeStatus varchar2(20) ,
      ExpectedRecord INTEGER );
      
create index idx_ETL_Job_Status on etlrep.ETL_Job_Status  (ETL_System ,ETL_Job ,JobSessionID );

COMMENT ON COLUMN etlrep.ETL_Job_Status.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Job_Status.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_Job_Status.JobSessionID IS 'Job Session ID';
COMMENT ON COLUMN etlrep.ETL_Job_Status.TXDate IS 'YYYY-MM-DD';
COMMENT ON COLUMN etlrep.ETL_Job_Status.ExpectedRecord IS 'Expected Record';


drop table etlrep.ETL_Job_Stream;

CREATE TABLE etlrep.ETL_Job_Stream
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      Stream_System CHAR(3)   NOT NULL,
      Stream_Job varchar2(50)   NOT NULL,
      Description varchar2(50) ,
      Enable CHAR(1) );
      
create index idx_ETL_Job_Stream on etlrep.ETL_Job_Stream  ( ETL_System ,ETL_Job ,Stream_System ,Stream_Job);

COMMENT ON COLUMN etlrep.ETL_Job_Stream.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Job_Stream.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_Job_Stream.Stream_System IS 'Down Stream System Name';
COMMENT ON COLUMN etlrep.ETL_Job_Stream.Stream_Job IS 'Down Stream Job Name';

drop table etlrep.ETL_Job_TimeWindow;

CREATE TABLE etlrep.ETL_Job_TimeWindow
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      Allow CHAR(1) ,
      BeginHour INTEGER,
      EndHour INTEGER);
      
create index idx_ETL_Job_TimeWindow on etlrep.ETL_Job_TimeWindow  ( ETL_System ,ETL_Job );

COMMENT ON COLUMN etlrep.ETL_Job_TimeWindow.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Job_TimeWindow.ETL_Job IS 'ETL Job Name';

drop table etlrep.ETL_Job_Trace;

CREATE TABLE etlrep.ETL_Job_Trace
     (
      ETL_System CHAR(3)  NOT NULL,
      ETL_Job varchar2(50)  NOT NULL,
      TXDate DATE NOT NULL,
      JobStatus varchar2(20) ,
      StartTime CHAR(19) ,
      EndTime CHAR(19)  
      );
      
create index idx_ETL_Job_Trace on etlrep.ETL_Job_Trace  ( ETL_System ,ETL_Job ,TXDate );

COMMENT ON COLUMN etlrep.ETL_Job_Trace.TXDate IS 'YY/MM/DD';
COMMENT ON COLUMN etlrep.ETL_Job_Trace.JobStatus IS 'JobStatus';
COMMENT ON COLUMN etlrep.ETL_Job_Trace.StartTime IS 'StartTime';
COMMENT ON COLUMN etlrep.ETL_Job_Trace.EndTime IS 'EndTime';

drop table etlrep.ETL_Notification;

CREATE TABLE etlrep.ETL_Notification
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      SeqID INTEGER  NOT NULL,
      DestType CHAR(1)  ,
      GroupName varchar2(15)  ,
      UserName varchar2(15) ,
      Timing CHAR(1) ,
      AttachLog CHAR(1) ,
      Email CHAR(1)  ,
      ShortMessage CHAR(1)  ,
      MessageSubject varchar2(160)  ,
      MessageContent varchar2(255)  );
      
create index idx_ETL_Notification on etlrep.ETL_Notification  ( ETL_System ,ETL_Job ,SeqID );

COMMENT ON COLUMN etlrep.ETL_Notification.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Notification.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_Notification.SeqID IS 'Sequence ID';
COMMENT ON COLUMN etlrep.ETL_Notification.DestType IS 'Destination Type';
COMMENT ON COLUMN etlrep.ETL_Notification.GroupName IS 'User Group Name';
COMMENT ON COLUMN etlrep.ETL_Notification.UserName IS 'UserName';
COMMENT ON COLUMN etlrep.ETL_Notification.Email IS 'Through Email';
COMMENT ON COLUMN etlrep.ETL_Notification.ShortMessage IS 'Through Short Message';
COMMENT ON COLUMN etlrep.ETL_Notification.MessageSubject IS 'Message Subject';
COMMENT ON COLUMN etlrep.ETL_Notification.MessageContent IS 'Message Content';

drop table etlrep.ETL_Received_File;

CREATE TABLE etlrep.ETL_Received_File
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      JobSessionID INTEGER  NOT NULL,
      ReceivedFile varchar2(50)   NOT NULL,
      FileSize DECIMAL(18,0) ,
      ExpectedRecord INTEGER ,
      ArrivalTime CHAR(19)  ,
      ReceivedTime CHAR(19)  ,
      Location varchar2(128) ,
      Status CHAR(1) );
      
create index idx_ETL_Received_File on etlrep.ETL_Received_File  ( ETL_System ,ETL_Job ,ReceivedFile);

COMMENT ON COLUMN etlrep.ETL_Received_File.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Received_File.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_Received_File.JobSessionID IS 'Job Session ID';
COMMENT ON COLUMN etlrep.ETL_Received_File.ReceivedFile IS 'Received File Name';
COMMENT ON COLUMN etlrep.ETL_Received_File.FileSize IS 'File Size';
COMMENT ON COLUMN etlrep.ETL_Received_File.ExpectedRecord IS 'Expected Record';
COMMENT ON COLUMN etlrep.ETL_Received_File.ArrivalTime IS 'Arrival Time';
COMMENT ON COLUMN etlrep.ETL_Received_File.ReceivedTime IS 'Received Time';

drop table etlrep.ETL_Record_Log;

CREATE TABLE etlrep.ETL_Record_Log
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      JobSessionID INTEGER  NOT NULL,
      RecordTime CHAR(19) ,
      InsertedRecord INTEGER ,
      UpdatedRecord INTEGER ,
      DeletedRecord INTEGER ,
      DuplicateRecord INTEGER ,
      OutputRecord INTEGER ,
      ETRecord INTEGER ,
      UVRecord INTEGER,
      ER1Record INTEGER);
      
create index idx_ETL_Record_Log on etlrep.ETL_Record_Log  ( ETL_System ,ETL_Job ,JobSessionID );

COMMENT ON COLUMN etlrep.ETL_Record_Log.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Record_Log.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_Record_Log.JobSessionID IS 'Job Session ID';
COMMENT ON COLUMN etlrep.ETL_Record_Log.InsertedRecord IS 'InsertedRecord';
COMMENT ON COLUMN etlrep.ETL_Record_Log.UpdatedRecord IS 'UpdatedRecord';
COMMENT ON COLUMN etlrep.ETL_Record_Log.DeletedRecord IS 'DeletedRecord';
COMMENT ON COLUMN etlrep.ETL_Record_Log.DuplicateRecord IS 'DuplicateRecord';
COMMENT ON COLUMN etlrep.ETL_Record_Log.OutputRecord IS 'OutputRecord';
COMMENT ON COLUMN etlrep.ETL_Record_Log.ETRecord IS 'ET Record';
COMMENT ON COLUMN etlrep.ETL_Record_Log.UVRecord IS 'UV Record';
COMMENT ON COLUMN etlrep.ETL_Record_Log.ER1Record IS 'ER1 Record';


drop table etlrep.ETL_RelatedJob;

CREATE TABLE etlrep.ETL_RelatedJob
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      RelatedSystem CHAR(3)   NOT NULL,
      RelatedJob varchar2(50)   NOT NULL,
      CheckMode CHAR(1)  ,
      Description varchar2(50) );
      
create index idx_ETL_RelatedJob on etlrep.ETL_RelatedJob  ( ETL_System ,ETL_Job ,RelatedSystem ,RelatedJob );

COMMENT ON COLUMN etlrep.ETL_RelatedJob.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_RelatedJob.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_RelatedJob.RelatedSystem IS 'Related Job System';
COMMENT ON COLUMN etlrep.ETL_RelatedJob.RelatedJob IS 'Related Job Name';
COMMENT ON COLUMN etlrep.ETL_RelatedJob.CheckMode IS 'Checking Mode';


drop table etlrep.ETL_Server;

CREATE TABLE etlrep.ETL_Server
     (
      ETL_Server varchar2(10)   NOT NULL,
      Description varchar2(50) ,
      IPAddress varchar2(15)  ,
      AgentPort INTEGER,
      LiveCount INTEGER);
      
create index idx_ETL_Server on etlrep.ETL_Server  (ETL_Server );

COMMENT ON COLUMN etlrep.ETL_Server.ETL_Server IS 'ETL Server Name';
COMMENT ON COLUMN etlrep.ETL_Server.IPAddress IS 'IP Address';
COMMENT ON COLUMN etlrep.ETL_Server.AgentPort IS 'Agent Port';

drop table etlrep.ETL_Sys;

CREATE TABLE etlrep.ETL_Sys
     (
      ETL_System CHAR(3)   NOT NULL,
      Description varchar2(50) ,
      DataKeepPeriod INTEGER ,
      LogKeepPeriod INTEGER ,
      RecordKeepPeriod INTEGER );
      
create index idx_ETL_Sys on etlrep.ETL_Sys  (ETL_System );

COMMENT ON COLUMN etlrep.ETL_Sys.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_Sys.DataKeepPeriod IS 'Data Keep Period';
COMMENT ON COLUMN etlrep.ETL_Sys.LogKeepPeriod IS 'Log File Keep Period';
COMMENT ON COLUMN etlrep.ETL_Sys.RecordKeepPeriod IS 'Record Keep Period';

drop table etlrep.ETL_TABLE;

CREATE TABLE etlrep.ETL_TABLE
     (
      tablename varchar2(40) ,
      tabledesc varchar2(60) );
      
create index idx_ETL_TABLE on etlrep.ETL_TABLE  ( tablename ,tabledesc );


drop table etlrep.ETL_TimeTrigger;

CREATE TABLE etlrep.ETL_TimeTrigger
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      TriggerType CHAR(1)  ,
      StartHour INTEGER ,
      StartMin INTEGER ,
      OffsetDay INTEGER ,
      LastRunDate INTEGER ,
      LastRunTime INTEGER );
      
create index idx_ETL_TimeTrigger on etlrep.ETL_TimeTrigger  (  ETL_System ,ETL_Job);

COMMENT ON COLUMN etlrep.ETL_TimeTrigger.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger.TriggerType IS 'Trigger Type';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger.StartHour IS 'Start Hour';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger.StartMin IS 'Start Min';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger.OffsetDay IS 'Offset Day';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger.LastRunDate IS 'Last Run Date';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger.LastRunTime IS 'Last Run Time';

drop table etlrep.ETL_TimeTrigger_Calendar;

CREATE TABLE etlrep.ETL_TimeTrigger_Calendar
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      Seq INTEGER NOT NULL,
      YearNum INTEGER ,
      MonthNum INTEGER ,
      DayNum INTEGER );
      
create index idx_ETL_TimeTrigger_Calendar on etlrep.ETL_TimeTrigger_Calendar  ( ETL_System ,ETL_Job ,Seq );

COMMENT ON COLUMN etlrep.ETL_TimeTrigger_Calendar.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger_Calendar.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger_Calendar.YearNum IS 'Year';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger_Calendar.MonthNum IS 'Month';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger_Calendar.DayNum IS 'Day';

drop table etlrep.ETL_TimeTrigger_Monthly;

CREATE TABLE etlrep.ETL_TimeTrigger_Monthly
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      Timing CHAR(31)  NOT NULL,
      EndOfMonth CHAR(1)   NOT NULL);
      
create index idx_ETL_TimeTrigger_Monthly on etlrep.ETL_TimeTrigger_Monthly  ( ETL_System ,ETL_Job );

COMMENT ON COLUMN etlrep.ETL_TimeTrigger_Monthly.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger_Monthly.ETL_Job IS 'ETL Job Name';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger_Monthly.EndOfMonth IS 'End Of Month';

drop table etlrep.ETL_TimeTrigger_Weekly;

CREATE TABLE etlrep.ETL_TimeTrigger_Weekly
     (
      ETL_System CHAR(3)   NOT NULL,
      ETL_Job varchar2(50)   NOT NULL,
      Timing CHAR(7)  NOT NULL);
      
create index idx_ETL_TimeTrigger_Weekly on etlrep.ETL_TimeTrigger_Weekly  ( ETL_System ,ETL_Job );

COMMENT ON COLUMN etlrep.ETL_TimeTrigger_Weekly.ETL_System IS 'ETL System Name';
COMMENT ON COLUMN etlrep.ETL_TimeTrigger_Weekly.ETL_Job IS 'ETL Job Name';

drop table etlrep.ETL_User;

CREATE TABLE etlrep.ETL_User
     (
      UserName varchar2(15)   NOT NULL,
      Description varchar2(50) ,
      Email varchar2(50)  ,
      Mobile varchar2(20)  ,
      Status CHAR(1)  );
      
create unique index idx_ETL_User on etlrep.ETL_User  (UserName );

COMMENT ON COLUMN etlrep.ETL_User.UserName IS 'User Name';
COMMENT ON COLUMN etlrep.ETL_User.Email IS 'Email Address';
COMMENT ON COLUMN etlrep.ETL_User.Mobile IS 'Mobile Number';
COMMENT ON COLUMN etlrep.ETL_User.Status IS 'User Status';

drop table etlrep.ETL_UserGroup;

CREATE TABLE etlrep.ETL_UserGroup
     (
      GroupName varchar2(15)   NOT NULL,
      Description varchar2(50) );
      
create unique index idx_ETL_UserGroup on etlrep.ETL_UserGroup  (GroupName );

COMMENT ON COLUMN etlrep.ETL_UserGroup.GroupName IS 'User Group Name';

