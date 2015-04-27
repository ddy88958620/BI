
CREATE TABLE DataCalendar (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       CalendarYear         INTEGER NOT NULL,
       SeqNum               INTEGER NOT NULL,
       CalendarMonth        INTEGER NOT NULL,
       CalendarDay          INTEGER NOT NULL,
       CheckFlag            CHAR(1)
)
       UNIQUE PRIMARY INDEX XPKDataCalendar (
              ETL_System,
              ETL_Job,
              CalendarYear,
              SeqNum
       )
;


CREATE TABLE DataCalendarYear (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       CalendarYear         INTEGER NOT NULL
)
       UNIQUE PRIMARY INDEX XPKDataCalendarYear (
              ETL_System,
              ETL_Job,
              CalendarYear
       )
;


CREATE TABLE ETL_Event (
       EventID              VARCHAR(20) NOT NULL TITLE 'Event ID',
       EventStatus          CHAR(1) NOT NULL TITLE 'Event Status',
       Severity             CHAR(1) NOT NULL TITLE 'Event Severity',
       Description          VARCHAR(200) TITLE 'Event Description',
       LogTime              CHAR(19) NOT NULL TITLE 'Log Time',
       CloseTime            CHAR(19) TITLE 'Close Time'
)
       UNIQUE PRIMARY INDEX XPKETL_Event (
              EventID
       )
;

CREATE TABLE ETL_GroupMember (
       UserName             VARCHAR(15) NOT NULL TITLE 'User Name',
       GroupName            VARCHAR(15) NOT NULL TITLE 'User Group Name'
)
       UNIQUE PRIMARY INDEX XPKETL_GroupMember (
              UserName,
              GroupName
       )
;


CREATE TABLE ETL_Job (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       ETL_Server           VARCHAR(10) TITLE 'ETL Server Name',
       Description          VARCHAR(50),
       Frequency            VARCHAR(30),
       JobType              CHAR(1),
       Enable               CHAR(1),
       Last_StartTime       CHAR(19),
       Last_EndTime         CHAR(19),
       Last_JobStatus       VARCHAR(20),
       Last_TXDate          DATE FORMAT 'YYYY-MM-DD',
       Last_FileCnt         INTEGER,
       Last_CubeStatus      CHAR(20),
       CubeFlag             CHAR(1) TITLE 'Last_CubeFlag',
       CheckFlag            CHAR(1),
       AutoOff              CHAR(1),
       CheckCalendar        CHAR(1),
       CalendarBU           VARCHAR(15),
       RunningScript        VARCHAR(50) TITLE 'Running Script Name',
       JobSessionID         INTEGER TITLE 'Job Session ID',
       ExpectedRecord       INTEGER TITLE 'Expected Record',
       CheckLastStatus      CHAR(1)
)
       UNIQUE PRIMARY INDEX XAK1ETL_Job (
              ETL_Job,
              ETL_System
       )
;


CREATE TABLE ETL_Job_Dependency (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       Dependency_System    CHAR(3) NOT NULL TITLE 'Dependency System Name',
       Dependency_Job       VARCHAR(50) NOT NULL TITLE 'Dependency Job Name',
       Description          VARCHAR(50),
       Enable               CHAR(1)
)
       PRIMARY INDEX XIE1ETL_Job_Dependency (
              ETL_Job,
              ETL_System
       )
;

CREATE UNIQUE INDEX XAK1ETL_Job_Dependency (
              ETL_System,
              ETL_Job,
              Dependency_System,
              Dependency_Job
       ) ON ETL_Job_Dependency
;


CREATE TABLE ETL_Job_Group (
       GroupName            VARCHAR(50) NOT NULL,
       Description          VARCHAR(50),
       ETL_System           CHAR(3) TITLE 'Head System Name',
       ETL_Job              VARCHAR(50) TITLE 'Head Job Name',
       AutoOnChild          CHAR(1)
)
       UNIQUE PRIMARY INDEX XAK1ETL_Job_Group (
              GroupName
       )
;


CREATE TABLE ETL_Job_GroupChild (
       GroupName            VARCHAR(50) NOT NULL,
       ETL_System           CHAR(3) NOT NULL TITLE 'Child System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'Child Job Name',
       Description          VARCHAR(50),
       Enable               CHAR(1)
)
       UNIQUE PRIMARY INDEX XAK1ETL_Job_GroupChild (
              GroupName,
              ETL_System,
              ETL_Job
       )
;

CREATE UNIQUE INDEX XAK2ETL_Job_GroupChild (
              ETL_System,
              ETL_Job
       ) ON ETL_Job_GroupChild
;


CREATE TABLE ETL_Job_Log (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       JobSessionID         INTEGER NOT NULL TITLE 'Job Session ID',
       ScriptFile           VARCHAR(60) NOT NULL,
       TXDate               DATE NOT NULL,
       StartTime            CHAR(19),
       EndTime              CHAR(19),
       ReturnCode           INTEGER,
       Seconds              INTEGER
)
       UNIQUE PRIMARY INDEX XAK1ETL_Job_Log (
              ETL_System,
              ETL_Job,
              JobSessionID,
              ScriptFile
       )
;

CREATE TABLE ETL_Job_Queue (
       ETL_Server           VARCHAR(10) NOT NULL TITLE 'ETL Server',
       SeqID                INTEGER NOT NULL,
       ETL_System           CHAR(3) NOT NULL,
       ETL_Job              VARCHAR(50) NOT NULL,
       TXDate               DATE,
       RequestTime          VARCHAR(19) TITLE 'Request Time'
)
       UNIQUE PRIMARY INDEX XAK1ETL_Job_Queue (
              ETL_Server,
              SeqID
       )
;

CREATE TABLE ETL_Job_Source (
       Source               VARCHAR(36) NOT NULL,
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       Conv_File_Head       VARCHAR(50) NOT NULL TITLE 'Convert File Header',
       AutoFilter           CHAR(1) TITLE 'Filter Out Duplicate File',
       Alert                CHAR(1) TITLE 'Alert When Missing',
       BeforeHour           INTEGER TITLE 'Before Hour',
       BeforeMin            INTEGER TITLE 'Before Min',
       OffsetDay            INTEGER TITLE 'Offset Day',
       LastCount            INTEGER TITLE 'Last Count'
)
       UNIQUE PRIMARY INDEX XAK1ETL_Job_Source (
              Source
       )
;

CREATE UNIQUE INDEX XAK2ETL_Job_Source (
              ETL_System,
              Conv_File_Head
       ) ON ETL_Job_Source
;

CREATE TABLE ETL_Job_Status (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       JobSessionID         INTEGER NOT NULL TITLE 'Job Session ID',
       TXDate               DATE NOT NULL FORMAT 'YYYY-MM-DD',
       StartTime            CHAR(19),
       EndTime              CHAR(19),
       JobStatus            VARCHAR(20),
       FileCnt              INTEGER,
       CubeStatus           VARCHAR(20),
       ExpectedRecord       INTEGER TITLE 'Expected Record'
)
       UNIQUE PRIMARY INDEX XAK1ETL_Job_Status (
              ETL_System,
              ETL_Job,
              JobSessionID
       )
;


CREATE TABLE ETL_Job_Stream (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       Stream_System        CHAR(3) NOT NULL TITLE 'Down Stream System Name',
       Stream_Job           VARCHAR(50) NOT NULL TITLE 'Down Stream Job Name',
       Description          VARCHAR(50),
       Enable               CHAR(1)
)
       PRIMARY INDEX XIE1ETL_Job_Stream (
              ETL_System,
              ETL_Job
       )
;

CREATE UNIQUE INDEX XAK1ETL_Job_Stream (
              ETL_System,
              ETL_Job,
              Stream_System,
              Stream_Job
       ) ON ETL_Job_Stream
;


CREATE TABLE ETL_Job_TimeWindow (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       Allow                CHAR(1),
       BeginHour            INTEGER,
       EndHour              INTEGER
)
       UNIQUE PRIMARY INDEX XPKETL_Job_TimeWindow (
              ETL_System,
              ETL_Job
       )
;

CREATE TABLE ETL_Job_Trace (
       ETL_System           CHAR(3) NOT NULL,
       ETL_Job              VARCHAR(50) NOT NULL,
       TXDate               DATE NOT NULL,
       JobStatus            VARCHAR(20) TITLE 'Job Status',
       StartTime            CHAR(19) TITLE 'Start Time',
       EndTime              CHAR(19) TITLE 'End Time'
)
       UNIQUE PRIMARY INDEX XPKETL_Job_Trace (
              ETL_System,
              ETL_Job,
              TXDate
       )
;

CREATE TABLE ETL_Notification (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       SeqID                INTEGER NOT NULL TITLE 'Sequence ID',
       DestType             CHAR(1) TITLE 'Destination Type',
       GroupName            VARCHAR(15) TITLE 'User Group Name',
       UserName             VARCHAR(15) TITLE 'User Name',
       Timing               CHAR(1),
       AttachLog            CHAR(1),
       Email                CHAR(1) TITLE 'Through Email',
       ShortMessage         CHAR(1) TITLE 'Through Short Message',
       MessageSubject       VARCHAR(160) TITLE 'Message Subject',
       MessageContent       VARCHAR(255) TITLE 'Message Content'
)
       UNIQUE PRIMARY INDEX XAK1ETL_Notification (
              ETL_System,
              ETL_Job,
              SeqID
       )
;


CREATE TABLE ETL_Received_File (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       JobSessionID         INTEGER NOT NULL TITLE 'Job Session ID',
       ReceivedFile         VARCHAR(50) NOT NULL TITLE 'Received File Name',
       FileSize             DECIMAL(18,0) TITLE 'File Size',
       ExpectedRecord       INTEGER TITLE 'Expected Record',
       ArrivalTime          CHAR(19) TITLE 'Arrival Time',
       ReceivedTime         CHAR(19) TITLE 'Received Time',
       Location             VARCHAR(128),
       Status               CHAR(1)
)
       UNIQUE PRIMARY INDEX XAK1ETL_Received_File (
              ETL_System,
              ETL_Job,
              ReceivedFile
       )
;


CREATE TABLE ETL_Record_Log (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       JobSessionID         INTEGER NOT NULL TITLE 'Job Session ID',
       RecordTime           CHAR(19),
       InsertedRecord       INTEGER TITLE 'Inserted Record',
       UpdatedRecord        INTEGER TITLE 'Updated Record',
       DeletedRecord        INTEGER TITLE 'Deleted Record',
       DuplicateRecord      INTEGER TITLE 'Duplicate Record',
       OutputRecord         INTEGER TITLE 'Output Record',
       ETRecord             INTEGER TITLE 'ET Record',
       UVRecord             INTEGER TITLE 'UV Record',
       ER1Record            INTEGER TITLE 'ER1 Record',
       ER2Record            INTEGER TITLE 'ER2 Record'
)
       UNIQUE PRIMARY INDEX XAK1ETL_Record_Log (
              ETL_System,
              ETL_Job,
              JobSessionID
       )
;


CREATE TABLE ETL_RelatedJob (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       ETL_Job              VARCHAR(50) NOT NULL TITLE 'ETL Job Name',
       RelatedSystem        CHAR(3) NOT NULL TITLE 'Related Job System',
       RelatedJob           VARCHAR(50) NOT NULL TITLE 'Related Job Name',
       CheckMode            CHAR(1) TITLE 'Checking Mode',
       Description          VARCHAR(50)
)
       UNIQUE PRIMARY INDEX XPKETL_RelatedJob (
              ETL_System,
              ETL_Job,
              RelatedSystem,
              RelatedJob
       )
;


CREATE TABLE ETL_Server (
       ETL_Server           VARCHAR(10) NOT NULL TITLE 'ETL Server Name',
       Description          VARCHAR(50),
       IPAddress            VARCHAR(15) TITLE 'IP Address',
       AgentPort            INTEGER TITLE 'Agent Port',
       LiveCount            INTEGER
)
       UNIQUE PRIMARY INDEX XPKETL_Server (
              ETL_Server
       )
;


CREATE TABLE ETL_Sys (
       ETL_System           CHAR(3) NOT NULL TITLE 'ETL System Name',
       Description          VARCHAR(50),
       DataKeepPeriod       INTEGER TITLE 'Data Keep Period',
       LogKeepPeriod        INTEGER TITLE 'Log File Keep Period',
       RecordKeepPeriod     INTEGER TITLE 'Record Keep Period'
)
       UNIQUE PRIMARY INDEX XAK1ETL_Sys (
              ETL_System
       )
;

CREATE TABLE ETL_User (
       UserName             VARCHAR(15) NOT NULL TITLE 'User Name',
       Description          VARCHAR(50),
       Email                VARCHAR(50) TITLE 'Email Address',
       Mobile               VARCHAR(20) TITLE 'Mobile Number',
       Status               CHAR(1) TITLE 'User Status'
)
       UNIQUE PRIMARY INDEX XAK1ETL_User (
              UserName
       )
;


CREATE TABLE ETL_UserGroup (
       GroupName            VARCHAR(15) NOT NULL TITLE 'User Group Name',
       Description          VARCHAR(50)
)
       UNIQUE PRIMARY INDEX XPKETL_UserGroup (
              GroupName
       )
;
