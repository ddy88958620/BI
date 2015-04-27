#ifndef _INCLUDE_COMM
#define _INCLUDE_COMM 1
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>


EXEC SQL BEGIN DECLARE SECTION;
  char CmdStr[500][500],TmpStrs[500][500];
EXEC SQL END DECLARE SECTION;

struct DirBufStruct  {
  char fname[60];   /* �ļ��� */
  time_t dates;     /* �޸����ڣ���ת�� */
  mode_t mode;      /* ���͡�Ȩ�ޣ�ʹ��8������ʾ */
  off_t size;       /* �ļ���С */
  int  mark;
} DirBuf[9000], *DirPtr;
int DirTotals;


int DeleteSpace( char *ss);
int SplitBuf1(char *buf1, char ch, char tc,int stno);
int SplitBuf2(char *buf2, char ch, char tc,int stno);
int GetCurrentTime(int secs,char *stime);
int GetCurrentDate(int secs,char *stime);
int GetStrTime(int secs,char *stime);
int ReplaceStr( char * s1, char * s2, char * s3 );
int CountFileLine(char * filename);
int GetKind(char *stime,char *etime);
int GetDaysOfYearmon(char *yearmon);


#endif
