###############################################################################
# Program  : unload_main.sh                                                    
# Argument :                                                                   
#     ����1: SYSID-ϵͳID                                                      
#     ����2: FLG-���򴥷���־ 1-�ȴ�OK�ļ����� 2-ֱ��ִ��                      
# Created by : WangYi 2013-7-6                                                 
# Modified by : WangYi                                                         
# Function : unload data from oracle database                                  
###############################################################################
����˵������ϵͳ���ΪXXX��ϵͳΪ��

1�����û�������,��.profile�ļ���������������
   export UNLOAD_WORKDIR=$HOME/data_unload

2������$UNLOAD_WORKDIR/.sys.lst�ļ������ļ���β������������
   XXX|���ݿ����Ӵ�|���ݱ�������|ϵͳ���ƣ�����S08ϵͳ��S08|gtp/gtp@11.1.1.123:1521/bbsp|10000|Ʊ��

3������ж�����ڲ���
   mkdir $UNLOAD_WORKDIR/cfg/XXX
   cd $UNLOAD_WORKDIR/cfg/XXX
   echo "YYYYMMDD" > XXX.cfg��YYYYMMDDΪ�������ж�����ڣ������ڲ��ܴ��ڵ�ǰϵͳ���ڣ�
   ˵��������ÿ�ɹ�ִ��һ�θ����ڻ��Զ���һ�������ظ�ж��ͬһ�������ݣ����ֹ��޸�XXX.cfg�ļ��е�����
   
4������ж���ű��ļ�
   mkdir $UNLOAD_WORKDIR/sql/XXX
   cd $UNLOAD_WORKDIR/sql/XXX
   ������༭����ж���ű��ļ���$UNLOAD_WORKDIR/sql/XXXĿ¼�£�ж���ű��ļ���Ϊ[Դϵͳ����ȫ��д].sql����abc��ű��ļ�ΪABC.sql��
   �ű��ļ�����Ϊselect��䣬������й淶��д
   ���ݱ�ж������Ϊ������ȡ����ʱ�����ڱ���ʹ��#*P_DATE*#�ַ����滻����abc��tran_date�ֶ�ȡ����������������дΪwhere tran_date='#*P_DATE*#'��

5������ж��Ŀ��Ŀ¼
   ȷ��$UNLOAD_WORKDIR/FILEĿ¼����
   ����$UNLOAD_WORKDIR/FILEĿ¼����Ϊ���ӣ�����Ŀ��ָ��/EDW/DATA/SOURCE
   

ʹ��˵������ϵͳ���ΪS08��ϵͳж��2013��7��6������Ϊ��
1���ֹ�ִ��
   ȷ��$UNLOAD_WORKDIR/cfg/S08/S08.cfg�ļ���һ������Ϊ20130706
   �ȴ�OK�ļ�������$UNLOAD_WORKDIR/bin/unload_main.sh S08 1
   ֱ��ִ�У�      $UNLOAD_WORKDIR/bin/unload_main.sh S08 2

2����ʱ������ÿ��21��30������Ϊ��
   ȷ��$UNLOAD_WORKDIR/cfg/S08/S08.cfg�ļ���һ������Ϊ20130706
   crontab -e��������һ������
   �ȴ�OK�ļ�������30 21 * * * /����·��/unload_main.sh S08 1 >>/��־�ļ�����·��/S08.log 2>&1   ����־�ļ�����·������Ϊ$UNLOAD_WORKDIR/log/S08��
   ֱ��ִ�У�      30 21 * * * /����·��/unload_main.sh S08 2 >>/��־�ļ�����·��/S08.log 2>&1   ����־�ļ�����·������Ϊ$UNLOAD_WORKDIR/log/S08��

00 05 * * * /home/gtp/data_unload/bin/unload_main.sh S01 1 >> /home/gtp/data_unload/log/S01.log 2>&1
15 00 * * * /home/gtp/data_unload/bin/unload_main.sh S04 2 >> /home/gtp/data_unload/log/S08.log 2>&1
00 21 * * * /home/gtp/data_unload/bin/unload_main.sh S06 2 >> /home/gtp/data_unload/log/S06.log 2>&1
00 02 * * * /home/gtp/data_unload/bin/unload_main.sh S08 2 >> /home/gtp/data_unload/log/S08.log 2>&1
15 00 * * * /home/gtp/data_unload/bin/unload_main.sh S09 2 >> /home/gtp/data_unload/log/S09.log 2>&1
15 00 * * * /home/gtp/data_unload/bin/unload_main.sh S10 2 >> /home/gtp/data_unload/log/S10.log 2>&1