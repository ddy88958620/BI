��ȷ�������裺
ж����3����ʱ�����ݣ������ġ������Σ�
һ���õ��Ĵ���ʱ�����˱��������ɻ������������ݣ������ݽ��б��ݣ�
�����Զ��������ʱ�����ݽ������ϣ���������֧���������Ŀ��ԭ���������У�
�����Զ�����ε����ϵ���ʱ��������������ɻ������������� ���б��ݣ�
�ġ������Ĵλ��ܱ��е������������Ŀ�滻�����ϳ����Ļ��ܵ������������Ŀ��    
�塢�������ɻ���������

һ��������ȶԣ�
���бȶԣ���������֤����һ��
һ��֧�л��ܱȶԣ�����Ϊ0����֤��ֵ��
����֧�л��ܱȶԣ� ����Ϊ0����֤��ֵ

������������ȶԣ�
���бȶ�:�����������������һ�£���֤�����������������һ�£�
һ��֧�У������������������һ�£���֤�����������������һ�£�
����֧�У������������������һ�£���֤�����������������һ�£�

��������ȶԣ�
 
��ʱ�������������Ӧ�ȶԣ�����һ�£�
�����㣺�������33����Ч���������ݣ����¼�¼����һ�£�Ŀǰ��δ�ҵ�ԭ��
��������3�����˱�����������֤�����ȶԣ����ݲ�һ�£������������������������⣩
 

-- check_nzjz.sql
psql -a -h 31.2.2.107 -U etluser whrcb_edw -f check_nzjz.sql | tee check_nzjz_107.log
psql -a -h 31.2.2.69 -U etluser whrcb_edw -f check_nzjz.sql | tee check_nzjz_69.log
-- һ��������ȶԣ�
--���бȶԣ���������֤����һ��
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum--���� 
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01900' and ccy_cd not in ('R01','U01','T01','Z01');
--13527162792.48;17616991470.78
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum_bak --��֤
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01900' and ccy_cd not in ('R01','U01','T01','Z01');
--13527162792.48;17616991470.78

--һ��֧�л��ܱȶԣ�����Ϊ0����֤��ֵ��
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum --���� 
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01904' and ccy_cd not in ('R01','U01','T01','Z01');
--0.00;0.00
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum_bak--��֤
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01904' and ccy_cd not in ('R01','U01','T01','Z01');
--303097468.54;571208548.40

--����֧�л��ܱȶԣ� ����Ϊ0����֤��ֵ
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum --���� 
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01059' and ccy_cd not in ('R01','U01','T01','Z01');
--0.00;0.00
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum_bak--��֤
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01059' and ccy_cd not in ('R01','U01','T01','Z01');
--23040565.26;36045461.48



-- ������������ȶԣ�
--ȫ�бȶԣ�
--��ʱ�����Ρ�������  ���ݲ�һ�£�
select sum(currdbbal),sum(currcrbal) from sdsdata.s01_glssubdyn1231 where subctrlcode like '1001%' ;
--5���Σ�701148208.72;0.00    
select sum(currdbbal),sum(currcrbal) from sdsdata.s01_glssubdyntmp11231 where subctrlcode like'1001%' and brccode ='01001' ;
--4���Σ�701157708.72;0.00


--�������˱� ��������֤һ��
select sum(cur_deb_bal),sum(cur_crd_bal) from bdsdata.T09_GL_ITEM_DYNAM_HIS 
where s_date<= date'20131231' and e_date >'20131231' and item_ctrl_field like '1001%';
-- ������701148208.72;0.00
select sum(cur_deb_bal),sum(cur_crd_bal) from bdsdata.T09_GL_ITEM_DYNAM_HIS_BAK 
where s_date<= date'20131231' and e_date >'20131231' and item_ctrl_field like '1001%';
--��֤��701148208.72;0.00

--����ĩ����Ŀ�� ���������������ݣ���֤������������
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_last_item_day_sum where statst_Dt ='20131231' and item_no like '1001%';
--������701157708.72
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_last_item_day_sum_bak where statst_Dt ='20131231' and item_no like '1001%';
--��֤������701148208.72

--����һ����Ŀ�����������������ݣ���֤������������
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_first_item_day_sum where statst_Dt ='20131231' and item_no ='1001';
--������701157708.72;
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_first_item_day_sum_bak where statst_Dt ='20131231' and item_no ='1001';
--��֤������701148208.72;

--�������˱����������������ݣ���֤������������
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum 
where statst_Dt ='20131231' and item_no = '1001' and org_no ='01900' and ccy_cd not in ('R01','U01','T01','Z01');
--������701157708.72
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum_bak
where statst_Dt ='20131231' and item_no = '1001' and org_no ='01900' and ccy_cd not in ('R01','U01','T01','Z01');
--��֤������701148208.72;0.00 




--����֧�У�

--�������˱����������������ݣ���֤�����������ݲ�һ�� 
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum 
where statst_Dt ='20131231' and item_no = '1001' and org_no not like '019%' and ccy_cd not in ('R01','U01','T01','Z01');
--������701157708.72
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum_bak
where statst_Dt ='20131231' and item_no = '1001' and org_no not like '019%' and ccy_cd not in ('R01','U01','T01','Z01');
--��֤������701148208.72




�����������⣺
--�ҳ����в�һ�����ݵĻ���
SELECT DISTINCT ORG_NO FROM (
SELECT * FROM BDSDATA.T09_GL_ITEM_DYNAM_HIS WHERE S_DATE<= '20131231' AND E_DATE >'20131231' --����
EXCEPT ALL
SELECT * FROM BDSDATA.T09_GL_ITEM_DYNAM_HIS_BAK WHERE S_DATE<= '20131231' AND E_DATE >'20131231' --��֤
) A


--�������Ƿ���Ч��
select DISTINCT FLAG_1_CD from bdsdata.t04_INTER_ORG_HIS WHERE ORG_no  in (
'01028'   
,'01161'  
,'01117'  
,'01225'  
,'01158'  
,'01226'  
,'01105'  
,'01228'  
,'01140'  
,'01064'  
,'01050'  
,'01106'  
,'01205'  
,'01237'  
,'01089'  
,'01236'  
,'01269'  
,'01046'  
,'01055'  
,'01060'  
,'01150'  
,'01187'  
,'01016'  
,'01085'  
,'01096'  
,'01014'  
,'01072'  
,'01208'  
,'01013'  
,'01181'  
,'01081'  
,'01180'  
,'01010'  
)                

                                                       