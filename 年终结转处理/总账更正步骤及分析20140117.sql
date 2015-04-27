正确操作步骤：
卸数共3份临时区数据（二、四、五批次）
一、拿第四次临时区总账表跑批生成基础、汇总数据，对数据进行备份；
二、对二、五次临时区数据进行整合；（将二级支行损益类科目还原到五批次中）
三、对二、五次的整合的临时表进行跑批，生成基础、汇总数据 进行备份；
四、将第四次汇总表中的总行损益类科目替换掉整合出来的汇总的总行损益类科目；    
五、最终生成汇总区数据

一、损益类比对：
总行比对：生产与验证环境一致
一级支行汇总比对：生产为0，验证有值；
二级支行汇总比对： 生产为0，验证有值

二、非损益类比对：
总行比对:生产与第四批次数据一致；验证环境与第五批次数据一致；
一级支行：生产与第四批次数据一致；验证环境与第五批次数据一致；
二级支行：生产与第四批次数据一致；验证环境与第五批次数据一致；

三、各层比对：
 
临时区：各批次相对应比对，数据一致；
基础层：生产多出33个无效机构的数据，导致记录数不一致，目前暂未找到原因；
汇总区：3张总账表，生产上与验证环境比对，数据不一致；（除了上述的总行损益类外）
 

-- check_nzjz.sql
psql -a -h 31.2.2.107 -U etluser whrcb_edw -f check_nzjz.sql | tee check_nzjz_107.log
psql -a -h 31.2.2.69 -U etluser whrcb_edw -f check_nzjz.sql | tee check_nzjz_69.log
-- 一、损益类比对：
--总行比对：生产与验证环境一致
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum--生产 
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01900' and ccy_cd not in ('R01','U01','T01','Z01');
--13527162792.48;17616991470.78
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum_bak --验证
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01900' and ccy_cd not in ('R01','U01','T01','Z01');
--13527162792.48;17616991470.78

--一级支行汇总比对：生产为0，验证有值；
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum --生产 
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01904' and ccy_cd not in ('R01','U01','T01','Z01');
--0.00;0.00
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum_bak--验证
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01904' and ccy_cd not in ('R01','U01','T01','Z01');
--303097468.54;571208548.40

--二级支行汇总比对： 生产为0，验证有值
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum --生产 
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01059' and ccy_cd not in ('R01','U01','T01','Z01');
--0.00;0.00
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum_bak--验证
where statst_Dt ='20131231' and item_no like '6%' and org_no ='01059' and ccy_cd not in ('R01','U01','T01','Z01');
--23040565.26;36045461.48



-- 二、非损益类比对：
--全行比对：
--临时四批次、五批次  数据不一致；
select sum(currdbbal),sum(currcrbal) from sdsdata.s01_glssubdyn1231 where subctrlcode like '1001%' ;
--5批次：701148208.72;0.00    
select sum(currdbbal),sum(currcrbal) from sdsdata.s01_glssubdyntmp11231 where subctrlcode like'1001%' and brccode ='01001' ;
--4批次：701157708.72;0.00


--基础总账表： 生产、验证一致
select sum(cur_deb_bal),sum(cur_crd_bal) from bdsdata.T09_GL_ITEM_DYNAM_HIS 
where s_date<= date'20131231' and e_date >'20131231' and item_ctrl_field like '1001%';
-- 生产：701148208.72;0.00
select sum(cur_deb_bal),sum(cur_crd_bal) from bdsdata.T09_GL_ITEM_DYNAM_HIS_BAK 
where s_date<= date'20131231' and e_date >'20131231' and item_ctrl_field like '1001%';
--验证：701148208.72;0.00

--汇总末级科目表： 生产第四批次数据，验证第五批次数据
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_last_item_day_sum where statst_Dt ='20131231' and item_no like '1001%';
--生产：701157708.72
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_last_item_day_sum_bak where statst_Dt ='20131231' and item_no like '1001%';
--验证环境：701148208.72

--汇总一级科目表：生产第四批次数据，验证第五批次数据
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_first_item_day_sum where statst_Dt ='20131231' and item_no ='1001';
--生产：701157708.72;
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_first_item_day_sum_bak where statst_Dt ='20131231' and item_no ='1001';
--验证环境：701148208.72;

--汇总总账表：生产第四批次数据，验证第五批次数据
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum 
where statst_Dt ='20131231' and item_no = '1001' and org_no ='01900' and ccy_cd not in ('R01','U01','T01','Z01');
--生产：701157708.72
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum_bak
where statst_Dt ='20131231' and item_no = '1001' and org_no ='01900' and ccy_cd not in ('R01','U01','T01','Z01');
--验证环境：701148208.72;0.00 




--二级支行：

--汇总总账表：生产第四批次数据，验证第五批次数据不一致 
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum 
where statst_Dt ='20131231' and item_no = '1001' and org_no not like '019%' and ccy_cd not in ('R01','U01','T01','Z01');
--生产：701157708.72
select sum(day_deb_bal),sum(day_crd_bal) from adsdata.c05_ccycom_item_day_sum_bak
where statst_Dt ='20131231' and item_no = '1001' and org_no not like '019%' and ccy_cd not in ('R01','U01','T01','Z01');
--验证环境：701148208.72




基础机构问题：
--找出所有不一样数据的机构
SELECT DISTINCT ORG_NO FROM (
SELECT * FROM BDSDATA.T09_GL_ITEM_DYNAM_HIS WHERE S_DATE<= '20131231' AND E_DATE >'20131231' --生产
EXCEPT ALL
SELECT * FROM BDSDATA.T09_GL_ITEM_DYNAM_HIS_BAK WHERE S_DATE<= '20131231' AND E_DATE >'20131231' --验证
) A


--检查机构是否有效；
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

                                                       