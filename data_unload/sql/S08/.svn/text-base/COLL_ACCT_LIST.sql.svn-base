select ID,AB_ID,SC_ID,TRAN_DT,TRAN_TM
,trim(regexp_replace(FORE_FLOW_NO,'/','//'))
,trim(regexp_replace(BUSI_TYPE,'/','//'))
,trim(regexp_replace(STATUS,'/','//'))
,trim(regexp_replace(BACK_CODE,'/','//'))
,trim(regexp_replace(BACK_FLOW_NO,'/','//')),OPER_ID
,trim(regexp_replace(BRCH_ID,'/','//'))
,trim(regexp_replace(BUSI_TRADE_TYPE,'/','//')) from COLL_ACCT_LIST WHERE TRAN_DT=TO_DATE('#*P_DATE*#','YYYY-MM-DD')
