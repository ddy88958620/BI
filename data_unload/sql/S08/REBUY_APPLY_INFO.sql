select REAP_ID
,trim(regexp_replace(BUSI_NO,'/','//'))
,trim(regexp_replace(AIM_BRCH_NO,'/','//'))
,trim(regexp_replace(PROD_NO,'/','//'))
,trim(regexp_replace(APPLY_TYPE,'/','//'))
,trim(regexp_replace(APPLY_CLASS,'/','//')),CUST_ID
,trim(regexp_replace(INNER_ACCOUNT,'/','//')),REBUY_DT,RESALE_DUE_DT,RATE
,trim(regexp_replace(RATE_TYPE,'/','//'))
,trim(regexp_replace(ACCRUAL_CAL_TYPE,'/','//'))
,trim(regexp_replace(IF_DUMMY,'/','//'))
,trim(regexp_replace(IF_INNER,'/','//')),BIDECT_DUE_DT,OPER_ID
,trim(regexp_replace(ACCRUAL_TYPE,'/','//'))
,trim(regexp_replace(BRCH_ID,'/','//'))
,trim(regexp_replace(IF_BIDIR_SALE,'/','//'))
,trim(regexp_replace(STATUS,'/','//')),CUMA_ID
,trim(regexp_replace(CUMA_NAME,'/','//'))
,trim(regexp_replace(DEPT_NO,'/','//')),CREATE_TIME
,trim(regexp_replace(IS_USER_CTRCT,'/','//'))
,trim(regexp_replace(CTRCT_NB,'/','//')) from REBUY_APPLY_INFO
