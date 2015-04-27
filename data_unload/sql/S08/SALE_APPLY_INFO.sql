select SAAP_ID
,trim(regexp_replace(BUSI_NO,'/','//'))
,trim(regexp_replace(AIM_BRCH_NO,'/','//'))
,trim(regexp_replace(PROD_NO,'/','//'))
,trim(regexp_replace(SAAP_TYPE,'/','//'))
,trim(regexp_replace(SAAP_CLASS,'/','//')),CUST_ID
,trim(regexp_replace(INNER_ACCOUNT,'/','//')),SALE_DT,REBUY_DUE_DT,RATE
,trim(regexp_replace(RATE_TYPE,'/','//'))
,trim(regexp_replace(ACCRUAL_CAL_TYPE,'/','//'))
,trim(regexp_replace(IF_DUMMY,'/','//'))
,trim(regexp_replace(IF_INNER,'/','//')),BIDECT_DUE_DT
,trim(regexp_replace(IF_RECOURSE,'/','//')),OPER_ID
,trim(regexp_replace(ACCRUAL_TYPE,'/','//'))
,trim(regexp_replace(BRCH_ID,'/','//'))
,trim(regexp_replace(IF_BIDIR_BUY,'/','//'))
,trim(regexp_replace(STATUS,'/','//')),CREATE_TIME
,trim(regexp_replace(IS_ONLINE,'/','//')),BUYBACK_OPEN_DT,BUYBACK_RATE
,trim(regexp_replace(IS_REDISC,'/','//'))
,trim(regexp_replace(SALE_TYPE,'/','//')),BUYBACK_MONEY
,trim(regexp_replace(FORBID_FLAG,'/','//'))
,trim(regexp_replace(REMARK,'/','//')),TEMP_NMFSID from SALE_APPLY_INFO
