select ID
,trim(regexp_replace(BUSI_TYPE,'/','//'))
,trim(regexp_replace(ACCOUNT_TYPE,'/','//')),BUSI_ID
,trim(regexp_replace(OPER_NO,'/','//'))
,trim(regexp_replace(BRCH_ID,'/','//')),BUSI_DATE
,trim(regexp_replace(FRONT_FLOW_NO,'/','//'))
,trim(regexp_replace(AFTER_FLOW_NO,'/','//'))
,trim(regexp_replace(ACCT_FLAG,'/','//'))
,trim(regexp_replace(ACCT_MSG,'/','//'))
,trim(regexp_replace(CONTRACTNO,'/','//'))
,trim(regexp_replace(CCY,'/','//'))
,trim(regexp_replace(REDISBILLTYPE,'/','//'))
,trim(regexp_replace(CUSTOMID,'/','//'))
,trim(regexp_replace(REDISBRC,'/','//'))
,trim(regexp_replace(REDISBRCNAME,'/','//'))
,trim(regexp_replace(SALBNKKIND,'/','//')),REDISOPENDATE,REDISMATURE,TOTALAMT,INTAMT,REDISRATE
,trim(regexp_replace(ACCTNO,'/','//'))
,trim(regexp_replace(TOTALNUM,'/','//'))
,trim(regexp_replace(FILENAME,'/','//'))
,trim(regexp_replace(BILLNO,'/','//'))
,trim(regexp_replace(REPAYWAY,'/','//'))
,trim(regexp_replace(ERASENO,'/','//')),AMT1,AMT2
,trim(regexp_replace(FLAG,'/','//'))
,trim(regexp_replace(SERSEQNO,'/','//')),REQ_DATE,RESP_DATE
,trim(regexp_replace(CORE_DT,'/','//'))
,trim(regexp_replace(IS_CZ,'/','//')),ACCT_ID
,trim(regexp_replace(COREERASENO,'/','//'))
,trim(regexp_replace(CHECK_OPER_NO,'/','//')) from CORE_MESSAGE WHERE CORE_DT='#*P_DATE*#'
