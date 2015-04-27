select SABI_ID
,trim(regexp_replace(BILL_TYPE,'/','//'))
,trim(regexp_replace(BILL_NO,'/','//')),OUT_BILL_DATE,END_BILL_DATE
,trim(regexp_replace(OUT_BILL_PERSON,'/','//'))
,trim(regexp_replace(OUT_BILL_ACCOUNT,'/','//'))
,trim(regexp_replace(OUT_BILL_BANK,'/','//'))
,trim(regexp_replace(OUT_BILL_BANK_NO,'/','//')),BILL_AMOUNT
,trim(regexp_replace(ACCEPTOR,'/','//'))
,trim(regexp_replace(PAYEE,'/','//'))
,trim(regexp_replace(PAYEE_BANK_NAME,'/','//'))
,trim(regexp_replace(PAYEE_ACCOUNT,'/','//'))
,trim(regexp_replace(BILL_BEFORE_OWNER,'/','//'))
,trim(regexp_replace(BILL_OWNER,'/','//'))
,trim(regexp_replace(BILL_CLASS,'/','//'))
,trim(regexp_replace(BILL_SOURCE,'/','//')),CUST_ID,OPER_ID
,trim(regexp_replace(OPER_STATUS,'/','//')),SAAP_ID,RGCT_ID
,trim(regexp_replace(ADSCRIPTION_ID,'/','//'))
,trim(regexp_replace(CUST_ACCOUNT_NO,'/','//'))
,trim(regexp_replace(IF_SAME_CITY,'/','//')),INTEREST,POSTPONE_DAYS,GALE_DATE,INTEREST_CAL_DAYS,RECEIVE_MONEY
,trim(regexp_replace(REMARK,'/','//')),CREATE_TIME
,trim(regexp_replace(IF_AUDITED,'/','//'))
,trim(regexp_replace(DRAWEE_ADDR,'/','//'))
,trim(regexp_replace(PAYEE_BANK_NO,'/','//'))
,trim(regexp_replace(CONFER_NO,'/','//'))
,trim(regexp_replace(IS_ACCP,'/','//')),LIMIT_BILL_ID
,trim(regexp_replace(LIMIT_PROD_NO,'/','//')),RATE
,trim(regexp_replace(RATE_TYPE,'/','//'))
,trim(regexp_replace(BUY_DEPT_NO,'/','//'))
,trim(regexp_replace(BUY_IF_INNER,'/','//'))
,trim(regexp_replace(REMITTER_CUST_NO,'/','//')),BUYBACK_MONEY,BUYBACK_INTEREST,SALE_INTEREST
,trim(regexp_replace(RE_OLD_PROD,'/','//')),RE_OLD_ID,TEMP_NMFSID from SALE_BILL_INFO
