select ID,CUST_ID
,trim(regexp_replace(QUALITY,'/','//'))
,trim(regexp_replace(ORG_SIZE,'/','//'))
,trim(regexp_replace(ORG_TYPE,'/','//'))
,trim(regexp_replace(CREDIT_CLASS,'/','//'))
,trim(regexp_replace(BANK_LEVEL,'/','//'))
,trim(regexp_replace(BANK_SORT,'/','//'))
,trim(regexp_replace(TRADE,'/','//'))
,trim(regexp_replace(FI_TYPE,'/','//'))
,trim(regexp_replace(CREDIT_AGENCY,'/','//')),CREDIT_DUE_DT from CUST_INFO_ATTR
