select ID,CUST_ID
,trim(regexp_replace(BELONG_BRANCH,'/','//'))
,trim(regexp_replace(IS_NEW,'/','//'))
,trim(regexp_replace(IS_ACCOUNT,'/','//'))
,trim(regexp_replace(IS_CREDIT,'/','//'))
,trim(regexp_replace(IS_GROUP,'/','//'))
,trim(regexp_replace(IS_PARTNER,'/','//'))
,trim(regexp_replace(IS_LC,'/','//'))
,trim(regexp_replace(IS_WARN_CUST,'/','//')),CUMA_ID from CUST_INFO_MANG
