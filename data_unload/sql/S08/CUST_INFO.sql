select ID
,trim(regexp_replace(CUST_NAME,'/','//'))
,trim(regexp_replace(CUST_TYPE,'/','//'))
,trim(regexp_replace(CUST_NO,'/','//'))
,trim(regexp_replace(LOAN_NO,'/','//'))
,trim(regexp_replace(ORG_CODE,'/','//'))
,trim(regexp_replace(FLAG,'/','//')),CREATE_TIME,UPDATE_TIME
,trim(regexp_replace(PARTNER_TYPE,'/','//'))
,trim(regexp_replace(GROUP_ID,'/','//'))
,trim(regexp_replace(PARENT_GROUP_ID,'/','//'))
,trim(regexp_replace(BRCH_ID,'/','//')) from CUST_INFO
