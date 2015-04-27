select ID,CUST_ID
,trim(regexp_replace(PROVINCE,'/','//'))
,trim(regexp_replace(CITY,'/','//'))
,trim(regexp_replace(TELEPHONE,'/','//'))
,trim(regexp_replace(MOBILE_PHONE,'/','//'))
,trim(regexp_replace(FAX,'/','//'))
,trim(regexp_replace(POST_CODE,'/','//'))
,trim(regexp_replace(EMAIL,'/','//'))
,trim(regexp_replace(ADDRESS,'/','//'))
,trim(regexp_replace(URL,'/','//'))
,trim(regexp_replace(CONTACT_PERSON,'/','//'))
,trim(regexp_replace(BANK_NO,'/','//')) from CUST_INFO_ADDR
