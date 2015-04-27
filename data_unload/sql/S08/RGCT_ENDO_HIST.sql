select ID,RGCT_ID
,trim(regexp_replace(TO_NAME,'/','//'))
,trim(regexp_replace(TO_SIGN,'/','//')),TO_SEQ_NUMBER,CUR_SEQ_NUMBER,ENDORSE_DT
,trim(regexp_replace(REMARK,'/','//')),CREATE_TIME
,trim(regexp_replace(DEL_FLAG,'/','//')),HIST_ID,LAST_ENDO_ID from RGCT_ENDO_HIST
