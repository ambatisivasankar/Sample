
export PROJECT_ID=squark_staging
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=winrisk


export INCLUDE_TABLES="LAB_RESULTS,LAB_DEMOGRAPHICS,NAME,POLICY_EVENTS,LAB_STANDARDS,SUMMARY"

echo "start Dt: " $strt_dt
echo "End Dt: " $end_dt
   
export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':
    {
        'LAB_RESULTS': {
            'sql_query': '(SELECT lb_result.* FROM LAB_RESULTS lb_result INNER JOIN LAB_DEMOGRAPHICS lb_demg ON lb_result.LAB_ID = lb_demg.LAB_ID WHERE cast (lb_demg.DATE_RECEIVED as date) BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery',
            'table_pk': 'LAB_ID,ID,RECORD_TYPE'
        },
        'LAB_DEMOGRAPHICS': {
            'sql_query': '(SELECT * FROM LAB_DEMOGRAPHICS WHERE cast(DATE_RECEIVED as date) BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery',
            'table_pk': 'LAB_ID'
        },
        'LAB_STANDARDS': {
            'sql_query': '(SELECT * FROM LAB_STANDARDS) as subquery'
        },
        'NAME': {
            'sql_query': '(SELECT name.* FROM  NAME name INNER JOIN (select * from  LAB_DEMOGRAPHICS where DATE_PERFORMED BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) lb_demg ON  name.NAME_IDNUMBER = ISNULL(lb_demg.ssn, lb_demg.matched_ssn) and NAME_IDNUMBER is not NULL and NOT (NAME_IDNUMBER  like '''000%''' or NAME_IDNUMBER like '''999%''' or NAME_IDNUMBER  in ('''123456789''')) UNION SELECT name.* FROM  NAME name INNER JOIN (select * from  LAB_DEMOGRAPHICS where DATE_PERFORMED BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) lb_demg ON name.NAME_POLNUM = lb_demg.POLICY_NUMBER UNION SELECT name.* FROM  name INNER JOIN (select * from  LAB_DEMOGRAPHICS where DATE_PERFORMED BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) lb_demg ON  NAME_FIRSTNAME = app_first_name and NAME_LASTNAME = app_last_name and name_dob = app_dob  ) as subquery',
            'table_pk': 'NAME_COMPANYCODE,NAME_POLNUM,NAME_SEQNO'
        },
        'POLICY_EVENTS': {
            'sql_query': '(SELECT plc_evn.* FROM POLICY_EVENTS plc_evn ) as subquery',
            'table_pk': 'NAME_COMPANYCODE,NAME_POLNUM,NAME_SEQNO'
        },
        'SUMMARY': {
        'sql_query': '(SELECT sum.* FROM SUMMARY SUM inner join NAME name on sum.sum_polnum = name.NAME_POLNUM INNER JOIN (select * from  LAB_DEMOGRAPHICS where DATE_PERFORMED BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) lb_demg ON  name.NAME_IDNUMBER = ISNULL(lb_demg.ssn, lb_demg.matched_ssn) and NAME_IDNUMBER is not NULL and NOT (NAME_IDNUMBER  like '''000%''' or NAME_IDNUMBER like '''999%''' or NAME_IDNUMBER  in ('''123456789''')) UNION SELECT sum.* FROM SUMMARY SUM inner join NAME name on sum.sum_polnum = name.NAME_POLNUM INNER JOIN (select * from  LAB_DEMOGRAPHICS where DATE_PERFORMED BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) lb_demg ON name.NAME_POLNUM = lb_demg.POLICY_NUMBER UNION SELECT sum.* FROM SUMMARY SUM inner join NAME name on sum.sum_polnum = name.NAME_POLNUM INNER JOIN (select * from  LAB_DEMOGRAPHICS where DATE_PERFORMED BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) lb_demg ON  NAME_FIRSTNAME = app_first_name and NAME_LASTNAME = app_last_name and name_dob = app_dob ) as subquery' 
        }
    }
}
"

