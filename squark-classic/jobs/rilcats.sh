export PROJECT_ID=rilcats
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
# Excluded due to discussion here: https://advana.atlassian.net/browse/INGEST-150
# export EXCLUDE_TABLES="wrk_txt_cmnt"
# 2018.08.31 adding an INCLUDE_TABLES to only those being accessed on Vertica, comment out above since it would be redundant
# 2019.01.31 adding subset of wrk_txt_cmnt requested within DS-18
export INCLUDE_TABLES="Acrcy_Adt,Anny_Wrk,Dept,div,Rqstr_Typ,Rsrc,stus,svc_chnl_cde,Wrk,wrk_cmnt,wrk_data,wrk_evnt,wrk_que,Wrk_Xtn,wrk_txt_cmnt"
export CONNECTION_ID=rilcats
# 2018.08.31, source count queries take long time and also may be doing filter queries soon
export SKIP_SOURCE_ROW_COUNT=1

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'Wrk': {
              'partitionColumn': 'wrk_ident',
              'lowerBound': 5000000000,
              'upperBound': 5100000000,
              'numPartitions': 100
            },
            'wrk_cmnt': {
              'partitionColumn': 'fk_wrk_ident',
              'lowerBound': 5000000000,
              'upperBound': 5100000000,
              'numPartitions': 100
            },
            'wrk_txn': {
              'partitionColumn': 'wrk_ident',
              'lowerBound': 5000000000,
              'upperBound': 5100000000,
              'numPartitions': 50
            },
            'Wrk_Xtn': {
              'partitionColumn': 'fk_wrk_ident',
              'lowerBound': 5019927040,
              'upperBound': 5100000000,
              'numPartitions': 50
            },
            'wrk_txt_cmnt': {
              'partitionColumn': 'fk_wrk_ident IN ( SELECT T1.fk_wrk_ident FROM WRK_TXT_CMNT T1 INNER JOIN WRK T2 ON T1.FK_WRK_IDENT = T2.WRK_IDENT WHERE T2.fk_div_cde = '''RM''' And T2.fk_dept_cde = '''LC''' AND T2.fk_wrk_evntevnt_nr In (6503, 7406, 9748, 9847, 9848, 9849, 9850, 9851, 9852, 10269, 10552, 10597, 10598, 10668, 10669, 10704, 10705, 10706, 10768, 10793, 10794, 10795, 10796, 9853, 878, 875, 7431, 9854, 872, 8459, 9825, 4222, 7475, 7476, 7477, 8852, 9690, 9691, 7479, 7480, 7481, 8853, 9692, 9693, 4865, 10345, 10346, 873, 876, 879, 4866, 5769, 7889, 9318, 9564, 9614, 9935, 10356, 10381, 10599, 7117, 7471, 7890, 10382, 9218, 7115, 7412, 10357, 10383, 7920, 8024, 8266, 8267, 8268, 10027, 10545, 10541, 10542, 10543, 10544) AND CAST(T2.rcvd_dt As DATE) >= '''1/1/2009''' ) AND fk_wrk_ident',
              'lowerBound': 5035210345,
              'upperBound': 5101825094,
              'numPartitions': 500
            }
        }
   }
}
"
