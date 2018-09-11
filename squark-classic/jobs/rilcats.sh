export PROJECT_ID=rilcats
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
# Excluded due to discussion here: https://advana.atlassian.net/browse/INGEST-150
# export EXCLUDE_TABLES="wrk_txt_cmnt"
# 2018.08.31 adding an INCLUDE_TABLES to only those being accessed on Vertica, comment out above since it would be redundant
export INCLUDE_TABLES="Acrcy_Adt,Anny_Wrk,Dept,div,Rqstr_Typ,Rsrc,stus,svc_chnl_cde,Wrk,wrk_cmnt,wrk_data,wrk_evnt,wrk_que,Wrk_Xtn"
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
            }
        }
   }
}
"
