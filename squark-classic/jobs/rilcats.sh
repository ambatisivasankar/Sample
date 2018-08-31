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

#export STATS_CONFIG="
#{
#    'profiles': {
#        'numeric': ['max','min','mean','countDistinct','count_null'],
#        'string': ['max', 'min','countDistinct','count_null'],
#        'datetype': ['max', 'min','countDistinct','count_null']
#    },
#    'field_types': {
#        'NumericType': 'numeric',
#        'StringType': 'string',
#        'DateType': 'datetype',
#        'TimestampType': 'datetype'
#    }
#}
#"
