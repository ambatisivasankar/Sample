export PROJECT_ID=rilcats
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
# Excluded due to discussion here: https://advana.atlassian.net/browse/INGEST-150
export EXCLUDE_TABLES="wrk_txt_cmnt"
export CONNECTION_ID=rilcats
# 2017.08.09, source row count queries adding < 20 minutes to reflex duration, worth it for now during testing & transition
#export SKIP_SOURCE_ROW_COUNT=1

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
