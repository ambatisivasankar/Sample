export PROJECT_ID=pol_warnings
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
#export EXCLUDE_TABLES=""
export INCLUDE_TABLES="MSG"
#export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=1
export CONNECTION_ID=pol_warnings
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
