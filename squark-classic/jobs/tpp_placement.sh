export PROJECT_ID=tpp_placement
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_TABLES="app_case,case_pclient_assoc,pclient"
#2017.07.10, we don't know why SKIP_ERRORS was enabled in the first place but disabling now
#export SKIP_ERRORS=1
export INCLUDE_VIEWS=1
export CONNECTION_ID=tpp
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
