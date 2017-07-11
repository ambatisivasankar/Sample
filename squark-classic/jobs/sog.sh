export PROJECT_ID=sog
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CHECK_PRIVS=1
export EXCLUDE_TABLES='cow,financial_plans,financial_tasks,online_class_survey_emails,online_class_resource_links,online_classes'
export CONNECTION_ID=sog
# Have to set the CONVERT_ARRAYS_TO_STRING option for SOG
export CONVERT_ARRAYS_TO_STRING=1
export SQUARK_METADATA=1

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
