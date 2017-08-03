# 2017.08.03, this schema is now deprecated in favor of winrisk, which has been updated to pull below table list
export PROJECT_ID=winrisk_placement
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=winrisk_placement
export INCLUDE_TABLES="NAME,SUMMARY,COVERAGES,BILLING,REPLACEMENT_SUMMARY,RATINGS,RIDERS,REQUIREMENTS,NOTES,POLICY_EVENTS,LAB_DEMOGRAPHICS,LAB_RESULTS,ADDITIONAL_RIDER_INFO"
export SKIP_ERRORS=1
export INCLUDE_VIEWS=1
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
