export PROJECT_ID=winrisk
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=winrisk
# 2017.08.03 updating winrisk to pull the tables winrisk_placement used to (as opposed to early pull-all-tables)
export INCLUDE_TABLES="NAME,SUMMARY,COVERAGES,BILLING,REPLACEMENT_SUMMARY,RATINGS,RIDERS,REQUIREMENTS,NOTES,POLICY_EVENTS,LAB_DEMOGRAPHICS,LAB_RESULTS,ADDITIONAL_RIDER_INFO,DEBITCREDIT_SUMMARY,REINSURANCE_DESC,REINSURANCE_DETAILS,REINSURANCE_SUMMARY"
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
