export PROJECT_ID=teradata_hist
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='CUST_AGMT_HIST_VW,CUST_DEMOGRAPHICS_HIST_VW,AGMT_CVG_HIST_VW,SLLNG_AGMT_HIST_VW,AGMT_HIST_VW'
export CONNECTION_ID=teradata

export SKIP_SOURCE_ROW_COUNT=1
export SPARK_MAX_EXECUTORS=15
# 2018.04.18, partition results on AGMT_HIST_VW pretty bad, AGREEMENT_ID has bimodal-ish dist for this date range
#   in order to use value in the subfilter, double-quote all innards of the JSON
export JSON_INFO='
{
    "PARTITION_INFO":{
        "tables": {
            "CUST_AGMT_HIST_VW": {
              "partitionColumn": "AGREEMENT_ID",
              "lowerBound": 1,
              "upperBound": 34000000,
              "numPartitions": 50
            },
            "CUST_DEMOGRAPHICS_HIST_VW": {
              "partitionColumn": "PRTY_ID",
              "lowerBound": 1,
              "upperBound": 22000000,
              "numPartitions": 50
            },
            "AGMT_CVG_HIST_VW": {
              "partitionColumn": "COALESCE(AGREEMENT_ID, 0)",
              "lowerBound": 5600000,
              "upperBound": 34000000,
              "numPartitions": 50
            },
            "AGMT_HIST_VW": {
              "partitionColumn": "ISSUE_DT >= """2015-01-01""" AND AGREEMENT_ID",
              "lowerBound": 5700000,
              "upperBound": 40000000,
              "numPartitions": 50
            }
        }
   }
}
'
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
