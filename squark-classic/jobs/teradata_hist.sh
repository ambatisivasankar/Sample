# Required
export PROJECT_ID=teradata_hist
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=teradata

# Optional
export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=1
export SPARK_MAX_EXECUTORS=10

include_tables_array=(
  "AGMT_CVG_HIST_VW"
  "AGMT_HIST_VW"
  "CUST_AGMT_HIST_VW"
  "CUST_DEMOGRAPHICS_HIST_VW"
  "SLLNG_AGMT_HIST_VW"
)
include_tables="$(IFS=, ; echo "${include_tables_array[*]}")"
export INCLUDE_TABLES=$include_tables
# 2018.04.18, partition results on AGMT_HIST_VW pretty bad, AGREEMENT_ID has bimodal-ish dist for this date range
#   in order to use value in the subfilter, double-quote all innards of the JSON
export JSON_INFO='
{
    "PARTITION_INFO":{
        "tables": {
            "CUST_AGMT_HIST_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 100",
              "lowerBound": 0,
              "upperBound": 100,
              "numPartitions": 100
            },
            "CUST_DEMOGRAPHICS_HIST_VW": {
              "partitionColumn": "PRTY_ID MOD 100",
              "lowerBound": 0,
              "upperBound": 100,
              "numPartitions": 100
            },
            "AGMT_CVG_HIST_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 100",
              "lowerBound": 0,
              "upperBound": 100,
              "numPartitions": 100
            },
            "AGMT_HIST_VW": {
              "partitionColumn": "ISSUE_DT >= """2015-01-01""" AND AGREEMENT_ID MOD 200",
              "lowerBound": 0,
              "upperBound": 200,
              "numPartitions": 200
            }
        }
   }
}
'
