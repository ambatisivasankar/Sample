# Required
export PROJECT_ID=teradata_prd
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'

# Optional
export INCLUDE_VIEWS=1
export CONNECTION_ID=teradata
export SPARK_YARN_QUEUE='datalayer'
export SPARK_MAX_EXECUTORS=10
export CONVERT_TIMESTAMPS_TO_AMERICA_NEW_YORK=1
export MAKE_DDL_FROM_TARGET=1

INCLUDE_TABLES_ARRAY=(
  "AGMT_ADDL_DATA_VW"
  "AGMT_CMN_VW"
  "AGMT_CVG_CMN_VW"
  "AGMT_FND_CMN_VW"
  "AGMT_FND_VAL_CMN_VW"
  "AGMT_GRP_CMN_VW"
  "AGMT_LOAN_CMN_VW"
  "AGMT_UWRT_CMN_VW"
  "AGMT_VAL_CMN_VW"
  "AGMT_WARNING_INFO_CMN_VW"
  "BENE_DATA_CMN_VW"
  "BP_CREDENTIAL_VW"
  "CUST_AGMT_CMN_VW"
  "CUST_DEMOGRAPHICS_VW"
  "FUND_CMN_VW"
  "PDCR_AGMT_CMN_VW"
  "PDCR_DEMOGRAPHICS_VW"
  "SLLNG_AGMT_CMN_VW"
)
export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"

# 2018.02.07 FND_ID = 425115 in FUND_CMN_VW does NOT want to write to S3, skipping via subquery
export JSON_INFO='
{
    "PARTITION_INFO":{
        "tables": {
            "AGMT_ADDL_DATA_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "AGMT_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "AGMT_CVG_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "AGMT_FND_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "AGMT_FND_VAL_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "AGMT_GRP_CMN_VW": {
              "partitionColumn": "EXTRACT(DAY FROM GROUPING_KEY_FR_DT) MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "AGMT_LOAN_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "AGMT_UWRT_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "AGMT_VAL_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 20",
              "lowerBound": 0,
              "upperBound": 20,
              "numPartitions": 20
            },
            "BENE_DATA_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
             "BP_CREDENTIAL_VW": {
              "partitionColumn": "PRTY_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "CUST_AGMT_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "CUST_DEMOGRAPHICS_VW": {
              "partitionColumn": "PRTY_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "FUND_CMN_VW": {
              "partitionColumn": "FND_ID NOT IN (425115) AND FND_ID",
              "lowerBound": 100000,
              "upperBound": 470000,
              "numPartitions": 50
            },
             "PDCR_AGMT_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            }
        }
   }
}
'
