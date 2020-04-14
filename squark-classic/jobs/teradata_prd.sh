# Required
export PROJECT_ID=teradata_prd
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=teradata

# Optional
export INCLUDE_VIEWS=1
export SPARK_YARN_QUEUE='datalayer'
export SPARK_MAX_EXECUTORS=10
export CONVERT_TIMESTAMPS_TO_AMERICA_NEW_YORK=0

include_tables_array=(
  "AGMT_ADDL_DATA_VW"
  "AGMT_CMN_VW"
  "AGMT_CVG_CMN_VW"
  "AGMT_FND_CMN_VW"
  "AGMT_FND_VAL_CMN_VW"
  "AGMT_GRP_CMN_VW"
  "AGMT_LOAN_CMN_VW"
  "AGMT_UWRT_CMN_VW"
  "AGMT_VAL_CMN_VW"
  "AGMT_SRVC_VW"
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

include_tables="$(IFS=, ; echo "${include_tables_array[*]}")"
export INCLUDE_TABLES=$include_tables

# 2018.02.07 FND_ID = 425115 in FUND_CMN_VW does NOT want to write to S3, skipping via subquery
export JSON_INFO='
{
    "SUPER_PROJECTION_SETTINGS":{
        "tables": {
            "AGMT_ADDL_DATA_VW": {
                "projection_name": "AGMT_ADDL_DATA_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "AGMT_CMN_VW": {
                "projection_name": "AGMT_CMN_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "AGMT_CVG_CMN_VW": {
                "projection_name": "AGMT_CVG_CMN_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "AGMT_FND_CMN_VW": {
                "projection_name": "AGMT_FND_CMN_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "AGMT_FND_VAL_CMN_VW": {
                "projection_name": "AGMT_FND_VAL_CMN_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "AGMT_GRP_CMN_VW": {
                "projection_name": "AGMT_GRP_CMN_VW_SQUARK",
                "order_by_columns": "GROUPING_KEY",
                "segment_by_columns": "GROUPING_KEY"
            },
            "AGMT_LOAN_CMN_VW": {
                "projection_name": "AGMT_LOAN_CMN_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "AGMT_SRVC_VW": {
                "projection_name": "AGMT_SRVC_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "AGMT_UWRT_CMN_VW": {
                "projection_name": "AGMT_UWRT_CMN_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "AGMT_VAL_CMN_VW": {
                "projection_name": "AGMT_VAL_CMN_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "AGMT_WARNING_INFO_CMN_VW": {
                "projection_name": "AGMT_WARNING_INFO_CMN_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": ""
            },
            "BENE_DATA_CMN_VW": {
                "projection_name": "BENE_DATA_CMN_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "BP_CREDENTIAL_VW": {
                "projection_name": "BP_CREDENTIAL_VW_SQUARK",
                "order_by_columns": "BP_ID",
                "segment_by_columns": "BP_ID"
            },
            "CUST_AGMT_CMN_VW": {
                "projection_name": "CUST_AGMT_CMN_VW_SQUARK",
                "order_by_columns": "PRTY_AGMT_RLE_CD,PRTY_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "CUST_DEMOGRAPHICS_VW": {
                "projection_name": "CUST_DEMOGRAPHICS_VW_SQUARK",
                "order_by_columns": "PRTY_ID",
                "segment_by_columns": "PRTY_ID"
            },
            "FUND_CMN_VW": {
                "projection_name": "FUND_CMN_VW_SQUARK",
                "order_by_columns": "FND_ID",
                "segment_by_columns": ""
            },
            "PDCR_AGMT_CMN_VW": {
                "projection_name": "PDCR_AGMT_CMN_VW_SQUARK",
                "order_by_columns": "PRTY_AGMT_RLE_CD,AGREEMENT_ID",
                "segment_by_columns": "PRTY_ID"
            },
            "PDCR_DEMOGRAPHICS_VW": {
                "projection_name": "PDCR_DEMOGRAPHICS_VW_SQUARK",
                "order_by_columns": "PRTY_ID",
                "segment_by_columns": ""
            },
            "SLLNG_AGMT_CMN_VW": {
                "projection_name": "SLLNG_AGMT_CMN_VW_SQUARK",
                "order_by_columns": "SLLNG_AGMT_STUS_CD,PARENT_SLLNG_AGMT_RLE,PARENT_BPID",
                "segment_by_columns": ""
            }
        }
    },
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
            "AGMT_SRVC_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
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
