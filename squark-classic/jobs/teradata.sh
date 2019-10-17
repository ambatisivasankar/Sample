# Required
export PROJECT_ID=teradata
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=teradata

# pPtional
export INCLUDE_VIEWS=1
export SPARK_MAX_EXECUTORS=15

include_tables_array=(
  "AGMT_CMN_VW"
  "AGMT_CVG_CMN_VW"
  "AGMT_DISB_TXN_CMN_VW"
  "AGMT_UWRT_CMN_VW"
  "AGMT_VAL_CMN_VW"
  "BENE_DATA_CMN_VW"
  "BENE_DATA_HIST_VW"
  "BP_CREDENTIAL_VW"
  "CUST_ADDL_AD_CMN_VW"
  "CUST_AGMT_CMN_VW"
  "CUST_DEMOGRAPHICS_VW"
  "CUST_PREFERENCE_VW"
  "PDCR_AGMT_CMN_VW"
  "PDCR_AGMT_HIST_VW"
  "PDCR_ALT_ID_CMN_VW"
  "PDCR_DEMOGRAPHICS_HIST_VW"
  "PDCR_DEMOGRAPHICS_VW"
  "SLLNG_AGMT_CMN_VW"
  )

include_tables="$(IFS=, ; echo "${include_tables_array[*]}")"
export INCLUDE_TABLES=$include_tables

export JSON_INFO='
{
"SUPER_PROJECTION_SETTINGS":{
        "tables": {
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
            "AGMT_DISB_TXN_CMN_VW": {
                "projection_name": "AGMT_DISB_TXN_CMN_VW_SQUARK",
                "order_by_columns": "HLDG_KEY",
                "segment_by_columns": ""
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
            "BENE_DATA_CMN_VW": {
                "projection_name": "BENE_DATA_CMN_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "BENE_DATA_HIST_VW": {
                "projection_name": "BENE_DATA_HIST_VW_SQUARK",
                "order_by_columns": "AGREEMENT_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "BP_CREDENTIAL_VW": {
                "projection_name": "BP_CREDENTIAL_VW_SQUARK",
                "order_by_columns": "BP_ID",
                "segment_by_columns": "BP_ID"
            },
            "CUST_ADDL_AD_CMN_VW": {
                "projection_name": "CUST_ADDL_AD_CMN_VW_SQUARK",
                "order_by_columns": "PRTY_ID",
                "segment_by_columns": "PRTY_ID"
            },
            "CUST_AGMT_CMN_VW": {
                "projection_name": "CUST_AGMT_CMN_VW_SQUARK",
                "order_by_columns": "PRTY_ID",
                "segment_by_columns": "AGREEMENT_ID"
            },
            "CUST_DEMOGRAPHICS_VW": {
                "projection_name": "CUST_DEMOGRAPHICS_VW_SQUARK",
                "order_by_columns": "PRTY_ID",
                "segment_by_columns": "PRTY_ID"
            },
            "CUST_PREFERENCE_VW": {
                "projection_name": "CUST_PREFERENCE_VW_SQUARK",
                "order_by_columns": "PRTY_ID",
                "segment_by_columns": "PRTY_ID"
            },
            "PDCR_AGMT_CMN_VW": {
                "projection_name": "PDCR_AGMT_CMN_VW_SQUARK",
                "order_by_columns": "PRTY_AGMT_RLE_CD,AGREEMENT_ID",
                "segment_by_columns": "PRTY_ID"
            },
            "PDCR_AGMT_HIST_VW": {
                "projection_name": "PDCR_AGMT_HIST_VW_SQUARK",
                "order_by_columns": "PRTY_AGMT_RLE_CD,AGREEMENT_ID",
                "segment_by_columns": "PRTY_ID"
            },
            "PDCR_ALT_ID_CMN_VW": {
                "projection_name": "PDCR_ALT_ID_CMN_VW_SQUARK",
                "order_by_columns": "ALT_ID_TYP_CD,PRTY_ID",
                "segment_by_columns": ""
            },
            "PDCR_DEMOGRAPHICS_VW": {
                "projection_name": "PDCR_DEMOGRAPHICS_VW_SQUARK",
                "order_by_columns": "PRTY_ID",
                "segment_by_columns": ""
            },
            "PDCR_DEMOGRAPHICS_HIST_VW": {
                "projection_name": "PDCR_DEMOGRAPHICS_HIST_VW_SQUARK",
                "order_by_columns": "PRTY_ID",
                "segment_by_columns": "PRTY_ID"
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
            "AGMT_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID",
              "lowerBound": 0,
              "upperBound": 52000000,
              "numPartitions": 50
            },
            "AGMT_CVG_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID",
              "lowerBound": 5500000,
              "upperBound": 52000000,
              "numPartitions": 50
            },
            "AGMT_UWRT_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID",
              "lowerBound": 5600000,
              "upperBound": 52000000,
              "numPartitions": 15
            },
            "AGMT_VAL_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID",
              "lowerBound": 0,
              "upperBound": 52000000,
              "numPartitions": 50
            },
            "BENE_DATA_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID",
              "lowerBound": 5500000,
              "upperBound": 52000000,
              "numPartitions": 50
            },
            "BENE_DATA_HIST_VW": {
              "partitionColumn": "AGREEMENT_ID",
              "lowerBound": 5500000,
              "upperBound": 52000000,
              "numPartitions": 50
            },
            "BP_CREDENTIAL_VW": {
              "partitionColumn": "PRTY_ID",
              "lowerBound": 8500000,
              "upperBound": 9000000,
              "numPartitions": 5
            },
            "CUST_ADDL_AD_CMN_VW": {
              "partitionColumn": "PRTY_ID",
              "lowerBound": 1000000,
              "upperBound": 32000000,
              "numPartitions": 25
            },
            "CUST_AGMT_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID",
              "lowerBound": 0,
              "upperBound": 52000000,
              "numPartitions": 100
            },
            "CUST_DEMOGRAPHICS_VW": {
              "partitionColumn": "PRTY_ID",
              "lowerBound": 0,
              "upperBound": 33000000,
              "numPartitions": 50
            },
            "PDCR_AGMT_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID",
              "lowerBound": 0,
              "upperBound": 52000000,
              "numPartitions": 100
            },
            "PDCR_AGMT_HIST_VW": {
              "partitionColumn": "AGREEMENT_ID",
              "lowerBound": 0,
              "upperBound": 52000000,
              "numPartitions": 100
            },
            "PDCR_ALT_ID_CMN_VW": {
              "partitionColumn": "PRTY_ID",
              "lowerBound": 8500000,
              "upperBound": 9000000,
              "numPartitions": 5
            },
            "PDCR_DEMOGRAPHICS_HIST_VW": {
              "partitionColumn": "PRTY_ID",
              "lowerBound": 8500000,
              "upperBound": 9000000,
              "numPartitions": 5
            }
        }
   }
}
'
