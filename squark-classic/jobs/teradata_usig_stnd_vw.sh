# Required
export PROJECT_ID=teradata_usig_stnd_vw
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=teradata_usig_stnd_vw

# Optional
export INCLUDE_VIEWS=1
export SPARK_MAX_EXECUTORS=15
export SKIP_SOURCE_ROW_COUNT=1

include_tables_array=(
  "AGMT_CMN_VW"
  "CUST_ADDL_AD_CMN_VW"
  "CUST_ADDL_ELEC_AD_CMN_VW"
  "CUST_ADDL_TEL_NR_CMN_VW"
  "PDCR_AGMT_CMN_VW"
  "SALES_HIERARCHY_VW"
  "SLLNG_AGMT_CMN_VW"
)

include_tables="$(IFS=, ;echo "${include_tables_array[*]}")"
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
            "CUST_ADDL_AD_CMN_VW": {
                "projection_name": "CUST_ADDL_AD_CMN_VW_SQUARK",
                "order_by_columns": "MEMBER_ID",
                "segment_by_columns": "MEMBER_ID"
            },
            "CUST_ADDL_ELEC_AD_CMN_VW": {
                "projection_name": "CUST_ADDL_ELEC_AD_CMN_VW_SQUARK",
                "order_by_columns": "MEMBER_ID",
                "segment_by_columns": "MEMBER_ID"
            },
            "CUST_ADDL_TEL_NR_CMN_VW": {
                "projection_name": "CUST_ADDL_TEL_NR_CMN_VW_SQUARK",
                "order_by_columns": "MEMBER_ID",
                "segment_by_columns": "MEMBER_ID"
            },
            "PDCR_AGMT_CMN_VW": {
                "projection_name": "PDCR_AGMT_CMN_VW_SQUARK",
                "order_by_columns": "PRTY_AGMT_RLE_CD,AGREEMENT_ID",
                "segment_by_columns": "PRTY_ID"
            },
            "SALES_HIERARCHY_VW": {
                "projection_name": "SALES_HIERARCHY_VW_SQUARK",
                "order_by_columns": "PRD_STD_CONTR_TYP,PRD_REL_END_DT",
                "segment_by_columns": "UNIT_PRTY_ID"
            },
            "SLLNG_AGMT_CMN_VW": {
                "projection_name": "SLLNG_AGMT_CMN_VW_SQUARK",
                "order_by_columns": "SLLNG_AGMT_STUS_CD,PARENT_SLLNG_AGMT_RLE,PARENT_BPID",
                "segment_by_columns": ""
            }
        }
    },
    "PARTITION_INFO": {
        "tables": {
            "AGMT_CMN_VW": {
              "partitionColumn": "AGREEMENT_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "CUST_ADDL_AD_CMN_VW": {
              "partitionColumn": "PRTY_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
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
