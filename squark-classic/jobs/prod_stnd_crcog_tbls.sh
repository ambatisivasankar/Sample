export PROJECT_ID=prod_stnd_crcog_tbls
# primary purpose of schema is to load the data to squark_staging and finally to prod_stnd_prty in Vertica. 
export WAREHOUSE_DIR="/_wh/"
export SQL_TEMPLATE="%s"
export INCLUDE_VIEWS=1
export CONNECTION_ID=prod_stnd_crcog_vw

INCLUDE_TABLES_ARRAY=(
  "ADVSR_CNTR_VW"
  "SM_VALIDATION_VW"
  "RCOG_DI_BONUS_VW"
  "UNIT_VW"
  "AGY_RENT_ADJ_VW"
  "SEC_COMP_SUM_VW"
  "SEC_COMP_ADJ_VW"
  "RCOG_QUAL_YR_VW"
)

export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"
export SPARK_MAX_EXECUTORS=10

export JSON_INFO='
{
    "PARTITION_INFO":{
        "tables": {
            "ADVSR_CNTR_VW": {
              "partitionColumn": "PDCR_BPID mod 10",
              "lowerBound": 0,
              "upperBound": 9,
              "numPartitions": 10
            },
             "SM_VALIDATION_VW": {
              "partitionColumn": "BP_ID mod 10",
              "lowerBound": 0,
              "upperBound": 9,
              "numPartitions": 10
            },
             "RCOG_DI_BONUS_VW": {
              "partitionColumn": "AGT_BPID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "UNIT_VW": {
              "partitionColumn": "UNIT_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "AGY_RENT_ADJ_VW": {
              "partitionColumn": "AGY_BPID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "SEC_COMP_SUM_VW": {
              "partitionColumn": "TRLGY_LEDG_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "SEC_COMP_ADJ_VW": {
              "partitionColumn": "PRTY_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "RCOG_QUAL_YR_VW": {
              "partitionColumn": "BP_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            }
     }
   }
}
'
