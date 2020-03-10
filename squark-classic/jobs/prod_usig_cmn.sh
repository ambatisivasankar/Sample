export PROJECT_ID=prod_usig_cmn
# primary purpose of schema is to load the data to squark_staging and finally to prod_usig_cmn in Vertica. 
export WAREHOUSE_DIR="/_wh/"
export SQL_TEMPLATE="%s"
export INCLUDE_VIEWS=1
export CONNECTION_ID=teradata_cmn

INCLUDE_TABLES_ARRAY=(
  "SRC_DATA_TRNSLT_VW"
  "PRODUCT_TRANSLATOR_VW"
)

export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"
export SPARK_MAX_EXECUTORS=5

export JSON_INFO='
{
    "PARTITION_INFO":{
        "tables": {
            "SRC_DATA_TRNSLT_VW": {
              "partitionColumn": "SRC_DATA_TRNSLT_IDENT mod 5",
              "lowerBound": 0,
              "upperBound": 9,
              "numPartitions": 5
            },
             "PRODUCT_TRANSLATOR_VW": {
              "partitionColumn": "PROD_ID mod 5",
              "lowerBound": 0,
              "upperBound": 9,
              "numPartitions": 5
            }
       }
   }
}
'
