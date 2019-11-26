export PROJECT_ID=trac_rpw_hist
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_TABLES="AHPS_MONEYTYPE"
export SKIP_MIN_MAX_ON_CAST=1
export CONNECTION_ID=trac_rpw_hist
export SKIP_SOURCE_ROW_COUNT=1

export TZ=GMT
export SPARK_MAX_EXECUTORS=10
export JSON_INFO='{
    "PARTITION_INFO": {
        "tables": {
            "AHPS_MONEYTYPE": {
                "partitionColumn": "AH_BTCH_DT >= """01-JAN-2016""" AND AH_RCRD_ID",
                "lowerBound": 400000000,
                "upperBound": 600000000,
                "numPartitions": 50
            }
        }
    }
}'
