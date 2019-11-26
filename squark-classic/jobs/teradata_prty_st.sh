#Required
export PROJECT_ID=teradata_prty_st
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'

#Optional
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='PRTY_ALT_ID_VW'
export CONNECTION_ID=teradata_prty_st

export JSON_INFO='
{
    "SUPER_PROJECTION_SETTINGS":{
        "tables": {
            "PRTY_ALT_ID_VW": {
                "projection_name": "PRTY_ALT_ID_VW_SQUARK",
                "order_by_columns": "PRTY_ID",
                "segment_by_columns": "PRTY_ID"
            }
        }
    },
    "PARTITION_INFO":{
        "tables": {
            "PRTY_ALT_ID_VW": {
              "partitionColumn": "PRTY_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            }
        }
    }
}
'
