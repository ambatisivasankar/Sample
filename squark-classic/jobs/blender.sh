export PROJECT_ID=blender
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=blender
export SQUARK_METADATA=1

export JSON_INFO='
{
    "PARTITION_INFO":{
        "tables": {
            "NOTIF": {
              "partitionColumn": "NOTIF_ID % 100",
              "lowerBound": 0,
              "upperBound": 100,
              "numPartitions": 100
            },
            "RULE_REQ": {
              "partitionColumn": "RULE_REQ_ID % 100",
              "lowerBound": 0,
              "upperBound": 100,
              "numPartitions": 100
            },
            "UW_POL": {
              "partitionColumn": "UW_POL_ID % 100",
              "lowerBound": 0,
              "upperBound": 100,
              "numPartitions": 100
            }
        }
   }
}
'
