export PROJECT_ID=haven_commhub
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
export CONNECTION_ID=haven_commhub
export SQUARK_METADATA=1
export CONVERT_ARRAYS_TO_STRING=1

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'zd_ticket': {
              'partitionColumn': 'id',
              'lowerBound': 27000,
              'upperBound': 600000,
              'numPartitions': 50
            },
            'zd_comment_raw': {
              'partitionColumn': 'id',
              'lowerBound': 27000,
              'upperBound': 600000,
              'numPartitions': 50
            }
        }
   }
}
"