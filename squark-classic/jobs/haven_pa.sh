export PROJECT_ID=haven_pa
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
export CONNECTION_ID=haven_pa
export SQUARK_METADATA=1

# 2018.10.01, batch_log is tiny but message column very long, with curr dataset "SECONDS/createdTime" isn't good but best available
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'policy_doc': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'batch_log': {
              'partitionColumn': 'DATE_PART('''SECONDS''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            }
        }
   }
}
"

