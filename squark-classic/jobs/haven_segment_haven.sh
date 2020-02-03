export PROJECT_ID=haven_segment_haven
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
export CONNECTION_ID=haven_segment_haven
export SPARK_MAX_EXECUTORS=30

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'application_question_answered': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"timestamp\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'pages': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"timestamp\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            }
        }
   }
}
"