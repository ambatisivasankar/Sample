export PROJECT_ID=haven_pa
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
export CONNECTION_ID=haven_pa
export SQUARK_METADATA=1

# 2019.04.15, batch_log choking on one .orc part file for batch_log, HavenLife gave permission to skip the table
#  each time load-to-vertica failed on part-00019 with "Failed to parse the footer" error, row count only went up by 5
#  rows vs. previous no-problems load, implies it may be only one+ of the new rows causing the issue?
export EXCLUDE_TABLES="batch_log"
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

