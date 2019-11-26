export PROJECT_ID=haven_cdmfounders
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
# coverpath db
export CONNECTION_ID=haven_cp
export SQUARK_METADATA=1


export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'producer_hierarchy_file_row': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            }
        }
   }
}
"
