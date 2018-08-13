export PROJECT_ID=haven_cdmproducers
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
# cdmproducers db
export CONNECTION_ID=haven_cdm
export SQUARK_METADATA=1


export JSON_INFO="
{
	'SAVE_TABLE_SQL_SUBQUERY':{
      'schema': 'dbo',
      'table_queries': {
      		'commissionable_event_info': '(SELECT \\\"id\\\" FROM commissionable_event_info) as subquery'
        }
    }
}"


