export PROJECT_ID=squark_staging
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=pdoncall


# Optional
export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=1
export SPARK_EXECUTOR_MEMORY="8G"




export INCLUDE_TABLES='MEDICAL'

export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':{
        'MEDICAL': 
        {
            'sql_query': '(select * from pdoncall.dbo.national_donotcall) as subquery'
            
        }
    }
 }   
 "


