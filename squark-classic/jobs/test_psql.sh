export PROJECT_ID=test_psql
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CHECK_PRIVS=1
export CONNECTION_ID=test_psql
export STATS_CONFIG="
{
    'profiles': {
        'numeric': ['max','min','mean','countDistinct'],
        'string': ['max', 'min','countDistinct'],
        'datetype': ['max', 'min','countDistinct']
    }, 
    'field_types': {
        'NumericType': 'numeric',
        'StringType': 'string',
        'DateType': 'datetype'
    }
}
"
