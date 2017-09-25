import re
import os

#from py4jdbc import *

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import HiveContext, functions as F, Row
import hashlib
from pyspark.sql.types import ArrayType

import squark.config.environment
import squark.stats
import sys
from decimal import Decimal
import json
import time
import socket
import requests
import datetime
import time
from collections import defaultdict

import utils
# For data_catalog-statscli to send stats
#import importlib
#dcstats = importlib.import_module('data_catalog-statscli.dcstats_cli')

PROJECT_ID = os.environ['PROJECT_ID']
CONNECTION_ID = os.environ['CONNECTION_ID']
SQUARK_TYPE = os.environ['SQUARK_TYPE']
VERTICA_CONNECTION_ID = os.environ.get('VERTICA_CONNECTION_ID')
BUILD_NUMBER = os.environ.get('BUILD_NUMBER', '')
JOB_NAME = os.environ.get('JOB_NAME', '')

#SOURCE_ID = os.environ['SOURCE_ID']
squarkenv = squark.config.environment.Environment()
CONNECTION_TYPE = squarkenv.sources[CONNECTION_ID].type
JDBC_USER = squarkenv.sources[CONNECTION_ID].user
JDBC_PASSWORD = squarkenv.sources[CONNECTION_ID].password
JDBC_URL = squarkenv.sources[CONNECTION_ID].url
try:
    JDBC_SCHEMA = squarkenv.sources[CONNECTION_ID].default_schema
except:
    JDBC_SCHEMA = ''

print('CONNECTION_ID:',CONNECTION_ID)
print('JDBC_USER:',JDBC_USER)
#print('JDBC_PASSWORD:',JDBC_PASSWORD)
print('JDBC_URL:',JDBC_URL)
print('JDBC_SCHEMA:',JDBC_SCHEMA)
print('CONNECTION_TYPE:',CONNECTION_TYPE)
WAREHOUSE_DIR = os.environ['WAREHOUSE_DIR']
print('WAREHOUSE_DIR:', WAREHOUSE_DIR)
DATA_DIR = os.path.join(WAREHOUSE_DIR, PROJECT_ID)
print('DATA_DIR:', DATA_DIR)
SQL_TEMPLATE = os.environ['SQL_TEMPLATE']
SPARK_JOB_NAME = '%s-squark-all-tables' % PROJECT_ID
SKIP_ERRORS = os.environ.get('SKIP_ERRORS')
INCLUDE_VIEWS = os.environ.get('INCLUDE_VIEWS')
SKIP_MIN_MAX_ON_CAST = os.environ.get('SKIP_MIN_MAX_ON_CAST')
SKIP_SOURCE_ROW_COUNT = os.environ.get('SKIP_SOURCE_ROW_COUNT', '').lower() in ['1', 'true', 'yes']

JSON_INFO = os.environ.get('JSON_INFO')
TABLES_WITH_SUBQUERIES = {}
TABLES_WITH_PARTITION_INFO = {}
if JSON_INFO:
    parsed_json = json.loads(JSON_INFO.replace("'",'"').replace('"""',"'"))
    if 'SAVE_TABLE_SQL_SUBQUERY' in parsed_json.keys():
        TABLES_WITH_SUBQUERIES = parsed_json['SAVE_TABLE_SQL_SUBQUERY']['table_queries']
        print('TABLES_WITH_SUBQUERIES: %r' % TABLES_WITH_SUBQUERIES)
    if 'PARTITION_INFO' in parsed_json.keys():
        TABLES_WITH_PARTITION_INFO = parsed_json['PARTITION_INFO']['tables']
        print('TABLES_WITH_PARTITION_INFO: %r' % TABLES_WITH_PARTITION_INFO)

SMD_SOURCE_ROW_COUNTS = 'source_row_counts'
SMD_SOURCE_ROW_COUNTS_AFTER = 'source_row_counts_after'
SMD_CONNECTION_INFO = 'connection_info'
SQUARK_METADATA_KEYS = [SMD_SOURCE_ROW_COUNTS, SMD_SOURCE_ROW_COUNTS_AFTER, SMD_CONNECTION_INFO]

INCLUDE_TABLES = os.environ.get('INCLUDE_TABLES')
if INCLUDE_TABLES is not None:
    INCLUDE_TABLES = [s.strip() for s in INCLUDE_TABLES.split(',') if s]

EXCLUDE_TABLES = os.environ.get('EXCLUDE_TABLES', [])
if EXCLUDE_TABLES:
    EXCLUDE_TABLES = [s.strip() for s in EXCLUDE_TABLES.split(',') if s]

WRITE_MODE = os.environ.get('WRITE_MODE', 'overwrite')
WRITE_CODEC = os.environ.get('WRITE_CODEC', 'org.apache.hadoop.io.compress.GzipCodec')
WRITE_FORMAT = os.environ.get('WRITE_FORMAT', 'orc')
CHECK_PRIVILEGES = os.environ.get('CHECK_PRIVILEGES', '').lower() in ['1', 'true', 'yes']
SPARKLOCAL = os.environ.get('SPARKLOCAL', '0').lower() in ['1', 'true', 'yes']
SPARKLOCAL_CORE_COUNT = os.environ.get('SPARKLOCAL_CORE_COUNT', 1)
TABLE_RETRY_NUM = int(os.environ.get('SQUARK_NUM_RETRY', '1'))
USE_CLUSTER_EMR = os.environ.get('USE_CLUSTER_EMR', '').lower() in ['1', 'true', 'yes']

# Get the environment variable for whether to stringify the columns which are array types (for SOG mainly)
CONVERT_ARRAYS_TO_STRING = os.environ.get('CONVERT_ARRAYS_TO_STRING')

# Check hostedgraphite and get settings
graphite = squarkenv.sources['hostedgraphite']
GRAPHITE_URL = graphite.cfg['url']
GRAPHITE_PORT = graphite.cfg['port']
GRAPHITE_TOKEN = graphite.cfg['token']

# DATACATALOG settings and information:
DATACATALOG_TOKEN = os.environ.get('DATACATALOG_TOKEN')
DATACATALOG_DOMAIN = os.environ.get('DATACATALOG_DOMAIN')

USE_HDFS = os.environ.get('USE_HDFS')
USE_AWS = os.environ.get('USE_AWS')
S3_CONNECTION_ID = os.environ.get('S3_CONNECTION_ID')
if USE_AWS:
    aws = squarkenv.sources[S3_CONNECTION_ID]
    AWS_ACCESS_KEY_ID = aws.cfg['access_key_id']
    AWS_SECRET_ACCESS_KEY = aws.cfg['secret_access_key']
    SQUARK_BUCKET = os.environ.get('SQUARK_BUCKET', 'squark')

vertica_conn = squarkenv.sources[VERTICA_CONNECTION_ID].conn

def add_md5_column(df):
    return df.withColumn('_advana_md5', F.md5(F.concat_ws('!@#$', *df.columns)))

def add_auto_incr_column(df):
    #return df.withColumn('_advana_id', F.monotonicallyIncreasingId())
    return df.withColumn('_advana_id', F.monotonically_increasing_id())

def add_load_datetime(df):
    return df.withColumn('_advana_load_date', F.lit(datetime.datetime.now()))

def sanitize_columns(df):
    def sanitize(name):
        return re.sub(r'\W+', '_', name)
    for col in df.schema.names:
        df = df.withColumnRenamed(col, sanitize(col))
    return df

def convert_array_to_string(df):
    sch=df.schema
    cols=[a.name for a in sch.fields if isinstance(a.dataType, ArrayType)]
    for col in cols:
        df = df.withColumn(col, df[col].cast('string'))
    return df

def log_source_row_count(sqlctx, table_name, properties, db_product_name):
    count = None
    # 'ase' = Sybase
    handled_db_prefixes = ['teradata','postgres','microsoft sql','ase','oracle','db2']
    if any(db_product_name.lower().startswith(db) for db in handled_db_prefixes) and not SKIP_SOURCE_ROW_COUNT:
        if db_product_name.lower().startswith('oracle'):
            sql_query = '(SELECT COUNT(*) as cnt FROM "{}")'.format(table_name)
        elif db_product_name.lower().startswith('db2'):
            sql_query = '(SELECT COUNT(*) as cnt FROM {}.{}) as query'.format(JDBC_SCHEMA, table_name)
        else:
            # double-quotes were helping with at least one postgres source, db2 doesn't like them
            sql_query = '(SELECT COUNT(*) as cnt FROM "{}") as query'.format(table_name)
        print('--- Executing source row count query: {}'.format(sql_query, flush=True))
        df = sqlctx.read.jdbc(JDBC_URL, sql_query, properties=properties)
        count = df.first()[0]
        print('--- SOURCE ROW COUNT: {}'.format(count, flush=True))
    else:
        print('--- Skip source row count query, not implemented for: {}'.format(db_product_name, flush=True))

    return count

# .orc does Julian/Gregorian calendar conversion, Vertica doesn't, 0001-01-01 values become a BC date in Vertica
# we are going to concentrate on fixing the main offenders - default min date/timestamps are common in the db
# other dates wouldn't necessarily be 2 days off, complicated conversion involved, 0001-01-03 works here
def post_date_teradata_dates_and_timestamps(df):
    post_date_date = F.to_date(F.lit('0001-01-03'))
    post_date_timestamp = F.to_utc_timestamp(F.lit('0001-01-03 00:00:00'), 'GMT+5')
    if USE_AWS:
        post_date_timestamp = F.to_utc_timestamp(F.lit('0001-01-03 05:00:00'), 'GMT+5')
    for field in df.schema.fields:
        if field.dataType.typeName().lower() == 'date':
            print('------- POST DATE CHECK on : {}'.format(field.name), flush=True)
            df = df.withColumn(field.name, F.when(df[field.name] == '0001-01-01', post_date_date)
                               .otherwise(df[field.name]))
        elif field.dataType.typeName().lower() == 'timestamp':
            print('------- POST TIMESTAMP CHECK on : {}'.format(field.name), flush=True)
            df = df.withColumn(field.name, F.when(df[field.name] == '0001-01-01 00:00:00', post_date_timestamp)
                               .otherwise(df[field.name]))
    return df

def conform_any_extreme_decimals(df):
    for field in df.schema.fields:
        vertica_max_precision = 37
        if field.dataType.typeName().lower() == 'decimal' and field.dataType.precision > vertica_max_precision:
            # value will mangle in Vertica if precision > 37, even if target ddl has correct precision
            print('------- PROBLEM DECIMAL for {}, precision > {}, check values before cast'.format(
                  field.name, vertica_max_precision),
                  flush=True)
            # set cast scale to that of source unless it was 38, in which case set to 37
            scale_def = field.dataType.scale if field.dataType.scale <= vertica_max_precision else vertica_max_precision

            if SKIP_MIN_MAX_ON_CAST:
                print('------- SKIP_MIN_MAX_ON_CAST is set, no the min/max query will be done', flush=True)
            else:
                df_min_max = df.select([F.max(field.name).alias('max_val'), F.min(field.name).alias('min_val')])
                row = df_min_max.first()
                min_val = row['min_val']
                max_val = row['max_val']
                print('------- Reported minimum value: {}\n------- Reported maximum value: {}'.format(min_val, max_val),
                      flush=True)

                # only going to get min_val = Null if all rows are null
                if min_val is not None:
                    too_big_num = int('9' * vertica_max_precision)
                    if abs(min_val) > too_big_num or abs(max_val) > too_big_num:
                        msg = 'Precision of actual numeric data in {} larger than Vertica will handle ({})\n'.format(
                            field.name,
                            vertica_max_precision
                        )
                        msg += 'Reported minimum value: {}\nReported maximum value: {}'.format(min_val, max_val)
                        raise OverflowError(msg)

                    # if incoming ddl like (38,8), need to make sure there are no numbers with 30 digits in integral part
                    # else will be forcing value into (37,8), leaving only room for 9 integral digits, Spark sets value=None
                    spread = vertica_max_precision - field.dataType.scale
                    if spread <= 0:
                        max_at_new_precision = Decimal('0.{}'.format('9' * vertica_max_precision))
                    else:
                        max_at_new_precision = int('9' * spread)

                    if abs(min_val) > max_at_new_precision or abs(max_val) > max_at_new_precision:
                        print('------- max number ({}) at new precision < data min/max value'.format(max_at_new_precision),
                              flush=True)
                        print('------- decrease scale from {} to {}'.format(scale_def, scale_def - 1), flush=True)
                        scale_def -= 1


            decimal_def = 'Decimal({},{})'.format(vertica_max_precision, scale_def)

            print('------- column will be cast to: {}'.format(decimal_def), flush=True)
            df = df.withColumn(field.name, df[field.name].cast(decimal_def))

    return df

def push_graphite_stats(stats, table_name):
    """
    Function to push data stats (such as count) to graphite.
    Takes only the stats dictionary as input, but expects to 
    find the 'hostedgraphite' url, token, and port in the 
    secrets file.
    """
    stats_time = int(time.time())
    message = "{API_TOKEN}.squark.{SQUARK_TYPE}.{PROJECT_ID}.{TABLE_NAME}.hdfs_rows {ROWS} {TIME}\n".format(
            API_TOKEN=GRAPHITE_TOKEN,
            SQUARK_TYPE=SQUARK_TYPE,
            PROJECT_ID=PROJECT_ID,
            TABLE_NAME=table_name,
            ROWS=stats['count'],
            TIME=stats_time
            )
    print("SENDING MESSAGE TO GRAPHITE:")
    print(message.replace(GRAPHITE_TOKEN,"*******"))

    conn = socket.create_connection((GRAPHITE_URL, GRAPHITE_PORT))
    conn.send(message.encode('utf8'))

    # Send error 
    message = "{API_TOKEN}.squark.{SQUARK_TYPE}.{PROJECT_ID}.{TABLE_NAME}.error_code {ERROR_CODE} {TIME}\n".format(
            API_TOKEN=GRAPHITE_TOKEN,
            SQUARK_TYPE=SQUARK_TYPE,
            PROJECT_ID=PROJECT_ID,
            TABLE_NAME=table_name,
            ERROR_CODE=stats['error_code'],
            TIME=stats_time
            )
    print(message.replace(GRAPHITE_TOKEN,"*******"))
    conn.send(message.encode('utf8'))
    conn.close()

def push_data_catalog_stats(stats, DOMAIN, TOKEN, SCHEMA, TABLE):
    """
    This function is going to be used to send stats to the datacatalog api.
    It requires that an API Token be set  - as well as the domain name.
    """
    # This value will be used for every column
    count = stats.get('count', 0)

    for COLUMN, col_stats in stats['fields'].items():
        print("------SENDING STATS FOR SCHEMA: {}  TABLE: {}  COLUMN: {}".format(SCHEMA,TABLE,COLUMN))

        # Example stats
        # {'fields': defaultdict(<class 'dict'>, {'f2': {'countDistinct': 3, 'max': 3, 'min': 1}, '_advana_id': {'max': 7, 'min': 0, 'mean': 3.5, 'countDistinct': 8}, 'f1': {'max': 88, 'min': 11, 'mean': 49.5, 'countDistinct': 8}, '_advana_md5': {'min': 32, 'max': 32, 'countDistinct': 1}}), 'count': 8}
        count_empty = col_stats.get('count_null', 0)
        count_unique = col_stats.get('countDistinct', 0)
        value_min = col_stats.get('min', 0)
        value_max = col_stats.get('max', 0)
        value_median = col_stats.get('median', 0)
        value_mean = col_stats.get('mean', 0)
        percentile_75 = col_stats.get('percentile_75', 0)
        percentile_25 = col_stats.get('percentile_25', 0)
        details = col_stats.get('details','{}')

        # Send the stats
        # NOTE: Had to use the .callback function due to the send_stats being decorated by click command.
        dcstats.send_stats.callback(
                schema=SCHEMA,
                table=TABLE,
                column=COLUMN,
                count=count,
                count_empty=count_empty,
                count_unique=count_unique,
                value_min=value_min,
                value_max=value_max,
                value_median=value_median,
                value_mean=value_mean,
                percentile_25=percentile_25,
                percentile_75=percentile_75,
                details=details,
                token=TOKEN,
                domain=DOMAIN
        )

def save_table(sqlctx, table_name, squark_metadata):
    dbtable = SQL_TEMPLATE % table_name
    print('********* EXECUTE SQL: %r' % dbtable)
    properties = dict(user=JDBC_USER, password=JDBC_PASSWORD)
    if USE_CLUSTER_EMR:
        print('--- USE_CLUSTER_EMR is a go')
        driver_name_for_spark = squark_metadata[SMD_CONNECTION_INFO].get('driver_name_for_spark', '')
        if driver_name_for_spark:
            print('--- ... and setting driver_name_for_spark: {}'.format(driver_name_for_spark))
            properties['driver'] = driver_name_for_spark

    db_name = squark_metadata[SMD_CONNECTION_INFO]['db_product_name']
    start_query_time = time.time()
    source_row_count = log_source_row_count(sqlctx, table_name, properties, db_name)
    row_count_query_duration = time.time() - start_query_time
    if source_row_count:
        print('--- BEFORE COUNT QUERY DURATION: {:.0f} seconds = {:.2f} minutes'.format(row_count_query_duration,
                                                                                 row_count_query_duration / 60),
              flush=True)
        row_count_info = {'count': source_row_count, 'query_time': datetime.datetime.now(),
                          'seconds_query_duration': row_count_query_duration}
        squark_metadata[SMD_SOURCE_ROW_COUNTS][table_name] = row_count_info

    if TABLES_WITH_SUBQUERIES and table_name.lower() in [table.lower() for table in TABLES_WITH_SUBQUERIES.keys()]:
        table_queries_lower = {k.lower():v for k,v in TABLES_WITH_SUBQUERIES.items()}
        sql_query = table_queries_lower[table_name.lower()]
        # NOTE: syntax on JDBC subquery differs among source db systems, e.g. Oracle doesn't take an alias on subquery
        print('--- Executing subquery: %r' % (sql_query), flush=True)
        df = sqlctx.read.jdbc(JDBC_URL, sql_query, properties=properties)

    elif TABLES_WITH_PARTITION_INFO and table_name.lower() in [table.lower() for table in
                                                               TABLES_WITH_PARTITION_INFO.keys()]:
        table_with_partitions_lower = {k.lower(): v for k, v in TABLES_WITH_PARTITION_INFO.items()}
        partition_info = table_with_partitions_lower[table_name.lower()]
        print('--- Partition info: %r' % partition_info, flush=True)
        partition_column = partition_info['partitionColumn']
        lower_bound = partition_info['lowerBound']
        upper_bound = partition_info['upperBound']
        num_partitions = partition_info['numPartitions']

        df = sqlctx.read.format('jdbc').options(
            url=JDBC_URL,
            dbtable=dbtable,
            user=JDBC_USER,
            password=JDBC_PASSWORD,
            partitionColumn=partition_column,
            lowerBound=lower_bound,
            upperBound=upper_bound,
            numPartitions=num_partitions).load()
    else:
        if db_name.lower().startswith('db2'):
            # per documentation, and logic, this is how we should be doing all queries, but would need to test broadly,
            # running every squark job through below, either all connections must have schema or only '.' when present
            df = sqlctx.read.jdbc(JDBC_URL, table='{}.{}'.format(JDBC_SCHEMA, dbtable), properties=properties)
        else:
            df = sqlctx.read.jdbc(JDBC_URL, table=dbtable, properties=properties)

    print('--- Sanitizing columns for %r: %r' % (dbtable, df.schema.names))
    df = sanitize_columns(df)
    print('--- Sanitized columns for %r are %r' % (dbtable, df.schema.names))
    print('--- Conforming DecimalType as necessary for %r: %r' % (dbtable, df.schema.names))
    df = conform_any_extreme_decimals(df)
    if CONVERT_ARRAYS_TO_STRING:
        print('--- Converting array fields to string for %r' % dbtable)
        df = convert_array_to_string(df)
    if db_name.lower().startswith('teradata'):
        print('--- Post-dating min teradata date/timestamp values for %r' % dbtable)
        df = post_date_teradata_dates_and_timestamps(df)
    print('--- Adding md5 column for %r' % dbtable)
    df = add_md5_column(df)
    print('--- Adding incr column for %r' % dbtable)
    df = add_auto_incr_column(df)
    print('--- Adding date load column for %r' % dbtable)
    df = add_load_datetime(df)
    print('----- Getting Stats:')
    #conf = os.environ.get("STATS_CONFIG", "{}").replace("'",'"')
    #conf = json.loads(conf)
    #stats = squark.stats.get_stats(df, conf)
    #print(stats)

    if USE_AWS:
        s2 = time.time()
        s3_file_system = 's3a' if USE_CLUSTER_EMR else 's3n'
        #s3_file_system = 's3n'
        save_path = "{S3_FILESYSTEM}://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}@{SQUARK_BUCKET}/{SQUARK_TYPE}/{PROJECT_ID}/{TABLE_NAME}/{TABLE_NAME}.orc/".format(
                S3_FILESYSTEM = s3_file_system,
                AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY,
                SQUARK_BUCKET=SQUARK_BUCKET,
                SQUARK_TYPE=SQUARK_TYPE,
                PROJECT_ID=PROJECT_ID,
                TABLE_NAME=table_name
        )
        print('******* SAVING TABLE TO S3: %r' % save_path.replace(AWS_SECRET_ACCESS_KEY,'********'))
        opts = dict(codec=WRITE_CODEC)
        # Adding retries to the save process for aws s3 saves
        print('-----------------------------------------')
        curr_retry = 0
        retry_bool = True
        while retry_bool and curr_retry < TABLE_RETRY_NUM:
            print('Attempting to save {table}: [Attempt {curr}/{tot}]'.format(
                    table=dbtable, curr=curr_retry+1, tot=TABLE_RETRY_NUM))
            try:
                df.write.format('orc').options(**opts).save(save_path, mode=WRITE_MODE)
                retry_bool = False
            except Exception as e:
                print('!! -- An Error occurred while trying to save table: %r'%(dbtable))
                print(str(e))
                print('Taking a quick 5 second nap and restarting the save for this table...')
                curr_retry += 1
                time.sleep(5)
        if retry_bool:
            print('ERROR! Number of retries exceeded!! Exiting...')
            raise
        print('Save successful...')       
        print('-----------------------------------------')
        
        e2 = time.time()
        print(' ----- Writing to S3 took: %.3f seconds'%(e2-s2))
    if USE_HDFS or (not USE_AWS and not USE_HDFS):
        s3 = time.time()
        save_path = "%s/%s" % (DATA_DIR, table_name)    
        print('******* SAVING TABLE TO HDFS: %r' % save_path)
        opts = dict(codec=WRITE_CODEC)
        df.write.format(WRITE_FORMAT).options(**opts).save(save_path, mode=WRITE_MODE)
        e3 = time.time()
        print(' ----- Writing to HDFS took: %.3f seconds'%(e3-s3))

    if source_row_count and row_count_query_duration < 60:
        start_query_time = time.time()
        source_row_count = log_source_row_count(sqlctx, table_name, properties, db_name)
        row_count_query_duration = time.time() - start_query_time
        if source_row_count:
            print('--- AFTER COUNT QUERY DURATION: {:.0f} seconds = {:.2f} minutes'.format(row_count_query_duration,
                                                                                     row_count_query_duration / 60),
                  flush=True)
            row_count_info = {'count': source_row_count, 'query_time': datetime.datetime.now(),
                              'seconds_query_duration': row_count_query_duration}
            squark_metadata[SMD_SOURCE_ROW_COUNTS_AFTER][table_name] = row_count_info


            #print('----- Sending stats to graphite:')
    #stats['error_code'] = 0
    #push_graphite_stats(stats, dbtable)

    #if conf and SQUARK_TYPE == 'squark-prod':
    #    print('---- Sending stats to Data Catalog:')
    #    try:
    #        push_data_catalog_stats(stats, DATACATALOG_DOMAIN, DATACATALOG_TOKEN, PROJECT_ID, table_name)
    #    except Exception as e:
    #        print('ERROR PUSHING STATS TO DATACATALOG:\n {}'.format(str(e)))
    #        print(' -- Continuing without sending stats....')

def main():
    try:
        conf = SparkConf()
        conf.set("spark.local.dir", "/hadoop/sparklocal")
        if SPARKLOCAL:
            conf.set("spark.master", "local[{}]".format(SPARKLOCAL_CORE_COUNT))
        sc = SparkContext(appName=SPARK_JOB_NAME, conf=conf)
        sqlctx = HiveContext(sc)
        source = squarkenv.sources[CONNECTION_ID]
        #conn = connect(JDBC_URL, JDBC_USER, JDBC_PASSWORD, gateway=gateway)
        squark_metadata = {k: {} for k in SQUARK_METADATA_KEYS}
        conn_metadata = utils.populate_connection_metadata(source.conn._metadata)
        squark_metadata[SMD_CONNECTION_INFO] = conn_metadata
        metadata_formatted = '\n\t'.join(('{}: {}'.format(k, v) for k, v in sorted(conn_metadata.items())))
        print('*************** JDBC connection metadata:\n\t{}'.format(metadata_formatted))

        table_name_key = 'table_name'
        db_product_name = squark_metadata[SMD_CONNECTION_INFO]['db_product_name']
        if db_product_name.lower().startswith('db2'):
            table_name_key = 'name'
            #get_tables().fetchall() or .fetchmany(#) where # > number of tables in schema are both failing via db2
            table_count = utils.get_number_of_tables_in_db2_schema(source.conn, JDBC_SCHEMA)
            print('*************** DB2 SCHEMA TABLE COUNT: {}'.format(table_count))
            tables = source.conn.get_tables(schema=JDBC_SCHEMA).fetchmany(table_count)
        else:
            tables = source.conn.get_tables(schema=JDBC_SCHEMA).fetchall()

        tables = [{k.lower(): v for (k,v) in x._asdict().items()} for x in tables]
        tables = [x for x in tables if x['table_type'] in ('TABLE','VIEW')]
        processed_tables = []
        print('*************** TABLES: %r' % list(tables))
        s0 = time.time()
        table_timing = []
        for table in tables:
            #table = {k.lower(): v for (k, v) in table._asdict().items()}
            s1 = time.time()
            if INCLUDE_TABLES is not None:
                if table[table_name_key] not in INCLUDE_TABLES:
                    print('*******SKIPPING NOT INCLUDED TABLE: %r' % table)
                    continue
    
            if table[table_name_key] in EXCLUDE_TABLES:
                print('*******SKIPPING EXCLUDE_TABLES TABLE: %r' % table)
                continue
    
            # Skip indexes and stuff
            if table['table_type'] not in ('TABLE', 'VIEW'):
                print('**********SKIPPING NON-TABLE/VIEW, table_type: {}'.format(table['table_type']))
                continue
    
            if CHECK_PRIVILEGES:
                # Skip if we can't read the table.
                privs = conn._metadata.getTablePrivileges(conn._jconn.getCatalog(), "%", table["table_name"])
                can_select = False
                while privs.next():
                    get = privs.getString
                    if get('GRANTEE') == JDBC_USER and get('PRIVILEGE').lower() == 'select':
                        can_select = True
                        break
                if not can_select:
                    print("**********SKIPPING TABLE (no select available): %r" % table)
                    continue
    
            if SKIP_ERRORS:
                try:
                    save_table(sqlctx, table[table_name_key], squark_metadata)
                except Exception as exc:
                    print(exc)
                    try:
                        os.mkdir('err')
                    except:
                        pass
                    with open('err/%s' % table[table_name_key], 'w') as f:
                        f.write(str(exc))
            else:
                save_table(sqlctx, table[table_name_key], squark_metadata)

            processed_tables.append(table[table_name_key])
            table_time = time.time() - s1
            print(' ------- Total Time for Table %s: %.3f seconds'%(table, table_time))
            table_timing.append([table[table_name_key], table_time])

    except Exception as e:
        error_message = str(e)
        #error_code = 1
        print('Error occurred: \n%{error_message}'.format(error_message=error_message))
        #remaining_tables = list(set([x[table_name_key] for x in tables]).difference(set(processed_tables)))
        #for table_name in remaining_tables:
        #    push_graphite_stats({'count': 0, 'error_code': error_code}, table_name)
        sys.exit(1)

    # Print timing results at end of the script:
    print('===============================================')
    print('Total times for each table:')
    print('-----------------------------------------------')
    print("\n".join([" - %s: %.3f seconds"%(x[0], x[1]) for x in table_timing]))
    print('===============================================')
    # Send the table timings to vertica
    utils.send_table_timings_to_vertica(vertica_conn, PROJECT_ID, table_timing, BUILD_NUMBER, JOB_NAME)


    source_row_counts_before = squark_metadata[SMD_SOURCE_ROW_COUNTS]
    if source_row_counts_before:
        print('Source row counts for each table:')
        print('--BEFORE PULL--------------------------------------------')
        print(' - table\tcount\tas of\tquery duration')
        row_counts = []
        for table in sorted(source_row_counts_before):
            count = source_row_counts_before[table]['count']
            as_of = source_row_counts_before[table]['query_time']
            query_duration = source_row_counts_before[table]['seconds_query_duration']
            is_after = 0
            print(' - {}\t{}\t{}\t{}'.format(table, count, as_of, query_duration))
            row_counts.append([table, count, as_of, query_duration, is_after])
        utils.send_source_row_counts_to_vertica(vertica_conn, PROJECT_ID, JDBC_SCHEMA, row_counts, BUILD_NUMBER,
                                                JOB_NAME)

    source_row_counts_after = squark_metadata[SMD_SOURCE_ROW_COUNTS_AFTER]
    if source_row_counts_after:
        print('--AFTER PULL---------------------------------------------')
        print(' - table\tcount\tas of\tquery duration')
        row_counts = []
        for table in sorted(source_row_counts_after):
            count = source_row_counts_after[table]['count']
            as_of = source_row_counts_after[table]['query_time']
            query_duration = source_row_counts_after[table]['seconds_query_duration']
            is_after = 1
            print(' - {}\t{}\t{}\t{}'.format(table, count, as_of, query_duration))
            row_counts.append([table, count, as_of, query_duration, is_after])
        utils.send_source_row_counts_to_vertica(vertica_conn, PROJECT_ID, JDBC_SCHEMA, row_counts, BUILD_NUMBER,
                                                JOB_NAME)
        print('===============================================')


if __name__ == "__main__":

  #  with GatewayProcess() as gw:
  main()
