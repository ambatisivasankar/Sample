import os
import re
import json
import time

from jinja2 import Template

from py4jdbc import GatewayProcess, connect
import squark.config.environment
import utils

# Vertica reserved words

reserved = ['ALL', 'ANALYSE', 'ANALYZE', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC',
'BINARY', 'BOTH', 'CASE', 'CAST', 'CHECK', 'COLUMN', 'CONSTRAINT',
'CORRELATION', 'CREATE', 'CURRENT_DATABASE', 'CURRENT_DATE', 'CURRENT_SCHEMA',
'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'DEFAULT', 'DEFERRABLE',
'DESC', 'DISTINCT', 'DO', 'ELSE', 'ENCODED', 'END', 'EXCEPT', 'FALSE', 'FLEX',
'FLEXIBLE', 'FOR', 'FOREIGN', 'FROM', 'GRANT', 'GROUP', 'GROUPED', 'HAVING',
'IN', 'INITIALLY', 'INTERSECT', 'INTERVAL', 'INTERVALYM', 'INTO', 'JOIN',
'KSAFE', 'LEADING', 'LIMIT', 'LOCALTIME', 'LOCALTIMESTAMP', 'MATCH', 'NEW',
'NOT', 'NULL', 'NULLSEQUAL', 'OFF', 'OFFSET', 'OLD', 'ON', 'ONLY', 'OR',
'ORDER', 'PINNED', 'PLACING', 'PRIMARY', 'PROJECTION', 'REFERENCES', 'SCHEMA',
'SEGMENTED', 'SELECT', 'SESSION_USER', 'SOME', 'SYSDATE', 'TABLE', 'THEN',
'TIMESERIES', 'TO', 'TRAILING', 'TRUE', 'UNBOUNDED', 'UNION', 'UNIQUE',
'UNSEGMENTED', 'USER', 'USING', 'WHEN', 'WHERE', 'WINDOW', 'WITH', 'WITHIN']


def sanitize(name):
    if name.upper() in reserved:
        name = 'x_%s' % name
    name = re.sub(r'\W+', '_', name)
    return name


class ColSpec:

    typemap = dict((
        # JDBC, vertica
        # ('BINARY', 'BINARY,')
        # ('VARBINARY', 'VARBINARY,')
        ('BIT', 'BOOLEAN'),
        ('NVARCHAR', 'VARCHAR'),
        ('LONGNVARCHAR', 'LONG VARCHAR'),
        ('LONGVARCHAR', 'LONG VARCHAR'),
        ('NCHAR', 'CHAR'),
        ('CLOB', 'LONG VARCHAR'),
        ('BLOB', 'LONG VARBINARY'),
        ('LONGVARBINARY', 'LONG VARBINARY'),
        # ('CHAR', 'CHAR')
        # ('VARCHAR', 'VARCHAR')
        # ('DATE', 'DATE')
        # ('TIMESTAMP', 'TIMESTAMP')
        # ('TIME', 'TIME')
        # ('TIMESTAMP', 'TIMESTAMP')
        ('DOUBLE', 'FLOAT'),
        #('BIGINT', 'INT,')
        # ('NUMERIC', 'NUMERIC')
        ))

    has_size = ('BINARY', 'VARBINARY', 'CHAR', 'VARCHAR', 'NVARCHAR')
    numby = ('DOUBLE', 'BIGINT', 'NUMERIC')

    def __init__(self, jdbc_spec, squark_spec, source_conn):
        self.spec = jdbc_spec
        self.squark_metadata = squark_spec
        self.is_db2 = squark_spec['conn_metadata']['db_product_name'].lower().startswith('db2')
        self.source_conn = source_conn

    def ddl(self):

        # if self.spec.TABLE_NAME == 'manual_asset':
        #     if self.spec.COLUMN_NAME == 'value':
        #         import pdb; pdb.set_trace()
        # if self.spec.IS_AUTOINCREMENT:
        #     return 'INTEGER'
        from_type = self.spec.data_type
        to_type = self.typemap.get(from_type, from_type)
        data = dict(zip((k.upper() for k in self.spec._fieldnames), self.spec))
        data.update(to_type=to_type)

        if from_type in ('ARRAY', 'OTHER'):
            # Hstore
            return 'VARCHAR(65000)'

        if from_type in ('NVARCHAR', 'NCHAR', 'NVARBINARY'):
            data['COLUMN_SIZE'] = data['COLUMN_SIZE'] * 3

        if 'CHAR' in from_type or 'BINARY' in from_type:
            if 65000 < (data['COLUMN_SIZE'] or 1):

                # TODO: add RUN_LIVE_MAX_LEN_QUERIES to haven job(s) as appropriate
                start_query_time = time.time()
                custom_column_definition = None
                data['COLUMN_SIZE'] = 65000
                print('#'*20, 'from_type: {}'.format(from_type))

                if self.squark_metadata and 'large_ddl' in self.squark_metadata:
                    large_ddl = self.squark_metadata['large_ddl']
                    if self.name in large_ddl:
                        data['COLUMN_SIZE'] = large_ddl[self.name]
                        custom_column_definition = 'squark_config_large_ddl table'
                # 2018.10.25, *_id check covers ~360 columns in curr haven db, any new *_id columns > 255 char = badness
                #  ddl-create w/combo of large_ddl table and live queries is curr < 5min, below saves up to 90 seconds
                if not custom_column_definition and RUN_LIVE_MAX_LEN_QUERIES:
                    if self.name.lower().endswith('_id'):
                        id_like_column_size = 255
                        data['COLUMN_SIZE'] = id_like_column_size
                        custom_column_definition = '.endswith("_id") to {}'.format(id_like_column_size)
                if not custom_column_definition and RUN_LIVE_MAX_LEN_QUERIES:
                    # use self.spec.COLUMN_NAME = the orig, non-sanitized column name
                    max_len = utils.get_postgres_col_max_data_length(self.source_conn, self.spec.TABLE_NAME, self.spec.COLUMN_NAME)
                    custom_column_definition = 'live query on source db'
                    if not max_len or max_len < 255:
                        max_len = 255
                    data['COLUMN_SIZE'] = max_len

                if custom_column_definition:
                    warning_msg = 'meh...'
                    max_len_query_duration = time.time() - start_query_time
                    if max_len_query_duration > 5:
                        warning_msg = 'LOOOOKOUT'
                    debug_msg = 'column_path: {}.{}  max_len: {:,}  max_len_query_duration: {:4f}  warning_msg: {}'.format(
                        self.spec.TABLE_NAME, self.spec.COLUMN_NAME, data['COLUMN_SIZE'], max_len_query_duration, warning_msg)
                    print(debug_msg, flush=True)

                    if data['COLUMN_SIZE'] > 65000:
                        data['to_type'] = 'LONG ' + data['to_type']
                    print('--- Overriding default 650000 length for {}, use value from {}, final ddl will be: {}({})'.format(
                        self.name,
                        custom_column_definition,
                        data['to_type'],
                        data['COLUMN_SIZE']
                    ), flush=True)

        if from_type in self.has_size:
            tmpl = '{to_type}({COLUMN_SIZE})'
        elif from_type == 'NUMERIC' or from_type == 'DECIMAL':
            # Max precision is 1024.
            if 1024 < data['COLUMN_SIZE']:
                data['COLUMN_SIZE'] = 1024
            elif 0 == data['COLUMN_SIZE'] and JDBC_URL.startswith('jdbc:oracle'):
                # spark/sql/jdbc/OracleDialect.scala sets Oracle NUMBER types to (38,10) if size == 0, do same
                data['COLUMN_SIZE'] = 38
                data['DECIMAL_DIGITS'] = 10
            if self.spec.DECIMAL_DIGITS is not None:
                tmpl = '{to_type}({COLUMN_SIZE},{DECIMAL_DIGITS})'
            else:
                tmpl = '{to_type}({COLUMN_SIZE})'
        else:
            tmpl = to_type

        return tmpl.format(**data)

    def _get_db2_column_name(self):
        first_name_index = self.spec._fieldnames.index('NAME')
        first_name = self.spec[first_name_index]
        assumed_column_name = first_name
        if self.spec._fieldnames.count('NAME') > 1:
            second_name_index = self.spec._fieldnames.index('NAME', first_name_index+1)
            assumed_column_name = self.spec[second_name_index]
            # going by limited observations, there will be 2 NAME references, 1st = table name & 2nd = column name
            # only reverse that interpretation if the 2nd NAME == source table name
            if assumed_column_name.upper() == self.squark_metadata['db2_table_name'].upper():
                assumed_column_name = first_name

        return assumed_column_name

    @property
    def name(self):
        if self.is_db2:
            return sanitize(self._get_db2_column_name())
        else:
            return sanitize(self.spec.COLUMN_NAME)

    @property
    def nullable(self):
        return self.spec.NULLABLE

# self.j2_env = Environment(**dict(
#     trim_blocks=True,
#     extensions=['jinja2.ext.with_']))

tmpl = Template('''
drop table if exists {{schema}}.{{table}} cascade;
create table if not exists {{schema}}.{{table}}(
{% for col in colspec %}
    {{ col.name }} {{ col.ddl() }}{% if not col.nullable %} NOT NULL{% endif %},

{% endfor %}
    _advana_md5 varchar(35),
    _advana_id int,
    _advana_load_date timestamp
);
''', trim_blocks=True)

tmpl_deleted_table = Template('''
drop table if exists {{schema}}.{{table}}{{deleted_table_suffix}} cascade;
create table if not exists {{schema}}.{{table}}{{deleted_table_suffix}} (
    {{pkid}} varchar(255)
);
''', trim_blocks=True)


def make_ddl(schema, table, source_conn, colspec, squark_metadata):
    colspec = map(lambda args: ColSpec(args[0], args[1], args[2]), [(spec, squark_metadata, source_conn) for spec in colspec])
    return tmpl.render(schema=schema, table=table, colspec=colspec)


def make_deleted_table_ddl(schema_name, base_table_name, pkid_column_name):
    return tmpl_deleted_table.render(schema=schema_name,
                                     table=base_table_name,
                                     deleted_table_suffix=SQUARK_DELETED_TABLE_SUFFIX,
                                     pkid=pkid_column_name)


def copy_table_ddl(
    from_conn, from_schema, from_table,
    to_conn, to_schema, to_table, squark_metadata):

    start_time = time.time()
    if SQUARK_METADATA:
        ddl_project_key = PROJECT_ID
        if PROJECT_ID in ['haven_daily','haven_weekly','haven_full']:
            ddl_project_key = 'haven'
        large_ddl = utils.get_large_data_ddl_def(to_conn, ddl_project_key, to_table)
        squark_metadata['large_ddl'] = large_ddl if large_ddl else dict()

    db_product_name = squark_metadata['conn_metadata']['db_product_name']
    is_db2 = db_product_name.lower().startswith('db2')
    if is_db2:
        # as with get_tables(), in db2 apparently we need to fetchmany() w/exact number of columns
        column_count = utils.get_number_of_columns_in_db2_table(from_conn, from_schema, from_table)
        print('*************** DB2 TABLE COLUMN COUNT: {}'.format(column_count))
        cols_connection = from_conn.get_columns(schema=from_schema, table=from_table).fetchmany(column_count)
        squark_metadata['db2_table_name'] = from_table
    else:
        cols_connection = from_conn.get_columns(schema=from_schema, table=from_table)

    from_cols = list(cols_connection)
    ddl = make_ddl(to_schema, to_table, from_conn, from_cols, squark_metadata)

    if not is_db2:
        cols_connection.close()
    print('creating table %r' % from_table)
    print(ddl)
    cur = to_conn.cursor()
    rs = cur.execute(ddl)

    if squark_metadata.get('is_incremental'):
        pkid_column_name = squark_metadata['pkid_column_name']
        deleted_table_ddl = make_deleted_table_ddl(to_schema, to_table, pkid_column_name)
        print('creating table {}{}'.format(from_table, SQUARK_DELETED_TABLE_SUFFIX))
        print(deleted_table_ddl)
        cur = to_conn.cursor()
        rs = cur.execute(deleted_table_ddl)

    if RUN_LIVE_MAX_LEN_QUERIES:
        time_taken = time.time() - start_time
        update_load_timings_with_ddl_create_duration(to_conn, table_name, time_taken)


def log_squark_metadata_contents(to_conn):

    large_ddl_table_name = 'squark_config_large_ddl'
    ddl_project_key = PROJECT_ID
    if PROJECT_ID in ['haven_daily', 'haven_weekly', 'haven_full']:
        ddl_project_key = 'haven'
    rs_large_ddl = utils.get_squark_metadata_for_project(to_conn, ddl_project_key, large_ddl_table_name)
    print('--- SQUARK_METADATA=TRUE, contents of {squark_metadata_table_name} for PROJECT_ID "{project_id}":'.format(
        squark_metadata_table_name=large_ddl_table_name,
        project_id=ddl_project_key))
    if rs_large_ddl:
        column_names = rs_large_ddl[0]._fieldnames
        print('\t'.join(column_names))
        print('\t'.join('-' * len(name) for name in column_names))
        for row in rs_large_ddl:
            print('\t'.join(str(val) for val in (list(row))))
    else:
        print('< NO ROWS RETURNED >')


def update_load_timings_with_ddl_create_duration(vertica_conn, base_table_name, time_taken):
    jenkins_name = JENKINS_URL.split('.')[0].split('/')[-1]
    attempt_count = 1
    source = 'n.a.'
    # there isn't straightforward way to get total number of tables/views that will get DDL'd before iteration
    total_table_count = 0
    final_table_name = '{}_SQUARK_DDL'.format(base_table_name)
    utils.send_load_timing_to_vertica(vertica_conn, jenkins_name, JOB_NAME, BUILD_NUMBER, PROJECT_ID, final_table_name,
                                      time_taken, attempt_count, source, total_table_count)


if __name__ == '__main__':

    PROJECT_ID = os.environ['PROJECT_ID']
    CONNECTION_ID = os.environ['CONNECTION_ID']
    USE_AWS = os.environ.get('USE_AWS')
    LOAD_FROM_AWS = os.environ.get('LOAD_FROM_AWS')
    LOAD_FROM_HDFS = os.environ.get('LOAD_FROM_HDFS')

    squarkenv = squark.config.environment.Environment()
    CONNECTION_TYPE = squarkenv.sources[CONNECTION_ID].type
    JDBC_USER = squarkenv.sources[CONNECTION_ID].user
    JDBC_PASSWORD = squarkenv.sources[CONNECTION_ID].password
    JDBC_URL = squarkenv.sources[CONNECTION_ID].url
    try:
        JDBC_SCHEMA = squarkenv.sources[CONNECTION_ID].default_schema
    except:
        JDBC_SCHEMA = 'public'

    try:
        VERTICA_CONNECTION_ID = os.environ['VERTICA_CONNECTION_ID']
    except:
        VERTICA_CONNECTION_ID = "vertica_dev"

    WAREHOUSE_DIR = os.environ['WAREHOUSE_DIR']
    DATA_DIR = os.path.join(WAREHOUSE_DIR, PROJECT_ID)
    INCLUDE_VIEWS = os.environ.get('INCLUDE_VIEWS')

    INCLUDE_TABLES = os.environ.get('INCLUDE_TABLES')
    if INCLUDE_TABLES is not None:
        INCLUDE_TABLES = [s.strip() for s in INCLUDE_TABLES.split(',') if s]

    JSON_INFO = os.environ.get('JSON_INFO')
    TABLES_WITH_PARTITION_INFO = {}
    if JSON_INFO:
        parsed_json = json.loads(JSON_INFO.replace("'", '"').replace('"""', "'"))
        if 'PARTITION_INFO' in parsed_json.keys():
            TABLES_WITH_PARTITION_INFO = parsed_json['PARTITION_INFO']['tables']
            print('TABLES_WITH_PARTITION_INFO: %r' % TABLES_WITH_PARTITION_INFO)

    JENKINS_URL = os.environ.get('JENKINS_URL', '')
    JOB_NAME = os.environ.get('JOB_NAME', '')
    BUILD_NUMBER = os.environ.get('BUILD_NUMBER', '-1')

    SQUARK_METADATA = os.environ.get('SQUARK_METADATA', '').lower() in ['1', 'true', 'yes']
    SKIP_ERRORS = os.environ.get('SKIP_ERRORS')
    SQUARK_DELETED_TABLE_SUFFIX = os.environ.get('SQUARK_DELETED_TABLE_SUFFIX', '_ADVANA_DELETED')
    RUN_LIVE_MAX_LEN_QUERIES = os.environ.get('RUN_LIVE_MAX_LEN_QUERIES', '').lower() in ['1', 'true', 'yes']

    from_conn = squarkenv.sources[CONNECTION_ID].conn
    to_conn = squarkenv.sources[VERTICA_CONNECTION_ID].conn
    #if LOAD_FROM_AWS:
    #    aws_conn = squarkenv.sources['vertica_aws'].conn

    from_schema = JDBC_SCHEMA
    to_schema = '_%s' % PROJECT_ID

    if SQUARK_METADATA:
        log_squark_metadata_contents(to_conn)
    squark_metadata = {}
    conn_metadata = utils.populate_connection_metadata(from_conn._metadata)
    db_product_name = conn_metadata['db_product_name']
    squark_metadata['conn_metadata'] = conn_metadata

    table_name_key = 'table_name'
    if db_product_name.lower().startswith('db2'):
        table_name_key = 'name'
        # get_tables().fetchall() or .fetchmany(#) where # > number of tables in schema are both failing via db2
        table_count = utils.get_number_of_tables_in_db2_schema(from_conn, JDBC_SCHEMA)
        print('*************** DB2 SCHEMA TABLE COUNT: {}'.format(table_count))
        tables = from_conn.get_tables(schema=JDBC_SCHEMA).fetchmany(table_count)
    else:
        tables = from_conn.get_tables(schema=from_schema)

    for table in tables:
        table = dict(zip([k.lower() for k in table._fieldnames], table))
        print("Checking table: {tbl} {tbltype}".format(tbl=table[table_name_key], tbltype=table['table_type']))
        if table['table_type'] is None:
            print('>>>>> skipping weird None table: %r' % table)
            continue
        if not INCLUDE_VIEWS and table['table_type'].upper() != 'TABLE':
            print('>>>> skipping non table: %s' % table[table_name_key])
            continue
        # 2018.07.05, similar to pull side, without resources for proper testing below is safest approach
        #   likely would want for any postgresql data sources, for now aiming only for good enough
        if PROJECT_ID.lower().startswith('haven'):
            if INCLUDE_VIEWS and table['table_type'].upper() not in ['TABLE','VIEW']:
                print('>>>> INCLUDE_VIEWS is enabled, skipping non table/view: %s' % table[table_name_key])
                continue
        table_name = sanitize(table[table_name_key])
        if INCLUDE_TABLES and table_name not in INCLUDE_TABLES:
            continue
        squark_metadata['is_incremental'] = False
        if TABLES_WITH_PARTITION_INFO and table_name.lower() in [table.lower() for table in
                                                                 TABLES_WITH_PARTITION_INFO.keys()]:
            table_with_partitions_lower = {k.lower(): v for k, v in TABLES_WITH_PARTITION_INFO.items()}
            partition_info = table_with_partitions_lower[table_name.lower()]
            print('>>>>>  Partition info: %r' % partition_info, flush=True)
            is_incremental = partition_info.get('is_incremental', '').lower() in ['1', 'true', 'yes']
            if is_incremental:
                squark_metadata['is_incremental'] = True
                print('>>>>>  is_incremental is True', flush=True)
                squark_metadata['pkid_column_name'] = partition_info['pkid_column_name']

        #if LOAD_FROM_HDFS:
        try:
            copy_table_ddl(
                from_conn, from_schema, table[table_name_key],
                to_conn, to_schema, table_name, squark_metadata)
        except Exception as exc:
            if SKIP_ERRORS:
                print('>>>> ERROR COPYING TABLE:')
                print(exc)
                continue
            else:
                raise exc
#        if LOAD_FROM_AWS:
#            print('-------- LAUNCHING AWS VERTICA DDL PUSH -----------')
#            try:
#                copy_table_ddl(
#                    from_conn, from_schema, table[table_name_key],
#                    to_conn, to_schema, table_name)
#            except Exception as exc:
#                if SKIP_ERRORS:
#                    print('>>>> ERROR COPYING TABLE:')
#                    print(exc)
#                    continue
#                else:
#                    raise exc

