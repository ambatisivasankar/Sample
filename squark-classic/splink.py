# XXX: This file has no identified uses
import os

from jinja2 import Template

from py4jdbc import GatewayProcess, connect


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

    has_size = ('BINARY', 'VARBINARY', 'CHAR', 'VARCHAR')
    numby = ('DOUBLE', 'BIGINT', 'NUMERIC')

    def __init__(self, jdbc_spec):
        self.spec = jdbc_spec

    def ddl(self):

        # if self.spec.TABLE_NAME == 'manual_asset':
        #     if self.spec.COLUMN_NAME == 'value':
        #         import pdb; pdb.set_trace()
        # if self.spec.IS_AUTOINCREMENT:
        #     return 'INTEGER'
        from_type = self.spec.data_type
        to_type = self.typemap.get(from_type, from_type)
        data = dict(zip((k.upper() for k in self.spec._fields), self.spec))
        data.update(to_type=to_type)

        if from_type in ('ARRAY', 'OTHER'):
            # Hstore
            return 'VARCHAR(65000)'

        if from_type in ('NVARCHAR', 'NCHAR', 'NVARBINARY'):
            data['COLUMN_SIZE'] = data['COLUMN_SIZE'] * 3

        if 'CHAR' in from_type or 'BINARY' in from_type:
            if 65000 < (data['COLUMN_SIZE'] or 1):
                data['COLUMN_SIZE'] = 65000

        if from_type in self.has_size:
            tmpl = '{to_type}({COLUMN_SIZE})'
        elif from_type == 'NUMERIC':
            # Max precision is 1024.
            if 1024 < data['COLUMN_SIZE']:
                data['COLUMN_SIZE'] = 1024
            if self.spec.DECIMAL_DIGITS is not None:
                tmpl = '{to_type}({COLUMN_SIZE},{DECIMAL_DIGITS})'
            else:
                tmpl = '{to_type}({COLUMN_SIZE})'
        else:
            tmpl = to_type

        return tmpl.format(**data)

    @property
    def name(self):
        return sanitize(self.spec.COLUMN_NAME)

    @property
    def nullable(self):
        return self.spec.NULLABLE

# self.j2_env = Environment(**dict(
#     trim_blocks=True,
#     extensions=['jinja2.ext.with_']))

tmpl = Template('''
drop table if exists {{table}} cascade;
create table if not exists {{schema}}.{{table}}(
{% for col in colspec %}
    {{ col.name }} {{ col.ddl() }}{% if not col.nullable %} NOT NULL{% endif %}{% if not loop.last %},{% endif %}

{% endfor %}
);
''', trim_blocks=True)

def make_ddl(schema, table, colspec):
    colspec = list(map(ColSpec, colspec))
    return tmpl.render(schema=schema, table=table, colspec=colspec)


def copy_table_ddl_with_splink(
    from_conn, from_schema, from_table,
    to_conn, to_schema, to_table):
    from_cols = list(from_conn.get_columns(schema=from_schema, table=from_table))
    import pdb; pdb.set_trace()
    ddl = make_ddl(to_schema, to_table, from_cols)
    print('creating table %r' % from_table)
    print(ddl)
    cur = to_conn.cursor()
    rs = cur.execute(ddl)
    to_conn._jconn.close()
    # to_conn.close()


def load_orc(to_conn, to_schema, tablename, table) :
    stmt = "copy %s.%s from 'webhdfs://devlx187:50070%s/%s/*orc' on any node orc;"
    stmt = stmt % (to_schema, tablename, DATA_DIR, table['table_name'])
    print(stmt)
    to_conn.cursor().execute(stmt)
    to_conn._jconn.close()


if __name__ == '__main__':

    PROJECT_ID = os.environ['PROJECT_ID']
    JDBC_USER = os.environ['JDBC_USER']
    JDBC_PASSWORD = os.environ['JDBC_PASSWORD']
    JDBC_URL = os.environ['JDBC_URL']
    JDBC_SCHEMA = os.environ.get('JDBC_SCHEMA', 'public')
    WAREHOUSE_DIR = os.environ['WAREHOUSE_DIR']
    DATA_DIR = os.path.join(WAREHOUSE_DIR, PROJECT_ID)
    VERTICA_HOST = os.environ['VERTICA_HOST']
    VERTICA_USER = os.environ['VERTICA_USER']
    VERTICA_PASSWORD = os.environ['VERTICA_PASSWORD']

    tablemap = dict(
        acxiom='acxiom.prospect',
        haven='',
        lms='',
        reflex='',
        td_cust='',
        
    gateway = GatewayProcess()

    from_conn = to_conn = connect(
        "jdbc:vertica://%s/advana" % VERTICA_HOST,
        VERTICA_USER, VERTICA_PASSWORD)

    from_schema = JDBC_SCHEMA
    to_schema = 'wh'

    with gateway:

        # Get tables from HDFS.
        hdfs_host = self.require('hdfs', 'namenode', 'host')
        hdfs_port = self.require('hdfs', 'namenode', 'port')
        hdfs_user = self.require('hdfs', 'user')
        hdfs = PyWebHdfsClient(host=hdfs_host, port=hdfs_port, user_name=hdfs_user)
        schema = self.require('all_tables', 'schema')
        warehouse_dir = '/data/splinkr/raw_with_eid'
        for table in hdfs.list_dir(warehouse_dir)['FileStatuses']['FileStatus']:
            qualname = '%s.%s' % (schema, table['pathSuffix'])
 
            # Get the table from Vertica via jdbc.
            table = to_conn.get_tables(table="table")
            table = dict(zip([k.lower() for k in table._fields], table))
            if table['table_type'] is None:
                print('>>>>> skipping weird None table: %r' % table)
                continue
            if table['table_type'].upper() != 'TABLE':
                print('>>>> skipping non table: %s' % table['table_name'])
                continue

            table_name = '%s_%s' % (PROJECT_ID, table['table_name'])

            # Copy the schema with EID cols.
            copy_table_ddl_with_splink(
                from_conn, from_schema, table['table_name'],
                to_conn, to_schema, table_name)

            # Load in the new table.
            load_orc(to_conn, 'splinkr', table_name, table) 
