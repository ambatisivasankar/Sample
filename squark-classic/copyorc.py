# XXX: This file has no identified uses
import os

from jinja2 import Template

from py4jdbc import GatewayProcess, connect


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
    return name



if __name__ == '__main__':

    gateway = GatewayProcess()

    # # From
    # from_conn = connect(
    #     "jdbc:jtds:sybase://hasql013.private.massmutual.com:2085/adcoind",
    #     "advana", "w4q9736", gateway=gateway)
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

    from_conn = connect(
        JDBC_URL, JDBC_USER, JDBC_PASSWORD,
        gateway=gateway)

    to_conn = connect(
        "jdbc:vertica://%s/advana" % VERTICA_HOST,
        VERTICA_USER, VERTICA_PASSWORD)

    from_schema = JDBC_SCHEMA
    to_schema = 'wh'

    fails = 0
    tries = 0
    with gateway:
        tables = from_conn.get_tables(schema=from_schema)
        for table in tables:
            table = dict(zip([k.lower() for k in table._fields], table))
            if table['table_type'] is None:
                print("skipping weird: %r" % table)
                continue
            if table['table_type'].upper() != 'TABLE':
                print('>>>> skipping non table: %s' % table['table_name'])
                continue
            tries += 1
            tablename = '%s_%s' % (PROJECT_ID, table['table_name'])
            tablename = tablename.replace('#', '_')
            stmt = "copy %s.%s from 'webhdfs://devlx187:50070%s/%s/*orc' on any node orc direct;"
            stmt = stmt % (to_schema, tablename, DATA_DIR, table['table_name'])
            print(stmt)
            try:
                to_conn.cursor().execute(stmt)
            except Exception as exc:
                fails += 1
                print(exc)
            finally:
                pass

    print('Fails: %d' % fails)
    print('Tries: %d' % tries)


