import re
import uuid
import textwrap

from jinja2 import Template

from squark.sql.dialects.base import BaseDialect


class Dialect(BaseDialect):

    engine = 'vertica'

    reserved_words = re.findall(r'\w+', '''
        ALL ANALYSE ANALYZE AND ANY ARRAY
        AS ASC BINARY BOTH CASE CAST CHECK COLUMN
        CONSTRAINT CORRELATION CREATE CURRENT_DATABASE CURRENT_DATE
        CURRENT_SCHEMA CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER
        DEFAULT DEFERRABLE DESC DISTINCT DO ELSE ENCODED
        END EXCEPT FALSE FLEX FLEXIBLE FOR FOREIGN FROM
        GRANT GROUP GROUPED HAVING IN INITIALLY INTERSECT
        INTERVAL INTERVALYM INTO JOIN KSAFE LEADING LIMIT
        LOCALTIME LOCALTIMESTAMP MATCH NEW NOT NULL
        NULLSEQUAL OFF OFFSET OLD ON ONLY OR ORDER
        PINNED PLACING PRIMARY PROJECTION REFERENCES SCHEMA
        SEGMENTED SELECT SESSION_USER SOME SYSDATE TABLE THEN
        TIMESERIES TO TRAILING TRUE UNBOUNDED UNION UNIQUE
        UNSEGMENTED USER USING WHEN WHERE WINDOW WITH WITH''')

    typemap = dict((
        ('BIT', 'BOOLEAN'),
        ('NVARCHAR', 'VARCHAR'),
        ('LONGNVARCHAR', 'LONG VARCHAR'),
        ('LONGVARCHAR', 'LONG VARCHAR'),
        ('NCHAR', 'CHAR'),
        ('CLOB', 'LONG VARCHAR'),
        ('BLOB', 'LONG VARBINARY'),
        ('LONGVARBINARY', 'LONG VARBINARY'),
        ('DOUBLE', 'FLOAT'),
        ))

    valid_table_permissions = {
        'SELECT', 'INSERT', 'UPDATE', 'DELETE',
        'REFERENCES', 'ALL', 'ALL PRIVILEGES'}

    valid_schema_permissions = {'CREATE', 'USAGE'}

    def render_column_ddl(self, column):
        from_type = column.spec.data_type
        to_type = self.typemap.get(from_type, from_type)
        data = column.spec._asdict()

        if from_type in ('NVARCHAR', 'NCHAR', 'NVARBINARY'):
            data['COLUMN_SIZE'] = data['COLUMN_SIZE'] * 3

        if 'CHAR' in from_type or 'BINARY' in from_type:
            if 65000 < (data['COLUMN_SIZE'] or 1):
                data['COLUMN_SIZE'] = 65000

        method_name = 'render_ddl_%s' % from_type
        render = getattr(self, method_name, render_ddl_default)

        return render(from_type, to_type, data)

    def render_ddl_default(self, from_type, to_type, data):
        return to_type

    def render_ddl_weird(self, from_type, to_type, data):
        return 'VARCHAR(65000)'

    render_ddl_ARRAY = render_ddl_weird
    render_ddl_OTHER = render_ddl_weird

    def render_ddl_has_size(self, from_type, to_type, data):
        return '{to_type}({COLUMN_SIZE})'.format(**data)

    render_ddl_BINARY = render_ddl_has_size
    render_ddl_VARBINARY = render_ddl_has_size
    render_ddl_CHAR = render_ddl_has_size
    render_ddl_VARCHAR = render_ddl_has_size

    def render_ddl_NUMERIC(self, from_type, to_type, data):
        if 1024 < data['COLUMN_SIZE']:
            data['COLUMN_SIZE'] = 1024
        if data['DECIMAL_DIGITS'] is not None:
            tmpl = '{to_type}({COLUMN_SIZE},{DECIMAL_DIGITS})'
        else:
            tmpl = '{to_type}({COLUMN_SIZE})'
        return tmpl.format(**data)

    # -----------------------------------------------------------------------
    # Permissions
    # -----------------------------------------------------------------------
    def schema_is_writable(self, schema):
        # Try to create a table.
        sql = 'create table "%s"."%s" (a int);'
        vals = (schema, uuid.uuid4())
        sql = sql % vals
        cur = self.conn.cursor()
        writable = False
        try:
            cur.execute(sql)
        except Exception as exc:
            msg = 'Schema writability check silently trapped this exception: "%s"'
            self.env.warning(msg, exc)
            return False
        else:
            writable = True
            sql = 'drop table "%s"."%s" cascade;' % vals
            cur.execute(sql)
            cur.close()
        return True
