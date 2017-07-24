import contextlib
from os.path import join

import jinja2

from squark.config.environment import Environment
from squark.utils.common import CachedAttr


class BaseColumn:

    def __init__(self, dialect, spec):
        self.dialect = spec
        self.spec = spec

    @property
    def name(self):
        return sanitize(self.spec.COLUMN_NAME)

    @property
    def ddl(self):
        return self.dialect.render_ddl_column(self)

    @property
    def nullable(self):
        return self.spec.NULLABLE


class BaseColumnGroup:

    def __init__(self, dialect, columns):
        self.dialect = dialect
        self.colums = columns

    def __iter__(self):
        for column in self.columns:
            col = self.dialect.Column(dialect=self.dialect, spec=column)
            yield col


class BaseDialect:

    Column = BaseColumn
    ColumnGroup = BaseColumnGroup
    engine = None

    def __init__(self, connection):
        self.env = Environment()
        self.conn = connection

    @CachedAttr
    def j2_env(self):
        dialect_templates = join(self.env.SQL_TEMPLATES, self.engine)
        default_templates = join(self.env.SQL_TEMPLATES, 'default')
        loader = jinja2.ChoiceLoader([
            jinja2.FileSystemLoader(dialect_templates),
            jinja2.FileSystemLoader(default_templates),
            ])
        options = dict(
            trim_blocks=True,
            extensions=['jinja2.ext.with_'],
            loader=loader)
        return jinja2.Environment(**options)

    @contextlib.contextmanager
    def execute_template(self, template_name, **ctx):
        tmpl = self.j2_env.get_template(template_name)
        sql = tmpl.render(ctx)
        self.env.info('Executing SQL: %s', sql)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        if not cursor._closed:
            cursor.close()

    @contextlib.contextmanager
    def execute_template_rs(self, template_name, **ctx):
        '''Execute template, but as a context manager that closes the
        cursor after the caller is finished using the ResultRet.
        '''
        tmpl = self.j2_env.get_template(template_name)
        sql = tmpl.render(ctx)
        self.env.info('Executing SQL: %s', sql)
        cursor = self.conn.cursor()
        rs = cursor.execute(sql)
        yield rs
        cursor.close()

    # -----------------------------------------------------------------------
    # Table operations
    # -----------------------------------------------------------------------
    def table_exists(self, table, schema=None):
        ctx = dict(table=table, schema=schema)
        with self.execute_template_rs('table_exists.sql', **ctx) as rs:
            return rs.fetchone() is not None

    @contextlib.contextmanager
    def create_table(self, columns, table, schema=None):
        column_group = self.ColumnGroup(columns)
        ctx = dict(table=table, schema=schema, columns=column_group)
        self.execute_template('create_table.sql', **ctx)

    def table_is_writable(self, table, schema=None):
        raise NotImplementedError()

    def drop_table(self, table, schema=None, force=False):
        ctx = dict(table=table, schema=schema, force=force)
        return self.execute_template('drop_table.sql', **ctx)

    def rename_table(
            self, src_table, src_schema,
            dest_table, force=False):
        ctx = dict(
            src_table=src_table, src_schema=src_schema,
            dest_table=dest_table, force=False)
        return self.execute_template('rename_table.sql', **ctx)

    def gen_column_spec(self, columns):
        for col in columns:
            yield self.Column(dialect=self, spec=col)

    # -----------------------------------------------------------------------
    # Schema operations
    # -----------------------------------------------------------------------
    def schema_exists(self, schema):
        tmpl = 'schema_exists.sql'
        with self.execute_template_rs(tmpl, schema=schema) as rs:
            return rs.fetchone() is not None

    def create_schema(self, schema):
        return self.execute_template('create_schema.sql', schema=schema)

    def schema_is_writable(self, schema):
        raise NotImplementedError()

    def drop_schema(self, schema, force=False):
        return self.execute_template('drop_schema.sql', schema=schema, force=True)

    def rename_schema(self, src_schema, dest_schema, force=False):
        ctx = dict(
            src_schema=src_schema,
            dest_schema=dest_schema,
            force=False)
        return self.execute_template('rename_schema.sql', **ctx)

    # -----------------------------------------------------------------------
    # Grant operations
    # -----------------------------------------------------------------------
    def filter_grants(self,
            grantee,
            object_name,
            object_type=None,
            permissions=None,
            grant_on_all=False,
            with_grant_option=False):

        ctx = dict(
            grantee=grantee,
            object_name=object_name,
            object_type=object_type,
            permissions=permissions,
            grant_on_all=grant_on_all,
            with_grant_option=with_grant_option)

        grant_order = (
            'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'REFERENCES',
            'USAGE', 'CREATE', 'TRUNCATE')

        perms = ctx.get('permissions')
        ctx['permissions'] = sorted(perms or [], key=grant_order.index)

        tmpl = 'grants/query_default.sql'
        with self.execute_template_rs(tmpl, **ctx) as rs:
            # Get the exist permissions.
            return rs.fetchall()

    def create_grant(self,
            grantee,
            object_name,
            object_type,
            permissions,
            grant_on_all=False,
            with_grant_option=False):

        ctx = dict(
            grantee=grantee,
            object_name=object_name,
            object_type=object_type,
            permissions=permissions,
            grant_on_all=grant_on_all,
            with_grant_option=with_grant_option)

        return self.execute_template('grants/default.sql', **ctx)

    def grant_exists(self, **grant):
        if 'permissions' in grant:
            grant['permissions'] = set(grant['permissions'])
        grants = self.filter_grants(**grant)
        exists = False
        for obj in grants:
            objdata = obj._asdict()
            objdata['permissions'] = set(objdata['privileges_description'].split(', '))
            if grant.items() < objdata.items():
                exists = True
        return exists

    def ensure_grant_exists(self, *args, **kwargs):
        if not grant_exists(*args, **kwargs):
            self.create_grant(*args, **kwargs)

    def revoke_grant(self, **grant):
        return self.execute_template('grants/revoke.sql', **grant)

    # ------------------------------------------------------------------------
    # User operations
    # ------------------------------------------------------------------------
    def fetch_user(self, user):
        tmpl = 'user_exists.sql'

        with self.execute_template_rs(tmpl, user=user) as rs:
            return rs.fetchone()

    def user_exists(self, user, **kwargs):
        user = self.fetch_user(user)
        return user is not None

    def create_user(self, user, **ctx):
        ctx.update(user=user)
        return self.execute_template('create_user.sql', **ctx)

    def remove_user(self, user, **ctx):
        ctx.update(user=user)
        return self.execute_template('drop_user.sql', user, *args, **ctx)

    # ------------------------------------------------------------------------
    # Role operations
    # ------------------------------------------------------------------------
    @contextlib.contextmanager
    def role_exists(self, role, *args, **kwargs):
        dialect = self.get_dialect(location)
        rs = self.execute_template('drop_role.sql', user, *args, **kwargs)
        with rs:
            return rs.fetchone() is not None

    def create_role(self, role, *args, **kwargs):
        return self.execute_template('create_role.sql', user, *args, **kwargs)

    def remove_role(self, role, *args, **kwargs):
        return self.execute_template('remove_role.sql', user, *args, **kwargs)
