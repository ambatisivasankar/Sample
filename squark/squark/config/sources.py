from urllib.parse import urlparse

import py4jdbc

from squark.base import ConfigType
from squark.exceptions import ConfigError
from squark.utils.common import CachedAttr, resolve_name


class Sources(ConfigType):

    def __init__(self, cfg, env):
        super().__init__(cfg)
        self.env = env
        self._build()

    def __iter__(self):
        for name, member in inspect.getmembers(self):
            if isinstance(member, SourceConfig):
                yield member

    def _build(self):
        types = {
            'hdfs': HdfsSourceConfig,
            'jdbc': JdbcSourceConfig,
            'file': FileSourceConfig,
        }
        for key, data in self.cfg.items():
            if key == 'DEFAULT':
                continue
            try:
                source_type = data['type']
            except KeyError:
                msg = ('Config section %r containing data %r must define '
                       'a "type" item describing the type of source this '
                       'data represents. The type must be one of %r')
                raise ConfigError(msg % (key, dict(data.items()), types.keys()))

            Cls = types[source_type]
            setattr(self, key, Cls(data, env=self.env))



class _Accessor:

    def __init__(self, name):
        self.name = name

    def __get__(self, inst, type=None):
        return inst.cfg[self.name]


class SourceConfig:

    def __init__(self, cfg_section, env):
        self.cfg = cfg_section
        self.env = env

    def __repr__(self):
        string = repr(list(self.cfg.items()))
        if 'password' in self.cfg:
            string = string.replace(self.password, '************')
        return '%s(%s)' % (self.__class__.__name__, string)

    type = _Accessor('type')


class FileSourceConfig(SourceConfig):
    tempdir = _Accessor('tempdir')


class HdfsSourceConfig(SourceConfig):
    host = _Accessor('host')
    port = _Accessor('port')
    user = _Accessor('user')
    keytab = _Accessor('keytab')


class JdbcSourceConfig(SourceConfig):
    url = _Accessor('url')
    user = _Accessor('user')
    password = _Accessor('password')
    default_schema = _Accessor('schema')
    tmp_schema = _Accessor('tmp_schema')

    @property
    def engine(self):
        _, url = self.url.split(":", 1)
        return urlparse(url).scheme

    @CachedAttr
    def dialect(self):
        cls_name = 'squark.sql.dialects.%s.Dialect' % self.engine
        Dialect = resolve_name(cls_name)
        return Dialect(connection=self.connection)

    @CachedAttr
    def connection(self):
        conn = py4jdbc.connect(
            jdbc_url=self.url,
            user=self.user,
            password=self.password)
            # gateway=self.env.ctx._gateway)
        self.env._connections.append(self)
        return conn

    @property
    def conn(self):
        return self.connection

    def check_obj(self, obj, spec):
        '''Return False if obj doesn't have all properties indicated by spec.
        '''
        for prop, value in spec.items():
            if not hasattr(obj, prop):
                return False
            if getattr(obj, prop) != value:
                return False
        return True

    # ------------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------------
    def user_exists(self, *args, **kwargs):
        return self.dialect.user_exists(*args, **kwargs)

    def create_user(self, *args, **kwargs):
        return self.dialect.create_user(*args, **kwargs)

    def remove_user(self, *args, **kwargs):
        return self.dialect.remove_user(*args, **kwargs)

    def ensure_user_exists(self, user, *args, **kwargs):
        if not self.user_exists(user, *args, **kwargs):
            self.create_user(user, *args, **kwargs)

    def ensure_user_removed(self, user, *args, **kwargs):
        if self.user_exists(user, *args, **kwargs):
            self.remove_user(user, *args, **kwargs)

    # ------------------------------------------------------------------------
    # Roles
    # ------------------------------------------------------------------------
    def role_exists(self, *args, **kwargs):
        return self.dialect.role_exists(*args, **kwargs)

    def create_role(self, *args, **kwargs):
        return self.dialect.create_role(*args, **kwargs)

    def remove_role(self, *args, **kwargs):
        return self.dialect.remove_role(*args, **kwargs)

    # ------------------------------------------------------------------------
    # Grants
    # ------------------------------------------------------------------------
    def grant_exists(self, **grant):
        return self.dialect.grant_exists(**grant)

    def create_grant(self, **grant):
        return self.dialect.grant(**grant)

    def ensure_grant(self, **grant):
        return self.dialect.create_grant(**grant)

    def revoke_grant(self, **grant):
        return self.dialect.revoke_grant(**grant)

    def ensure_grant_removed(self, **grant):
        if self.grant_exists(**grant):
            self.revoke_grant(**grant)

