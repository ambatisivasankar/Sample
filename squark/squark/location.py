import re
import os
import uuid
import tempfile
from collections import ChainMap

from squark.base import LocationType
from squark.utils.common import CachedAttr
from squark.config.environment import Environment

class Location(LocationType):
    rgxs = [
        r'^(?P<source>\S+?)://(?P<host>\w+):(?P<port>\d+)(?P<path>/[\S+]*)$',
        r'^(?P<source>\S+?)://(?P<host>\w+)(?P<path>/[\S+]*)$',
        r'^(?P<source>\S+?)://(?P<path>/[\S+]*)$',
        ]

    def __init__(self, location_or_string, **overrides):
        self.env = Environment()

        # Get stringy location path.
        obj = location_or_string
        if isinstance(obj, LocationType):
            obj = str(obj)
        elif not isinstance(obj, str):
            msg = 'Location must be LocationType or str, not %r'
            raise TypeError(msg % location_or_str)
        self.string = obj

        # Parse it up and store.
        parts = ChainMap(overrides, self.parts)
        if '+' in parts['source']:
            self.source, self.format = parts['source'].split('+')
        else:
            self.source = parts['source']
            msg = ("No format specified for dataframe: %r. Assuming default "
                   "config value of dataframe.write_mode (%s) applies.")
            fmt = self.env.cfg['dataframe']['write_format']
            self.env.debug(msg, self.string, fmt)
            self.format = fmt

        self.path = parts['path']
        self.host = parts.get('host', '')
        self.port = parts.get('port', '')

    def __str__(self):
        if self.host:
            if self.port:
                tmpl = '{0.source}://{0.host}:{0.port}{0.path}'
            else:
                tmpl = '{0.source}://{0.host}{0.path}'
        else:
            tmpl = '{0.source}://{0.path}'
        return tmpl.format(self)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, str(self))

    @property
    def parts(self):
        for rgx in self.rgxs:
            m = re.match(rgx, self.string)
            if m:
                return m.groupdict()
        raise ValueError("Couldn't parse location %r" % self.string)

    @property
    def parent(self):
        newpath = os.path.dirname(self.path)
        return self.__class__(self.string, path=newpath)

    @property
    def source_obj(self):
        '''Retrieve the actual source associated with this location's string
        `source` attribute.
        '''
        return self.env.sources[self.source]

    def clone(self, **overrides):
        '''Return a copy of this location, which one or more properties
        overridden by the passed-in `overrides` dict.
        '''
        return self.__class__(str(self), **overrides)

    @property
    def type(self):
        return self.source_obj.type

    def is_glob(self):
        return '*' in self.path


class FileLocation(Location):

    @classmethod
    def frompath(Cls, path):
        return Cls('file://' + path)


class HdfsLocation(Location):
    pass


class JdbcLocation(Location):

    rgxs = [
        r'^(?P<source>\S+?):///(?P<table>\w+?)/?$',
        r'^(?P<source>\S+?)://(?P<schema>[^/]+?)/?$',
        r'^(?P<source>\S+?)://(?P<schema>[^/]+?)(?P<path>/[\S+]+)$',
        # r'^(?P<source>\S+?)://(?P<path>/[\S+]*)$',
        ]

    def __init__(self, string, **overrides):
        self.env = Environment()
        self.string = string
        defaults = dict(schema=None, table=None)
        parts = ChainMap(overrides, self.parts, defaults)
        if '+' in parts['source']:
            self.source, self.format = parts['source'].split('+')
        else:
            self.source = parts['source']
            self.format = None
        self._schema = parts['schema']
        self.path = parts.get('path')
        self._table = parts.get('table')

    def __str__(self):
        vals = dict(
            source=self.source,
            schema=self.schema,
            table=self.table or '')
        if self.schema:
            tmpl = '{source}://{schema}/{table}'
        else:
            tmpl = '{source}:///{table}'
        return tmpl.format(**vals)

    @property
    def table(self):
        if self._table:
            return self._table
        elif self.path:
            return self.path.lstrip('/')

    @property
    def schema(self):
        schema = self._schema
        if self._schema in (':tmp:', ':temp:'):
            schema = self.source_obj.tmp_schema
        return schema

    def is_database_table(self):
        return bool(self.table)


def get_location(string: str) -> LocationType:
    source = re.match(r'^(\S+?)(\+|\:)', string).group(1)
    source_obj = Environment().sources[source]
    Cls = dict(
        jdbc=JdbcLocation,
        hdfs=HdfsLocation,
        file=FileLocation)[source_obj.type]
    return Cls(string)
