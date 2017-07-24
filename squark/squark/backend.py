import re
import os
import uuid
import glob
import shutil
import contextlib

import py4jdbc
from pyspark.sql.dataframe import DataFrame

from squark.base import TableType, LocationType
from squark.location import Location, FileLocation
from squark.utils.common import CachedAttr, resolve_name
from squark.config.environment import Environment
from squark.exceptions import (
    LocationNotWritable, LocationAlreadyExists)


class Backend:

    @property
    def env(self):
        return Environment()

    @contextlib.contextmanager
    def maybe_transaction(self, nocommit):
        '''If the backend supports transactions AND if nocommit is false,
        then start a transaction on __enter__ and commit in on __exit__.
        This base method is a noop; only RDBMS backends will actually
        implement this method with real transactions.
        '''
        yield

    # --------------------------------------------------------------------------
    # Methods for working with LocationType objects.
    # --------------------------------------------------------------------------
    def prep_location(self, location: LocationType) -> str:
        '''If any changes are required to Location objects before they can
        be passed directly to backend methods, like .location_exists, this
        method can make those changes. By default it stringifies the location
        object.
        '''
        return str(location)

    @contextlib.contextmanager
    def mktemp(self, location: LocationType) -> None:
        '''This method must be a context manager that:
            1. on __enter__ generates a unique temporary write location,
            2. ensures that the parent directory (or schema) exists, and
            3. on __exit__ deletes the temporary location.
       '''
        temp = self.new_temp_location(location)
        # This is a basic check on the integrity of unique locations
        # implemented in subclasses.
        if self.location_exists(temp):  # pragma: no cover
            msg = ('The temporary location returned by %r.new_temp_location, '
                   '%s, already exists.')
            raise RuntimeError(msg % (self.__class__.__name__, location))

        self.ensure_location_parent_exists(temp)
        self.ensure_location_parent_is_writable(temp)
        yield temp
        if self.location_exists(temp):
            self.remove_location(temp)

    def location_is_ready(self, location: LocationType) -> bool:
        if not self.location_parent_exists(location):
            self.env.warning('Parent location not found for %s', location)
            return False
        if not self.location_parent_is_writable(location):
            self.env.warning('Parent location not writable for %s', location)
            return False
        return True

    def ensure_location_ready(self, location: LocationType) -> None:
        self.ensure_location_parent_exists(location)
        self.ensure_location_parent_is_writable(location)

    def location_parent_exists(self, location: LocationType) -> bool:
        '''Returns True/False whether the location currently exists.
        '''
        return self.location_exists(location.parent)

    def location_parent_is_writable(self, location: LocationType) -> bool:
        '''Returns True/False whether the location currently exists.
        '''
        return self.location_is_writable(location.parent)

    def ensure_location_parent_exists(self, location: LocationType) -> None:
        '''If the parent directory (or schema) for this location doesn't
        already exist, this method must create it.
        '''
        if not self.location_exists(location.parent):
            self.create_location_parent(location)

    def ensure_location_parent_is_writable(self, location: LocationType) -> bool:
        '''Returns True/False whether the location is writable by the user
        squark is currently running as.
        '''
        if not self.location_is_writable(location.parent):
            msg = 'Location parent is not writable: %r'
            raise LocationNotWritable(msg % location)

    def ensure_location_removed(self, location: LocationType, force=False):
        if self.location_exists(location):
            self.remove_location(location, force=force)

    # --------------------------------------------------------------------------
    # Subclasses must define these ones.
    # --------------------------------------------------------------------------
    def create_location_parent(self, location: LocationType) -> None:
        '''Location is a directory in FileBackend and HdfsBackend, and
        a schema or table in jdbc backends.
        '''
        raise NotImplementedError()

    def location_exists(self, location: LocationType) -> bool:
        '''Returns True/False whether the location currently exists.
        '''
        raise NotImplementedError()

    def new_temp_location(self, location: LocationType) -> LocationType:
        '''This method must generate a new, unique location in the backend.
        '''
        raise NotImplementedError()

    def location_is_writable(self, location: LocationType) -> bool:
        '''Returns True/False whether the location is writable by the user
        squark is currently running as.
        '''
        raise NotImplementedError()

    # --------------------------------------------------------------------------
    # Basic I/O methods.
    # --------------------------------------------------------------------------
    def load(self, table: TableType, *args, **kwargs) -> None:
        raise NotImplementedError()

    def save(self, table: TableType, *args, **kwargs) -> None:
        raise NotImplementedError()

    def save_as(self, table: TableType, dest: LocationType) -> None:
        self.save(table, dest=dest)

    def copy(self, table: TableType, dest: LocationType) -> None:
        raise NotImplementedError()

    def remove_location(self, table: TableType, force: bool = False) -> None:
        raise NotImplementedError()

    def move_location(self,
            table: Location, dest: Location) -> None:
        raise NotImplementedError()

    # --------------------------------------------------------------------------
    # Methods that gather metadata about tables.
    # --------------------------------------------------------------------------
    def get_partfile_metadata(self, table: TableType) -> None:
        raise NotImplementedError()

    def expand_glob(self, location: Location):
        raise NotImplementedError()


class DataframeBackendMixin:

    def load(self, table: TableType, *args, **kwargs) -> DataFrame:
        sqlctx = self.env.sqlctx
        df = sqlctx.read.load(str(table.location), *args, **kwargs)
        return df

    def save(self,
                table: TableType,
                dest: LocationType = None,
                *args, **kwargs) -> None:
        if dest is None:
            dest = table.location
        table.df.write.save(str(dest), *args, **kwargs)


class FileBackend(DataframeBackendMixin, Backend):

    def prep_location(self, location: LocationType) -> str:
        if isinstance(location, LocationType):
            location = str(location)
        return re.sub(r'^file://', '', location)

    def new_temp_location(self, location: LocationType):
        # Create a new, mangled basename for the file.
        path = location.path.rstrip(os.sep)
        basename = os.path.basename(path)
        temp_basename = '%s_temp_%s' % (basename, uuid.uuid4())
        # And put it in the configured temp dir.
        tempdir = location.source_obj.tempdir
        temp = os.path.join(tempdir, temp_basename)
        return location.clone(path=temp)

    def location_is_writable(self, location: LocationType) -> bool:
        '''Returns True/False whether the location is writable by the user
        squark is currently running as.
        '''
        return os.access(self.prep_location(location), os.W_OK)

    def location_exists(self, location: LocationType):
        try:
            exists = bool(os.stat(self.prep_location(location)))
        except FileNotFoundError:
            exists = False
        if exists:
            msg = 'Location %s already exists.'
        else:
            msg = 'Location %s does not exist yet.'
        self.env.debug(msg, location)
        return exists

    def create_location_parent(self, location: LocationType) -> None:
        path = self.prep_location(location.parent)
        self.env.debug('Creating dir: %r', path)
        os.makedirs(path)

    def move_location(self, src: LocationType, dest: LocationType) -> None:
        '''Move one location to another location in the same backend.
        '''
        if self.location_exists(dest):
            raise RuntimeError('Location already exists: %r' % dest)
        src_path = self.prep_location(src)
        dest_path = self.prep_location(dest)
        os.rename(src_path, dest_path)

    def remove_location(self, location: LocationType, force=False) -> None:
        path = self.prep_location(location)
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)

    def get_partfile_metadata(self, table: TableType):
        '''Query the local filesystem to get number and size of
        existing partitions.
        '''
        import pdb; pdb.set_trace()

    def expand_glob(self, location: Location):
        for path in glob.glob(location.path):
            yield FileLocation.frompath(path)


class HdfsBackend(DataframeBackendMixin, Backend):

    def get_partfile_metadata(self, table: TableType):
        '''Use the webhdfs api here.
        '''
        pass



class JdbcBackend(Backend):

    def get_dialect(self, location: Location):
        source = location.source_obj
        dialect = source.dialect
        return dialect

    def get_connection(self, location: Location):
        source = location.source_obj
        return source.connection

    def create_location_parent(self, location: LocationType) -> None:
        '''If the location is table, create the schema.
        If it's a schema? Do nothing, I guess.
        '''
        if location.is_database_table():
            dialect = self.get_dialect(location)
            dialect.create_schema(schema=location.schema)
        else:
            msg = 'Not creating location parent for schema: %r'
            self.env.info(msg % location)

    def location_exists(self, location: LocationType) -> bool:
        '''Returns True/False whether the location currently exists.
        '''
        dialect = self.get_dialect(location)
        if location.is_database_table():
            return dialect.table_exists(
                table=location.table,
                schema=location.schema)
        else:
            return dialect.schema_exists(location.schema)

    def new_temp_location(self, location: Location) -> Location:
        '''This method must generate a new, unique location in the backend.
        '''
        kwargs = dict(schema=location.source_obj.tmp_schema)
        if location.is_database_table():
            kwargs['table'] = '%s_tmp_%s' % (location.table, uuid.uuid4())
            # Verify the configged temp schema exists.
            if not self.location_exists(location.parent):
                self.create_schema(location.parent)
        return location.clone(**kwargs)

    def location_is_writable(self, location: LocationType) -> bool:
        dialect = self.get_dialect(location)
        if location.is_database_table():
            raise NotImplementedError()
        else:
            return dialect.schema_is_writable(schema=location.schema)

    # --------------------------------------------------------------------------
    # Basic I/O methods.
    # --------------------------------------------------------------------------
    def load(self, table: TableType) -> None:
        '''Parallelized load no yet implemented.
        '''
        sqlctx = self.env.sqlctx
        source = table.location.source_obj
        properties = dict(user=source.user, password=source.password)
        df = sqlctx.read.jdbc(source.url, table=table.sql_name, properties=properties)
        return df

    def save(self, table: TableType) -> None:
        raise NotImplementedError()

    def save_as(self, table: TableType, dest: LocationType) -> None:
        raise NotImplementedError()

    def remove_location(self, location: Location, force: bool = False) -> None:
        dialect = self.get_dialect(location)
        if location.is_database_table():
            dialect.drop_table(
                table=location.table,
                schema=location.schema,
                force=force)
        else:
            dialect.drop_schema(schema=location.schema, force=force)

    def move_location(self, src: Location, dest: Location) -> None:
        dialect = self.get_dialect(src)
        if src.is_database_table():
            dialect.rename_table(
                src_table=src.table,
                src_schema=src.schema,
                dest_table=dest.table)
        else:
            dialect.rename_schema(schema=src.schema, force=force)

    # ------------------------------------------------------------------------
    # Metadata methods.
    # ------------------------------------------------------------------------
    def expand_glob(self, location: Location):
        tables = location.source_obj.connection.get_tables(
            schema=location.schema, table=location.table.replace('*', '%'))
        for table in tables:
            yield location.clone(table=table.table_name)

    # ------------------------------------------------------------------------
    # JDBC-specific methods
    # ------------------------------------------------------------------------
    def copyddl(self, src, dest):
        # XXX: Note: add thing to close connections on termination or
        # on error.
        dialect = self.get_dialect(location)
        columns = src.connection.get_columns
        dest.dialect.create_table(columns, dest.table, dest.schema)
        dest.save_from(src)

    # ------------------------------------------------------------------------
    # Schemas
    # ------------------------------------------------------------------------
    def create_schema(self, location: Location, *args, **kwargs):
        dialect = self.get_dialect(location)
        return dialect.create_schema(schema=location.schema, *args, **kwargs)
