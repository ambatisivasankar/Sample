import squark.stats
from squark.base import TableType
from squark.location import get_location, Location
from squark.backend import FileBackend, HdfsBackend, JdbcBackend
from squark.utils.common import CachedAttr
from squark.config.environment import Environment


class Table(TableType):
    '''Represents configuration for a single dataframe, including
    its storage backend and details. It can represent an
    existing dataframe or a dataframe in a desired fture state.
    '''
    backend = None

    def __init__(self, location, save_format=None):
        self.env = Environment()
        if isinstance(location, str):
            location = get_location(location)
        self.location = location
        self.save_format = save_format

    def __repr__(self):
        return '%s(location=%r)' % (self.__class__.__name__, self.location)

    # ------------------------------------------------------------------------
    # Basic IO methods.
    # ------------------------------------------------------------------------
    @CachedAttr
    def df(self):
        '''This table, loaded into a pyspark dataframe.
        '''
        return self.backend.load(self)

    def save(self, *args, **kwargs):
        '''If the destination file already exists, overwrites the dataframe
        by writing to a temp location, then rotating the temp file into
        the place of the original.
        '''
        self.backend.save(self, *args, **kwargs)

    def save_as(self, other: TableType):
        self.backend.save_as(self, other)

    def save_from(self, other: TableType):
        other.save_as(self)

    def copy_to(self, other: Location):
        raise NotImplementedError()

    def exists(self):
        return self.backend.location_exists(self.location)

    def move_to(self, other):
        if isinstance(other, Table):
            other = other.location
        self.backend.move_location(self.location, other)

    def remove(self):
        self.backend.remove_location(self.location)

    def symlink_to(self, other):
        other.symlink_from(self)

    def symlink_from(self, other):
        self.backend.symlink_location(other, self)

    # ------------------------------------------------------------------------
    # Info about the dataframe.
    # ------------------------------------------------------------------------
    def get_size(self):
        raise NotImplementedError()

    def get_columns(self):
        raise NotImplementedError()

    def get_stats(self, config):
        return squark.stats.get_stats(self.df, config)

    def add_stats(self, stats_config=None, stats=None):
        stats_config = stats_config or self.env.stats_config
        stats = stats or self.get_stats(stats_config)
        squark.stats.add_stats_to_table(self, stats)

    @property
    def stats(self):
        return squark.stats.get_stats_from_table(self)

    # ------------------------------------------------------------------------
    # Other operations
    # ------------------------------------------------------------------------
    def repartition(self, num_partitions='auto'):
        #meta = self.backend.get_partfile_metadata(self)
        if num_partitions == 'auto':
            self.repartition_auto()
        self.df = self.df.repartition(num_partitions)
        return self

    def repartition_auto(self):
        raise NotImplementedError()

    def reformat(self, num_partitions=None, save_format=None):
        if num_partitions is not None:
            self.repartition(num_partitions)
        if save_format is not None:
            self.save_format = save_format

    # ------------------------------------------------------------------------
    # Handy things
    # ------------------------------------------------------------------------
    @property
    def source_obj(self):
        return self.location.source_obj

    @property
    def source_type(self):
        return self.source_obj.type

    def mktemp(self):
        return self.backend.mktemp(self.location)


class FileTable(Table):
    backend = FileBackend()


class HdfsTable(Table):
    backend = HdfsBackend()


class JdbcTable(Table):
    backend = JdbcBackend()

    def truncate(self) -> None:
        pass

    @property
    def name(self):
        return self.location.table

    @property
    def schema(self):
        return self.location.schema

    @property
    def dialect(self):
        return self.location.source_obj.dialect

    @property
    def sql_name(self):
        return self.dialect.render_template(
            "table_name.j2", table=self.name, schema=self.schema)

clsdict = {
    'file': FileTable,
    'hdfs': HdfsTable,
    'jdbc': JdbcTable,
}

def get_table(location, save_format: str = None):
    env = Environment()
    if isinstance(location, str):
        location = get_location(location)
    source = getattr(env.sources, location.source)
    Cls = clsdict.get(source.type)
    if Cls is not None:
        return Cls(location, save_format=save_format)
    msg = "Couldn't find a backend for location %r"
    raise ValueError(msg % location)
