import os
import shutil
import tempfile

from squark.location import Location, JdbcLocation, get_location


def test_hdfs_simple():
    string = 'hdfs:///path/to/the/data'
    loc = Location(string)
    assert loc.source == 'hdfs'
    assert loc.path == '/path/to/the/data'
    assert str(loc) == string
    assert str(loc.parent) == 'hdfs:///path/to/the'


def test_hdfs_simple_with_format():
    string = 'hdfs+orc:///path/to/the/data'
    loc = Location(string)
    assert loc.source == 'hdfs'
    assert loc.path == '/path/to/the/data'
    assert str(loc) == 'hdfs:///path/to/the/data'
    assert str(loc.parent) == 'hdfs:///path/to/the'
    assert str(loc.format) == 'orc'


def test_hdfs_namenode():
    string = 'hdfs://namenode/path/to/the/data'
    loc = Location(string)
    assert loc.source == 'hdfs'
    assert loc.host == 'namenode'
    assert loc.path == '/path/to/the/data'
    assert str(loc) == string
    assert str(loc.parent) == 'hdfs://namenode/path/to/the'


def test_hdfs_namenode_port():
    string ='hdfs://namenode:50070/path/to/the/data'
    loc = Location(string)
    assert loc.source == 'hdfs'
    assert loc.host == 'namenode'
    assert loc.port == '50070'
    assert loc.path == '/path/to/the/data'
    assert str(loc) == string
    assert str(loc.parent) == 'hdfs://namenode:50070/path/to/the'


def test_file_simple():
    string = 'file:///path/to/the/data.orc'
    loc = Location(string)
    assert loc.source == 'file'
    assert loc.path == '/path/to/the/data.orc'
    assert str(loc) == string
    assert str(loc.parent) == 'file:///path/to/the'


def test_file_simple_with_format():
    string = 'file+orc:///path/to/the/data.orc'
    loc = Location(string)
    assert loc.source == 'file'
    assert loc.path == '/path/to/the/data.orc'
    assert str(loc) == 'file:///path/to/the/data.orc'
    assert str(loc.parent) == 'file:///path/to/the'
    assert loc.format == 'orc'


def test_jdbc_with_schema():
    string ='jdbc://dbo/best_table'
    loc = JdbcLocation(string)
    assert loc.source == 'jdbc'
    assert loc.schema == 'dbo'
    assert loc.table == 'best_table'
    assert str(loc) == string
    assert str(loc.parent) == 'jdbc://dbo/'


def test_jdbc_with_schema_and_format():
    string ='jdbc+orc://dbo/best_table'
    loc = JdbcLocation(string)
    assert loc.source == 'jdbc'
    assert loc.schema == 'dbo'
    assert loc.table == 'best_table'
    assert str(loc) == 'jdbc://dbo/best_table'
    assert str(loc.parent) == 'jdbc://dbo/'
    assert loc.format == 'orc'


def test_glob_parse_file():
    string = 'file:///path/to/files/*'
    loc = get_location(string)
    assert loc.is_glob()
    assert loc.path == '/path/to/files/*'


def test_glob_parse_hdfs():
    string = 'hdfs:///path/to/files/*'
    loc = get_location(string)
    assert loc.is_glob()

    string = 'hdfs://hdfs_host/path/to/files/*'
    loc = get_location(string)
    assert loc.is_glob()
    assert loc.path == '/path/to/files/*'


def test_glob_parse_jdbc_schema():
    string = 'vertica://schema1/*'
    loc = get_location(string)
    assert loc.is_glob()
    assert loc.schema == 'schema1'
    assert loc.table == '*'

def test_expand_glob_file_backend(fs):
    tmp = tempfile.mkdtemp()
    test1 = os.path.join(tmp, 'test1')
    test2 = os.path.join(tmp, 'test2')
    open(test1, 'w').close()
    open(test2, 'w').close()
    loc = get_location("file://%s/*" % tmp)
    locs = list(fs.expand_glob(loc))
    assert {l.path for l in locs} == set([test1, test2])
    shutil.rmtree(tmp)
    assert not os.path.isdir(tmp)


