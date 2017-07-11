import os
import uuid
import textwrap

import yaml
import pytest

import squark.stats
from squark.backend import FileBackend
from squark.location import get_location
from squark.exceptions import LocationNotWritable


#---------------------------------------------------------------------------
# Location existence.
#----------------------------------------------------------------------------
def test_location_exists_pass(fs):
    assert fs.location_exists(get_location("file:///tmp"))

def test_location_exists_pass(fs):
    assert not fs.location_exists(get_location("file:///cow"))

def test_temp_location_exists_pass(fs):
    location = get_location("file:///tmp/squark-test")
    tmp = fs.new_temp_location(location)
    assert not fs.location_exists(tmp)

def test_temp_location_exists_fail(fs):
    location = get_location("file:///cow/squark-test")
    tmp = fs.new_temp_location(location)
    assert not fs.location_exists(tmp)

def test_mktemp_pass(fs):
    path = "file:///tmp/%s/%s" % (uuid.uuid4(), uuid.uuid4())
    location = get_location(path)
    with fs.mktemp(location) as tmp:
        assert fs.location_parent_exists(tmp)
        assert fs.location_parent_is_writable(tmp)
        assert fs.location_is_ready(tmp)
        # Now create it so we can prove the file gets
        # deleted on __exit__.
        assert not fs.location_exists(tmp)
        path = fs.prep_location(tmp)
        with open(path, 'w') as f:
            f.write('test')
        assert fs.location_exists(tmp)
    # By here it should be gone.
    assert not fs.location_exists(tmp)

#---------------------------------------------------------------------------
# Location readiness.
#----------------------------------------------------------------------------
def test_location_is_ready_pass(fs):
    location = get_location("file:///tmp/squark-test")
    assert fs.location_is_ready(location)

def test_location_is_ready_fail_existence(fs):
    location = get_location("file:///cow/squark-test")
    assert not fs.location_exists(location.parent)
    assert not fs.location_is_ready(location)

def test_location_is_ready_fail_writability(fs):
    path = "file:///tmp/%s/%s" % (uuid.uuid4(), uuid.uuid4())
    location = get_location(path)

    # Create the parent dir.
    parent = fs.prep_location(location.parent)
    os.mkdir(parent)

    # Make it unwritable and expect failure.
    os.chmod(parent, 0o000)
    assert not fs.location_parent_is_writable(location)
    assert not fs.location_is_ready(location)

    # Now make it writeable and see if the test passes.
    os.chmod(parent, 0o700)
    assert fs.location_parent_is_writable(location)
    assert fs.location_is_ready(location)

    # Delete the temp dir
    fs.remove_location(location.parent)

def test_ensure_location_ready_pass(fs):
    path = "file:///tmp/%s/%s" % (uuid.uuid4(), uuid.uuid4())
    location = get_location(path)

    parent = fs.prep_location(location.parent)

    # Verify that parent dir doesn't exist but gets created.
    assert not fs.location_parent_exists(location)
    fs.ensure_location_ready(location)
    assert fs.location_parent_exists(location)

    # Cleanup.
    fs.remove_location(location.parent)

def test_ensure_location_ready_fail_writability(fs):
    path = "file:///tmp/%s/%s" % (uuid.uuid4(), uuid.uuid4())
    location = get_location(path)

    # Create the parent dir
    fs.ensure_location_parent_exists(location)
    assert fs.location_is_ready(location)

    # But make it unwritable and expect failure.
    parent = fs.prep_location(location.parent)
    os.chmod(parent, 0o000)
    assert not fs.location_parent_is_writable(location)

    # The "ensure" operation should throw an error.
    with pytest.raises(LocationNotWritable):
        fs.ensure_location_ready(location)

    # But if we fix the permissions it succeeds.
    os.chmod(parent, 0o700)
    fs.location_is_ready(location)

    fs.remove_location(location.parent)


#---------------------------------------------------------------------------
# Destructive operations.
#----------------------------------------------------------------------------
def test_remove_location(fs):
    location = get_location("file:///tmp/squark-test")
    fs = FileBackend()
    tmp = fs.new_temp_location(location)
    with open(fs.prep_location(tmp), 'w') as f:
        f.write("test")
    assert fs.location_exists(tmp)
    fs.remove_location(tmp)
    assert not fs.location_exists(tmp)


def test_move_location(fs):
    src = get_location("file:///tmp/%s/%s" % (uuid.uuid4(), uuid.uuid4()))
    dest = get_location("file:///tmp/%s/%s" % (uuid.uuid4(), uuid.uuid4()))

    # Write the src file.
    fs.ensure_location_ready(src)
    assert fs.location_is_ready(src)
    content = str(uuid.uuid4())
    with open(fs.prep_location(src), 'w') as f:
        f.write(content)

    # Src should exist and dest shouldn't.
    assert fs.location_exists(src)
    assert not fs.location_exists(dest)

    # Move it.
    fs.ensure_location_ready(dest)
    assert fs.location_is_ready(dest)
    fs.move_location(src, dest)

    # Now dest should exist and src shouldn't.
    assert not fs.location_exists(src)
    assert fs.location_exists(dest)

    # Verify the content is correct.
    with open(fs.prep_location(dest)) as f:
        moved_content = f.read()
        assert content == moved_content

    # Cleanup
    fs.remove_location(src.parent)
    fs.remove_location(dest.parent)


#---------------------------------------------------------------------------
# Stats
#----------------------------------------------------------------------------
def test_stats_field_types_explicit(fs, df1):
    cfg = yaml.load(textwrap.dedent('''
        ---
        dataframe:
            - count
        field_types:
            NumericType:
                - max
                - min
                - avg
                - stddev_pop
                - stddev_samp
                - var_pop
                - var_samp
                - percent_null
            StringType:
                - max
                - min
                - avg
                - stddev_pop
                - stddev_samp
                - var_pop
                - var_samp
                - percent_null
    '''))
    #df1.add_stats(cfg)
    #stats = squark.stats.get_stats_from_table(df1)
    #'avg' in stats['string']


#---------------------------------------------------------------------------
# Repartitioning
#----------------------------------------------------------------------------
def test_repartition(fs):
    pass
    #import pdb; pdb.set_trace()
