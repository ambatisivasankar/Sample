import os
import uuid

import pytest

from squark.backend import FileBackend
from squark.location import get_location
from squark.exceptions import LocationNotWritable


#---------------------------------------------------------------------------
# Grants
#----------------------------------------------------------------------------
def test_grant_where_schema_exists(jdbc, vertica):
    '''Test granting create an usage on a schema that already exists
    works.
    '''
    user = 'testuser1'
    loc = get_location("vertica://_squark_test_schema")
    if not jdbc.location_exists(loc):
        jdbc.create_schema(loc)
    grant = dict(
        grantee=user,
        object_name='_squark_test_schema',
        object_type='SCHEMA',
        permissions=('CREATE', 'USAGE'))

    vertica.ensure_user_exists(user)
    # Make sure its absent.
    vertica.ensure_grant_removed(**grant)
    assert not vertica.grant_exists(**grant)
    # Not make sure it exists.
    vertica.ensure_grant(**grant)
    assert vertica.grant_exists(**grant)
    # It already exists--call ensure_exists again.
    vertica.ensure_grant(**grant)
    assert vertica.grant_exists(**grant)
    # Make sure it's absent again.
    vertica.ensure_grant_removed(**grant)
    assert not vertica.grant_exists(**grant)


def test_grant_where_schema_not_exists(jdbc, vertica):
    user = 'testuser1'
    loc = get_location("vertica://_squark_test_schema")
    if not jdbc.location_exists(loc):
        jdbc.create_schema(loc)
    grant = dict(
        grantee=user,
        object_name='_squark_test_schema',
        object_type='SCHEMA',
        permissions=('CREATE', 'USAGE'))

    vertica.ensure_user_exists(user)
    # Make sure its absent.
    vertica.ensure_grant_removed(**grant)
    assert not vertica.grant_exists(**grant)
    # Now make sure it exists.
    vertica.ensure_grant(**grant)
    assert vertica.grant_exists(**grant)
    # Make sure it's absent again.
    vertica.ensure_grant_removed(**grant)
    assert not vertica.grant_exists(**grant)

#---------------------------------------------------------------------------
# Location existence.
#----------------------------------------------------------------------------
def test_location_exists_pass(jdbc, vertica_conn):
    loc = get_location("vertica:///_squark_test")
    if not jdbc.location_exists(loc):
        cur = vertica_conn.cursor()
        cur.execute("create table %s (a int);" % loc.table)
        cur.close()
    assert jdbc.location_exists(loc)
    jdbc.remove_location(loc)

def test_location_exists_fail(jdbc, vertica_conn):
    loc = get_location("vertica:///_squark_test_table_exists_fail")
    if jdbc.location_exists(loc):
        jdbc.remove_location(loc)
    assert not jdbc.location_exists(loc)

def test_ensure_location_removed_schema_exists(jdbc, vertica_conn):
    '''Ensure schema removed, where the schema already exists.
    '''
    loc = get_location("vertica://_squark_test_schema")
    if not jdbc.location_exists(loc):
        dialect = jdbc.get_dialect(loc)
        dialect.create_schema(schema=loc.schema)
    assert jdbc.location_exists(loc)
    jdbc.ensure_location_removed(loc)
    assert not jdbc.location_exists(loc)

def test_ensure_location_removed_schema_not_exists(jdbc, vertica_conn):
    '''Ensure schema removed, where the schema does not currently exists.
    '''
    loc = get_location("vertica://_squark_test_schema")
    if jdbc.location_exists(loc):
        jdbc.remove_location(loc)
    assert not jdbc.location_exists(loc)
    jdbc.ensure_location_removed(loc)
    assert not jdbc.location_exists(loc)


def test_get_temp_location(jdbc):
    location = get_location("vertica://v_catalog/sessions")
    tmp = jdbc.new_temp_location(location)
    assert not jdbc.location_exists(tmp)
    assert str(tmp) != str(location)


def test_mktemp_pass(jdbc, vertica):
    schema = vertica.tmp_schema
    table = str(uuid.uuid4()).replace('-', '_')
    path = "vertica://%s/%s" % (schema, table)
    location = get_location(path)
    with jdbc.mktemp(location) as tmp:
        assert jdbc.location_parent_exists(tmp)
        assert jdbc.location_parent_is_writable(tmp)
        assert jdbc.location_is_ready(tmp)
        # Now create it so we can prove the file gets
        # deleted on __exit__.
        assert not jdbc.location_exists(tmp)
        cur = vertica.conn.cursor()
        sql = 'create table "%s"."%s" (a int);'
        sql = sql % (tmp.schema, tmp.table)
        cur.execute(sql)
        assert jdbc.location_exists(tmp)
    # By here it should be gone.
    assert not jdbc.location_exists(tmp)

#---------------------------------------------------------------------------
# Location readiness.
#----------------------------------------------------------------------------
def test_location_is_ready_pass(jdbc):
    location = get_location("vertica://:tmp:/squark-test")
    assert jdbc.location_is_ready(location)

def test_location_is_ready_fail_existence(jdbc):
    location = get_location("vertica://cow/squark-test")
    assert not jdbc.location_exists(location.parent)
    assert not jdbc.location_is_ready(location)

# def test_location_is_ready_fail_writability(jdbc):
#     path = "vertica://tmp/%s/%s" % (uuid.uuid4(), uuid.uuid4())
#     location = get_location(path)

#     # Create the parent dir.
#     parent = jdbc.prep_location(location.parent)
#     os.mkdir(parent)

#     # Make it unwritable and expect failure.
#     os.chmod(parent, 0o000)
#     assert not jdbc.location_parent_is_writable(location)
#     assert not jdbc.location_is_ready(location)

#     # Now make it writeable and see if the test passes.
#     os.chmod(parent, 0o700)
#     assert jdbc.location_parent_is_writable(location)
#     assert jdbc.location_is_ready(location)

#     # Delete the temp dir
#     jdbc.remove_location(location.parent)

def test_ensure_location_ready_pass(jdbc):
    path = "vertica://%s/%s" % (uuid.uuid4(), uuid.uuid4())
    location = get_location(path)

    # Verify that parent dir doesn't exist but gets created.
    assert not jdbc.location_parent_exists(location)
    jdbc.ensure_location_ready(location)
    assert jdbc.location_parent_exists(location)

    # Cleanup.
    jdbc.remove_location(location.parent, force=True)

def test_ensure_location_ready_fail_writability(jdbc):
    # System table definitely not "writable"
    path = "vertica://v_monitor/sessions"
    location = get_location(path)
    assert not jdbc.location_is_ready(location)


#---------------------------------------------------------------------------
# Destructive operations.
#----------------------------------------------------------------------------
def test_remove_location(jdbc, vertica):
    location = get_location("vertica://:tmp:/squark-test")
    tmp = jdbc.new_temp_location(location)

    jdbc.ensure_location_removed(tmp)
    assert not jdbc.location_exists(tmp)
    cur = vertica.conn.cursor()
    sql = 'create table "%s"."%s" (a int);'
    sql = sql % (tmp.schema, tmp.table)
    cur.execute(sql)
    assert jdbc.location_exists(tmp)
    jdbc.remove_location(tmp)
    assert not jdbc.location_exists(tmp)


def test_move_location_schema(jdbc, vertica):
    src = get_location("vertica://:tmp:/%s" % uuid.uuid4())
    dest = get_location("vertica://:tmp:/%s" % uuid.uuid4())

    # Create the src file.
    cur = vertica.conn.cursor()
    sql = 'create table "%s"."%s" (a int);'
    sql = sql % (src.schema, src.table)
    cur.execute(sql)
    assert jdbc.location_exists(src)

    # Add a row.
    sql = 'insert into "%s"."%s" (a) values (12345);'
    sql = sql % (src.schema, src.table)
    cur.execute(sql)

    # Src should exist and dest shouldn't.
    assert jdbc.location_exists(src)
    assert not jdbc.location_exists(dest)

    # Move it.
    jdbc.ensure_location_ready(dest)
    assert jdbc.location_is_ready(dest)
    jdbc.move_location(src, dest)

    # Now dest should exist and src shouldn't.
    assert not jdbc.location_exists(src)
    assert jdbc.location_exists(dest)

    # Verify the content is corrent.
    sql = 'select * from "%s"."%s";'
    sql = sql % (dest.schema, dest.table)
    rs = cur.execute(sql)
    assert rs.fetchone().a == 12345

    # Cleanup
    jdbc.ensure_location_removed(src)
    jdbc.ensure_location_removed(dest)


def test_expand_glob(jdbc, vertica_conn):
    '''Ensure schema removed, where the schema already exists.
    '''
    schema = '_squark_test_' + str(uuid.uuid4()).replace('-', '_')
    loc = get_location("vertica://%s/*" % schema)

    # Create the parent schema.
    if not jdbc.location_exists(loc.parent):
        dialect = jdbc.get_dialect(loc)
        dialect.create_schema(schema=loc.schema)
    assert jdbc.location_exists(loc.parent)

    # Create some test tables.
    cur = vertica_conn.cursor()
    cur.execute("create table %s.test1(a int);" % schema)
    cur.execute("create table %s.test2(b int);" % schema)

    # Confirm the glob pattern matches them.
    table1, table2 = jdbc.expand_glob(loc)
    assert table1.schema == schema
    assert table1.table == 'test1'
    assert table2.schema == schema
    assert table2.table == 'test2'

    jdbc.ensure_location_removed(loc.parent, force=True)
    assert not jdbc.location_exists(table1)
    assert not jdbc.location_exists(table2)
    assert not jdbc.location_exists(loc)


