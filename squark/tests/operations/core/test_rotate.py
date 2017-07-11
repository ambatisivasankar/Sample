import os
import uuid
import random

import pytest

from squark.operations import core
from squark.table import get_table
from squark.location import get_location
from squark.exceptions import LocationAlreadyExists


def test_table_rotation_vertica(jdbc, vertica):
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
    core.rotate(get_table(src), get_table(dest))

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


def test_forcible_table_rotation_vertica(jdbc, vertica):
    src = get_location("vertica://:tmp:/%s" % uuid.uuid4())
    dest = get_location("vertica://:tmp:/%s" % uuid.uuid4())

    # Create the src table.
    cur = vertica.conn.cursor()
    sql = 'create table "%s"."%s" (a int);'
    sql = sql % (src.schema, src.table)
    cur.execute(sql)
    assert jdbc.location_exists(src)

    # Add a row.
    sql = 'insert into "%s"."%s" (a) values (12345);'
    sql = sql % (src.schema, src.table)
    cur.execute(sql)

    # Src should exist.
    assert jdbc.location_exists(src)
    assert not jdbc.location_exists(dest)

    # Create the src table.
    cur = vertica.conn.cursor()
    sql = 'create table "%s"."%s" (a int);'
    sql = sql % (dest.schema, dest.table)
    cur.execute(sql)
    assert jdbc.location_exists(dest)

    # Add a row.
    sql = 'insert into "%s"."%s" (a) values (6789);'
    sql = sql % (dest.schema, dest.table)
    cur.execute(sql)

    # Now dest should exist.
    assert jdbc.location_exists(dest)

    # Rotate them.
    jdbc.ensure_location_ready(dest)
    assert jdbc.location_is_ready(dest)

    # If fails with out force=True
    with pytest.raises(LocationAlreadyExists):
        core.rotate(get_table(src), get_table(dest))

    core.rotate(get_table(src), get_table(dest), force=True)

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
