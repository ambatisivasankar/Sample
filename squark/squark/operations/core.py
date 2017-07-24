import squark.exceptions
import squark.stats
from squark.base import TableType, BackendType, OperationType, ReportType
from squark.table import Table, get_table
from squark.location import Location
from squark.operations.base import (
    funcify_operation, UnaryOperation, BinaryOperation,
    SharedBackendOperation, MultiBackendOperation)


class Move(MultiBackendOperation):

    def __init__(self, src, dest, nocommit=False):
        self.src = src
        self.dest = dest
        self._nocommit = False

    def run_one(self, location: Location):
        if location.source_obj == self.dest_location.source_obj:
            return Rotate(location, self.dest, nocommit=True).run()
        else:
            return Copy(location, self.dest, nocommit=True).run()

move = funcify_operation(Move)


class Rotate(MultiBackendOperation):
    '''Replace table `dest` with table `src` by:
        1. `mv`'ing `dest` from its canonical location to a temporary location
        2. `mv`'ing `src` into the canonical location formerly occupied by `dest`
    Note that `src` must be written out before the rotate operation is invoked.
    '''
    def __init__(self, src, dest, force=False, copy_permissions=False, nocommit=False):
        self.src = src
        self.dest = dest
        self.force = force
        self.copy_permissions = copy_permissions
        self._nocommit = nocommit

    def run_one(self, location: Location=None):
        # Intra-backend rotate.
        if location.source_obj == self.dest.source_obj:
            with self.dest_table.mktemp() as dest_tmp:
                if self.dest_table.exists():
                    # If dest already exists, move it to dest_tmp.
                    if not self.force:
                        msg = "Refusing to overwrite %r" % self.dest
                        raise squark.exceptions.LocationAlreadyExists(msg)
                    self.dest_table.backend.ensure_location_ready(dest_tmp)
                    self.dest_table.move_to(dest_tmp)

                # Move location to dest.
                get_table(location).move_to(self.dest)

                # If here, the mv succeeded, so delete dest_tmp.
                dest_tmp = get_table(dest_tmp)
                if dest_tmp.exists():
                    dest_tmp.remove()

        # Interbackend rotate.
        else:
            with self.dest_table.mktemp() as dest_tmp:
                # First copy src to a tmp location in the dest backend.
                copy(location, dest_tmp)
                # Then do an intra-backend rotate with dest and the temp location.
                rotate(dest_tmp, self.dest, self.force, self.copy_permissions)


rotate = funcify_operation(Rotate)


def symlink(src: Table, dest: Table, force=False, copy_permissions=False):
    if src.source_obj != dest.source_obj:
        msg = ("Locations in different backends cannot "
               " be symlink'd: src=%r, dest=%s")
        raise Exception(msg % (src, dest))

    if dest.exists() and force:
        dest.remove()

    dest.symlink_from(src)

    if copy_permissions:
        raise NotImplementedError()


def write(src, dest=None, force=False):
    '''Write `src` out to a temporary location, then rotate into the location
    occupied by `dest`.
    '''
    if dest is None:
        dest = src
    with dest.backend.mktemp(dest.location) as tmp:
        src.save_as(tmp)
        rotate(tmp, dest, force=force)


def overwrite(src, dest=None):
    write(src, dest, force=True)


def reformat(table, **format_options):
    '''Load up the table and apply any new partition or format specifications.
    '''
    table.reformat(**format_options)
    overwrite(table)


def repartition(table, **format_options):
    '''Load up and repartition, then write to a temporary location and
    rotate in place of the existing table.
    '''
    if not format_options.get('num_partitions'):
        format_options['num_partitions'] = 'auto'
    reformat(table, **format_options)


def copy(src, dest, force=False, **format_options):
    '''Optionally reformat, then overwrite `dest` with `src`.
    '''
    if format_options:
        src.reformat(**format_options)

    if src.source_obj == dest.source_obj:
        src.copy_to(dest)
    else:
        write(src, dest, force=force)


def stats(table):
    '''Compute stats for this dataframe and add them to its schema fields
    as pickled, base64-encoded blobs.

    "But that's horrible! You are a monster, Thom!"

    I know, I know. Data types like decimals can't be elegantly represented
    as JSON without defining a custom encoder, which is probably what I
    should have done here instead.
    '''
    squark.stats.add_stats_to_table(table)


def job(conf):
    pass
