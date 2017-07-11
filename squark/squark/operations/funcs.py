import squark.exceptions
from squark.base import TableType, BackendType, OperationType, ReportType
from squark.table import Table, get_table
from squark.location import Location
from squark.operations.base import UnaryOperation, BinaryOperation


def move(src: Table, dest: Table):
    if src.location.source_obj == dest.location.source_obj:
        rotate(src, dest)
    else:
        copy(src, dest)


def rotate(src: Table, dest: Table, force=False, copy_permissions=False):
    '''Replace table `dest` with table `src` by:
        1. `mv`'ing `dest` from its canonical location to a temporary location
        2. `mv`'ing `src` into the canonical location formerly occupied by `dest`
    Note that `src` must be written out before the rotate operation is invoked.
    '''
    # Intra-backend rotate.
    if src.source_obj == dest.source_obj:
        with dest.mktemp() as dest_tmp:
            if dest.exists() and not force:
                msg = "Refusing to overwrite %r" % dest
                raise squark.exceptions.LocationAlreadyExists(msg)
            if dest.exists():
                dest.backend.ensure_location_ready(dest_tmp)
                dest.move_to(dest_tmp)
            src.move_to(dest.location)

            dest_tmp = get_table(dest_tmp)
            if dest_tmp.exists():
                dest_tmp.remove()

    # Interbackend rotate.
    else:
        with dest.mktemp() as dest_tmp:
            # First copy src to a tmp location in the dest backend.
            copy(src, dest_tmp)
            # Then do an intra-backend rotate with dest and the temp location.
            rotate(dest_tmp, dest, force, copy_permissions)


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
        tmp.save_from(src)
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


def job(conf):
    pass
