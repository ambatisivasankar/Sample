import os
import sys
import functools
import traceback

import click

from squark import cliparams as params
from squark.utils.common import resolve_name
from squark.exceptions import SquarkInvocationError

from squark.table import get_table
from squark.location import Location, JdbcLocation
from squark.config.environment import Environment
from squark.operations import core as opscore


pass_config = click.make_pass_decorator(Environment)
LOG_LEVELS = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL', 'FATAL']


@click.group()
@click.version_option('0.0')
@click.pass_context
@click.option('--conf', '-c', multiple=True, help='Path to an extra config file')
@click.option('--spark-conf', multiple=True, help='Equivalent to spark-submit --conf')
@click.option('--sc', multiple=True, help='Equivalent to spark-submit --conf')
@click.option('--pdb', is_flag=True, help='Enable post-mortem debugging with pdb')
@click.option('--pudb', is_flag=True, help='Enable post-mortem debugging with pudb')
@click.option('--debug', is_flag=True, help='Set log level to DEBUG')
@click.option('--info', is_flag=True, help='Set log level to INFO')
@click.option('-v', '--verbose', count=True, help='Set loglevel with verbosity flags')
@click.option('--loglevel', type=click.Choice(LOG_LEVELS), help='Explicitly pass log level')
@click.option('--color/--no-color', default=True, help='Whether to colorize log output')
def cli(ctx, conf, spark_conf, sc, pdb, pudb, debug, info, verbose, loglevel, color):
    '''Squark aims to provide a customizable CLI for automating common and
    repetitive actions on Spark dataframes, such as:

    \b
      * repartitioning,
      * converting from one format to another, and
      * copying from one backend to another, such as from a JDBC source to HFDS.

    A secondary goal of squark is to provide high-level versions of common
    operations like `copy`, where instead of overwriting the target location,
    the possibly-lengthy write operation occurs on a temp file, which is then
    quickly moved into the place of the target file. If the target file already
    exists, it is quickly moved to another temp location, then deleted when the
    write operation is complete.

    Squark identifies dataframes via simplified identifier strings passed on
    the command line.

    \b
    Examples
    --------

    To copy a dataframe from HDFS to the a local file:

        squark cp hdfs:///path/to/file file:///data/path/to/file

    To print basic info about an ORC dataframe in HFDS, including number of
    partfiles, average partfile size, and degree of skew in the set of
    partfile sizes:

        squark info hdfs+orc://path/to/file

    To convert the same dataframe to ORC and automatically repartition the
    dataframe on disk (see `squark repartition --help` for details):

        squark reformat parquet hdfs+orc://path/to/file --repartition

    For more detail, run `squark man`.
    '''
    env = Environment()
    ctx.env = env
    # --------------------------------------------------------------------------
    # Initial logging config.
    # --------------------------------------------------------------------------
    logvals = (debug, info, verbose, loglevel)
    if 1 < logvals.count(True):
        raise SquarkInvocationError('Conflicting loglevel args given.')
    logopts = set(logvals) - set([None, 0, False])
    if 1 < len(logopts):
        raise SquarkInvocationError('Multiple loglevel args given.')

    _loglevel = None
    if info:
        _loglevel = 'INFO'
    elif debug:
        _loglevel = 'DEBUG'
    elif verbose:
        _loglevel = LOG_LEVELS[verbose]
    elif loglevel:
        _loglevel = loglevel

    env.init_logging(loglevel, color)

    # --------------------------------------------------------------------------
    # Read in any extra config files passed via the "--conf" option.
    # --------------------------------------------------------------------------
    for path in conf:
        path = os.path.abspath(path)
        if not os.path.isfile(path):
            msg = "The config file path passed to --conf, %r, doesn't exist."
            raise FileNotFoundError(msg % path)
        env.cfg.read(path)

    # --------------------------------------------------------------------------
    # Allow spark conf properties to be overridden by "--sc" and "--spark-conf"
    # --------------------------------------------------------------------------
    confobj = env.spark.conf
    for prop in spark_conf + sc:
        key, val = prop.split("=")
        existing_val = confobj.get(key)
        if existing_val is not None:
            msg = ('Existing park conf property "%s=%s" overridded by '
                   'property %s=%s passed via CLI flag "--sc".')
            logger.debug(msg, key, existing_val, key, val)
        confobj.set(key, val)

    # --------------------------------------------------------------------------
    # Support for post-mortem debugging.
    # --------------------------------------------------------------------------
    debugger = None
    if pdb:
        import pdb as debugger
    if pudb:
        import pudb as debugger

    if debugger is not None:
        # turn on PDB-on-error mode
        # stolen from http://stackoverflow.com/questions/1237379/
        # if this causes problems in interactive mode check that page
        def _tb_info(type, value, tb):
            traceback.print_exception(type, value, tb)
            debugger.pm()
        sys.excepthook = _tb_info


@cli.command()
@click.argument('dataframe')
@click.option('--num-partitions', '-n', type=int)
@click.option('--src-fmt', '--src-format')
def repartition(dataframe, num_partitions, src_format):
    '''Repartitions a dataframe on disk.

    And a longer explanation goes here.
    '''
    table = get_table(dataframe)
    table.repartition(table, num_partitions)
    import pdb; pdb.set_trace()


@cli.command()
@click.argument('dataframe')
@click.argument('destination-format')
@click.option('--num-partitions', '-n', type=int)
@click.option('--repartition', '-r', is_flag=True)
@click.option('--stats', '-s', is_flag=True)
def reformat(dataframe, destination_format, num_partitions, repartition, stats):
    '''Converts a dataframe to another format.

    And a longer explanation goes here.
    '''
    import pdb; pdb.set_trace()
    table = get_table(dataframe)

    if num_partitions or repartition:
        n = num_partitions or 'auto'
        table.repartition(num_partitions=n)

 #   if stats:
 #       table.add_stats()

    table.reformat(save_format=destination_format)

    opscore.write(table)


@click.argument('dest', type=params.table)
@click.argument('src', type=params.table)
@click.option('--force', '-f', is_flag=True)
@click.option('--copy-permissions', '-p', is_flag=True)
@cli.command()
def rotate(src, dest, force=False, copy_permissions=False):
    '''Quasi-atomically replaces [src] with [dest].

    If the locations share the same backend, first `dest` will be
    moved to a temporary location, then `src` will be renamed
    to `dest`, and the old `dest` data is removed.

    If the locations do not share the same backend, `src` is first
    copied to a temporary location in the same backend as `dest`, then
    the same move operation described above occurs.

    If `dest` already exists, this operation will fail unless the --force
    flag is passed.
    '''
    opscore.rotate(src, dest, force, copy_permissions)


@click.argument('dest', type=params.table)
@click.argument('src', type=params.table)
@click.option('--force', '-f', is_flag=True)
@click.option('--copy-permissions', '-p', is_flag=True)
@cli.command()
def symlink(src, dest, force=False, copy_permissions=False):
    '''Create a pseudo symlink from `src` to `dest`

    If the backend supports it, this operation creates a
    real symlink if supported, or something similar to a symlink,
    such as a view at `dest` pointing back to `src` in a relational
    database.

    If `dest` already exists, the force flag must be passed, and the
    existing `dest` will be rotate'd out and then deleted.
    '''
    opscore.symlink(src, dest, force, copy_permissions)


@click.argument('jobconf', type=click.File('rb'))
@cli.command()
def job(jobconf):
    '''Execute a series of operations in a YAML file
    '''
    import yaml
    data = yaml.safe_load(jobconf)
    opscore.job(data)


@cli.command()
def man():
    '''More detail about how squark works.
    '''
    squark_doc = '''
    Squark identifies dataframes via simplified identifier strings passed on
    the command line. For dataframes saved in HDFS, s3 or the local filesystem,
    the identifiers must conform to any of the following formats

        r'^(?P<source>\S+?)://(?P<host>\w+):(?P<port>\d+)(?P<path>/[\S+]*)$'
        r'^(?P<source>\S+?)://(?P<host>\w+)(?P<path>/[\S+]*)$'
        r'^(?P<source>\S+?)://(?P<path>/[\S+]*)$'

    where
        * `source` is a string like 'hdfs' or 'file' or 's3', optionally
          followed by a '+' and a storage format like 'orc' or 'parquet'
        * `host` is the domain name of the HFDS name node
        * `port` is the name node's HDFS port (defaults to 50070)
        * `path` is the path to the file.

    For dataframes accessed via a JDBC connection, the identifiers must conform
    to any of the following formats

        * r'^(?P<source>\S+?)://(?P<schema>\w+)(?P<path>/[\S+]*)$'
        * r'^(?P<source>\S+?)://(?P<path>/[\S+]*)$'

    where `schema` optionally specifies what schema the table resides in.

    \b
    Dataframe Identifier Examples
    -----------------------------

    The following ID refers to a dataframe stored in the filesystem at the
    indicated path:

        file:///path/to/the/data

    This slightly different version specifies the format of the same dataframe:

        file+orc:///path/to/the/data

    This dataframe refers to a parquet dataframe stored in HDFS:

        hdfs:///path/to/the/data

    And this dataframe refers to a table acccessed via JDBC, in a source
    defined in $SQUARK_CONFIG_DIR/secrets.cfg, which must be encrypted using
    ansible-vault:

        customerdb://dbo/customers
    '''

    click.echo_via_pager(squark_doc)


if __name__ == "__main__":
    cli()
