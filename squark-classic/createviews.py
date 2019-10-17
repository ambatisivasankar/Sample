import os
import sys
import logging
import json

import utils
import new_utils
import squark.config.environment

# Environmental Variables
ENV_VARS_TO_LOAD_AS_IS = [
    "PROJECT_ID",
    "VERTICA_TRUSTSTOREPATH",
]

ENV_VARS_TO_LOAD_WITH_DEFAULTS = [
    ("VERTICA_CONNECTION_ID", None),
    ("SQUARK_DELETED_TABLE_SUFFIX", "_ADVANA_DELETED"),
    ("INCLUDE_TABLES", None),
    ("EXCLUDE_TABLES", None),
    ("JSON_INFO", None),
]

# Load Environment Variables
env_vars = new_utils.load_env_vars(
    vars_as_is=ENV_VARS_TO_LOAD_AS_IS,
    vars_with_defaults=ENV_VARS_TO_LOAD_WITH_DEFAULTS,
)

try:
    VERTICA_CONNECTION_ID = env_vars["VERTICA_CONNECTION_ID"]
except:
    VERTICA_CONNECTION_ID = "vertica_aws_nprd_dev"

PROJECT_ID = env_vars["PROJECT_ID"]
SKIP_ERRORS = False
SQUARK_DELETED_TABLE_SUFFIX = env_vars["SQUARK_DELETED_TABLE_SUFFIX"]
INCLUDE_TABLES = env_vars["INCLUDE_TABLES"]
EXCLUDE_TABLES = env_vars["EXCLUDE_TABLES"]
JSON_INFO = env_vars["JSON_INFO"]

if INCLUDE_TABLES is not None:
    INCLUDE_TABLES = [s.strip() for s in INCLUDE_TABLES.split(',') if s]

if EXCLUDE_TABLES:
    EXCLUDE_TABLES = [s.strip() for s in EXCLUDE_TABLES.split(',') if s]

TABLES_WITH_PARTITION_INFO = {}
if JSON_INFO:
    parsed_json = json.loads(JSON_INFO.replace("'", '"').replace('"""', "'"))
    if 'PARTITION_INFO' in parsed_json.keys():
        TABLES_WITH_PARTITION_INFO = parsed_json['PARTITION_INFO']['tables']
        print('TABLES_WITH_PARTITION_INFO: %r' % TABLES_WITH_PARTITION_INFO)

VIEW_DDL_PRIMARY_ONLY = """
CREATE VIEW {facing_schema}.{facing_table_name} AS
    SELECT * FROM {primary_schema}.{base_table_name};"""

VIEW_DDL_PRIMARY_AND_INCREMENTAL = """
CREATE VIEW {facing_schema}.{facing_table_name} AS
    WITH cteNew AS (
        SELECT *
        FROM {incremental_schema}.{base_table_name}
    )
    ,cteOld AS (
        SELECT *
        FROM {base_schema}.{base_table_name}
        WHERE {pkid} NOT IN (SELECT {pkid} FROM cteNew
                          UNION
                          SELECT {pkid} FROM {incremental_schema}.{base_table_name}{deleted_suffix})
    )
    SELECT * FROM cteNew
    UNION ALL
    SELECT * FROM cteOld
"""

logging.basicConfig(level=logging.DEBUG)


def create_view(conn, facing_schema, primary_schema, base_table_name, facing_table_name, base_schema=None, pkid=None):
    if base_schema:
        view_ddl = VIEW_DDL_PRIMARY_AND_INCREMENTAL.format(facing_schema=facing_schema,
                                                           base_schema=base_schema,
                                                           pkid=pkid,
                                                           incremental_schema=primary_schema,
                                                           base_table_name=base_table_name,
                                                           facing_table_name=facing_table_name,
                                                           deleted_suffix=SQUARK_DELETED_TABLE_SUFFIX)
    else:
        view_ddl = VIEW_DDL_PRIMARY_ONLY.format(facing_schema=facing_schema,
                                                primary_schema=primary_schema,
                                                base_table_name=base_table_name,
                                                facing_table_name=facing_table_name)
    print('creating view {} for base table {}'.format(facing_table_name, base_table_name))
    print(view_ddl)
    cur = conn.cursor()
    rs = cur.execute(view_ddl)


def main():
    # facing schema should probably always be the 1st arg
    # 2nd arg will be for foo schema, e.g. haven_daily, used in the simple SELECT * FROM foo.table view definitions
    # base_schema, used only in incremental view defs, will be pulled from the JSON_INFO for now, on a per-table basis
    facing_schema = '_{}'.format(sys.argv[1])
    primary_source_schema = sys.argv[2]
    print('>>>> arg[1] facing_schema: {} | arg[2] primary_source_schema: {}'.format(facing_schema, primary_source_schema))

    squarkenv = squark.config.environment.Environment()
    destination_vertica = squarkenv.sources[env_vars["VERTICA_CONNECTION_ID"]]
    destination_vertica.url = utils.format_vertica_url(
        destination_vertica.url, trust_store_path=env_vars["VERTICA_TRUSTSTOREPATH"]
    )
    vert_conn = destination_vertica.conn

    tables = vert_conn.get_tables(schema=primary_source_schema)
    for table in tables:
        table = dict(zip([k.lower() for k in table._fieldnames], table))
        table_name = table['table_name']
        print("Checking table: {tbl} {tbltype}".format(tbl=table_name, tbltype=table['table_type']), flush=True)
        if table['table_type'].upper() != 'TABLE':
            print('>>>> skipping non table: {}'.format(table_name))
            continue
        if INCLUDE_TABLES and table_name not in INCLUDE_TABLES:
            print('>>>> skipping table not in INCLUDE_TABLES: {}'.format(table_name), flush=True)
            continue

        is_incremental_table = False
        base_schema = None
        pkid_column_name = None
        if TABLES_WITH_PARTITION_INFO and table_name.lower() in [table.lower() for table in
                                                                 TABLES_WITH_PARTITION_INFO.keys()]:
            table_with_partitions_lower = {k.lower(): v for k, v in TABLES_WITH_PARTITION_INFO.items()}
            partition_info = table_with_partitions_lower[table_name.lower()]
            print('--- Partition info: %r' % partition_info, flush=True)
            is_incremental_table = partition_info.get('is_incremental', '').lower() in ['1', 'true', 'yes']
            if is_incremental_table:
                base_schema = partition_info['base_schema_name']
                pkid_column_name = partition_info['pkid_column_name']

        #2018.06.13 for some temporary but non-trivial time haven is renaming 10+ tables with "old_" prefix but wants the
        # views within Vertica to reference the orig table name, i.e. haven.interaction view pulls from old_interaction
        base_table_name = table_name
        facing_table_name = table_name
        if table_name.startswith('old_'):
            # 2018.10.29, should probably allow EXCLUDE_TABLES for any table but add here to limit testing
            #  will be a period in which valid tables with both orig & "old_" names will exist, allow skip of latter
            if table_name not in EXCLUDE_TABLES:
                facing_table_name = table_name[4:]
            else:
                print('>>>> skipping "old" table in EXCLUDE_TABLES: {}'.format(table_name), flush=True)
                continue


        try:
            if is_incremental_table and base_schema and pkid_column_name:
                print('>>>> table: {} is set to be handled as incremental'.format(table_name))
                print('>>>> facing_schema: {} | primary_source_schema: {} | base_schema: {} | pkid_column_name: {}'.format(
                    facing_schema, primary_source_schema, base_schema, pkid_column_name))
                create_view(vert_conn,
                            facing_schema,
                            primary_source_schema,
                            base_table_name,
                            facing_table_name,
                            base_schema,
                            pkid_column_name)
            else:
                print('>>>> table: {} is will be handled as "full", i.e. not incremental'.format(table_name))
                print('>>>> facing_schema: {} | primary_source_schema: {}'.format(facing_schema, primary_source_schema))
                create_view(vert_conn, facing_schema, primary_source_schema, base_table_name, facing_table_name)
        except Exception as exc:
            if SKIP_ERRORS:
                print('>>>> ERROR CREATING VIEW:')
                print(exc)
                continue
            else:
                raise exc


if __name__ == '__main__':
    main()

