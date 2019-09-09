import os
import re
import json
import time

from jinja2 import Template

import squark.config.environment
import utils
import new_utils
import textwrap

# Vertica reserved words


# Environmental Variables

# vars loaded with os.environ() which raises KeyError
ENV_VARS_TO_LOAD_AS_IS = (
    "PROJECT_ID",
    "CONNECTION_ID",
    "VERTICA_TRUSTSTOREPATH",
)

# vars which are considered to be truthy
ENV_VARS_TO_LOAD_AS_BOOL = (
    # Empty list currently
    "SQUARK_METADATA",
    "RUN_LIVE_MAX_LEN_QUERIES",
    "MAKE_DDL_FROM_TARGET",
)

# vars loaded with os.environ.get() which defaults in case of KeyError
ENV_VARS_TO_LOAD_WITH_DEFAULTS = (
    ("VERTICA_CONNECTION_ID", "vertica_dev"),
    ("INCLUDE_VIEWS", None),
    ("INCLUDE_TABLES", None),  # Logic
    ("EXCLUDE_SCHEMA", None),
    ("JSON_INFO", None),
    ("JENKINS_URL", ""),
    ("JOB_NAME", ""),
    ("BUILD_NUMBER", "-1"),
    ("SKIP_ERRORS", None),
    ("SQUARK_DELETED_TABLE_SUFFIX", "_ADVANA_DELETED"),
    ("CONVERT_ARRAYS_TO_STRING", None),
)

# vars which are to be cast to int after loading
# These should also appear in the above lists
ENV_VARS_TO_CAST_TO_INT = (
    # Empty list currently
)

UNICODE_TYPES = ("NVARCHAR", "NCHAR", "NVARBINARY")
MAX_VARCHAR_LEN = 65000


class ColSpec:

    typemap = dict(
        (
            # JDBC, vertica
            # ('BINARY', 'BINARY,')
            # ('VARBINARY', 'VARBINARY,')
            ("BIT", "BOOLEAN"),
            ("NVARCHAR", "VARCHAR"),
            ("LONGNVARCHAR", "LONG VARCHAR"),
            ("LONGVARCHAR", "LONG VARCHAR"),
            ("NCHAR", "CHAR"),
            ("CLOB", "LONG VARCHAR"),
            ("BLOB", "LONG VARBINARY"),
            ("LONGVARBINARY", "LONG VARBINARY"),
            # ('CHAR', 'CHAR')
            # ('VARCHAR', 'VARCHAR')
            # ('DATE', 'DATE')
            # ('TIMESTAMP', 'TIMESTAMP')
            # ('TIME', 'TIME')
            # ('TIMESTAMP', 'TIMESTAMP')
            ("DOUBLE", "FLOAT"),
            # ('BIGINT', 'INT,')
            # ('NUMERIC', 'NUMERIC')
        )
    )

    has_size = ("BINARY", "VARBINARY", "CHAR", "VARCHAR", "NVARCHAR")
    numby = ("DOUBLE", "BIGINT", "NUMERIC")

    def __init__(
        self,
        jdbc_spec,
        squark_spec,
        source_conn,
        convert_arrays_to_string,
        run_live_max_len_queries,
        jdbc_url,
    ):
        self.spec = jdbc_spec
        self.squark_metadata = squark_spec
        self.is_db2 = (
            squark_spec["conn_metadata"]["db_product_name"].lower().startswith("db2")
        )
        self.source_conn = source_conn
        self.convert_arrays_to_string = convert_arrays_to_string
        self.run_live_max_len_queries = run_live_max_len_queries
        self.jdbc_url = jdbc_url

    def ddl(self):

        # if self.spec.TABLE_NAME == 'manual_asset':
        #     if self.spec.COLUMN_NAME == 'value':
        #         import pdb; pdb.set_trace()
        # if self.spec.IS_AUTOINCREMENT:
        #     return 'INTEGER'
        from_type = self.spec.data_type
        to_type = self.typemap.get(from_type, from_type)
        data = dict(zip((k.upper() for k in self.spec._fieldnames), self.spec))
        data.update(to_type=to_type)

        if from_type in ("ARRAY", "OTHER"):
            print(
                '--- "{name}" self.spec.data_type: {type}, unless ARRAY + CONVERT_ARRAYS_TO_STRING, set as VARCHAR({max_varchar_len})'.format(
                    name=self.name, type=from_type, max_varchar_len=MAX_VARCHAR_LEN
                ),
                flush=True,
            )
            if from_type == "ARRAY" and self.convert_arrays_to_string:
                from_type = "VARCHAR"
                data["to_type"] = "VARCHAR"
            else:
                # Hstore
                return "VARCHAR({max_varchar_len})".format(
                    max_varchar_len=MAX_VARCHAR_LEN
                )

        # unicode types require more ~2x more space than their non-unicode brethren
        # not sure why this is 3x ??
        if from_type in UNICODE_TYPES:
            data["COLUMN_SIZE"] = data["COLUMN_SIZE"] * 3

        if "CHAR" in from_type or "BINARY" in from_type:
            if MAX_VARCHAR_LEN < (data["COLUMN_SIZE"] or 1):

                start_query_time = time.time()
                custom_column_definition = None
                data["COLUMN_SIZE"] = MAX_VARCHAR_LEN

                if self.squark_metadata and "large_ddl" in self.squark_metadata:
                    large_ddl = self.squark_metadata["large_ddl"]
                    if self.name in large_ddl:
                        data["COLUMN_SIZE"] = large_ddl[self.name]
                        custom_column_definition = "squark_config_large_ddl table"
                # 2018.10.25, *_id check covers ~360 columns in curr haven db, any new *_id columns > 255 char = badness
                #  ddl-create w/combo of large_ddl table and live queries is curr < 5min, below saves up to 90 seconds
                if not custom_column_definition and self.run_live_max_len_queries:
                    if self.name.lower().endswith("_id"):
                        id_like_column_size = 255
                        data["COLUMN_SIZE"] = id_like_column_size
                        custom_column_definition = '.endswith("_id") to {}'.format(
                            id_like_column_size
                        )
                if not custom_column_definition and self.run_live_max_len_queries:
                    # use self.spec.COLUMN_NAME = the orig, non-sanitized column name
                    max_len = utils.get_postgres_col_max_data_length(
                        self.source_conn, self.spec.TABLE_NAME, self.spec.COLUMN_NAME
                    )
                    custom_column_definition = "live query on source db"
                    if not max_len or max_len < 245:
                        max_len = 255
                    else:
                        # need gap between actual & defined length, else vertica check-for-truncation SQL won't work
                        max_len += 10
                    data["COLUMN_SIZE"] = max_len

                if custom_column_definition:
                    warning_msg = "meh..."
                    max_len_query_duration = time.time() - start_query_time
                    if max_len_query_duration > 5:
                        warning_msg = "LOOOOKOUT"
                    debug_msg = "column_path: {table}.{column}  max_len: {size:,}  max_len_query_duration: {duration:4f}  warning_msg: {msg}".format(
                        table=self.spec.TABLE_NAME,
                        column=self.spec.COLUMN_NAME,
                        size=data["COLUMN_SIZE"],
                        duration=max_len_query_duration,
                        msg=warning_msg,
                    )
                    print(debug_msg, flush=True)

                    if data["COLUMN_SIZE"] > MAX_VARCHAR_LEN:
                        data["to_type"] = "LONG " + data["to_type"]
                    print(
                        "--- Overriding default {max_varchar_len} length for {name}, use value from {custom_def}, final ddl will be: {to_type}({size})".format(
                            max_varchar_len=MAX_VARCHAR_LEN,
                            name=self.name,
                            custom_def=custom_column_definition,
                            to_type=data["to_type"],
                            size=data["COLUMN_SIZE"],
                        ),
                        flush=True,
                    )

        if from_type in self.has_size:
            tmpl = "{to_type}({COLUMN_SIZE})"
        elif from_type == "NUMERIC" or from_type == "DECIMAL":
            # Max precision is 1024.
            if 1024 < data["COLUMN_SIZE"]:
                data["COLUMN_SIZE"] = 1024
            elif 0 == data["COLUMN_SIZE"] and self.jdbc_url.startswith("jdbc:oracle"):
                # spark/sql/jdbc/OracleDialect.scala sets Oracle NUMBER types to (38,10) if size == 0, do same
                data["COLUMN_SIZE"] = 38
                data["DECIMAL_DIGITS"] = 10
            if self.spec.DECIMAL_DIGITS is not None:
                tmpl = "{to_type}({COLUMN_SIZE},{DECIMAL_DIGITS})"
            else:
                tmpl = "{to_type}({COLUMN_SIZE})"
        else:
            tmpl = to_type

        return tmpl.format(**data)

    def _get_db2_column_name(self):
        first_name_index = self.spec._fieldnames.index("NAME")
        first_name = self.spec[first_name_index]
        assumed_column_name = first_name
        if self.spec._fieldnames.count("NAME") > 1:
            second_name_index = self.spec._fieldnames.index(
                "NAME", first_name_index + 1
            )
            assumed_column_name = self.spec[second_name_index]
            # going by limited observations, there will be 2 NAME references, 1st = table name & 2nd = column name
            # only reverse that interpretation if the 2nd NAME == source table name
            if (
                assumed_column_name.upper()
                == self.squark_metadata["db2_table_name"].upper()
            ):
                assumed_column_name = first_name

        return assumed_column_name

    @property
    def name(self):
        if self.is_db2:
            return utils.sanitize(self._get_db2_column_name())
        else:
            return utils.sanitize(self.spec.COLUMN_NAME)

    @property
    def nullable(self):
        return self.spec.NULLABLE


# self.j2_env = Environment(**dict(
#     trim_blocks=True,
#     extensions=['jinja2.ext.with_']))


def create_super_projection_query(
    schema, table, projection_name, order_by_columns, segment_by_columns
):
    if segment_by_columns:
        segmented_template = textwrap.dedent(
            """
        SEGMENTED BY
            hash({{ segment_by_columns }}) ALL NODES
        ;
        """
        )
    else:
        segmented_template = textwrap.dedent(
            """
            UNSEGMENTED ALL NODES
        ;
        """
        )
    template_sql = textwrap.dedent(
        """
        CREATE PROJECTION {{ schema }}.{{ projection_name }} AS 
            SELECT *
        FROM
            {{ schema }}.{{ table }}
        ORDER BY
            {{ order_by_columns }}
    """
    )
    template_sql += segmented_template
    tmpl = Template(template_sql, trim_blocks=True)
    rendered_template = tmpl.render(
        schema=schema,
        table=table,
        projection_name=projection_name,
        order_by_columns=order_by_columns,
        segment_by_columns=segment_by_columns,
    )
    return rendered_template


def make_ddl(
    schema,
    table,
    source_conn,
    colspec,
    squark_metadata,
    convert_arrays_to_string,
    run_live_max_len_queries,
    jdbc_url,
):
    template_sql = textwrap.dedent(
        """
    drop table if exists {{schema}}.{{table}} cascade;
    create table if not exists {{schema}}.{{table}}(
    {% for col in colspec %}
        {{ col.name }} {{ col.ddl() }}{% if not col.nullable %} NOT NULL{% endif %},

    {% endfor %}
        _advana_md5 varchar(35),
        _advana_id int,
        _advana_load_date timestamp
    );
    """
    )

    tmpl = Template(template_sql, trim_blocks=True)

    colspec = map(
        lambda args: ColSpec(
            args[0],
            args[1],
            args[2],
            convert_arrays_to_string,
            run_live_max_len_queries,
            jdbc_url,
        ),
        [(spec, squark_metadata, source_conn) for spec in colspec],
    )

    rendered_template = tmpl.render(schema=schema, table=table, colspec=colspec)
    return rendered_template


def make_ddl_from_target(schema, table, project_id):
    template_sql = textwrap.dedent(
        """
    drop table if exists {{schema}}.{{table}} cascade;
    create table if not exists {{schema}}.{{table}} 
    LIKE  {{project_id}}.{{table}} 
    INCLUDING PROJECTIONS
    ;
    """
    )

    tmpl = Template(template_sql, trim_blocks=True)

    return tmpl.render(schema=schema, table=table, project_id=project_id)


def make_deleted_table_ddl(
    schema_name, base_table_name, pkid_column_name, squark_deleted_table_suffix
):
    template_sql = textwrap.dedent(
        """
    drop table if exists {{schema}}.{{table}}{{deleted_table_suffix}} cascade;
    create table if not exists {{schema}}.{{table}}{{deleted_table_suffix}} (
        {{pkid}} varchar(255)
    );
    """
    )
    tmpl_deleted_table = Template(template_sql, trim_blocks=True)
    return tmpl_deleted_table.render(
        schema=schema_name,
        table=base_table_name,
        deleted_table_suffix=squark_deleted_table_suffix,
        pkid=pkid_column_name,
    )


# squark_metadata_flag = env_vars["SQUARK_METADATA"]
def copy_table_ddl(
    from_conn,
    from_schema,
    from_table,
    to_conn,
    to_schema,
    to_table,
    squark_metadata,
    squark_metadata_flag,
    project_id,
    job_name,
    build_number,
    squark_deleted_table_suffix,
    run_live_max_len_queries,
    convert_arrays_to_string,
    jdbc_url,
    copy_ddl_from_target,
    jenkins_url,
    table_super_projection_settings,
):

    start_time = time.time()

    if copy_ddl_from_target:
        ddl = make_ddl_from_target(
            schema=to_schema, table=to_table, project_id=project_id
        )
    else:
        if squark_metadata_flag:
            ddl_project_key = project_id
            if project_id in ["haven_daily", "haven_weekly", "haven_full", "haven_uw"]:
                ddl_project_key = "haven"
            large_ddl = utils.get_large_data_ddl_def(to_conn, ddl_project_key, to_table)
            squark_metadata["large_ddl"] = large_ddl if large_ddl else dict()

        db_product_name = squark_metadata["conn_metadata"]["db_product_name"]
        is_db2 = db_product_name.lower().startswith("db2")
        if is_db2:
            # as with get_tables(), in db2 apparently we need to fetchmany() w/exact number of columns
            column_count = utils.get_number_of_columns_in_db2_table(
                from_conn, from_schema, from_table
            )
            print(
                "*************** DB2 TABLE COLUMN COUNT: {count}".format(
                    count=column_count
                )
            )
            cols_connection = from_conn.get_columns(
                schema=from_schema, table=from_table
            ).fetchmany(column_count)
            squark_metadata["db2_table_name"] = from_table
        else:
            cols_connection = from_conn.get_columns(
                schema=from_schema, table=from_table
            )

        from_cols = list(cols_connection)

        ddl = make_ddl(
            to_schema,
            to_table,
            from_conn,
            from_cols,
            squark_metadata,
            convert_arrays_to_string,
            run_live_max_len_queries,
            jdbc_url,
        )

        if not is_db2:
            cols_connection.close()

    print("creating table {from_table!r}".format(from_table=from_table))
    print(ddl)
    cur = to_conn.cursor()
    rs = cur.execute(ddl)
    if table_super_projection_settings:
        order_cols = table_super_projection_settings["order_by_columns"].split(",")
        segment_cols = table_super_projection_settings["segment_by_columns"].split(",")

        order_by_columns = ", ".join([utils.sanitize(col) for col in order_cols])
        segment_by_columns = ", ".join([utils.sanitize(col) for col in segment_cols])
        super_projection_query = create_super_projection_query(
            to_schema,
            to_table,
            table_super_projection_settings["projection_name"],
            order_by_columns,
            segment_by_columns,
        )
        print(
            "---- Super projection: {super_projection_query}".format(
                super_projection_query=super_projection_query
            )
        )
        try:
            rs = cur.execute(super_projection_query)
        except Exception as e:
            print("could not make super projection.")
            raise e

    if squark_metadata.get("is_incremental"):
        pkid_column_name = squark_metadata["pkid_column_name"]
        deleted_table_ddl = make_deleted_table_ddl(
            to_schema, to_table, pkid_column_name, squark_deleted_table_suffix
        )

        print(
            "creating table {from_table}{suffix}".format(
                from_table=from_table, suffix=squark_deleted_table_suffix
            )
        )
        print(deleted_table_ddl)
        cur = to_conn.cursor()
        rs = cur.execute(deleted_table_ddl)

    if run_live_max_len_queries:
        time_taken = time.time() - start_time
        update_load_timings_with_ddl_create_duration(
            vertica_conn=to_conn,
            base_table_name=to_table,
            time_taken=time_taken,
            jenkins_url=jenkins_url,
            job_name=job_name,
            build_number=build_number,
            project_id=project_id,
        )


def log_squark_metadata_contents(to_conn, project_id):

    large_ddl_table_name = "squark_config_large_ddl"
    ddl_project_key = project_id
    if project_id in ["haven_daily", "haven_weekly", "haven_full"]:
        ddl_project_key = "haven"
    rs_large_ddl = utils.get_squark_metadata_for_project(
        to_conn, ddl_project_key, large_ddl_table_name
    )
    print(
        '--- SQUARK_METADATA=TRUE, contents of {squark_metadata_table_name} for PROJECT_ID "{project_id}":'.format(
            squark_metadata_table_name=large_ddl_table_name, project_id=ddl_project_key
        )
    )
    if rs_large_ddl:
        column_names = rs_large_ddl[0]._fieldnames
        print("\t".join(column_names))
        print("\t".join("-" * len(name) for name in column_names))
        for row in rs_large_ddl:
            print("\t".join(str(val) for val in (list(row))))
    else:
        print("< NO ROWS RETURNED >")


def update_load_timings_with_ddl_create_duration(
    vertica_conn,
    base_table_name,
    time_taken,
    jenkins_url,
    job_name,
    build_number,
    project_id,
):
    jenkins_name = jenkins_url.split(".")[0].split("/")[-1]
    attempt_count = 1
    source = "n.a."
    # there isn't straightforward way to get total number of tables/views that will get DDL'd before iteration
    total_table_count = 0
    final_table_name = "{}_SQUARK_DDL".format(base_table_name)
    utils.send_load_timing_to_vertica(
        vertica_conn,
        jenkins_name,
        job_name,
        build_number,
        project_id,
        final_table_name,
        time_taken,
        attempt_count,
        source,
        total_table_count,
    )


if __name__ == "__main__":

    env_vars = new_utils.load_env_vars(
        vars_as_is=ENV_VARS_TO_LOAD_AS_IS,
        vars_as_bool=ENV_VARS_TO_LOAD_AS_BOOL,
        vars_with_defaults=ENV_VARS_TO_LOAD_WITH_DEFAULTS,
        vars_to_cast_as_int=ENV_VARS_TO_CAST_TO_INT,
    )

    squarkenv = squark.config.environment.Environment()
    source_jdbc = squarkenv.sources[env_vars["CONNECTION_ID"]]
    destination_vertica = squarkenv.sources[env_vars["VERTICA_CONNECTION_ID"]]
    destination_vertica.url = utils.format_vertica_url(
        destination_vertica.url, env_vars["VERTICA_TRUSTSTOREPATH"]
    )

    from_conn = source_jdbc.conn
    to_conn = destination_vertica.conn

    to_schema = "_{project_id}".format(project_id=env_vars["PROJECT_ID"])

    try:
        jdbc_schema = source_jdbc.default_schema
    except:  # TODO: Make this except statement more targeted
        jdbc_schema = ""

    exclude_schema = new_utils.split_strip_str(env_vars["EXCLUDE_SCHEMA"])
    include_tables = new_utils.split_strip_str(env_vars["INCLUDE_TABLES"])

    json_info = new_utils.parse_json(env_vars["JSON_INFO"])

    tables_with_partition_info = new_utils.get_tables_with_partition_info_from_json(
        json_info
    )
    tsps = new_utils.get_tables_with_super_projection_settings_from_json(json_info)
    tables_with_super_projection_settings = tsps

    if env_vars["SQUARK_METADATA"]:
        log_squark_metadata_contents(to_conn, env_vars["PROJECT_ID"])

    squark_metadata = {}
    conn_metadata = utils.populate_connection_metadata(from_conn._metadata)
    db_product_name = conn_metadata["db_product_name"]
    squark_metadata["conn_metadata"] = conn_metadata

    table_name_key = "table_name"
    if db_product_name.lower().startswith("db2"):
        table_name_key = "name"
        # get_tables().fetchall() or .fetchmany(#) where # > number of tables in schema are both failing via db2
        table_count = utils.get_number_of_tables_in_db2_schema(from_conn, jdbc_schema)
        print(
            "*************** DB2 SCHEMA TABLE COUNT: {count}".format(count=table_count)
        )
        tables = from_conn.get_tables(schema=jdbc_schema).fetchmany(table_count)
    else:
        tables = from_conn.get_tables(schema=jdbc_schema)

    for table in tables:
        table = dict(zip([k.lower() for k in table._fieldnames], table))
        print(
            "Checking table: {table} {table_type}".format(
                table=table[table_name_key], table_type=table["table_type"]
            )
        )
        if table["table_type"] is None:
            print(">>>>> skipping weird None table: {table!r}".format(table=table))
            continue
        if exclude_schema is not None and table["table_schem"] in exclude_schema:
            print(
                ">>>>> skipping table from excluded schema: {table!r}".format(
                    table=table
                )
            )
            continue
        if not env_vars["INCLUDE_VIEWS"] and table["table_type"].upper() != "TABLE":
            print(">>>> skipping non table: %s" % table[table_name_key])
            continue
        # 2018.07.05, similar to pull side, without resources for proper testing below is safest approach
        #   likely would want for any postgresql data sources, for now aiming only for good enough
        if env_vars["PROJECT_ID"].lower().startswith("haven"):
            if env_vars["INCLUDE_VIEWS"] and table["table_type"].upper() not in [
                "TABLE",
                "VIEW",
            ]:
                print(
                    ">>>> INCLUDE_VIEWS is enabled, skipping non table/view: {name}".format(
                        name=table[table_name_key]
                    )
                )
                continue
        table_name = table[table_name_key]

        if include_tables and table_name not in include_tables:
            continue

        super_projection_settings = new_utils.get_super_projection_settings_for_table(
            tables_with_super_projection_settings, table[table_name_key]
        )
        partition_info = new_utils.get_partition_info_for_table(
            tables_with_partition_info, table[table_name_key]
        )

        if partition_info is not None:
            is_incremental = new_utils.str_is_truthy(
                partition_info.get("is_incremental")
            )
            if is_incremental:
                print("--- Haven 'is_incremental' is True")
                squark_metadata["is_incremental"] = True
                squark_metadata["pkid_column_name"] = partition_info["pkid_column_name"]
            else:
                squark_metadata["is_incremental"] = False

        try:
            copy_table_ddl(
                from_conn,
                jdbc_schema,
                table[table_name_key],
                to_conn,
                to_schema,
                utils.sanitize(table_name),
                squark_metadata,
                squark_metadata_flag=env_vars["SQUARK_METADATA"],
                project_id=env_vars["PROJECT_ID"],
                job_name=env_vars["JOB_NAME"],
                build_number=env_vars["BUILD_NUMBER"],
                squark_deleted_table_suffix=env_vars["SQUARK_DELETED_TABLE_SUFFIX"],
                run_live_max_len_queries=env_vars["RUN_LIVE_MAX_LEN_QUERIES"],
                convert_arrays_to_string=env_vars["CONVERT_ARRAYS_TO_STRING"],
                jdbc_url=source_jdbc.url,
                copy_ddl_from_target=env_vars["MAKE_DDL_FROM_TARGET"],
                jenkins_url=env_vars["JENKINS_URL"],
                table_super_projection_settings=super_projection_settings,
            )

        except Exception as exc:
            if env_vars["SKIP_ERRORS"]:
                print(">>>> ERROR COPYING TABLE:")
                print(exc)
                continue
            else:
                raise exc
