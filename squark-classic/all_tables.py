import datetime
import json
import os
import re
import sys
import time
from decimal import Decimal

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import HiveContext, functions as sql_functions
from pyspark.sql.types import ArrayType, TimestampType

import new_utils
import squark.config.environment
import squark.exceptions
import squark.stats
import utils


# Environmental Variables
ENV_VARS_TO_LOAD_AS_IS = [
    "PROJECT_ID",
    "CONNECTION_ID",
    "SQUARK_TYPE",
    "WAREHOUSE_DIR",
    "SQL_TEMPLATE",
]

ENV_VARS_TO_LOAD_AS_BOOL = [
    "SKIP_SOURCE_ROW_COUNT",
    "CHECK_PRIVILEGES",
    "SPARKLOCAL",
    "USE_CLUSTER_EMR",
    "WIDE_COLUMNS_MD5",
    "CONVERT_TIMESTAMPS_TO_AMERICA_NEW_YORK",
]

ENV_VARS_TO_LOAD_WITH_DEFAULTS = [
    ("VERTICA_CONNECTION_ID", None),
    ("BUILD_NUMBER", ""),
    ("JOB_NAME", ""),
    ("SKIP_ERRORS", None),  # TODO: Could this be a load_as_bool?
    ("INCLUDE_VIEWS", None),  # NOTE: This is used as a bool in main()
    ("SKIP_MIN_MAX_ON_CAST", None),
    ("SQUARK_DELETED_TABLE_SUFFIX", "_ADVANA_DELETED"),
    ("JSON_INFO", None),
    ("INCLUDE_TABLES", None),  # special case, split(",") and strip after load
    ("EXCLUDE_TABLES", None),  # special case, split(",") and strip after load
    ("EXCLUDE_SCHEMA", None),  # special case, split(",") and strip after load
    ("WRITE_MODE", "overwrite"),
    ("WRITE_CODEC", "org.apache.hadoop.io.compress.GzipCodec"),
    ("WRITE_FORMAT", "orc"),
    ("SPARKLOCAL_CORE_COUNT", 1),
    ("SQUARK_NUM_RETRY", "1"),  # special case, should cast to int after load
    ("DATACATALOG_TOKEN", None),
    ("DATACATALOG_DOMAIN", None),
    ("USE_HDFS", None),  # TODO: Could this be a load_as_bool?
    ("USE_AWS", None),  # TODO: Could this be a load_as_bool?
    ("S3_CONNECTION_ID", None),
    ("SQUARK_BUCKET", None),
    ("CONVERT_ARRAYS_TO_STRING", None),
]

ENV_VARS_TO_CAST_TO_INT = ["SQUARK_NUM_RETRY"]

# Other constants
SMD_SOURCE_ROW_COUNTS = "source_row_counts"
SMD_SOURCE_ROW_COUNTS_AFTER = "source_row_counts_after"
SMD_CONNECTION_INFO = "connection_info"
SQUARK_METADATA_KEYS = [
    SMD_SOURCE_ROW_COUNTS,
    SMD_SOURCE_ROW_COUNTS_AFTER,
    SMD_CONNECTION_INFO,
]

TABLE_TYPE_TABLE = "TABLE"
TABLE_TYPE_VIEW = "VIEW"
VALID_TABLE_TYPES = [TABLE_TYPE_TABLE, TABLE_TYPE_VIEW]


def print_env_vars(env_vars, source_jdbc, destination_vertica, jdbc_schema):
    print("CONNECTION_ID:", env_vars["CONNECTION_ID"])
    print("JDBC_USER:", source_jdbc.user)
    print("JDBC_URL:", source_jdbc.url)
    print("JDBC_SCHEMA:", jdbc_schema)
    print("CONNECTION_TYPE:", source_jdbc.type)

    print("VERTICA_CONNECTION_ID:", env_vars["VERTICA_CONNECTION_ID"])
    print("VERTICA_JDBC_USER:", destination_vertica.user)
    print("VERTICA_JDBC_URL:", destination_vertica.url)
    print("VERTICA_CONNECTION_TYPE:", destination_vertica.type)

    print("WAREHOUSE_DIR:", env_vars["WAREHOUSE_DIR"])
    print("DATA_DIR:", os.path.join(env_vars["WAREHOUSE_DIR"], env_vars["PROJECT_ID"]))

    return None


def add_md5_column(df, wide_columns_md5):
    # 20171213, AR-268 large number of columns leading to StackOverflow error in AWS/EMR environment
    #   before using on a given table want to confirm limiting to first 400 columns will still result in unique MD5
    if wide_columns_md5:
        return df.withColumn(
            "_advana_md5",
            sql_functions.md5(sql_functions.concat_ws("!@#$", *df.columns[:400])),
        )
    else:
        return df.withColumn(
            "_advana_md5",
            sql_functions.md5(sql_functions.concat_ws("!@#$", *df.columns)),
        )


def add_auto_incr_column(df):
    # return df.withColumn('_advana_id', F.monotonicallyIncreasingId())
    return df.withColumn("_advana_id", sql_functions.monotonically_increasing_id())


def add_load_datetime(df):
    return df.withColumn(
        "_advana_load_date", sql_functions.lit(datetime.datetime.now())
    )


def sanitize_columns(df):
    def sanitize(name):
        return re.sub(r"\W+", "_", name)

    for col in df.schema.names:
        df = df.withColumnRenamed(col, sanitize(col))
    return df


def convert_array_to_string(df):
    sch = df.schema
    cols = [a.name for a in sch.fields if isinstance(a.dataType, ArrayType)]
    for col in cols:
        df = df.withColumn(col, df[col].cast("string"))
    return df


def convert_timestamp_values_to_america_new_york(df):
    sch = df.schema
    cols = [a.name for a in sch.fields if isinstance(a.dataType, TimestampType)]
    for col in cols:
        df = df.withColumn(
            col, sql_functions.to_utc_timestamp(df[col], "America/New_York")
        )
    return df


def log_source_row_count(
    sqlctx,
    table_name,
    properties,
    db_product_name,
    skip_source_row_count,
    jdbc_schema,
    jdbc_url,
):
    count = None
    # 'ase' = Sybase
    handled_db_prefixes = [
        "teradata",
        "postgres",
        "microsoft sql",
        "ase",
        "oracle",
        "db2",
    ]
    db_product_name_lower = db_product_name.lower()
    if (
        any(db_product_name_lower.startswith(db) for db in handled_db_prefixes)
        and not skip_source_row_count
    ):
        if db_product_name_lower.startswith("oracle"):
            sql_query = '(SELECT COUNT(*) as cnt FROM "{table_name}")'.format(
                table_name=table_name
            )
        elif db_product_name_lower.startswith("db2"):
            sql_query = "(SELECT COUNT(*) as cnt FROM {jdbc_schema}.{table_name}) as query".format(
                jdbc_schema=jdbc_schema, table_name=table_name
            )
        else:
            # double-quotes were helping with at least one postgres source, db2 doesn't like them
            sql_query = '(SELECT COUNT(*) as cnt FROM "{table_name}") as query'.format(
                table_name=table_name
            )
        print(
            "--- Executing source row count query: {sql_query}".format(
                sql_query=sql_query, flush=True
            )
        )
        df = sqlctx.read.jdbc(jdbc_url, sql_query, properties=properties)
        count = df.first()[0]
        print("--- SOURCE ROW COUNT: {count}".format(count=count, flush=True))
    else:
        print(
            "--- Skip source row count query, not implemented for: {db_product_name}".format(
                db_product_name=db_product_name, flush=True
            )
        )

    return count


# NOTE: .orc does Julian/Gregorian calendar conversion, Vertica doesn't, 0001-01-01 values become a BC date in Vertica
#   we are going to concentrate on fixing the main offenders - default min date/timestamps are common in the db
#   other dates wouldn't necessarily be 2 days off, complicated conversion involved, 0001-01-03 works here
def post_date_teradata_dates_and_timestamps(df, use_aws):
    post_date_date = sql_functions.to_date(sql_functions.lit("0001-01-03"), format=None)
    post_date_timestamp = sql_functions.to_utc_timestamp(
        sql_functions.lit("0001-01-03 00:00:00"), "GMT+5"
    )

    # NOTE: USE_AWS is loaded as an env with a None default
    #   This may cause unexpected behavior
    if use_aws:
        post_date_timestamp = sql_functions.to_utc_timestamp(
            sql_functions.lit("0001-01-03 05:00:00"), "GMT+5"
        )

    for field in df.schema.fields:
        if field.dataType.typeName().lower() == "date":
            print(
                "------- POST DATE CHECK on : {field_name}".format(
                    field_name=field.name
                ),
                flush=True,
            )

            df = df.withColumn(
                field.name,
                sql_functions.when(
                    df[field.name] == "0001-01-01", post_date_date
                ).otherwise(df[field.name]),
            )
        elif field.dataType.typeName().lower() == "timestamp":
            print(
                "------- POST TIMESTAMP CHECK on : {field_name}".format(
                    field_name=field.name
                ),
                flush=True,
            )

            df = df.withColumn(
                field.name,
                sql_functions.when(
                    df[field.name] == "0001-01-01 00:00:00", post_date_timestamp
                ).otherwise(df[field.name]),
            )
    return df


def conform_any_extreme_decimals(df, skip_min_max_on_cast):
    for field in df.schema.fields:

        # NOTE: This should probaly be set as a default argument or set as a global constant
        vertica_max_precision = 37
        if (
            field.dataType.typeName().lower() == "decimal"
            and field.dataType.precision > vertica_max_precision
        ):
            # value will mangle in Vertica if precision > 37, even if target ddl has correct precision
            print(
                "------- PROBLEM DECIMAL for {field_name}, precision > {vertica_max_precision}, check values before cast".format(
                    field_name=field.name, vertica_max_precision=vertica_max_precision
                ),
                flush=True,
            )
            # set cast scale to that of source unless it was 38, in which case set to 37
            scale_def = (
                field.dataType.scale
                if field.dataType.scale <= vertica_max_precision
                else vertica_max_precision
            )

            # NOTE: SKIP_MIN_MAX_ON_CAST is loaded as an env with a None default
            #   This may cause unexpected behavior
            if skip_min_max_on_cast:
                print(
                    "------- SKIP_MIN_MAX_ON_CAST is set, no the min/max query will be done",
                    flush=True,
                )
            else:
                df_min_max = df.select(
                    [
                        sql_functions.max(field.name).alias("max_val"),
                        sql_functions.min(field.name).alias("min_val"),
                    ]
                )
                row = df_min_max.first()
                min_val = row["min_val"]
                max_val = row["max_val"]
                print(
                    "------- Reported minimum value: {min_val}\n------- Reported maximum value: {max_val}".format(
                        min_val=min_val, max_val=max_val
                    ),
                    flush=True,
                )

                # only going to get min_val = Null if all rows are null
                if min_val is not None:
                    too_big_num = int("9" * vertica_max_precision)
                    if abs(min_val) > too_big_num or abs(max_val) > too_big_num:
                        msg = "Precision of actual numeric data in {} larger than Vertica will handle ({})\n".format(
                            field.name, vertica_max_precision
                        )
                        msg += "Reported minimum value: {min_val}\nReported maximum value: {max_val}".format(
                            min_val=min_val, max_val=max_val
                        )
                        raise OverflowError(msg)

                    # if incoming ddl like (38,8), need to make sure there are no numbers with 30 digits in integral
                    # part else will be forcing value into (37,8),
                    # leaving only room for 9 integral digits, Spark sets value=None
                    spread = vertica_max_precision - field.dataType.scale
                    if spread <= 0:
                        max_at_new_precision = Decimal(
                            "0.{}".format("9" * vertica_max_precision)
                        )
                    else:
                        max_at_new_precision = int("9" * spread)

                    if (
                        abs(min_val) > max_at_new_precision
                        or abs(max_val) > max_at_new_precision
                    ):
                        print(
                            "------- max number ({max_at_new_precision}) at new precision < data min/max value".format(
                                max_at_new_precision=max_at_new_precision
                            ),
                            flush=True,
                        )
                        print(
                            "------- decrease scale from {scale_def} to {scale_def_minus_one}".format(
                                scale_def=scale_def, scale_def_minus_one=scale_def - 1
                            ),
                            flush=True,
                        )
                        scale_def -= 1

            decimal_def = "Decimal({vertica_max_precision},{scale_def})".format(
                vertica_max_precision=vertica_max_precision, scale_def=scale_def
            )

            print(
                "------- column will be cast to: {decimal_def}".format(
                    decimal_def=decimal_def
                ),
                flush=True,
            )
            df = df.withColumn(field.name, df[field.name].cast(decimal_def))

    return df


def save_table(
    sqlctx,
    table_name,
    squark_metadata,
    env_vars,
    source_jdbc,
    jdbc_schema,
    destination_vertica,
    squarkenv,
):

    dbtable = env_vars["SQL_TEMPLATE"] % table_name
    is_incremental = False

    print("********* EXECUTE SQL: {dbtable!r}".format(dbtable=dbtable))
    properties = dict(user=source_jdbc.user, password=source_jdbc.password)

    if env_vars["USE_CLUSTER_EMR"]:
        print("--- USE_CLUSTER_EMR is a go")

        driver_name_for_spark = squark_metadata[SMD_CONNECTION_INFO].get(
            "driver_name_for_spark", ""
        )
        if driver_name_for_spark:
            print(
                "--- ... and setting driver_name_for_spark: {driver_name_for_spark}".format(
                    driver_name_for_spark=driver_name_for_spark
                )
            )
            properties["driver"] = driver_name_for_spark

    db_name = squark_metadata[SMD_CONNECTION_INFO]["db_product_name"]
    db_name_lower = db_name.lower()

    if db_name_lower.startswith("oracle"):
        # NOTE: ORA-00604: error occurred at recursive SQL level 1 -> ORA-01882: timezone region  not found
        properties["oracle.jdbc.timezoneAsRegion"] = "False"

    start_query_time = time.time()
    source_row_count = log_source_row_count(
        sqlctx,
        table_name,
        properties,
        db_name,
        env_vars["SKIP_SOURCE_ROW_COUNT"],
        jdbc_schema,
        source_jdbc.url,
    )
    row_count_query_duration = time.time() - start_query_time
    if source_row_count:
        print(
            "--- BEFORE COUNT QUERY DURATION: {seconds:.0f} seconds = {minutes:.2f} minutes".format(
                seconds=row_count_query_duration, minutes=row_count_query_duration / 60
            ),
            flush=True,
        )
        row_count_info = {
            "count": source_row_count,
            "query_time": datetime.datetime.now(),
            "seconds_query_duration": row_count_query_duration,
        }
        squark_metadata[SMD_SOURCE_ROW_COUNTS][table_name] = row_count_info

    tables_with_subqueries, tables_with_partition_info = {}, {}
    json_info = env_vars["JSON_INFO"]
    if json_info:
        parsed_json = json.loads(json_info.replace("'", '"').replace('"""', "'"))
        if "SAVE_TABLE_SQL_SUBQUERY" in parsed_json.keys():
            tables_with_subqueries = parsed_json["SAVE_TABLE_SQL_SUBQUERY"][
                "table_queries"
            ]
            print(
                "TABLES_WITH_SUBQUERIES: {tables_with_subqueries!r}".format(
                    tables_with_subqueries=tables_with_subqueries
                )
            )
        if "PARTITION_INFO" in parsed_json.keys():
            tables_with_partition_info = parsed_json["PARTITION_INFO"]["tables"]
            print(
                "TABLES_WITH_PARTITION_INFO: {tables_with_partition_info!r}".format(
                    tables_with_partition_info=tables_with_partition_info
                )
            )

    table_name_lower = table_name.lower()
    if tables_with_subqueries and table_name_lower in [
        table.lower() for table in tables_with_subqueries.keys()
    ]:
        table_queries_lower = {k.lower(): v for k, v in tables_with_subqueries.items()}
        sql_query = table_queries_lower[table_name_lower]

        # NOTE: syntax on JDBC subquery differs among source db systems, e.g. Oracle doesn't take an alias on subquery
        print(
            "--- Executing subquery: {sql_query!r}".format(sql_query=sql_query),
            flush=True,
        )
        df = sqlctx.read.jdbc(source_jdbc.url, sql_query, properties=properties)

    elif tables_with_partition_info and table_name_lower in [
        table.lower() for table in tables_with_partition_info.keys()
    ]:
        table_with_partitions_lower = {
            k.lower(): v for k, v in tables_with_partition_info.items()
        }

        partition_info = table_with_partitions_lower[table_name_lower]
        print(
            "--- Partition info: {partition_info!r}".format(
                partition_info=partition_info
            ),
            flush=True,
        )
        partition_column = partition_info["partitionColumn"]
        lower_bound = partition_info["lowerBound"]
        upper_bound = partition_info["upperBound"]
        num_partitions = partition_info["numPartitions"]

        is_incremental = new_utils._str_is_truthy(
            partition_info.get("is_incremental", "").lower()
        )
        if is_incremental:
            # TODO: We should test this some more, update pattern may be partially implemented.
            # NOTE: 2018.07.06, to date incremental solely developed/tested against PROJECT_ID="haven",
            #   the "prod" postgres db
            if not env_vars["PROJECT_ID"].lower().startswith("haven"):
                raise NotImplementedError(
                    "Incremental data pull only implemented for haven job"
                )
            vertica_conn = destination_vertica.conn
            base_schema_name = partition_info["base_schema_name"]
            last_updated_column_name = partition_info["last_updated_column_name"]
            pkid_column_name = partition_info["pkid_column_name"]
            msg = (
                "--- Incremental, parse PARTITION_INFO and get last updated time from "
            )
            print(
                (
                    msg + "{base_schema_name}.{table_name}.{last_updated_column_name}"
                ).format(
                    base_schema_name=base_schema_name,
                    table_name=table_name,
                    last_updated_column_name=last_updated_column_name,
                ),
                flush=True,
            )
            max_last_updated_time = utils.get_haven_max_last_updated_time(
                vertica_conn,
                schema_name=base_schema_name,
                table_name=table_name,
                column_name=last_updated_column_name,
            )
            print(
                "--- Incremental, updated partitionColumn value: {partition_column}".format(
                    partition_column=partition_column
                ),
                flush=True,
            )
            partition_column = "\"{column_name}\" >= '{max_last_updated_time}' AND {orig_partition_column}".format(
                column_name=last_updated_column_name,
                max_last_updated_time=max_last_updated_time,
                orig_partition_column=partition_column,
            )
            print(
                "--- Incremental, updated partitionColumn value: {partition_column}".format(
                    partition_column=partition_column
                ),
                flush=True,
            )

            curr_pk_sql_query = "(SELECT {pkid_column_name} FROM {table_name}) as subquery".format(
                table_name=table_name, pkid_column_name=pkid_column_name
            )
            df_curr = sqlctx.read.jdbc(
                source_jdbc.url, curr_pk_sql_query, properties=properties
            )
            # print('### DEBUG: df_curr.count(): {}'.format(df_curr.count()))

            # get all the _id values that are currently in Vertica's _weekly schema for this table
            vert_pk_sql_query = "(SELECT {pkid_column_name} FROM {schema_name}.{table_name}) as subquery".format(
                pkid_column_name=pkid_column_name,
                schema_name=base_schema_name,
                table_name=table_name,
            )
            vert_properties = dict(
                user=destination_vertica.user, password=destination_vertica.password
            )
            vert_properties["driver"] = "com.vertica.jdbc.Driver"
            df_vertica = sqlctx.read.jdbc(
                destination_vertica.url, vert_pk_sql_query, properties=vert_properties
            )
            # print('### DEBUG: df_vertica.count(): {}'.format(df_vertica.count()))

        lazy_read = sqlctx.read.format("jdbc").options(
            url=source_jdbc.url,
            dbtable=dbtable,
            user=source_jdbc.user,
            password=source_jdbc.password,
            partitionColumn=partition_column,
            lowerBound=lower_bound,
            upperBound=upper_bound,
            numPartitions=num_partitions,
        )

        if db_name_lower.startswith("oracle"):
            lazy_read = lazy_read.option("oracle.jdbc.timezoneAsRegion", "False")

        if "driver" in properties:
            # if no source_row_count, driver_name hasn't been set and need to do so on this connection
            lazy_read = lazy_read.option("driver", properties["driver"])

        df = lazy_read.load()
    else:
        if db_name_lower.startswith("db2"):
            # per documentation, and logic, this is how we should be doing all queries, but would need to test broadly,
            # running every squark job through below, either all connections must have schema or only '.' when present
            df = sqlctx.read.jdbc(
                source_jdbc.url,
                table="{jdbc_schema}.{dbtable}".format(
                    jdbc_schema=jdbc_schema, dbtable=dbtable
                ),
                properties=properties,
            )
        else:
            df = sqlctx.read.jdbc(source_jdbc.url, table=dbtable, properties=properties)

    print(
        "--- Sanitizing columns for {dbtable!r}: {df_schema_names!r}".format(
            dbtable=dbtable, df_schema_names=df.schema.names
        )
    )
    df = sanitize_columns(df)
    print(
        "--- Sanitized columns for {dbtable!r} are {df_schema_names!r}".format(
            dbtable=dbtable, df_schema_names=df.schema.names
        )
    )

    print(
        "--- Conforming DecimalType as necessary for {dbtable!r}: {df_schema_names!r}".format(
            dbtable=dbtable, df_schema_names=df.schema.names
        )
    )

    df = conform_any_extreme_decimals(df, env_vars["SKIP_MIN_MAX_ON_CAST"])

    if env_vars["CONVERT_ARRAYS_TO_STRING"]:
        print(
            "--- Converting array fields to string for {dbtable!r}".format(
                dbtable=dbtable
            )
        )
        df = convert_array_to_string(df)

    if db_name_lower.startswith("teradata"):
        print(
            "--- Post-dating min teradata date/timestamp values for {dbtable!r}".format(
                dbtable=dbtable
            )
        )
        df = post_date_teradata_dates_and_timestamps(df, env_vars["USE_AWS"])

        # only going to test this with Teradata, restrict usage to Teradata
        if env_vars["CONVERT_TIMESTAMPS_TO_AMERICA_NEW_YORK"]:
            print(
                "--- Converting timestamp fields to string for {dbtable!r}".format(
                    dbtable=dbtable
                )
            )
            df = convert_timestamp_values_to_america_new_york(df)

    print("--- Adding md5 column for {dbtable!r}".format(dbtable=dbtable))
    df = add_md5_column(df, env_vars["WIDE_COLUMNS_MD5"])

    print("--- Adding incr column for {dbtable!r}".format(dbtable=dbtable))
    df = add_auto_incr_column(df)

    print("--- Adding date load column for {dbtable!r}".format(dbtable=dbtable))
    df = add_load_datetime(df)

    if env_vars["USE_AWS"] and new_utils.squark_bucket_is_valid(env_vars["SQUARK_BUCKET"]):
        aws_access_key_id, aws_secret_access_key = new_utils.get_aws_credentials_from_squark(
            squarkenv, env_vars["S3_CONNECTION_ID"]
        )
        s3_file_system = "s3a" if env_vars["USE_CLUSTER_EMR"] else "s3n"
        opts = dict(codec=env_vars["WRITE_CODEC"])

        s2 = time.time()

        save_path = (
            "{S3_FILESYSTEM}://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}"
            "@{SQUARK_BUCKET}/{SQUARK_TYPE}/{PROJECT_ID}/{TABLE_NAME}/{TABLE_NAME}.orc/".format(
                S3_FILESYSTEM=s3_file_system,
                AWS_ACCESS_KEY_ID=aws_access_key_id,
                AWS_SECRET_ACCESS_KEY=aws_secret_access_key,
                SQUARK_BUCKET=env_vars["SQUARK_BUCKET"],
                SQUARK_TYPE=env_vars["SQUARK_TYPE"],
                PROJECT_ID=env_vars["PROJECT_ID"],
                TABLE_NAME=table_name,
            )
        )
        print(
            "******* SAVING TABLE TO S3: {path!r}".format(
                path=save_path.replace(aws_secret_access_key, "********")
            )
        )

        print("Attempting to save {table}".format(table=dbtable))
        try:
            df.write.format("orc").options(**opts).save(
                save_path, mode=env_vars["WRITE_MODE"]
            )
        except Exception as e:
            print(
                "!! -- An Error occurred while trying to .save table: {dbtable!r}".format(
                    dbtable=dbtable
                )
            )
            print(str(e))
            raise squark.exceptions.SaveToS3Error("save to S3 failed", e)

        print("Save successful...")
        print("-----------------------------------------")
        e2 = time.time()
        print(
            " ----- Writing to S3 took: {time_delta:0.3f} seconds".format(
                time_delta=e2 - s2
            )
        )

        if is_incremental:
            print(
                "******* IS_INCREMENTAL SAVE {deleted_table_suffix} TABLE TO S3: ********".format(
                    deleted_table_suffix=env_vars["SQUARK_DELETED_TABLE_SUFFIX"]
                )
            )
            # TODO: Assign df_vertica and df_curr more safely, they may be unassigned here
            df_deleted = df_vertica.subtract(df_curr)

            # print('### DEBUG: df_deleted.count(): {}'.format(df_deleted.count()))
            table_name_deleted = "{table}{deleted_table_suffix}".format(
                table=table_name,
                deleted_table_suffix=env_vars["SQUARK_DELETED_TABLE_SUFFIX"],
            )
            s2d = time.time()
            save_path = "{S3_FILESYSTEM}://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}".format(
                S3_FILESYSTEM=s3_file_system,
                AWS_ACCESS_KEY_ID=aws_access_key_id,
                AWS_SECRET_ACCESS_KEY=aws_secret_access_key,
            )
            save_path += "@{SQUARK_BUCKET}/{SQUARK_TYPE}/{PROJECT_ID}/{TABLE_NAME}/{TABLE_NAME}.orc/".format(
                SQUARK_BUCKET=env_vars["SQUARK_BUCKET"],
                SQUARK_TYPE=env_vars["SQUARK_TYPE"],
                PROJECT_ID=env_vars["PROJECT_ID"],
                TABLE_NAME=table_name_deleted,
            )
            print(
                "******* SAVING TABLE TO S3: {aws_secret_access_key!r}".format(
                    aws_secret_access_key=save_path.replace(
                        aws_secret_access_key, "********"
                    )
                )
            )
            print("Attempting to save {table}".format(table=table_name_deleted))
            try:
                df_deleted.write.format("orc").options(**opts).save(
                    save_path, mode=env_vars["WRITE_MODE"]
                )
            except Exception as e:
                print(
                    "!! -- An Error occurred while trying to .save table: {table_name_deleted!r}".format(
                        table_name_deleted=table_name_deleted
                    )
                )
                print(str(e))
                raise squark.exceptions.SaveToS3Error("save to S3 failed", e)

            print("Save successful...")
            print("-----------------------------------------")
            e2d = time.time()
            # in terms of squark timing metadata being saved to Vertica, this time will be folded into parent table's
            print(
                " ----- Writing to S3 took: {time_delta:0.3f} seconds".format(
                    time_delta=e2d - s2d
                )
            )

    if env_vars["USE_HDFS"] or (not env_vars["USE_AWS"] and not env_vars["USE_HDFS"]):
        s3 = time.time()

        data_dir = os.path.join(env_vars["WAREHOUSE_DIR"], env_vars["PROJECT_ID"])
        save_path = "{data_dir}/{table_name}".format(
            data_dir=data_dir, table_name=table_name
        )

        print("******* SAVING TABLE TO HDFS: {save_path!r}".format(save_path=save_path))
        opts = dict(codec=env_vars["WRITE_CODEC"])
        df.write.format(env_vars["WRITE_FORMAT"]).options(**opts).save(
            save_path, mode=env_vars["WRITE_MODE"]
        )
        e3 = time.time()
        print(
            " ----- Writing to HDFS took: {time_delta:0.3f} seconds".format(
                time_delta=e3 - s3
            )
        )

    # NOTE: 2017.11.14, increase duration limit, re doing an AFTER count, from 60 to 90 seconds
    if source_row_count and row_count_query_duration < 90:

        start_query_time = time.time()
        source_row_count = log_source_row_count(
            sqlctx,
            table_name,
            properties,
            db_name,
            env_vars["SKIP_SOURCE_ROW_COUNT"],
            jdbc_schema,
            source_jdbc.url,
        )
        row_count_query_duration = time.time() - start_query_time

        if source_row_count:
            print(
                "--- AFTER COUNT QUERY DURATION: {seconds:.0f} seconds = {minutes:.2f} minutes".format(
                    seconds=row_count_query_duration,
                    minutes=row_count_query_duration / 60,
                ),
                flush=True,
            )
            row_count_info = {
                "count": source_row_count,
                "query_time": datetime.datetime.now(),
                "seconds_query_duration": row_count_query_duration,
            }
            squark_metadata[SMD_SOURCE_ROW_COUNTS_AFTER][table_name] = row_count_info


def main():

    env_vars = new_utils.load_env_vars(
        vars_as_is=ENV_VARS_TO_LOAD_AS_IS,
        vars_as_bool=ENV_VARS_TO_LOAD_AS_BOOL,
        vars_with_defaults=ENV_VARS_TO_LOAD_WITH_DEFAULTS,
        vars_to_cast_as_int=ENV_VARS_TO_CAST_TO_INT,
    )

    squarkenv = squark.config.environment.Environment()
    source_jdbc = squarkenv.sources[env_vars["CONNECTION_ID"]]
    destination_vertica = squarkenv.sources[env_vars["VERTICA_CONNECTION_ID"]]
    # Jdbc ource tables
    try:
        jdbc_schema = source_jdbc.default_schema
    except:  # TODO: Make this except statement more targeted
        jdbc_schema = ""

    # Raise an error if the bucket is invalid
    new_utils.squark_bucket_is_valid(env_vars["SQUARK_BUCKET"])
    print_env_vars(env_vars, source_jdbc, destination_vertica, jdbc_schema)

    try:

        # Setup SparkConf
        conf = SparkConf()
        if not env_vars["USE_CLUSTER_EMR"]:
            conf.set("spark.local.dir", "/hadoop/sparklocal")
        if env_vars["SPARKLOCAL"]:
            conf.set(
                "spark.master",
                "local[{sparklocal_core_count}]".format(
                    sparklocal_core_count=env_vars["SPARKLOCAL_CORE_COUNT"]
                ),
            )

        # Setup SparkContext
        spark_job_name = "{project_id}-squark-all-tables".format(
            project_id=env_vars["PROJECT_ID"]
        )
        sc = SparkContext(appName=spark_job_name, conf=conf)

        # Setup HiveContext
        sqlctx = HiveContext(sc)

        # Squrk metadata
        squark_metadata = {k: {} for k in SQUARK_METADATA_KEYS}
        conn_metadata = utils.populate_connection_metadata(source_jdbc.conn._metadata)
        squark_metadata[SMD_CONNECTION_INFO] = conn_metadata
        metadata_formatted = "\n\t".join(
            ("{k}: {v}".format(k=k, v=v) for k, v in sorted(conn_metadata.items()))
        )
        print(
            "*************** JDBC connection metadata:\n\t{metadata_formatted}".format(
                metadata_formatted=metadata_formatted
            )
        )

        table_name_key = "table_name"
        db_product_name = squark_metadata[SMD_CONNECTION_INFO]["db_product_name"]

        if db_product_name.lower().startswith("db2"):
            table_name_key = "name"

            table_count = utils.get_number_of_tables_in_db2_schema(
                source_jdbc.conn, jdbc_schema
            )
            print(
                "*************** DB2 SCHEMA TABLE COUNT: {table_count}".format(
                    table_count=table_count
                )
            )
            tables = source_jdbc.conn.get_tables(schema=jdbc_schema).fetchmany(
                table_count
            )
        else:
            tables = source_jdbc.conn.get_tables(schema=jdbc_schema).fetchall()

        tables = [{k.lower(): v for (k, v) in x._asdict().items()} for x in tables]
        tables = [x for x in tables if x["table_type"] in VALID_TABLE_TYPES]

        if env_vars["EXCLUDE_SCHEMA"] is not None:
            exclude_schema = new_utils.split_strip_str(env_vars["EXCLUDE_SCHEMA"])
            tables = [x for x in tables if x["table_schem"] not in exclude_schema]

        include_tables = new_utils.split_strip_str(env_vars["INCLUDE_TABLES"])
        exclude_tables = new_utils.split_strip_str(env_vars["EXCLUDE_TABLES"])

        print("*************** TABLES: {tables!r}".format(tables=list(tables)))
        processed_tables = []
        table_timing = []
        for table in tables:

            s1 = time.time()

            if env_vars["INCLUDE_TABLES"] is not None:
                if table[table_name_key] not in include_tables:
                    print(
                        "*******SKIPPING NOT INCLUDED TABLE: {table!r}".format(
                            table=table
                        )
                    )
                    continue

            if env_vars["EXCLUDE_TABLES"] is not None:
                if table[table_name_key] in exclude_tables:
                    print(
                        "*******SKIPPING EXCLUDE_TABLES TABLE: {table!r}".format(
                            table=table
                        )
                    )
                    continue

            # Skip indexes and stuff
            if table["table_type"] not in VALID_TABLE_TYPES:
                print(
                    "**********SKIPPING NON-TABLE/VIEW, table_type: {table_type}".format(
                        table_type=table["table_type"]
                    )
                )
                continue

            # NOTE: 2018.06.12 apparently INCLUDE_VIEWS never implemented on the pull side, i.e. in this file
            #   w/o any resources to test wider impact need to add yet another special-case for haven

            if env_vars["PROJECT_ID"].lower().startswith("haven"):

                # NOTE: INCLUDE_VIEWS is loaded as an env with a None default
                #   This may cause unexpected behavior (not None is always True)
                if (
                    not env_vars["INCLUDE_VIEWS"]
                    and table["table_type"] == TABLE_TYPE_VIEW
                ):
                    print("**********SKIPPING VIEW, view: {table}".format(table=table))
                    continue

            # NOTE: SKIP_ERRORS is loaded as an env with a None default
            #   This may cause unexpected behavior (not None is always True)
            if env_vars["SKIP_ERRORS"]:
                try:
                    save_table(
                        sqlctx,
                        table[table_name_key],
                        squark_metadata,
                        env_vars,
                        source_jdbc,
                        jdbc_schema,
                        destination_vertica,
                        squarkenv,
                    )
                except Exception as exc:
                    print(exc)
                    try:
                        os.mkdir("err")
                    except:  # NOTE: This bare and empty except may cause unexpected bahavior
                        pass
                    with open(
                        "err/{table}".format(table=table[table_name_key]), "w"
                    ) as f:
                        f.write(str(exc))
            else:
                # Moving retries for aws s3 saves to outside of save_table(), only retry if the df.write() fails
                print("-----------------------------------------")
                curr_retry = 0
                retry_bool = True
                while retry_bool and curr_retry < env_vars["SQUARK_NUM_RETRY"]:
                    print(
                        "Attempting to save {table}: [Attempt {curr}/{tot}]".format(
                            table=table[table_name_key],
                            curr=curr_retry + 1,
                            tot=env_vars["SQUARK_NUM_RETRY"],
                        )
                    )
                    try:
                        save_table(
                            sqlctx,
                            table[table_name_key],
                            squark_metadata,
                            env_vars,
                            source_jdbc,
                            jdbc_schema,
                            destination_vertica,
                            squarkenv,
                        )
                        retry_bool = False
                    except squark.exceptions.SaveToS3Error as e:
                        print(
                            "!! -- An Error occurred during save_table(): {table!r}".format(
                                table=(table[table_name_key])
                            )
                        )
                        print(str(e))
                        print(
                            "Taking a quick 5 second nap and restarting the save for this table..."
                        )
                        curr_retry += 1
                        time.sleep(5)
                    except Exception as e:
                        print(
                            "!! -- Exiting after unknown error occurred during save_table(): {table!r}".format(
                                table=table[table_name_key]
                            )
                        )
                        print(str(e))
                        raise
                if retry_bool:
                    print("ERROR! Number of retries exceeded!! Exiting...")
                    raise

            processed_tables.append(table[table_name_key])
            table_time = time.time() - s1
            print(
                " ------- Total Time for Table {table}: {table_time:0.3f} seconds".format(
                    table=table, table_time=table_time
                )
            )
            table_timing.append([table[table_name_key], table_time])

    except Exception as e:
        error_message = str(e)

        # TODO: Check to see if this % should be here, seems vestigal
        print("Error occurred: \n{error_message}".format(error_message=error_message))

        sys.exit(1)

    # Print timing results at end of the script:
    print("===============================================")
    print("Total times for each table:")
    print("-----------------------------------------------")
    print(
        "\n".join(
            [
                " - {table}: {table_time:0.3f} seconds".format(
                    table=x[0], table_time=x[1]
                )
                for x in table_timing
            ]
        )
    )
    print("===============================================")
    # Send the table timings to vertica
    vertica_conn = destination_vertica.conn
    utils.send_table_timings_to_vertica(
        vertica_conn,
        env_vars["PROJECT_ID"],
        table_timing,
        env_vars["BUILD_NUMBER"],
        env_vars["JOB_NAME"],
    )

    source_row_counts_before = squark_metadata[SMD_SOURCE_ROW_COUNTS]
    if source_row_counts_before:

        print("Source row counts for each table:")
        print("--BEFORE PULL--------------------------------------------")
        print(" - table\tcount\tas of\tquery duration")

        row_counts = []
        for table in sorted(source_row_counts_before):

            count = source_row_counts_before[table]["count"]
            as_of = source_row_counts_before[table]["query_time"]
            query_duration = source_row_counts_before[table]["seconds_query_duration"]
            is_after = 0

            print(
                " - {table}\t{count}\t{as_of}\t{query_duration}".format(
                    table=table, count=count, as_of=as_of, query_duration=query_duration
                )
            )
            row_counts.append([table, count, as_of, query_duration, is_after])

        utils.send_source_row_counts_to_vertica(
            vertica_conn,
            env_vars["PROJECT_ID"],
            jdbc_schema,
            row_counts,
            env_vars["BUILD_NUMBER"],
            env_vars["JOB_NAME"],
        )

    source_row_counts_after = squark_metadata[SMD_SOURCE_ROW_COUNTS_AFTER]

    # NOTE: WHy is this an if? source_row_counts_after always exists (its a constant value)
    if source_row_counts_after:
        print("--AFTER PULL---------------------------------------------")
        print(" - table\tcount\tas of\tquery duration")

        row_counts = []
        for table in sorted(source_row_counts_after):
            count = source_row_counts_after[table]["count"]
            as_of = source_row_counts_after[table]["query_time"]
            query_duration = source_row_counts_after[table]["seconds_query_duration"]
            is_after = 1

            print(
                " - {table}\t{count}\t{as_of}\t{query_duration}".format(
                    table=table, count=count, as_of=as_of, query_duration=query_duration
                )
            )
            row_counts.append([table, count, as_of, query_duration, is_after])

        utils.send_source_row_counts_to_vertica(
            vertica_conn,
            env_vars["PROJECT_ID"],
            jdbc_schema,
            row_counts,
            env_vars["BUILD_NUMBER"],
            env_vars["JOB_NAME"],
        )
        print("===============================================")


if __name__ == "__main__":

    main()
