import datetime
import os
import re
import sys
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

from jinja2 import Template
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import Column, DataFrame, HiveContext, SQLContext
from pyspark.sql import functions as sql_functions
from pyspark.sql.types import (
    ArrayType,
    StructType,
    TimestampType,
    StructField,
    DecimalType,
    DataType,
)

import new_utils
import squark.config.environment
import squark.exceptions
import squark.stats
import utils

VERTICA_MAX_DECIMAL_PRECISION = 37

HANDLED_DB_PREFIXES = (
    "ase",  # 'ase' = Sybase
    "db2",
    "microsoft sql",
    "oracle",
    "postgres",
    "teradata",
)


# Environmental Variables
ENV_VARS_TO_LOAD_AS_IS = [
    "PROJECT_ID",
    "CONNECTION_ID",
    "SQUARK_TYPE",
    "WAREHOUSE_DIR",
    "SQL_TEMPLATE",
    "VERTICA_TRUSTSTOREPATH",
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
    ("USE_HDFS", False),
    ("USE_AWS", True),
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
    print("JDBC_URL:", source_jdbc.url.split(";")[0])
    print("JDBC_SCHEMA:", jdbc_schema)
    print("CONNECTION_TYPE:", source_jdbc.type)

    print("VERTICA_CONNECTION_ID:", env_vars["VERTICA_CONNECTION_ID"])
    print("VERTICA_JDBC_USER:", destination_vertica.user)
    print("VERTICA_JDBC_URL:", destination_vertica.url)
    print("VERTICA_CONNECTION_TYPE:", destination_vertica.type)

    print("WAREHOUSE_DIR:", env_vars["WAREHOUSE_DIR"])
    print("DATA_DIR:", os.path.join(env_vars["WAREHOUSE_DIR"], env_vars["PROJECT_ID"]))

    return None


def add_md5_column(df: DataFrame, wide_columns_md5: bool = False) -> DataFrame:
    """Add a MD5 checksum column to DataFrame.

    2017.12.13 AR-268
    Very wide set of columns leading to StackOverflow error in AWS/EMR environment.
    Limiting MD5 to first 400 columns removes the error.

    Before using wide_columns_md5=True on a given table confirm that limiting
     to first 400 columns will still result in a unique MD5.

    :param df: DataFrame to add Checksum column to
    :param wide_columns_md5: If true, only concat the first 400 columns.
    :return: DataFrame
    """
    if wide_columns_md5:
        cutoff = 400
        df = df.withColumn("_advana_md5", _get_checksum_column_for_df(df, cutoff))
    else:
        df = df.withColumn("_advana_md5", _get_checksum_column_for_df(df))
    return df


def _concatenate_dataframe_columns(
    df: DataFrame, cutoff: Optional[int] = None, separator: str = "!@#$"
) -> Column:
    """Concatenate Dataframe columns using separator.

    :param df: DataFrame to use
    :param cutoff: If not None, will only concat columns up to this index
    :param separator: Separator to use when concating
    :return: Column of concatted columns
    """
    column = sql_functions.concat_ws(separator, *df.columns[:cutoff])
    return column


def _hash_column_with_md5(col: Column) -> Column:
    """Get MD5 hash for a column.

    :param col: Column to hash
    :return: Hashed column
    """
    column = sql_functions.md5(col)
    return column


def _get_checksum_column_for_df(df: DataFrame, cutoff: Optional[int] = None) -> Column:
    """Get a checksum column for DataFrame.

    :param: df DataFrame to get checksum for
    :return: Column
    """
    concatenated_column = _concatenate_dataframe_columns(df, cutoff)
    hashed_column = _hash_column_with_md5(concatenated_column)
    return hashed_column


def add_auto_incr_column(df: DataFrame) -> DataFrame:
    """Add a monotonically increasing ID column to DataFrame.

    :param df: DataFrame to add ID column to
    :return: DataFrame
    """
    return df.withColumn("_advana_id", _monotonically_increasing_id_column())


def _monotonically_increasing_id_column() -> Column:
    """Get monotonically increasing ID column.

    :return: ID Column
    """
    return sql_functions.monotonically_increasing_id()


def add_load_datetime(df: DataFrame) -> DataFrame:
    """Add a datetime column to DataFrame.

    :param df: DataFrame to add datetime column to
    :return: DataFrame with datetime column
    """
    return df.withColumn("_advana_load_date", _column_of_current_datetime())


def _column_of_current_datetime() -> Column:
    """Get column containing current datetime.

    :return: Column
    """
    return sql_functions.lit(datetime.datetime.now())


def sanitize_columns(df: DataFrame) -> DataFrame:
    """Sanitize column names.

    :param df: DataFrame with columns to sanitize
    :return: DataFrame
    """
    for column_name in df.schema.names:
        df = df.withColumnRenamed(column_name, _sanitize(column_name))
    return df


def _sanitize(word: str) -> str:
    """Replace non-alphanumerics in word with underscore.

    :param word: string to sanitize
    """
    return re.sub(r"\W+", "_", word)


def convert_array_to_string(df: DataFrame) -> DataFrame:
    """Convert array-type columns in DataFrame to string type.

    Casts all values to string and concats together.

    :param df: DataFrame with columns to convert
    :return: DataFrame
    """
    cols = _get_array_type_column_names(df)
    df = _cast_columns_to_string(df, cols)
    return df


def _get_array_type_column_names(df: DataFrame) -> List[str]:
    """Get column names for columns that are type array.

    :param df: DataFrame to check
    :return: List of column names
    """
    return _get_columns_from_dataframe_schema_of_datatype(df.schema, ArrayType)


def _cast_columns_to_string(df: DataFrame, columns: List[str]) -> DataFrame:
    """Cast columns to string-type in DataFrame.

    :param df: DataFrame with columns to cast.
    :param columns: Names of columns to cast.
    :return: DataFrame with cast columns
    """
    for col in columns:
        df = df.withColumn(col, _cast_dataframe_column_to_type(df[col], "string"))
    return df


def _cast_dataframe_column_to_type(col: Column, column_type: str) -> Column:
    """Cast DataFrame column to column_type (string, decimal, etc).

    :param col: Column to cast
    :param column_type: String representation of type to cast to.
    :return: Cast Column
    """
    return col.cast(column_type)


def convert_timestamp_values_to_america_new_york(df: DataFrame) -> DataFrame:
    """Get DataFrame with timestamp columns converted  to UTC NY timestamp.

    :param df: DataFrame with columns to convert
    :return: DataFrame with converted columns
    """
    cols = _get_columns_from_dataframe_schema_of_datatype(df.schema, TimestampType)
    df = _convert_columns_to_utc_new_york(df, cols)
    return df


def _convert_dataframe_timestamp_column_to_utc_tz(col: Column, tz: str) -> Column:
    """Convert DataFrame timestamp column to UTC timestamp.

    See spark to_utc_timestamp() docs!

    :param col: Column to convert
    :param tz: String representation of timezome to shift to.
    :return: Converted Column
    """
    return sql_functions.to_utc_timestamp(col, tz)


def _convert_dataframe_timestamp_column_to_utc_new_york(col: Column) -> Column:
    """Convert DataFrame timestamp column to UTC New York timestamp.

    See spark to_utc_timestamp() docs!

    :param col: Column to convert
    :return: Converted Column
    """
    return _convert_dataframe_timestamp_column_to_utc_tz(col, "America/New_York")


def _convert_columns_to_utc_new_york(df: DataFrame, columns: List[str]) -> DataFrame:
    """Convert columns to UTC New York in DataFrame.

    :param df: DataFrame with columns to convert.
    :param columns: Names of columns to convert.
    :return: DataFrame with converted columns.
    """
    for col in columns:
        df = df.withColumn(
            col, _convert_dataframe_timestamp_column_to_utc_new_york(df[col])
        )
    return df


def _get_timestamp_type_column_names(df: DataFrame) -> List[str]:
    """Get column names for columns that are type TimestampType.

    :param df: DataFrame to check
    :return: List of column names
    """
    return _get_columns_from_dataframe_schema_of_datatype(df.schema, TimestampType)


def _get_columns_from_dataframe_schema_of_datatype(
    schema: StructType, data_type: Any
) -> List[str]:
    """Get column names for columns that are type data_type.

    :param schema: Schema to check
    :param data_type: A valid Spark column-dataType
    :return: List of column names
    """
    return [a.name for a in schema.fields if isinstance(a.dataType, data_type)]


def log_source_row_count(
    sqlctx: SQLContext,
    table_name: str,
    properties: Dict,
    db_product_name: str,
    skip_source_row_count: bool,
    jdbc_schema: str,
    jdbc_url: str,
) -> Optional[int]:
    """Log the row count of table and return it.

    :param sqlctx: The active SQLContext
    :param table_name: The table to get the count for
    :param properties:  a dictionary of JDBC database connection arguments.
    :param db_product_name: The database type name (teradata, oracle, etc)
    :param skip_source_row_count: Skip count if true.
    :param jdbc_schema: The schema where the table exists.
    :param jdbc_url: JDBC URL
    :return: The row count or None
    """
    count = None
    if all(
        [_database_is_supported_by_squark(db_product_name), not skip_source_row_count]
    ):
        count = _get_row_count_for_jdbc_table(
            sqlctx, table_name, properties, db_product_name, jdbc_schema, jdbc_url
        )
        print("--- SOURCE ROW COUNT: " + str(count))
    else:
        print(
            "--- Skip source row count query. "
            "Not implemented for: " + db_product_name
        )
    return count


def _database_is_supported_by_squark(db_name: str) -> bool:
    """Determine if database is one of the squark supported types.

    See HANDLED_DB_PREFIXES
    :param db_name: Database name to check
    :return: True if supported
    """
    return any(db_name.lower().startswith(db) for db in HANDLED_DB_PREFIXES)


def _database_name_startswith_prefix(db_name: str, prefix: str) -> bool:
    """Check if database name starts with prefix.

    :param db_name: Database name to check
    :return: True if starts with prefix
    """
    return db_name.lower().startswith(prefix)


def _database_is_oracle(db_name: str) -> bool:
    """Database is Oracle if db_name starts with oracle.

    :param db_name: Database name to check
    :return: True if Oracle
    """
    return _database_name_startswith_prefix(db_name, "oracle")


def _database_is_db2(db_name: str) -> bool:
    """Database is DB2 if db_name starts with db2.

    :param db_name: Database name to check
    :return: True if DB2
    """
    return _database_name_startswith_prefix(db_name, "db2")


def _get_oracle_row_count_sql_query(table_name: str) -> str:
    """Get row count sql query for Oracle db.

    :param table_name: Name of table to get query for
    :return: SQL query
    """
    raw_template = '(SELECT COUNT(*) as cnt FROM "{{ table_name }}")'
    template = Template(raw_template)
    sql_query = template.render(table_name=table_name)
    return sql_query


def _get_db2_row_count_sql_query(jdbc_schema: str, table_name: str) -> str:
    """Get row count sql query for DB2 db.

    :param jdbc_schema: Name of schema where table exists
    :param table_name: Name of table to get query for
    :return: SQL query
    """
    raw_template = (
        "(SELECT COUNT(*) as cnt FROM" " {{ jdbc_schema }}.{{ table_name }}) as query"
    )
    template = Template(raw_template)
    sql_query = template.render(jdbc_schema, table_name=table_name)
    return sql_query


def _get_generic_row_count_sql_query(table_name: str) -> str:
    """Get row count sql query for generic db.

    Unknown if this works for all DB types.

    :param table_name: Name of table to get query for
    :return: SQL query
    """
    raw_template = '(SELECT COUNT(*) as cnt FROM "{{ table_name }}") as query'
    template = Template(raw_template)
    sql_query = template.render(table_name=table_name)
    return sql_query


def _get_row_count_query(
    db_product_name: str, jdbc_schema: str, table_name: str
) -> str:
    if _database_is_oracle(db_product_name):
        sql_query = _get_oracle_row_count_sql_query(table_name)
    elif _database_is_db2(db_product_name):
        sql_query = _get_db2_row_count_sql_query(jdbc_schema, table_name)
    else:
        sql_query = _get_generic_row_count_sql_query(table_name)
    return sql_query


def _get_query_results_from_jdbc(
    sqlctx: SQLContext, jdbc_url: str, sql_query: str, properties: Dict
) -> DataFrame:
    """Get results of SQL Query from JDBC.

    :param sqlctx: The active SQLContext
    :param jdbc_url: JDBC URL
    :param sql_query: Query to execute in JDBC
    :param properties: a dictionary of JDBC database connection arguments.
    :return: DataFrame of query results
    """
    df = sqlctx.read.jdbc(jdbc_url, sql_query, properties=properties)
    return df


def _get_row_count_for_jdbc_table(
    sqlctx: SQLContext,
    table_name: str,
    properties: Dict,
    db_product_name: str,
    jdbc_schema: str,
    jdbc_url: str,
) -> int:
    """Get the row count of the JDBC table.

    :param sqlctx: The active SQLContext
    :param table_name: The table to get the count for
    :param properties:  a dictionary of JDBC database connection arguments.
    :param db_product_name: The database type name (teradata, oracle, etc)
    :param jdbc_schema: The schema where the table exists.
    :param jdbc_url: JDBC URL
    :return: The row count
    """
    sql_query = _get_row_count_query(db_product_name, jdbc_schema, table_name)
    print("--- Executing source row count query: " + sql_query)
    df = _get_query_results_from_jdbc(sqlctx, jdbc_url, sql_query, properties)
    count = df.first()[0]
    return count


def post_date_teradata_dates_and_timestamps(df: DataFrame, use_aws: bool) -> DataFrame:
    """Post date Teradata date and timestamp fields.

    .orc does Julian/Gregorian calendar conversion, Vertica doesn't.
    0001-01-01 values become a BC date in Vertica.
    We are going to concentrate on fixing the main offenders...
    Default min date/timestamps are common in the db,
     other dates wouldn't necessarily be 2 days off.
    Complicated conversion involved, 0001-01-03 works here

    :param df: DataFrame to post date fields
    :param use_aws: Set to true if running in AWS
    :return: DataFrame with post dated fields
    """
    if use_aws:
        # Note: We only use AWS so why bother checking?
        #  also, investigate why there is a 5 hour offset.
        time_stamp_col = _get_literal_column("0001-01-03 05:00:00")
    else:
        time_stamp_col = _get_literal_column("0001-01-03 00:00:00")

    post_date_timestamp = _convert_dataframe_timestamp_column_to_utc_tz(
        time_stamp_col, "GMT+5"
    )
    date_col = _get_literal_column("0001-01-03")
    post_date_date = _convert_string_column_to_timestamp(date_col)

    for field in df.schema.fields:
        if _dataframe_field_is_date_field(field):
            print("--- POST DATE CHECK on : " + field.name)
            df = _replace_dataframe_column_value_with_other_value(
                df, field.name, "0001-01-01", post_date_date
            )

        elif _dataframe_field_is_timestamp_field(field):
            print("--- POST TIMESTAMP CHECK on : " + field.name)
            df = _replace_dataframe_column_value_with_other_value(
                df, field.name, "0001-01-01 00:00:00", post_date_timestamp
            )
    return df


def _get_literal_column(value: Any) -> Column:
    """Get column of literal value.

    :param value: Value to populate column with
    :return: Column of value
    """
    return sql_functions.lit(value)


def _convert_string_column_to_timestamp(
    col: Column, fmt: Optional[str] = None
) -> Column:
    """Convert column of StringType to column of TimeStampType

    :param col: Column to convert
    :param fmt: date format (see spark to_date() docs)
    :return: Converted column
    """
    return sql_functions.to_date(col, format=fmt)


def _dataframe_field_is_data_type(field: StructField, data_type: str) -> bool:
    """Determine if field is a data_type.

    :param field: Field to check
    :return True if data_type
    """
    return field.dataType.typeName() == data_type


def _dataframe_field_is_date_field(field: StructField) -> bool:
    """Determine if field is a DateType.

    :param field: Field to check
    :return True if DateType
    """
    return _dataframe_field_is_data_type(field, "date")


def _dataframe_field_is_timestamp_field(field: StructField) -> bool:
    """Determine if field is a TimeStampe.

    :param field: Field to check
    :return True if DateType
    """
    return _dataframe_field_is_data_type(field, "timestamp")


def _replace_dataframe_column_value_with_other_value(
    df: DataFrame, col_name: str, value: Any, other_value: Column
):
    """Replace value in column col_name in DataFarme with values in other_value.

    :param df: DataFrame with column
    :param col_name: Name of column to parse
    :param value: Value to replace
    :param other_value: Column of a literal value
    :return: Column with values replaced
    """
    df = df.withColumn(
        col_name,
        sql_functions.when(df[col_name] == value, other_value).otherwise(df[col_name]),
    )
    return df


def conform_any_extreme_decimals(
    df: DataFrame, skip_min_max_on_cast: bool
) -> DataFrame:
    """Conform extreme decimals for Vetica requirements.

    :param df: DataFrame with values to conform
    :param skip_min_max_on_cast: Skip min/max during cast, or not
    :return: conformed DataFrame
    """
    for field in df.schema.fields:
        data_type = field.dataType
        if not isinstance(data_type, DecimalType):
            continue
        else:
            df = _conform_extreme_decimal_in_dataframe_column(
                df, field.name, data_type, skip_min_max_on_cast
            )
    return df


def _conform_extreme_decimal_in_dataframe_column(
    df: DataFrame, col_name: str, data_type: DecimalType, skip_min_max_on_cast: bool
):
    """Conform exteme decimals in col_name

    :param df:  DataFrame with values to conform
    :param col_name: Name of column to conform
    :param data_type: DecimalType of column
    :param skip_min_max_on_cast: Skip min/max during cast, or not
    :return: DataFrame with conformed column
    """
    if data_type.precision <= VERTICA_MAX_DECIMAL_PRECISION:
        return df
    else:
        print(
            (
                "--- PROBLEM DECIMAL for {name}, precision > {_max}, "
                "check values before cast"
            ).format(name=col_name, _max=VERTICA_MAX_DECIMAL_PRECISION)
        )

        scale_def = _get_decimal_scale_def(data_type.scale)

        if skip_min_max_on_cast:
            print("--- SKIP_MIN_MAX_ON_CAST is set. No min/max query will be done.")
        else:
            scale_def = _get_new_scale_def_for_field(df, col_name, data_type, scale_def)

        decimal_def = "Decimal({vertica_max_precision},{scale_def})".format(
            vertica_max_precision=VERTICA_MAX_DECIMAL_PRECISION, scale_def=scale_def
        )

        print(
            "--- Column will be cast to: {decimal_def}".format(decimal_def=decimal_def)
        )
        new_column = _cast_dataframe_column_to_type(df[col_name], decimal_def)
        df = df.withColumn(col_name, new_column)
    return df


def _get_decimal_scale_def(scale: int) -> int:
    """Get the scale def.

    From the Spark Docs:
    The DecimalType must have fixed precision (the maximum total number of digits)
    and scale (the number of digits on the right of dot). For example, (5, 2) can
    support the value from [-999.99 to 999.99].

    The precision can be up to 38, the scale must be less or equal to precision.

    :param scale: Decimal scale value.
    :return: Source scale or max_precision
    """
    scale_def = (
        scale
        if scale <= VERTICA_MAX_DECIMAL_PRECISION
        else VERTICA_MAX_DECIMAL_PRECISION
    )
    return scale_def


def _get_min_max_for_column_in_df(df: DataFrame, col_name: str) -> Tuple[Any, Any]:
    """Get the min and max value for column in DataFrmae.

    :param df: DataFrame with column to check
    :param col_name: Column to check
    :return: Tuple of min, max
    """
    df = df.select(
        [
            sql_functions.max(col_name).alias("max_val"),
            sql_functions.min(col_name).alias("min_val"),
        ]
    )
    row = df.first()
    min_val = row["min_val"]
    max_val = row["max_val"]

    return min_val, max_val


def _raise_if_value_out_of_range(col_name: str, min_val: float, max_val: float) -> None:
    """Raise OverflowError is value out of range for Vertica.

    :param col_name: Column name, for message.
    :param min_val: Min value in column
    :param max_val: Max value in column
    :return: None
    :raise ValueError: if value of of range
    """

    # 99999999999999999999999999999999999999
    too_big_num = int("9" * VERTICA_MAX_DECIMAL_PRECISION)
    if abs(min_val) > too_big_num or abs(max_val) > too_big_num:
        msg = (
            "Precision of actual numeric data in {name} "
            "larger than Vertica will handle ({_max}).\n"
            "Reported minimum value: {min_val}. "
            "Reported maximum value: {max_val}."
        ).format(
            name=col_name,
            _max=VERTICA_MAX_DECIMAL_PRECISION,
            min_val=min_val,
            max_val=max_val,
        )
        raise OverflowError(msg)
    return None


def _get_max_value_at_new_precision(scale: int) -> Union[Decimal, int]:
    """Get max column value at the new colume precision, determined via scale.

    If incoming ddl like (38,8), need to make sure there are no numbers
     with 30 digits in integral part, else will be forcing value into (37,8),
     leaving only room for 9 integral digits, Spark sets value=None.

    :param scale: Current scale
    :return: Decimal or int version of the max value for the new preciesion
    """

    spread = VERTICA_MAX_DECIMAL_PRECISION - scale
    if spread <= 0:
        return Decimal("0." + ("9" * VERTICA_MAX_DECIMAL_PRECISION))
    else:
        return int("9" * spread)


def _get_new_scale_def_for_field(
    df: DataFrame, col_name: str, data_type: DecimalType, scale_def
) -> int:
    """

    :param df: DataFrame with column to get new scale def
    :param col_name: Name of columnto get new scale def for
    :param data_type: DataType for column
    :param scale_def: Current scale def
    :return:
    """
    min_val, max_val = _get_min_max_for_column_in_df(df, col_name)  # type: float, float

    print("--- Reported minimum value: {min_val}".format(min_val=min_val))
    print("--- Reported maximum value: {max_val}".format(max_val=max_val))

    # only going to get min_val = Null if all rows are null
    if min_val is None:
        return scale_def

    else:
        _raise_if_value_out_of_range(col_name, min_val, max_val)
        max_at_new_precision = _get_max_value_at_new_precision(data_type.scale)

        if abs(min_val) > max_at_new_precision or abs(max_val) > max_at_new_precision:
            print(
                (
                    "--- Max number ({max_at_new_precision}) at new "
                    "precision < data min/max value"
                ).format(max_at_new_precision=max_at_new_precision)
            )
            print(
                (
                    "--- Decreasing scale from {scale_def} " "to {scale_def_minus_one}"
                ).format(scale_def=scale_def, scale_def_minus_one=scale_def - 1)
            )
            scale_def -= 1
        return scale_def


def save_table(
    sqlctx,
    table_name,
    squark_metadata,
    env_vars,
    source_jdbc,
    jdbc_schema,
    destination_vertica,
    squarkenv,
    sql_query: Optional[str] = None,
    partition_info: Optional[Dict] = None,
    incremental_info: Optional[Dict] = None,
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

    if sql_query is not None:
        df = sqlctx.read.jdbc(source_jdbc.url, sql_query, properties=properties)
    
    elif incremental_info is not None:
        
        print("Preparing spark data frame for Table with sql and properties ", incremental_info)
        if source_jdbc.url.startswith("jdbc:teradata"):
            mod_url = source_jdbc.url + ",MAYBENULL=ON"
        else :
            mod_url = source_jdbc.url
        print("Modified URL",mod_url)
        
        sql_query =incremental_info['sql_query']

        if set(['numPartitions', 'partitionColumn','upperBound','lowerBound']).issubset(incremental_info.keys()):
            print(" All required Partition keys are defined and proceeding to exeucte ")
            df = sqlctx.read.format("jdbc").options(
                url=mod_url,
                dbtable=sql_query,
                user=source_jdbc.user,
                password=source_jdbc.password,
                partitionColumn = incremental_info['partitionColumn'],
                lowerBound = incremental_info['lowerBound'],
                upperBound = incremental_info['upperBound'],
                numPartitions = incremental_info['numPartitions'],
                ).load()
        else :                
            print("Proceeding to execute the sql query without setting 'numpartitions', 'partitionColumn','upperBound','lowerBound' ")
            df = sqlctx.read.jdbc(mod_url, sql_query, properties=properties)       
        
        print('--- Executing subquery: %r' % (sql_query), flush=True)
               
    elif partition_info is not None:
        partition_column = partition_info["partitionColumn"]
        lower_bound = partition_info["lowerBound"]
        upper_bound = partition_info["upperBound"]
        num_partitions = partition_info["numPartitions"]

        is_incremental = new_utils.str_is_truthy(
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

            # spark sets these in its conf
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
        "--- DF Schema Definitions for {dbtable!r}".format(
            dbtable=dbtable
        )
    )
    df_schema_def = df.printSchema()        
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

    if env_vars["USE_AWS"] and new_utils.squark_bucket_is_valid(
        env_vars["SQUARK_BUCKET"]
    ):
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

    # if env_vars["USE_HDFS"] or (not env_vars["USE_AWS"] and not env_vars["USE_HDFS"]):
    #     s3 = time.time()
    #
    #     data_dir = os.path.join(env_vars["WAREHOUSE_DIR"], env_vars["PROJECT_ID"])
    #     save_path = "{data_dir}/{table_name}".format(
    #         data_dir=data_dir, table_name=table_name
    #     )
    #
    #     print("******* SAVING TABLE TO HDFS: {save_path!r}".format(save_path=save_path))
    #     opts = dict(codec=env_vars["WRITE_CODEC"])
    #     df.write.format(env_vars["WRITE_FORMAT"]).options(**opts).save(
    #         save_path, mode=env_vars["WRITE_MODE"]
    #     )
    #     e3 = time.time()
    #     print(
    #         " ----- Writing to HDFS took: {time_delta:0.3f} seconds".format(
    #             time_delta=e3 - s3
    #         )
    #     )

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
    destination_vertica.url = utils.format_vertica_url(
        destination_vertica.url, trust_store_path=env_vars["VERTICA_TRUSTSTOREPATH"]
    )
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

        json_info = new_utils.parse_json(env_vars["JSON_INFO"])

        tables_with_subqueries = new_utils.get_tables_with_subqueries_from_json(
            json_info
        )
        tables_with_partition_info = new_utils.get_tables_with_partition_info_from_json(
            json_info
        )




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

            sql_query = new_utils.get_sql_subquery_for_table(
                tables_with_subqueries, table[table_name_key]
            )
            partition_info = new_utils.get_partition_info_for_table(
                tables_with_partition_info, table[table_name_key]
            )
            incremental_info = new_utils.get_incremental_info_for_table(json_info, table[table_name_key])
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
                        sql_query,
                        partition_info,
                        incremental_info,
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
                            sql_query,
                            partition_info,
                            incremental_info,
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
