import os
import json

from typing import Dict
from typing import Optional


def str_is_truthy(string, truthy_vals=("1", "true", "yes")) -> bool:
    """
    Checks if a string equates to true
    :param string:
    :param truthy_vals:
    :return:
    """
    if string is not None:
        string = string.lower()
    return string in truthy_vals


def _load_env_vars_as_is(vars_to_load):
    """
    Loads environment variables exactly as they are.
    If key is missing os.environ will raise a KeyError
    :param vars_to_load:
    :return:
    """
    env_vars = {key: os.environ[key] for key in vars_to_load}

    return env_vars


def _load_env_vars_as_bool(vars_to_load, truthy_vals=("1", "true", "yes")):
    """
    Loads environment variables as bools by comparing to truthy_vals.
    True if in truthy_vals, otherwise False
    If key is missing, sets to False
    :param vars_to_load:
    :param truthy_vals:
    :return:
    """

    env_vars = {
        key: str_is_truthy(os.environ.get(key), truthy_vals) for key in vars_to_load
    }

    return env_vars


def _load_env_vars_with_defaults(vars_and_defaults):
    """
    Loads environment variables as they are, or as their default value if they are missing
    :param vars_and_defaults: {key: default_value}
    :return:
    """

    env_vars = {
        key: os.environ.get(key, default_value)
        for key, default_value in vars_and_defaults
    }

    return env_vars


def _cast_env_vars_to_int(env_vars, vars_to_cast):
    """
    Casts vars_to_cast to int and overwrites their non-int from in env_vars
    :param env_vars:
    :param vars_to_cast:
    :return:
    """

    for key in vars_to_cast:
        env_vars[key] = int(env_vars[key])

    return env_vars


def split_strip_str(to_split_strip, split_on=","):
    """
    Splits a str THEN strips them
    :param to_split_strip:
    :param split_on:
    :return:
    """
    if to_split_strip is not None:
        split_and_stripped = [s.strip() for s in to_split_strip.split(split_on) if s]

        return split_and_stripped
    else:
        return None


def load_env_vars(
    vars_as_is=None,
    vars_as_bool=None,
    vars_with_defaults=None,
    vars_to_cast_as_int=None,
):
    """
    Loads environmental variables with various patterns
    :param vars_as_is:
    :param vars_as_bool:
    :param vars_with_defaults:
    :param vars_to_cast_as_int:
    :return:
    """
    vars_as_is = vars_as_is or []
    vars_as_bool = vars_as_bool or []
    vars_with_defaults = vars_with_defaults or []
    vars_to_cast_as_int = vars_to_cast_as_int or []

    env_vars_as_is = _load_env_vars_as_is(vars_as_is)
    env_vars_as_bools = _load_env_vars_as_bool(vars_as_bool)
    env_vars_as_defaults = _load_env_vars_with_defaults(vars_with_defaults)

    env_vars = {**env_vars_as_is, **env_vars_as_bools, **env_vars_as_defaults}
    env_vars = _cast_env_vars_to_int(env_vars, vars_to_cast_as_int)

    return env_vars


def squark_bucket_is_valid(squark_bucket, invalid_buckets=("squark", "squark-dsprd")):
    """
    Checks if bucket in list of bad buckets
    :param squark_bucket:
    :param invalid_buckets:
    :return: True if not in list of bad buckes
    :raises: ValueError if bucket is bad
    """
    if squark_bucket.lower() in invalid_buckets:
        raise ValueError(
            "Invalid bucket specified: {squark_bucket}".format(
                squark_bucket=squark_bucket
            )
        )
    else:
        return True


def get_aws_credentials_from_squark(squark_env, s3_connection_id):
    """
    Retrieves aws credentials from the squark environment
    :param squark_env:
    :param s3_connection_id:
    :return:
    """
    destination_aws = squark_env.sources[s3_connection_id]
    aws_access_key_id = destination_aws.cfg["access_key_id"]
    aws_secret_access_key = destination_aws.cfg["secret_access_key"]

    return aws_access_key_id, aws_secret_access_key


def parse_json(json_string: str) -> Optional[Dict]:
    """Fix the quotes in json string and return as dict.

    :param json_string: JSON_INFO from a job file
    :return Dict of JSON_INFO
    """
    if json_string is None:
        return None
    else:
        return json.loads(json_string.replace("'", '"').replace('"""', "'"))


def get_tables_with_subqueries_from_json(info: Optional[Dict]) -> Optional[Dict]:
    """Get the 'tables_with_subqueries' dict from the json_info dict.

    :param info: JSON_INFO dict
    :return: Dict[table_names, subqueries], or None
    """
    if info is None:
        return None
    try:
        subqueries = info["SAVE_TABLE_SQL_SUBQUERY"]["table_queries"]
    except KeyError:
        print("--- No SAVE_TABLE_SQL_SUBQUERY::table_queries")
        return None
    else:
        print("--- Tables with subqueries: {tables}".format(tables=subqueries))
        return subqueries


def get_incremental_info_for_table(info: Optional[Dict], table_name: str) -> Optional[Dict]:
    """Get the 'incremetnal_info' dict from the json_info dict.

    :param info: JSON_INFO dict
    :param table_name: table name to get.
    :return: Dict[table_names, subqueries], or None
    """
    if info is None:
        return None
    try:
        incremental_info = info["SAVE_TABLE_SQL_SUBQUERY"][table_name]
    except KeyError:
        print("--- No SAVE_TABLE_SQL_SUBQUERY::{table_name}".format(table_name=table_name))
        return None
    else:
        print("--- Incremental info: {incremental_info}".format(incremental_info=incremental_info))
        return incremental_info


def get_tables_with_partition_info_from_json(info: Optional[Dict]) -> Optional[Dict]:
    """Get the 'tables_with_partition_info' dict from the json_info dict.

    :param info: JSON_INFO dict
    :return: Dict[table_names, partitioninfo], or None
    """
    if info is None:
        return None
    try:
        partitions = info["PARTITION_INFO"]["tables"]
    except KeyError:
        print("--- No PARTITION_INFO::tables")
        return None
    else:
        print("--- Tables with partition info: {tables}".format(tables=partitions))
        return partitions


def get_tables_with_super_projection_settings_from_json(
    info: Optional[Dict]
) -> Optional[Dict]:
    """Get the 'tables_with_super_projection_settings' dict from the json_info dict.

    :param info: JSON_INFO dict
    :return: Dict[table_names, supersinfo], or None
    """
    if info is None:
        return None
    try:
        supers = info["SUPER_PROJECTION_SETTINGS"]["tables"]
    except KeyError:
        print("--- No SUPER_PROJECTION_SETTINGS::tables")
        return None
    else:
        print(
            "--- Tables with super projection settings: {tables}".format(tables=supers)
        )
        return supers


def get_sql_subquery_for_table(info: Optional[Dict], table_name: str) -> Optional[str]:
    """Get the SQL subqueriy for a table.

    :param info: `tables_with_subqueries` dict
    :param table_name: name of table
    :return: subquery
    """
    if info is None:
        return None
    try:
        sql_query = info[table_name]
    except KeyError:
        print("--- No subquery for table '{name}'".format(name=table_name))
        return None
    else:
        print(
            "--- Subquery for table '{name}' = {sql}".format(
                name=table_name, sql=sql_query
            )
        )
        return sql_query


def get_partition_info_for_table(
    info: Optional[Dict], table_name: str
) -> Optional[Dict]:
    """Get partition info dict for table.

    :param info: 'tables_with_partition_info' dict
    :param table_name: name of table
    :return: partition info dict
    """
    if info is None:
        return None
    try:
        partition_info = info[table_name]
    except KeyError:
        print("--- No partition info for table '{name}'".format(name=table_name))
        return None
    else:
        print(
            "--- Partition info for table '{name}' = {info}".format(
                name=table_name, info=partition_info
            )
        )
        return partition_info


def get_super_projection_settings_for_table(
    info: Optional[Dict], table_name: str
) -> Optional[Dict]:
    """Get partition info dict for table.

    :param info: 'tables_with_super_projection_settings' dict
    :param table_name: name of table
    :return: super_projection info dict
    """
    if info is None:
        return None
    try:
        super_projection = info[table_name]
    except KeyError:
        print(
            "--- No super projection settings fortable '{name}'".format(name=table_name)
        )
        return None
    else:
        print(
            "--- Super projection settings for table '{name}' = {info}".format(
                name=table_name, info=super_projection
            )
        )
        return super_projection


def get_table_map(info: Optional[Dict]) -> Optional[Dict]:
    """Get table map for job.

    :param info: 'JSON_INFO' dict
    :return: Table map dict {source_name: target_name, ...
    """
    if info is None:
        return None
    try:
        table_map = info["TABLE_MAP"]  # type: Dict[str, str]
        return table_map
    except KeyError:
        print(
            "--- No table map for job"
        )
        return None


def get_table_name_from_map(table_map: Optional[Dict], source_table_name: str) -> str:
    """Get table map for job.

    :param table_map: 'tables_with_super_projection_settings' dict
    :param source_table_name: Name of table in source
    :return: Table map dict {source_name: target_name, ...
    """
    if table_map is None:
        return source_table_name
    try:
        target_table_name = table_map[source_table_name]  # type: str
        return target_table_name
    except KeyError:
        print(
            "--- No super table map for table {}".format(source_table_name)
        )
        return source_table_name
    except Exception as e:
        print(str(e))
        return source_table_name


def get_inverted_dict(source_dict: Optional[Dict]) -> Optional[Dict]:
    if source_dict is None:
        return None
    return {v: k for k, v in source_dict.items()}
