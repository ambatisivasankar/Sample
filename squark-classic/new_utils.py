import os


def _str_is_truthy(string, truthy_vals=("1", "true", "yes")) -> bool:
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
        key: _str_is_truthy(os.environ.get(key), truthy_vals) for key in vars_to_load
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
