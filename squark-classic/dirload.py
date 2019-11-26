import glob
import logging
import os
import re
import sys
import time
from collections import defaultdict

import boto3
from jinja2 import Template

import squark.config.environment
import utils
import new_utils

logging.basicConfig(level=logging.DEBUG)

# Environmental Variables

# vars loaded with os.environ() which raises KeyError
ENV_VARS_TO_LOAD_AS_IS = [
    # Empty list currently
    "VERTICA_TRUSTSTOREPATH",
]

# vars which are considered to be truthy
ENV_VARS_TO_LOAD_AS_BOOL = ["SKIP_UNIQUE_ID_CHECK", "ANALYZE_STATISTICS"]

# vars loaded with os.environ.get() which defaults in case of KeyError
ENV_VARS_TO_LOAD_WITH_DEFAULTS = [
    ("ANALYZE_STATISTICS", True),
    ("ANALYZE_STATISTICS_PERCENTAGE", 10),
    ("VERTICA_CONNECTION_ID", "vertica_dev"),
    ("VERTICA_PARALLELISM", 10),  # MAX_CONNS  # Cast to int
    ("PROJECT_ID", None),
    ("SQUARK_TYPE", None),
    ("LOAD_FROM_AWS", True),
    ("LOAD_FROM_HDFS", False),  # Logic
    ("S3_FUSE_LOCATION", "/mnt/s3/"),
    ("TABLE_NUM_RETRY", "1"),  # Cast to int
    ("S3_CONNECTION_ID", None),
    ("JENKINS_URL", ""),
    ("JOB_NAME", None),
    ("BUILD_NUMBER", "-1"),  # Cast to int
    ("INCLUDE_TABLES", None),  # Logic
    ("EXCLUDE_TABLES", None),  # Logic
    ("SQUARK_BUCKET", None),
    ("HDFS_HOST", None),
    ("HDFS_PORT", None),
    ("HDFS_USER", None),
    ("SQUARK_DELETED_TABLE_SUFFIX", "_ADVANA_DELETED"),
    ("WRITE_FORMAT", "orc"),
]

# vars which
ENV_VARS_TO_CAST_TO_INT = [
    "VERTICA_PARALLELISM",  # MAX_CONNS
    "TABLE_NUM_RETRY",
    "BUILD_NUMBER",
    "ANALYZE_STATISTICS_PERCENTAGE",
]


def get_analyze_statistics_query(destination_schema, destination_table, percentage):
    """Get ANALYZE_STATISTICS SQL query.

    :param destination_schema: Schema to pass into query.
    :param destination_table: Table to pass into query.
    :param percentage: Percentage to analyze.
    :return:
    """
    raw_template = (
        """SELECT ANALYZE_STATISTICS ('{{ schema }}.{{ table }}', {{ percentage }});"""
    )
    template = Template(raw_template, trim_blocks=True)
    query = template.render(
        schema=destination_schema, table=destination_table, percentage=percentage
    )
    return query


def get_s3_urls(
    project_id,
    aws_access_key_id,
    aws_secret_access_key,
    squark_type,
    squark_bucket,
    include_tables,
    squark_deleted_table_suffix,
    exclude_tables,
    write_format,
):

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name="us-east-1",
    )
    client = session.client("s3")
    prefix = "{squark_type}/{project_id}/".format(
        squark_type=squark_type, project_id=project_id
    )
    tmp_paths = client.list_objects(Bucket=squark_bucket, Prefix=prefix)
    paths = [x["Key"] for x in tmp_paths["Contents"]]
    while tmp_paths["IsTruncated"]:
        nextMarker = paths[-1]
        tmp_paths = client.list_objects(
            Bucket=squark_bucket, Prefix=prefix, Marker=nextMarker
        )
        paths.extend([x["Key"] for x in tmp_paths["Contents"]])

    print(
        "--- Total pulled paths: {num_paths}    --- Total set of pulled paths: {num_unique_paths}".format(
            num_paths=(len(paths)), num_unique_paths=len(set(paths))
        )
    )
    all_spark_files = [x for x in paths if glob.re.search(r".*\.{WRITE_FORMAT}/.*\.{WRITE_FORMAT}".format(WRITE_FORMAT=write_format), x)]
    urls = defaultdict(list)
    for spark_file in all_spark_files:
        tablename = spark_file.replace(prefix, "").strip("/").split("/")[0]

        if include_tables is not None:

            include_tables_variants = [
                "{}{}".format(s, squark_deleted_table_suffix) for s in include_tables
            ]
            include_tables_all = include_tables + include_tables_variants

            if tablename not in include_tables_all:
                print(
                    "*******SKIPPING NOT INCLUDED TABLE: {tablename!r}".format(
                        tablename=tablename
                    )
                )
                continue

        if exclude_tables is not None:
            if tablename in exclude_tables:
                print(
                    "*******SKIPPING EXCLUDE_TABLES TABLE: {tablename!r}".format(
                        tablename=tablename
                    )
                )
                continue

        urls[tablename].append(spark_file)
    return urls


def get_urls(dirname, hdfs_host, hdfs_port, hdfs_user):
    raise NotImplementedError("HDFS no longer implemented.")
    # urls = defaultdict(list)
    # # PyWebHdfs had a dependecy error
    # hdfs = PyWebHdfsClient(host=hdfs_host, port=hdfs_port, user_name=hdfs_user)
    # for child in hdfs.list_dir(dirname)["FileStatuses"]["FileStatus"]:
    #     if child["pathSuffix"].startswith("_"):
    #         continue
    #     table_dir = os.path.join(dirname, child["pathSuffix"])
    #     for orcfile in hdfs.list_dir(table_dir)["FileStatuses"]["FileStatus"]:
    #         if orcfile["pathSuffix"].startswith("_"):
    #             continue
    #         url = "'webhdfs://%s:%s%s/%s'" % (
    #             hdfs_host,
    #             hdfs_port,
    #             table_dir,
    #             orcfile["pathSuffix"],
    #         )
    #         urls[child["pathSuffix"]].append(url)
    # return urls


def do_s3_copyfrom(
    schema_name,
    destination_schema,
    table_name,
    table_prefix,
    vertica_conn,
    squark_bucket,
    squark_type,
    table_num_retry,
    analyze_statistics,
    analyze_percentage,
    write_format,
):
    curr_retry = 0
    table_url = "'s3://{squark_bucket}/{squark_type}/{destination_schema}/{table_name}/{table_name}.{write_format}/*.{write_format}'".format(
        squark_bucket=squark_bucket,
        squark_type=squark_type,
        destination_schema=destination_schema,
        table_name=table_name,
        write_format=write_format,
    )
    tmpl = "copy %s.%s from %s on any node %s direct;"
    table_name = table_prefix + table_name
    sql = tmpl % (schema_name, table_name, table_url, write_format)
    # sql = tmpl % (schema_name, table_name, ',\n'.join([os.path.join(S3_FUSE_LOCATION, x) for x in _urls]))

    logging.info("sql: %r", sql)
    # vertica_conn = squarkenv.sources[VERTICA_CONNECTION_ID].conn
    logging.info("---- Launching s3 copy command...")
    # Add retries for loading data from s3
    # curr_retry = 0
    retry_bool = True
    print("----------------------------")
    analyze_statistics_query = get_analyze_statistics_query(
        schema_name, table_name, analyze_percentage
    )
    while retry_bool and curr_retry < table_num_retry:
        print(
            "Attempting to load table {table}: [Attempt {curr}/{tot}]".format(
                table=table_name, curr=curr_retry + 1, tot=table_num_retry
            )
        )
        try:
            cursor = vertica_conn.cursor()
            cursor.execute(sql)
            if analyze_statistics:
                start_analyze_statistics = time.time()
                cursor.execute(analyze_statistics_query)
                duration_analyze_statistics = time.time() - start_analyze_statistics
                print(
                    "----- Statistics analyzed for {table_name} in {duration:.02f} seconds".format(
                        table_name=table_name, duration=duration_analyze_statistics
                    )
                )
            cursor.close()
            retry_bool = False
        except Exception as e:
            print(
                "!! -- An Error occurred while trying to load -- waiting 5 seconds to retry!"
            )
            print(str(e))
            curr_retry += 1
            time.sleep(5)
    if retry_bool:
        print("ERROR!! Number of allowed retries exceeded!! Exiting")
        raise
    print("Load Successful...")
    print("----------------------------")

    return curr_retry + 1


def do_copyfrom(
    schema_name, table_name, table_prefix, urls, vertica_conn, max_num_connections
):
    urls = urls[:]
    while urls:
        _urls = []
        for i in range(max_num_connections):
            try:
                _urls.append(urls.pop(0))
            except IndexError:
                break
        tmpl = "copy %s.%s from %s on any node orc direct;"
        table_name = table_prefix + table_name
        sql = tmpl % (schema_name, table_name, ",\n".join(_urls))
        logging.info("sql: %r", sql)
        # vertica_conn = squarkenv.sources[VERTICA_CONNECTION_ID].conn
        cursor = vertica_conn.cursor()
        logging.info("---- Launching copy command...")
        cursor.execute(sql)
        cursor.close()


def update_squark_load_timings(
    project_id,
    table_name,
    time_taken,
    attempt_count,
    source,
    total_table_count,
    vertica_conn,
    jenkins_url,
    job_name,
    build_number,
):
    jenkins_name = jenkins_url.split(".")[0].split("/")[-1]
    # vertica_conn = squarkenv.sources[VERTICA_CONNECTION_ID].conn
    utils.send_load_timing_to_vertica(
        vertica_conn,
        jenkins_name,
        job_name,
        build_number,
        project_id,
        table_name,
        time_taken,
        attempt_count,
        source,
        total_table_count,
    )


def main():
    schema_name = sys.argv[1]
    dirname = sys.argv[2]
    try:
        table_prefix = sys.argv[3]
    except IndexError:
        table_prefix = ""
    env_vars = new_utils.load_env_vars(
        vars_as_is=ENV_VARS_TO_LOAD_AS_IS,
        vars_as_bool=ENV_VARS_TO_LOAD_AS_BOOL,
        vars_with_defaults=ENV_VARS_TO_LOAD_WITH_DEFAULTS,
        vars_to_cast_as_int=ENV_VARS_TO_CAST_TO_INT,
    )

    squarkenv = squark.config.environment.Environment()
    destination_vertica = squarkenv.sources[env_vars["VERTICA_CONNECTION_ID"]]
    destination_vertica.url = utils.format_vertica_url(
        destination_vertica.url, env_vars["VERTICA_TRUSTSTOREPATH"]
    )
    vertica_conn = destination_vertica.conn

    if env_vars["LOAD_FROM_AWS"] and new_utils.squark_bucket_is_valid(
        env_vars["SQUARK_BUCKET"]
    ):

        aws_access_key_id, aws_secret_access_key = new_utils.get_aws_credentials_from_squark(
            squarkenv, env_vars["S3_CONNECTION_ID"]
        )
        include_tables = new_utils.split_strip_str(env_vars["INCLUDE_TABLES"])
        exclude_tables = new_utils.split_strip_str(env_vars["EXCLUDE_TABLES"])
        aws_urls = get_s3_urls(
            project_id=env_vars["PROJECT_ID"],
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            squark_type=env_vars["SQUARK_TYPE"],
            squark_bucket=env_vars["SQUARK_BUCKET"],
            include_tables=include_tables,
            squark_deleted_table_suffix=env_vars["SQUARK_DELETED_TABLE_SUFFIX"],
            exclude_tables=exclude_tables,
            write_format=env_vars["WRITE_FORMAT"],
        )
        total_table_count = len(aws_urls.keys())
        print(
            "DEBUG: S3 file url listing, sorted: {urls}".format(
                urls=sorted(aws_urls.items())
            )
        )
        # a "part" file, e.g. part-00000-c4492a53-615d-4787-b284-96f6848c0aee-c000.snappy.orc
        p = re.compile(r"part-?\d{5}-?(\w{8}-?\w{4}-?\w{4}-?\w{4}-?\w{12}-?\w{4})")
        # sort by table to match all_tables processing -> last written table will be last loaded, better for S3 store
        for table_name, aws_urls in sorted(aws_urls.items()):
            unique_ids = set(p.findall("|".join(aws_urls)))
            print(
                "table {table_name}, unique id values in part files: {files}".format(
                    table_name=table_name, files=", ".join(unique_ids)
                )
            )
            if not env_vars["SKIP_UNIQUE_ID_CHECK"]:
                if len(unique_ids) > 1:
                    raise ValueError(
                        "S3 folder for table {} contains part files from multiple operations, unique ids: {}".format(
                            table_name, unique_ids
                        )
                    )
            print(
                "XXX: Loading S3 {table_name} ({num_urls} files)".format(
                    table_name=table_name, num_urls=len(aws_urls)
                )
            )
            s1 = time.time()
            num_attempts = do_s3_copyfrom(
                schema_name=schema_name,
                destination_schema=env_vars["PROJECT_ID"],
                table_name=table_name,
                table_prefix=table_prefix,
                vertica_conn=vertica_conn,
                squark_bucket=env_vars["SQUARK_BUCKET"],
                squark_type=env_vars["SQUARK_TYPE"],
                table_num_retry=env_vars["TABLE_NUM_RETRY"],
                analyze_statistics=env_vars["ANALYZE_STATISTICS"],
                analyze_percentage=env_vars["ANALYZE_STATISTICS_PERCENTAGE"],
                write_format=env_vars["WRITE_FORMAT"],
            )
            table_time = time.time() - s1
            # admin table will be updated after each table is loaded to vertica, i.e. even if full job later fails
            update_squark_load_timings(
                project_id=env_vars["PROJECT_ID"],
                table_name=table_name,
                time_taken=table_time,
                attempt_count=num_attempts,
                source="s3",
                total_table_count=total_table_count,
                vertica_conn=vertica_conn,
                jenkins_url=env_vars["JENKINS_URL"],
                job_name=env_vars["JOB_NAME"],
                build_number=env_vars["BUILD_NUMBER"],
            )

    if env_vars["LOAD_FROM_HDFS"]:
        urls = get_urls(
            dirname, env_vars["HDFS_HOST"], env_vars["HDFS_PORT"], env_vars["HDFS_USER"]
        )
        total_table_count = len(urls.keys())
        items = list(urls.items())
        items.sort(key=lambda item: len(item[1]), reverse=True)
        for table_name, urls in urls.items():
            print(
                "XXX: Loading {table_name} ({num_urls} files)".format(
                    table_name=table_name, num_urls=len(urls)
                )
            )
            s1 = time.time()
            do_copyfrom(
                schema_name=schema_name,
                table_name=table_name,
                table_prefix=table_prefix,
                urls=urls,
                vertica_conn=vertica_conn,
                max_num_connections=env_vars["VERTICA_PARALLELISM"],
            )
            table_time = time.time() - s1
            update_squark_load_timings(
                project_id=env_vars["PROJECT_ID"],
                table_name=table_name,
                time_taken=table_time,
                attempt_count=1,
                source="hdfs",
                total_table_count=total_table_count,
                vertica_conn=vertica_conn,
                jenkins_url=env_vars["JENKINS_URL"],
                job_name=env_vars["JOB_NAME"],
                build_number=env_vars["BUILD_NUMBER"],
            )


if __name__ == "__main__":
    main()
