
"""
All the utility functions needed by squark
"""

import config

def check_and_commit(vertica_conn):
    if not vertica_conn.autocommit:
        vertica_conn.commit()


def get_vertica_timezone_setting(vertica_conn):
    """
    Function: get_vertica_timezone_setting - Return timezone setting of selected Vertica instance
    Args:
        vertica_conn - The connection to the vertica instance
    Returns: single string value, e.g. 'America/New_York' or 'US/Eastern'
    """
    sql_query = """
        SHOW TIMEZONE;
        """
    cursor = vertica_conn.cursor()
    cursor.execute(sql_query)
    timezone_setting = cursor.fetchone()[1]
    print('"' + ' '.join(sql_query.split()) + '" -> {timezone_setting}'.format(timezone_setting=timezone_setting))

    return timezone_setting


def get_haven_max_last_updated_time(vertica_conn, schema_name, table_name, column_name='lastUpdatedTime'):
    """
    Function: get_haven_max_last_updated_time - Return latest updated time from selected table in haven weekly schema
    Args:
        vertica_conn - The connection to the vertica instance
        schema_name (str) - name of weekly/base haven schema
        table_name (str) - Name of source haven table to query
        column_name (str) - Name of column in table_name from which to get latest timestamp value
    Returns: single timestamp value
    """
    sql_query = """
        SELECT MAX(COALESCE({column_name}, '1900-01-01'))
        FROM {schema_name}.{table_name};
        """.format(schema_name=schema_name, table_name=table_name, column_name=column_name)
    cursor = vertica_conn.cursor()
    cursor.execute(sql_query)
    last_updated_time = cursor.fetchone()[0]
    print('"' + ' '.join(sql_query.split()) + '" -> {last_updated_time}'.format(last_updated_time=last_updated_time))

    # could do a get-timestamp-string-at-vertica-timezone-setting but we are only looking at haven behavior right now
    vertica_timezone_setting = get_vertica_timezone_setting(vertica_conn)
    # fold the timestamp value into a string that includes time zone setting, get that as a GMT value to match haven RDS
    sql_query_strict = """
        SELECT TIMESTAMP '{last_updated_time} {vertica_timezone_setting}' AT TIME ZONE 'GMT';
    """.format(last_updated_time=last_updated_time, vertica_timezone_setting=vertica_timezone_setting)
    # really need to grab any that are really close to the cutoff, found a newly created analytics_event row
    # that showed up following day and had a lastUpdatedTime older than latest lastUpdatedTime in previous day's RDS
    # probably within a few seconds would be good enough but start out w/any >= curr lastUpdatedTime - 5 minutes
    sql_query_flex = """
        SELECT TIMESTAMPADD(MINUTE, -5,
            (SELECT TIMESTAMP '{last_updated_time} {vertica_timezone_setting}' AT TIME ZONE 'GMT'));
    """.format(last_updated_time=last_updated_time, vertica_timezone_setting=vertica_timezone_setting)

    cursor = vertica_conn.cursor()
    cursor.execute(sql_query_flex)
    last_updated_time_adjusted = cursor.fetchone()[0]
    print('"' + ' '.join(sql_query_flex.split()) + '" -> {last_updated_time}'.format(last_updated_time=last_updated_time_adjusted))

    return last_updated_time_adjusted


def send_table_timings_to_vertica(vertica_conn, project_id, table_timings, build_number, job_name):
    """
    Function: send_table_timings_to_vertica - Send the time taken for each table at the given time to the vertica table
    Args:
        vertica_conn - The connection to the vertica instance.
        project_id (str) - The name of the project.
        table_timings (list) - A list of lists, where each element in the main list is a list where the first element is 
                                the name of the table, and the second element is the time taken.
    """
    query = """INSERT INTO {TIMING_SCHEMA}.{TIMING_TABLE} (project_id, table_name, seconds_taken, date_loaded, build_number, job_name) VALUES (
            '{PROJECT_ID}', '{TABLE_NAME}', {SECONDS_TAKEN}, CURRENT_TIMESTAMP, '{BUILD_NUMBER}', '{JOB_NAME}');"""
    cursor = vertica_conn.cursor()
    print('Initiating sending timings to vertica...')
    for table_name, time_taken in table_timings:
        rs = cursor.execute(query.format(
            TIMING_SCHEMA=config.ADMIN_SCHEMA,
            TIMING_TABLE=config.ADMIN_TIMING_TABLE,
            PROJECT_ID=project_id,
            TABLE_NAME=table_name,
            SECONDS_TAKEN=time_taken,
            BUILD_NUMBER=build_number,
            JOB_NAME=job_name))
        check_and_commit(vertica_conn)
    print('Finished sending the timings to vertica...')


def send_source_row_counts_to_vertica(vertica_conn, project_id, source_schema, row_counts, build_number, job_name):
    """
    Function: send_source_row_counts_to_vertica - Send orig row count, via query like COUNT(*) on source tables, to vertica
    Args:
        vertica_conn - The connection to the vertica instance.
        project_id (str) - The name of the project.
        row_counts (list) - A list of lists, where each element in the main list is a list where the first element is
                                the name of the table, second element is source row count, third element is the
                                time of row count query, and fourth is how long the query took to return count.
    """
    query = """INSERT INTO {ROW_COUNT_SCHEMA}.{ROW_COUNT_TABLE}
                    (project_id, source_schema, table_name, row_count, query_date,
                        build_number, job_name, seconds_query_duration, is_after)
                VALUES ('{PROJECT_ID}', '{SOURCE_SCHEMA}', '{TABLE_NAME}', {ROW_COUNT}, '{QUERY_DATE}',
                            '{BUILD_NUMBER}', '{JOB_NAME}', '{QUERY_DURATION}', '{IS_AFTER}');"""
    cursor = vertica_conn.cursor()
    print('Initiating sending row counts to vertica...')
    for table_name, row_count, query_time, query_duration, is_after_count in row_counts:
        rs = cursor.execute(query.format(
            ROW_COUNT_SCHEMA=config.ADMIN_SCHEMA,
            ROW_COUNT_TABLE=config.ADMIN_SOURCE_ROW_COUNT_TABLE,
            PROJECT_ID=project_id,
            SOURCE_SCHEMA=source_schema,
            TABLE_NAME=table_name,
            ROW_COUNT=row_count,
            QUERY_DATE=query_time,
            BUILD_NUMBER=build_number,
            JOB_NAME=job_name,
            QUERY_DURATION=query_duration,
            IS_AFTER=is_after_count))
    print('Finished sending the row counts to vertica...')


def send_load_timing_to_vertica(vertica_conn, jenkins_name, job_name, build_number, project_id, table_name, time_taken, attempt_count, source, total_table_count):
    """
    Function: send_load_timing_to_vertica - send timing on number of seconds to load a table from source to Vertica using COPY.
            This function takes, and inserts, a single row of data at a time.
    Args:
        vertica_conn - The connection to the vertica instance.
        jenkins_name (str) - The name originating Jenkins instance.
        job_name (str) - Name of Jenkins job initiating the squark load.
        build_number (int) - Build number from Jenkins job.
        project_id (str) - Squark project_id, matches the target schema name in Vertica.
        table_name (str) - Table name.
        time_taken (float) - Number of seconds to load into Vertica.
        attempt_count (int) - Number of attempts made on this table, relevant when source = s3, 1 if no retries
        source (str) - Where the data is being loaded from, initially either 'hdfs' or 's3'.
    """
    query = """INSERT INTO {TIMING_SCHEMA}.{TIMING_TABLE} (jenkins_name, job_name, build_number, project_id, table_name, seconds_taken, attempt_count, source, total_table_count, date_loaded) VALUES (
            '{JENKINS_NAME}', '{JOB_NAME}', {BUILD_NUMBER}, '{PROJECT_ID}', '{TABLE_NAME}', {SECONDS_TAKEN}, {ATTEMPT_COUNT}, '{SOURCE}', {TOTAL_TABLE_COUNT}, CURRENT_TIMESTAMP);"""
    cursor = vertica_conn.cursor()
    print('---- Initiating sending load timings to vertica...', flush=True)
    rs = cursor.execute(query.format(
        TIMING_SCHEMA=config.ADMIN_SCHEMA,
        TIMING_TABLE=config.ADMIN_LOAD_TIMING_TABLE,
        JENKINS_NAME=jenkins_name,
        JOB_NAME=job_name,
        BUILD_NUMBER=build_number,
        PROJECT_ID=project_id,
        TABLE_NAME=table_name,
        SECONDS_TAKEN=time_taken,
        ATTEMPT_COUNT=attempt_count,
        SOURCE=source,
        TOTAL_TABLE_COUNT=total_table_count,
        ))
    check_and_commit(vertica_conn)
    print('---- Finished sending load timings to vertica...', flush=True)


def get_large_data_ddl_def(vertica_conn, project_id, table_name):
    """
    Function: get_large_data_ddl_def - Query config table for custom length value(s) tied to columns with long src data
    Args:
        vertica_conn - The connection to the vertica instance holding config data.
        project_id (str) - The name of the project.
        table_name (str) - Name of source table which may have one or more columns defined in large ddl config table
    Returns: per-table dictionary of column(s) needing >65k LENGTH, e.g. {'NOTIF_TXT': 1500000}, empty dict if no data
    """
    sql_query = """
        SELECT columnName, lengthToUse
        FROM admin.squark_config_large_ddl
        WHERE schemaName = '{project_id}' AND tableName = '{table_name}';
        """.format(project_id=project_id, table_name=table_name)
    cursor = vertica_conn.cursor()
    cursor.execute(sql_query)
    # do a fetchmany to avoid WARNING messages from py4jdbc, 10k is way more than needed
    rs = cursor.fetchmany(10000)
    large_ddl = dict(rs)
    print('large_ddl returned for table {}: {}'.format(table_name, large_ddl))

    return large_ddl


def get_col_max_data_length(postgres_conn, table_name, column_name):
    """
    Function: get_col_max_data_length
    Args:
        postgres_conn - The connection to the postgres instance being queried
        table_name (str) -
        column_name (str) -
    Returns: int
    """
    sql_query = """
        SELECT MAX(LENGTH("{column_name}")) 
        FROM {table_name}
        """.format(column_name=column_name, table_name=table_name)
    cursor = postgres_conn.cursor()
    cursor.execute(sql_query)
    max_len = cursor.fetchone()[0]

    return max_len


def get_squark_metadata_for_project(vertica_conn, project_id, squark_metadata_table_name, limit=1000):
    """
    Function: get_squark_metadata_for_project - Return contents of specified table in admin schema
    Args:
        vertica_conn - The connection to the vertica instance holding config data.
        project_id (str) - The name of the project.
        squark_metadata_table_name (str) - Name of squark metadata table in the admin schema
        limit (int, default=1000) - Number of rows to return
    Returns: recordset
    """
    sql_query = """
        SELECT *
        FROM admin.{squark_metadata_table_name}
        WHERE schemaName = '{project_id}'
        LIMIT {limit};
        """.format(project_id=project_id, squark_metadata_table_name=squark_metadata_table_name, limit=limit)
    cursor = vertica_conn.cursor()
    cursor.execute(sql_query)
    rs = cursor.fetchmany(limit)

    return rs


#TODO: possibly pull out into separate per-db system utils file/class
def get_number_of_tables_in_db2_schema(db2_conn, schema_name):
    """
    Function: get_number_of_tables_in_db2_schema - Return number of tables in a given schema
    Args:
        db2_conn - The connection to the DB2 instance being queried
        schema_name (str) - Name of the schema to lookup in db2 system table
    Returns: int
    """
    sql_query = """
        SELECT COUNT(*)
        FROM SYSCAT.TABLES
        WHERE TABSCHEMA = '{schema_name}'
        """.format(schema_name=schema_name)
    cursor = db2_conn.cursor()
    cursor.execute(sql_query)
    count = cursor.fetchone()[0]

    return count


def get_number_of_columns_in_db2_table(db2_conn, schema_name, table_name):
    """
    Function: get_number_of_columns_in_db2_table - Return number of columns in a given schema.table
    Args:
        db2_conn - The connection to the DB2 instance being queried
        schema_name (str) - Name of the schema to lookup in db2 system table
        schema_name (str) - Name of the table to lookup in above schema
    Returns: int
    """
    sql_query = """
        SELECT COUNT(*)
        FROM SYSCAT.COLUMNS
        WHERE TABSCHEMA = '{schema_name}' AND TABNAME = '{table_name}'
        """.format(schema_name=schema_name, table_name=table_name)
    cursor = db2_conn.cursor()
    cursor.execute(sql_query)
    count = cursor.fetchone()[0]

    return count


def populate_connection_metadata(md):
    """
    Function: populate_connection_metadata - Return a dictionary populated with selected metadata values
    Args:
        md - Vendor specific implementation of JDBC DatabaseMetaData interface, i.e. py4jdbc connection._metadata
        Returns: dict
    """
    conn_md = {key: 'UNKNOWN' for key in ['db_product_name', 'db_product_version', 'driver_name', 'driver_version']}

    def db_match(db_name):
        return conn_md['db_product_name'].lower().startswith(db_name)

    try:
        conn_md['db_product_name'] = md.getDatabaseProductName()
        conn_md['db_product_version'] = md.getDatabaseProductVersion()
        conn_md['driver_name'] = md.getDriverName()
        conn_md['driver_version'] = md.getDriverVersion()
        # some jdbc drivers don't need to have their driver values set on connecting under spark 2.1+
        # lack of a driver_name_for_spark key means driver property won't be set, but below values would work if need be
        #   Microsoft SQL Server: 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        #   Oracle: 'oracle.jdbc.OracleDriver'
        #   Postgresql: 'org.postgresql.Driver'
        if db_match('teradata'):
            conn_md['driver_name_for_spark'] = conn_md['driver_name']
        elif db_match('vertica'):
            conn_md['driver_name_for_spark'] = 'com.vertica.jdbc.Driver'
        elif db_match('db2'):
            conn_md['driver_name_for_spark'] = 'com.ibm.db2.jcc.DB2Driver'
        elif db_match('ase'):
            conn_md['driver_name_for_spark'] = 'net.sourceforge.jtds.jdbc.Driver'
        elif db_match('microsoft sql'):
            conn_md['driver_name_for_spark'] = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        elif db_match('oracle'):
            conn_md['driver_name_for_spark'] = 'oracle.jdbc.OracleDriver'
        elif db_match('postgresql'):
            conn_md['driver_name_for_spark'] = 'org.postgresql.Driver'

    except Exception as exc:
        print('****** ERROR DURING METADATA REQUEST:')
        print(exc)

    return conn_md