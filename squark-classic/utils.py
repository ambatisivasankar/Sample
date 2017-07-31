
"""
All the utility functions needed by squark
"""

import config

def check_and_commit(vertica_conn):
    if not vertica_conn.autocommit:
        vertica_conn.commit()

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
                        build_number, job_name, seconds_query_duration)
                VALUES ('{PROJECT_ID}', '{SOURCE_SCHEMA}', '{TABLE_NAME}', {ROW_COUNT}, '{QUERY_DATE}',
                            '{BUILD_NUMBER}', '{JOB_NAME}', '{QUERY_DURATION}');"""
    cursor = vertica_conn.cursor()
    print('Initiating sending row counts to vertica...')
    for table_name, row_count, query_time, query_duration in row_counts:
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
            QUERY_DURATION=query_duration))
    print('Finished sending the row counts to vertica...')


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
    conn_md = {key: 'UNKNOWN' for key in ['db_product_name', 'db_product_version', 'driver_name', 'driver_version']}
    try:
        conn_md['db_product_name'] = md.getDatabaseProductName()
        conn_md['db_product_version'] = md.getDatabaseProductVersion()
        conn_md['driver_name'] = md.getDriverName()
        conn_md['driver_version'] = md.getDriverVersion()
    except Exception as exc:
        print('****** ERROR DURING METADATA REQUEST:')
        print(exc)

    return conn_md
