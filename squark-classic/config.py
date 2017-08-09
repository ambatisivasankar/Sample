
"""
config.py

Home to all the base configurations for squark
"""

##### CONSTANTS ####
ADMIN_SCHEMA='admin'
ADMIN_TIMING_TABLE='squark_table_timings'
ADMIN_SOURCE_ROW_COUNT_TABLE='squark_source_row_counts'





#### SQL ####
ADMIN_TIMING_TABLE_DDL="""CREATE TABLE {SCHEMA}.{TABLE} (
    project_id varchar(50),
    table_name varchar(100), 
    seconds_taken NUMERIC, 
    date_loaded TIMESTAMP,
    build_number varchar(9),
    job_name varchar(75));""".format(SCHEMA=ADMIN_SCHEMA, TABLE=ADMIN_TIMING_TABLE)

ADMIN_ROW_COUNT_TABLE_DDL="""CREATE TABLE {SCHEMA}.{TABLE} (
    project_id varchar(50),
    source_schema varchar(100),
    table_name varchar(100),
    row_count INT,
    query_date TIMESTAMP,
    build_number INT,
    job_name varchar(75),
    seconds_query_duration NUMERIC(37,2),
    is_after BOOLEAN
    );""".format(SCHEMA=ADMIN_SCHEMA, TABLE=ADMIN_SOURCE_ROW_COUNT_TABLE)
