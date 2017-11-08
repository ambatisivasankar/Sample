CREATE SCHEMA admin;


CREATE TABLE admin.squark_load_timings
(
	jenkins_name varchar(50),
    job_name varchar(75),
    build_number int,
    project_id varchar(50),
    table_name varchar(100),
    seconds_taken numeric(37,2),
    attempt_count int,
    source varchar(50),
    total_table_count int,
    date_loaded timestamp
);

CREATE TABLE admin.ddl_truncate_info
(
    schemaName varchar(255),
    tableName varchar(255),
    columnName varchar(255),
    dataType varchar(100),
    definedLen int,
    maxLen int
);

CREATE TABLE admin.row_count
(
    build_number int,
    schema_name varchar(255),
    table_name varchar(255),
    row_count int,
    schema_create_time timestamp,
    timestamp_now timestamp
);

CREATE TABLE admin.squark_config_large_ddl
(
    schemaName varchar(255),
    tableName varchar(255),
    columnName varchar(255),
    lengthToUse int
);

CREATE TABLE admin.squark_source_row_counts
(
    project_id varchar(50),
    source_schema varchar(100),
    table_name varchar(100),
    row_count int,
    query_date timestamp,
    build_number int,
    job_name varchar(75),
    seconds_query_duration numeric(37,2),
    is_after boolean
);

CREATE TABLE admin.squark_table_timings
(
    project_id varchar(50),
    table_name varchar(100),
    seconds_taken numeric(37,15),
    date_loaded timestamp,
    build_number varchar(9),
    job_name varchar(75)
);

