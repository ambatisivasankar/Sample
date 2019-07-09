# Job Files Description

Here we discuss the purpose, file type, naming convention, location, and structure of job files used by `squark`.

***

### Purpose

Job files are used by `squark` to set variables for a specific job. These variables include information about things like: source, destination schema, Spark settings, and other things.
 

### File Type

Job files are created as bash scripts, with the extension `.sh`. This file type is used for its ability to set environmental variables in the active shell, which `squark` relies on.

### Naming Convention

Job file names reflect the specific job that will run, `<squark_name>.sh`. The variable `squark_name` is set in the Jenkins Build Execute-Shell command, and is passed to `launch_squark_job.sh` as an unnamed but required parameter.
  
### Location

Job files are stored in `./squark-classic/jobs/`.

### Structure

As the purppose of the job file is to set some environmental variables, the structure of the file consists of a series of export statements.

#### General Strucure
```
export=<required settings>

export=<optional settings>

export=JSON_INFO
```

#### Required Content
There is a minimum required collection of variables that must be set for `squark` to operate. Other variables may be used in addition.
The minimal content of a job file is:

```
export PROJECT_ID=<squark_name>
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=<connection_id>
```

Where `PROJECT_ID` is the same as `squark_name`, and `CONNECTION_ID` identifies the source in `sources.cfg`.

#### Lists of Tables

For long lists please use the style as below, where lists have one table per row. Lists of tables should be alphabetical.

```
INCLUDE_TABLES_ARRAY=(
  "AGMT_CMN_VW"
  "BENE_DATA_CMN_VW"
  "CUST_ADDL_TEL_NR_CMN_VW"
  "PDCR_AGMT_CMN_VW"
  "PDCR_ALT_ID_CMN_VW"
  "PDCR_DEMOGRAPHICS_VW"
  "SLLNG_AGMT_CMN_VW"
  )

export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"
```

NOTE: It is critical that these `*_ARRAY` variables have a different
name than the actual variables that will be used later by `squark`.
See: https://stackoverflow.com/q/51272394/877069

#### JSON

Some parameters can be kept in the variable `JSON_INFO`. These can be accessed later using a JSON parser.
Please alphabetize the `tables` section of `PARTITION_INFO`, if used.

```
export JSON_INFO="
{
	'SAVE_TABLE_SQL_SUBQUERY':{
      'schema': 'dbo',
      'table_queries': {
            'policy_doc': '(SELECT \\\"_id\\\",\\\"_template\\\",\\\"__version__\\\",NULL AS doc,\\\"type\\\",\\\"docType\\\",\\\"docSource\\\",\\\"appType\\\",\\\"subType\\\",\\\"policyId\\\",\\\"name\\\",\\\"date\\\",\\\"uploadedBy\\\",\\\"language\\\",\\\"roles\\\",\\\"order\\\",\\\"follow_up_qa_id\\\",\\\"createdTime\\\",\\\"lastUpdatedTime\\\",\\\"status\\\" FROM policy_doc) as subquery'
        }
    },
    'PARTITION_INFO':{
        'tables': {
            'analytics_container': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'analytics_event': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            }
        }
   }
}
"
```

### Action Item
Job files that do not follow the above guide should be restructured.
