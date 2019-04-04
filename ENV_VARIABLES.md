
## environment variables used by `squark`


### Spark related

---
**SPARK_DRIVER_MEMORY** 
- passed on as `--driver-memory` in `spark-submit`, default value of "2G"

**SPARK_EXECUTOR_MEMORY** 
- passed on as `--executor-memory` in `spark-submit`, default value of "2G"
- in our current deploy mode this is the one more likely to have material effect (vs. driver memory)
- likely use-case would be a table (e.g. `haven.interaction`) that has large BLOB data in one or more columns. More 
memory may be necessary to get the data compressed and written to s3. Increasing this value may also generally speed up 
the write-to-s3 process, but primary usage is in relation to these large columns

**SPARK_MAX_EXECUTORS**
- passed on as part of `--conf "spark.dynamicAllocation.maxExecutors=` in `spark-submit`, default value of 2
- for jobs that are partitioned, making multiple SQL queries on a given source database at once, we would generally 
want to limit the parallelization factor via this variable - so as not to overstress the source system
- no specific guidelines but note that some jobs, e.g. datalayer, pull from multiple tables in parallel, and a limit 
of 10 executors might still result in 20 or more simultaneous queries
- for AWS RDS's sent to us from HavenLife, e.g. `haven` job, we (Advanced Analytics) basically own db. In those cases 
the source can be treated purely as a read-only reporting db, and we can ramp up this number

**SPARK_YARN_QUEUE** (DEPRECATED)
- passed on as part of `--conf "spark.yarn.queue=` in `spark-submit`, default value of "default-jenkins"
- this was used on-premises, where CDH was configured to give priority to a specific queue name, allocating more 
executors to "this" job vs. all others. In AWS the auto-scaling properties of EMR make resource contention less of a 
concern

<br/>

--- 

### usually set only in job file  

---
**PROJECT_ID**
- sets the subdirectory in which data is being written, may be used elsewhere in `squark`(?), normally matches name of 
schema in Vertica

**CONNECTION_ID**
- the key value used to retrieve encrypted connection info from the .cfg file
- more than one job can share the same **CONNECTION_ID**, they may be pulling different sets of tables, or even from 
different schemas from within the same database

<br/>

---

### often set in the job file 
- but also sometimes in the Jenkins shell, for temporary or ad-hoc reasons  

---
**INCLUDE_TABLES**
- comma-delimited string of tables to retrieve from the source database. If this is not set `squark` would attempt to 
pull all available tables
- note, the table/view names are **case-sensitive** - if JDBC says the table name is mixed-case and it is stored here 
in all upper case letters, table will be skipped. And yes, it is possible, at least in PostgresSQL (why-oh-why?), 
to have two tables with the same name and differing only in case

**EXCLUDE_TABLES**
- comma-delimited string of tables, similar to above, only listing the tables that should NOT be pulled
- if a source db has 50 tables and you want 48 of them, can use this to specifiy only the two do-not-collect tables

**INCLUDE_VIEWS**
 - by default `squark` will only attemp to pull db objects identified by JDBC as type = "TABLE", set this to also get 
 items of type = "VIEW"
 - some sources, e.g. Teradata, provide a majority of their table via views, with direct access to underlying tables 
 prohibited
  
**CONVERT_ARRAYS_TO_STRING** 
- certain db's have array-flavored data types that Vertica won't be able to handle natively, if this is set then any 
columns that are registered as Array type (via Spark and/or JDBC depending on `squark` phase) will have its data cast 
to simple string type
- thus far have only encountered this scenario in postgres

**CONVERT_TIMESTAMPS_TO_AMERICA_NEW_YORK**
- the time-translation effect of pulling datetime values from source database via Spark, persisting in s3, and then 
loading into Vertica is complicated and requires deep analysis
- in the meantime there was enough concern as regards data being loaded from Teradata into mm.com/datalayer Vertica 
clusters to warrant making sure to keep datetime values were persisted from Teradata to these Vertica instances. The 
process was extensively tested, but only for a select few tables, and only as represented in mm.com Vertica. Setting 
this variable performs the necessary timezone translations during the Spark phase.

**WIDE_COLUMNS_MD5**
- added this after running into a table that was consistently failing with StackOverflow error if more than 430 columns 
were present when `add_md5_column(df)` was run. If this env var is set only the first 400 columns will be used to calculate 
the md5 value that gets stored in `_advana_md5` column in Vertica - so it is important to check each row can be uniquely 
identified by the first 400 columns of a given table, which seems likely. If not, rows may receive the same md5 hash value 
despite being unique, assuming the unique data is introduced past the 400th column.
- see AR-268 in JIRA for further details
 
**SKIP_SOURCE_ROW_COUNT**
- by default `squark` will perform a `COUNT(*)` on each source table, before and after the data is pulled, so that a 
later reconciliation check can confirm that the number of rows that made it into Vertica lies between these two values.  
    - which isn't a definitive check, 500 rows could have been deleted and another 500 added while the data pull was 
occurring - but it is a decent start  
    - and the post-row count query will only be executed if the initial query took less than N seconds  
- some source systems (`rilcats` oh my) can take several minutes to multiple hours to return a simple `COUNT(*)`, for 
those jobs this value should be set, given obvious tradeoffs  

**SKIP_MIN_MAX_ON_CAST**
- a function (`conform_any_extreme_decimals(df)`) was added to handle scenarios where some systems were reporting 
decimal/numeric data types with a precision > 37. Part of that fix involved potentially shifting around precision/scale 
values in order to preserve information that would otherwise be lost
- this setting was essentially an escape valve, in case some novel data/data-types/database-systems caused this 
function to blow up. Evidence indicates some tables in Oracle were not playing well here and this variable is set for a 
couple of those jobs
  
**JSON_INFO**
- see [below](#JSON_INFO)
  
<br/>

---

### usually set in jenkins shell
 - mostly for historical reasons where the `squark` was running simultaneously on on-prem and AWS Jenkinses
 - both pulled from the same repo so additional configuration was necessary for the AWS versions  
 
---
**AWS_VERTICA_HOST**  
**AWS_VERTICA_PORT**  
- above relate to the Vertica cluster being populated, at this point think all the instances are available via port 
5433 _within_ the AWS VPC

**SQUARK_NUM_RETRY**
- default is 1; for both the save-to-s3 and load-to-vertica phases this number control how many attempts are made to 
complete the process for a given table, with a `sleep()` in between each attempt. With s3's "evntually consistent" model
it can sometimes help to try, try again.

**SQUARK_BUCKET**
- Arguments to `launch_squark_job.sh` affect the sub-directories into which `squark` writes/loads but this sets the 
base s3 bucket
- Should be either `nonprd-squark-dev` or `dsprd-squark-prd` only

<br/>

----

### JSON_INFO
- extensible method for passing detailed configuration into a given job  
- currently only two keys in use, **SAVE_TABLE_SQL_SUBQUERY** and **PARTITION_INFO**  
- can also be set in the shell but the ways in which both bash and `squark` interpret various quotation characters can 
make it rather tricky  

---
**SAVE_TABLE_SQL_SUBQUERY** 
- normally the pull-data process is executed via Spark with a `SELECT * FROM some_schema.some_table`
- if this is set for a given table, the applicable query will be executed instead. This query is executed directly on 
source db system, so the syntax must match a combination of that system + Spark. Recommended to look at existing jobs 
that share same source system for syntax examples.
- expected usage would involve filtering the rows that are being extracted using some column's value, e.g. only pull 
data with an `UPDATED_DATE >= '2018-01-31'` 
- in the case that `squark` continues to lacks the ability to select a subset of columns from source table a subquery can 
also be used to only pull data from certain columns. Since `squark` will otherwise be ignorant of the subsetting when 
it comes time to create the DDL in Vertica the skipped columns would still need to be present in the `SELECT` query but 
can be set to `NULL` 
    - specifiying column names renders the pipeline more fragile and any unexpected changes in source DDL can cause the 
    job to immediately fail
 
**PARTITION_INFO** 
- see references at http://spark.apache.org/docs/latest/sql-data-sources-jdbc.html* to `partitionColumn`, 
`lowerBound`, `upperBound`, and `numPartitions` for details on the Spark side but short version is that this allows Spark 
to execute parallel reads on source tables, thereby speeding up time it takes to write data to s3  
    \* this links to latest  Spark version and details will likely be different on EMR's Spark, e.g direct support for 
    timestamp and date columns in `partitionColumn` was only added as of Spark 2.4
    - see also notes re **SPARK_MAX_EXECUTORS** in terms of not overstressing the source db  
- general idea is to select a non-NULL column with values that, for a given `numPartitions`/`upperBound`/`lowerBound` 
combination, results in reads with approximately equal durations. The easiest proxy for that is probably number 
of rows, i.e. come up with partitioning parameters that result in queries returning same number of rows per range query
    - under Spark 1.6, and earlier 2.x releases, the documentation specified an **INTEGER** column. That seems to have 
    changed but we will still be restricted by whichever version of Spark is available on the EMR cluster
- the `partitionColumn` would normally be just that, a normal column name providing a good distribution of values. 
Turns out an expression that evaluates to a number also works on Spark 2.2 and earlier, though it must be SQL 
that correctly runs on the source db system  
    - an example of that would be in `haven`, where the PostgresSQL **DATE_PART** command is used to parse integer values 
    (via either second or minute depending on which returns better distribution) from a date-time column  
    - per note above about current (2.4 as of this writing) Spark release, date or timestamp data types may work as-is 
    with newer versions of Spark  
- a subquery filter can also be combined with partitioning, but the filter must not be in **SAVE_TABLE_SQL_SUBQUERY**, 
where it would override any partitioning info. Instead realize that Spark takes the value from `partitionColumn` and 
prepends it with a **WHERE** predicate, follwed by the **BETWEEN** queries that relate to creating the row bins. 
If `partitionColumn` is SQL expression, the first "part" of it can be the filter and the **BETWEEN** queries will 
follow onto the second "part".
    - e.g. below **PARTITION_INFO** snippet
        ```"AGMT_HIST_VW": {
              "partitionColumn": "ISSUE_DT >= """2015-01-01""" AND AGREEMENT_ID",
              "lowerBound": 5700000,
              "upperBound": 40000000,
              "numPartitions": 50
            }  ```  
    
    would result in ~50 separate SQL queries containting text like below (actual numbers are gross estimates)
    ```
        SELECT * FROM AGMT_HIST_VW WHERE ISSUE_DT >= '2015-01-01' AND AGREEMENT_ID < 5700000;
        SELECT * FROM AGMT_HIST_VW WHERE ISSUE_DT >= '2015-01-01' AND AGREEMENT_ID BETWEEN 5700001 AND 6400000;
        SELECT * FROM AGMT_HIST_VW WHERE ISSUE_DT >= '2015-01-01' AND AGREEMENT_ID BETWEEN 6400001 AND 7100000;
        ...  
     ```

- one resource to aid in optimizing partitioning configurations, i.e. row count distributions, can be found in Jupyter 
notebook sql_partitioning.ipynb in [squark-research](https://github.com/massmutual/squark-research/tree/master/jupyter). 
This will only help as-is **if data is already in Vertica**, and isn't exactly user friendly in current state.

<br/>

---

### random vars to be aware of

---
**BUILD_NUMBER**  
**JOB_NAME**
- expectation is that both are automatically set by Jenkins job, as they are for any Jenkins job. They are used within
`squark` to persist certain metadata like row counts and table-save durations

**USE_CLUSTER_EMR**
- yet another artifact of needing to run `squark` in on-prem + AWS locations at same time. Might be considered deprecated 
except for the fact that current code expects it to be positively set. Initial need was driven by different needs for 
2.x releases of Spark, where on-prem was running exclusively on v1.6.

**STAT_SCHEMA_NAME**
- used in conjunction with **ANALYZE STATISTICS** functionality. Below represents the simplest Jenkins shell contents 
necessary to update statistics on all of the tables in a given Vertica schema. Current `squark` will **DROP** the entire 
pre-existing schema, so any statistics related to that would be lost and need to be re-created:
```
    source "./aws-env-cluster.sh"
    export AWS_VERTICA_HOST=$AWS_VERTICA_PROD
    export AWS_VERTICA_PORT=5433
    export STAT_SCHEMA_NAME="haven_daily"
    ./squark-classic/analyze_statistics_standalone.sh
```

**CREATE_PROJECTIONS**  
**FACING_SCHEMA**  
**IS_INCREMENTAL_SCHEMA**  
- three above are NOT to be set externally, is only used by `squark` if relevant arguments are used in call to 
`launch_squark_job.sh`

<br/>

---

### related to incremental mode
- created, targeted, and tested as regards the `haven` schema, which is sourced from a PostgresSQL db
- adapting to other PostgresSQL dbs received from HavenLife shouldn't be too much of a lift  
    - but anything else would require more work
- fullter description of the associated workflow would be document unto itself and as of this writing the process 
hasn't made it out of beta mode
---

**SQUARK_DELETED_TABLE_SUFFIX**

