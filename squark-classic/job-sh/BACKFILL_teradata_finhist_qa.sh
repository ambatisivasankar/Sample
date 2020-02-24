# This process relies on a dates.txt file that looks like this:
#
# '''2006-01-02''','''2006-01-02''','''2006-01-02'''
# '''2007-06-05''','''2007-06-12''','''2007-11-11'''
#
# The dates.txt file can be generated using `rdbv-dates`, a pyhon program in the MM artifactory
# which currently only supports Terdata<->Vertica
while IFS= read -r line; do
    echo "Dates to process: $line"
    export BACKFILL=1
    export DATES=$line
    export OTHER_VARIABLES="-e -v schema_name=squark_staging -At -o output_file "

    ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/truncate_staging_AGMT_FIN_TXN_CMN_VW_qa.sql
    ./launch_squark_job.sh --"${ENV}" --parquet --skip-schema ${squark_name}
    ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/insert_IDL_batch_AGMT_FIN_TXN_CMN_VW_qa.sql
 done < dates.txt
