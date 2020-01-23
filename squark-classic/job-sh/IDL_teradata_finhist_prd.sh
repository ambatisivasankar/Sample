
# This should go up to the number of records in the table squark_staging.WRK_LD_BTCH_AGMT_FIN_TXN_CMN_VW_prd
export IDL=1
NUM_LOOPS=${NUM_LOOPS:-125}
TRUNCATE=${TRUNCATE:-0}
if ((TRUNCATE == 1))
  then
    echo "WILL TRUNCATE ACTUAL TABLE BEFORE IDL"
    ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/count_actual_AGMT_FIN_TXN_CMN_VW.sql
    ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/truncate_actual_AGMT_FIN_TXN_CMN_VW.sql
  else
    echo "NOT TRUNCATING ACTUAL TABLE"
fi

for ((i=0;i<=NUM_LOOPS; i++));
  do
    echo "Loop Number: ${i}"
    export OTHER_VARIABLES="-e -v schema_name=squark_staging -At -o output_file "

    ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/count_staging_AGMT_FIN_TXN_CMN_VW_prd.sql
    ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/truncate_staging_AGMT_FIN_TXN_CMN_VW_prd.sql
    ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/get_batch_dates_AGMT_FIN_TXN_CMN_VW_prd.sql

    export strt_dt=`cat output_file | cut -d'|' -f2`
    export end_dt=`cat output_file | cut -d'|' -f3`

    echo "Start date=${strt_dt}"
    echo "End date=${end_dt}"

  if [ -z "${strt_dt}" ]
    then
      echo "No start date - skipping IDL"
      exit 0
    else
      export OTHER_VARIABLES="-e -v schema_name=squark_staging -At -o output_file -v strt_dt='${strt_dt}' -v end_dt='${end_dt}'"
      ./launch_squark_job.sh --"${ENV}" --parquet --skip-schema ${squark_name}
      ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/insert_IDL_batch_AGMT_FIN_TXN_CMN_VW_prd.sql
      ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/update_batch_dates_AGMT_FIN_TXN_CMN_VW_prd.sql

  fi

done
