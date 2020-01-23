
# This should go up to the number of records in the table squark_staging.WRK_LD_BTCH_AGMT_FIN_TXN_CMN_VW_qa
export IDL=1
NUM_LOOPS=${NUM_LOOPS:-125}

./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/count_actual_AGMT_FIN_TXN_CMN_VW.sql
./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/truncate_actual_AGMT_FIN_TXN_CMN_VW.sql

for _ in {1..121}
  do

  export OTHER_VARIABLES="-e -v schema_name=squark_staging -At -o output_file "

  ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/count_staging_AGMT_FIN_TXN_CMN_VW_qa.sql
  ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/truncate_staging_AGMT_FIN_TXN_CMN_VW_qa.sql
  ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/get_batch_dates_AGMT_FIN_TXN_CMN_VW_qa.sql

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
      ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/insert_IDL_batch_AGMT_FIN_TXN_CMN_VW_qa.sql
      ./load_tables.bash "${ENV}" --sql-file=squark-classic/job-sql/${squark_name}/update_batch_dates_AGMT_FIN_TXN_CMN_VW_qa.sql

  fi

done
