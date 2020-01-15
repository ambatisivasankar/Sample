
# This should go up to the number of records in the table squark_staging.WRK_LD_BTCH_AGMT_FIN_TXN_CMN_VW
export IDL=1
for _ in {1..121}
  do

  export OTHER_VARIABLES="-e -v schema_name=squark_staging -At -o output_file "

  ./load_tables.bash dev --sql-file=squark-classic/job-sql/${squark_name}/truncate_AGMT_FIN_TXN_CMN_VW.sql
  ./load_tables.bash dev --sql-file=squark-classic/job-sql/${squark_name}/get_batch_dates_AGMT_FIN_TXN_CMN_VW.sql

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
      ./launch_squark_job.sh --dev --parquet --skip-schema ${squark_name}
      ./load_tables.bash dev --sql-file=squark-classic/job-sql/${squark_name}/insert_batch_AGMT_FIN_TXN_CMN_VW.sql
      ./load_tables.bash dev --sql-file=squark-classic/job-sql/${squark_name}/update_batch_dates_AGMT_FIN_TXN_CMN_VW.sql
  fi

done
