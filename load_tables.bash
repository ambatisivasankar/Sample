#!/bin/bash


############################################################################################
############################################################################################

#                                   VERTICA DEFINITION 
#export VERTICA_USER=
#export AWS_VERTICA_PASSWORD=

if [ $1 == 'dev' ]
then
    export AWS_VERTICA_HOST='vertica-edw-dev.dsawsnprd.massmutual.com'
elif [ $1 == 'qa' ]
then
    export AWS_VERTICA_HOST='vertica-edw-qa.dsawsnprd.massmutual.com'
elif [ $1 == 'prod' ]
then
    export AWS_VERTICA_HOST='vertica-edw-prod.dsawsprd.massmutual.com'
else
    export AWS_VERTICA_HOST='vertica-edw-prod.dsawsprd.massmutual.com'
fi


export VERTICA_DATABASE=advana
export AWS_VERTICA_PORT=443

echo "AWS_VERTICA_USER" $AWS_VERTICA_USER
echo "AWS_VERTICA_PASSWORD" $AWS_VERTICA_PASSWORD
############################################################################################
############################################################################################
#               VSQL DEFINITION, SCHEMA , VARIABLES
############################################################################################
############################################################################################

export STG_SCHEMA=${STG_SCHEMA:='SQUARK_STAGING'}
export TGT_SCHEMA=${TGT_SCHEMA:=$squark_name}
export JOB_NAME=${JOB_NAME:=$squark_name}
export OTHER_VARIABLES=${OTHER_VARIABLES:=""}


export retry_ind=${retry_ind:=0}
export SQUARK_ADVANA=${SQUARK_ADVANA:=''}


for i in "$@"; do
    case $i in
        --tgt-schema=*)
            TGT_SCHEMA="${i#*=}"
            set_tgt_schema=1
        ;;
        --stg-schema=*)            
            STG_SCHEMA="${i#*=}"    
            set_stg_schema=1
        ;;
        --sql-file=*)
            SQL_FILE="${i#*=}"
            set_sql_file=1
        ;;
        --table-list)
            set_table_list=1
        ;;
        --load-type=*)
            set_load_type=1
            set_load_type="${i#*=}"
        ;;
        *)
            # Unknown option -- assume to be job_name
            JOB_FILE_NAME=${squark_name}
        ;;
    esac
done
OIFS=$IFS;
IFS=,

Table_failure=${TGT_SCHEMA}.out
file_path=${SQUARK_ADVANA}squark-classic/job-sql/$JOB_NAME
rm -f ${Table_failure}
touch $Table_failure
if [ $set_table_list ]
then
    TABLE_ARRAY=($INCLUDE_TABLES);
    for ((i=0; i<${#TABLE_ARRAY[@]}; ++i));
    do
            sh execute_vsql_file.sh --file-name=$file_path/${TABLE_ARRAY[$i]}_MRG.sql --table-name=${TABLE_ARRAY[$i]} > ${TABLE_ARRAY[$i]}.out
            exec_status=$?
        echo $exec_status
        if [ $exec_status -ne 0 ]
        then 
            echo "job Loading Failed for the table ${TABLE_ARRAY[$i]} and continuing for the reamining tables"
            echo "Job Loading Failed for the table ${TABLE_ARRAY[$i]}">>$Table_failure
        else 
            echo "Successful for table ${TABLE_ARRAY[$i]}"
        fi
    done
    fail_count=`cat $Table_failure | wc -l`
    echo "Table Failure" $fail_count
    if [ $fail_count -ne 0 ]
    then 
            cat $Table_failure
        echo "Exiting with Failure exit -1"
        exit 1
    else
        echo "Exiting after table loading completion exit 0"
        exit 0
    fi
elif [ $set_sql_file ]
then
    if [ -f $SQL_FILE ]; then
      echo "File exists."
    else
      echo "File does not exist. Exiting"
      exit 1
    fi
    sh execute_vsql_file.sh --file-name=$SQL_FILE --table-name=$SQL_FILE  > $SQL_FILE.out
    exec_status=$?
    cat ${SQL_FILE}.out
    echo $exec_status
    if [ $exec_status -ne 0 ] 
    then 
        echo "Failure in execution $SQL_FILE"
        exit 1
    else
        exit 0
    fi
fi
