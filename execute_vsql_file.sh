set -e
for i in "$@"; do
    case $i in
        -tgt-schema=*)
            TGT_SCHEMA="${i#*=}"
            set_src_schema=1
        ;;
        --stg-schema=*)            
            STG_SCHEMA="${i#*=}"    
            set_stg_schema=1
        ;;
        --output-file=*)
            TABLE_OUT="${i#*=}"
            set_ouput_file=1
        ;;
        --table-name=*)
            TABLE_NAME="${i#*=}"
            set_table_name=1
        ;;
        --file-name=*)
            FILE_NAME="${i#*=}"
            set_file_name=1
        ;;
        *)
            # Unknown option -- assume to be job_name
            JOB_FILE_NAME=${JOB_FILE_NAME}' '${i}
        ;;
    esac
done
if [ $set_file_name -ne 1 ]
then
    echo "File Name not passed"
    exit 1
fi


echo $FILE_NAME
export TABLE_OUT=${TABLE_NAME}.sql.out
echo "Executing VSQL for " $FILE_NAME
cat $FILE_NAME
vsql -m require -e -f $FILE_NAME -o $TABLE_OUT -h $AWS_VERTICA_HOST -p $AWS_VERTICA_PORT -U $AWS_VERTICA_USER -w $AWS_VERTICA_PASSWORD -d $VERTICA_DATABASE -v ON_ERROR_STOP=1 -v STG_SCHEMA=${STG_SCHEMA} -v TGT_SCHEMA=${TGT_SCHEMA} -v SOURCE_SCHEMA=${SOURCE_SCHEMA} ${OTHER_VARIABLES}
