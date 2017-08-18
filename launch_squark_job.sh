#!/bin/bash
# This script is used to launch a squark job.
# it accepts and arg to determine whether this is prod or dev.
# Setting a dev or prod will determine which WAREHOUSE_DIR and VERTICA_CONNECTION_ID are to be used.
set -e

VERTICA_CONNECTION_ID="vertica_dev"
VERTICA_HOST="vertica-dev"
WH_DIR="/_wh_dev/"
HELP=NO
#SKIP_HDFS_LOAD=NO
#SKIP_VERTICA_LOAD=NO
SQUARK_TYPE=squark-dev
for i in "$@"; do
    case $i in
        --dev|--develop)
            VERTICA_CONNECTION_ID="vertica_dev"
            VERTICA_HOST="vertica-dev"
            WH_DIR="/_wh_dev/"
            SQUARK_TYPE=squark-dev
            TMP_SQUARK_WAREHOUSE="/wh_dev/"
            TMP_SQUARK_ARCHIVE="/wh_dev_archive/"
        ;;
        --prod|--production)
            VERTICA_CONNECTION_ID="vertica_prod"
            VERTICA_HOST="vertica"
            WH_DIR="/_wh/"
            SQUARK_TYPE=squark-prod
            TMP_SQUARK_WAREHOUSE="/wh/"
            TMP_SQUARK_ARCHIVE="/wh_archive/"
        ;;
        --test)
            VERTICA_CONNECTION_ID="test_vertica"
            VERTICA_HOST="vertica"
            WH_DIR="/_wh_dev/"
            SQUARK_TYPE=squark-test
            TMP_SQUARK_WAREHOUSE="/wh_dev/"
            TMP_SQUARK_ARCHIVE="/wh_dev_archive/"
        ;;
        -h|--help|help)
            HELP=YES
        ;;
        -w=*|--warehouse-dir=*)
            WH_DIR="${i#*=}"
        ;;
        -v=*|--vertica-id=*)
            VERTICA_CONNECTION_ID="${i#*=}"
            CUSTOM_VERT_CONN_ID=1
        ;;
        -s=*|--s3-id=*)
            S3_CONNECTION_ID="${i#*=}"
        ;;
        --skip-hdfs-load)
            SKIP_HDFS_LOAD=YES
        ;;
        --skip-vertica-load)
            SKIP_VERTICA_LOAD=YES
        ;;
       --use-aws)
           USE_AWS=1
        ;;
       --use-hdfs)
           USE_HDFS=1
        ;;
       --load-from-aws)
           LOAD_FROM_AWS=1
        ;;
       --load-from-hdfs)
           LOAD_FROM_HDFS=1
        ;;
       --force-cutover)
           FORCE_CUTOVER=1
        ;;
       --skip-cutover)
           SKIP_CUTOVER=1
        ;;
        *)
            # Unknown option -- assume to be job_name
            JOB_FILE_NAME=${i}
        ;;
    esac
done

if [ $HELP == YES ]; then
    echo "launch_squark_job.sh USAGE:"
    echo "  ./launch_squark_job.sh [[-v=*] [-w=*]] [-h] [[--dev|--prod]] <job_name>"
    echo "This script will launch a squark job. The default behaviour is to launch a dev job."
    echo "MAKE SURE to include the job name so the script knows which file to source."
    echo "Default values are:"
    echo " VERTICA_CONNECTION_ID='vertica_dev'"
    echo " WAREHOUSE_DIR='/_wh_dev/'"
    echo "If you want to run a prod job, just include --prod or --production as an argument."
    echo "The script also accepts --dev or --develop to run a dev job."
    echo "The below options can be used to override the default dev and prod variables if needed."
    echo "However, you cannot override and select an environment (dev|prod). The last value will always trump the first ones."
    echo "OPTIONS:"
    echo " -v | --vertica-id     : Override the default vertica_connection_id with value passed."
    echo " -w | --warehouse-dir  : Override the default warehouse_dir with the value passed."
    echo " --dev | --develop     : Set default values for a dev job."
    echo " --prod | --production : Set default values for a prod job." 
    echo " --skip-hdfs-load      : Skip the loading of the data into hdfs."
    echo " --skip-vertica-load   : Skip the loading of the data into vertica."
    echo " --use-aws             : Save data to aws s3."
    echo " --use-hdfs            : Save data to HDFS (if neither use-aws or use-hdfs is supplied, this is default)."
    echo " --load-from-aws       : Load data from aws s3 into aws vertica."
    echo " --load-from-hdfs      : Load data from HDFS into onprem vertica."
    echo " --force-cutover       : The wh_cutover will only happen if a full run occurs, or this flag is specified."
    exit 0
fi

if [ -z ${JOB_FILE_NAME+x} ]; then
    echo "ERROR! : NO JOB NAME WAS FOUND! PLEASE SUPPLY A JOB NAME WITH THE COMMAND."
    echo " -- The job name will be the name of the bash file in the jobs directory in squark-classic."
    echo "Quitting..."
    exit 1
fi

cd squark-classic
source jobs/${JOB_FILE_NAME}.sh

if [[ ( -z $LOAD_FROM_HDFS && -z $LOAD_FROM_AWS ) ]]; then
    LOAD_FROM_HDFS=1
fi


# Set the vertica options based on run type:
if [[ ( -z $CUSTOM_VERT_CONN_ID && $USE_AWS ) ]]; then
    # If we are using aws and no custom vertica id has been set
    if [ $SQUARK_TYPE == "squark-dev" ]; then
        # Set aws vertica dev properties.
        VERTICA_CONNECTION_ID="vertica_aws_nprd"
    elif [ $SQUARK_TYPE == "squark-prod" ]; then
        VERTICA_CONNECTION_ID="vertica_aws"
    fi
fi
# Set the vertica options based on run type:
#if [ $USE_AWS ]; then
#    # If we are using aws and no custom vertica id has been set
#    if [ $SQUARK_TYPE == "squark-dev" ]; then
#        # Set aws vertica dev properties.
#        S3_CONNECTION_ID="s3_nprd"
#    elif [ $SQUARK_TYPE == "squark-prod" ]; then
#        S3_CONNECTION_ID="s3_prd"
#    fi
#fi

export VERTICA_CONNECTION_ID=$VERTICA_CONNECTION_ID
export WAREHOUSE_DIR=$WH_DIR
export SQUARK_TYPE=$SQUARK_TYPE
export VERTICA_HOST=$VERTICA_HOST
export USE_AWS=$USE_AWS
export USE_HDFS=$USE_HDFS
export LOAD_FROM_AWS=$LOAD_FROM_AWS
export LOAD_FROM_HDFS=$LOAD_FROM_HDFS
export SQUARK_TEMP=$WAREHOUSE_DIR
export S3_CONNECTION_ID=$S3_CONNECTION_ID
if [ -z $SQUARK_WAREHOUSE ]; then
    export SQUARK_WAREHOUSE=$TMP_SQUARK_WAREHOUSE
fi
if [ -z $SQUARK_ARCHIVE ]; then
    export SQUARK_ARCHIVE=$TMP_SQUARK_ARCHIVE;
fi
if [ -z $JENKINS_URL ]; then
    export JENKINS_URL=https://advana-jenkins.private.massmutual.com
fi

echo "====================================================="
echo "RUNNING SQUARK WITH THE FOLLOWING VALUES:"
echo " -- VERTICA_CONNECTION_ID: $VERTICA_CONNECTION_ID"
echo " -- VERTICA_HOST: $VERTICA_HOST"
echo " -- WAREHOUSE_DIR: $WAREHOUSE_DIR"
echo " -- JOB_FILE_NAME: $JOB_FILE_NAME"
echo " -- SQUARK_TYPE: $SQUARK_TYPE"
echo "-------- CUTOVER DIRS INFO ----------"
echo " -- SQUARK_WAREHOUSE: $SQUARK_WAREHOUSE"
echo " -- SQUARK_TEMP: $SQUARK_TEMP"
echo " -- SQUARK_ARCHIVE: $SQUARK_ARCHIVE"
echo "====================================================="

if [ -z $SKIP_HDFS_LOAD ]; then
    echo " --- Running Loading data"
    ./run.sh
else
    echo " --- SKIPPING LOADING DATA INTO HDFS!"
fi

if [ -z $SKIP_VERTICA_LOAD ]; then
    echo " --- Running Load wh..."
    ./load_wh.sh ${JOB_FILE_NAME}
else
    echo " --- SKIPPING LOADING DATA INTO VERTICA!"
fi

# Do the cutover
# NOTE: Only run if neither skip hdfs or vertica options are given, or the --force-cutover option is given
echo "Checking values for cutover:"
echo "SKIP HDFS: $SKIP_HDFS_LOAD"
echo "SKIP VERTICA: $SKIP_VERTICA_LOAD"
echo "FORCE CUTOVER: $FORCE_CUTOVER"
echo "SKIP CUTOVER: $SKIP_CUTOVER"
if [[ ( -z $SKIP_HDFS_LOAD && -z $SKIP_VERTICA_LOAD && -z $USE_AWS ) || $FORCE_CUTOVER ]]; then
    if [ -z $SKIP_CUTOVER ]; then
        echo "Running the CUTOVER script..."
        $PYTHON_VENV/bin/python wh_dir_cutover.py $JOB_FILE_NAME
    fi
fi

# Run only if SKIP_SOURCE_ROW_COUNT. Practically, probably won't be helpful for skip-vertica jobs but can adjust in future.
echo "Row count reconciliation:"
echo "SKIP_SOURCE_ROW_COUNT: $SKIP_SOURCE_ROW_COUNT"
echo "SKIP VERTICA: $SKIP_VERTICA_LOAD"
if [[ ( -z $SKIP_VERTICA_LOAD && -z $SKIP_SOURCE_ROW_COUNT ) ]]; then
    if [ $LOAD_FROM_AWS ]; then
        vsql="$VERTICA_VSQL -C -h $AWS_VERTICA_HOST -p $AWS_VERTICA_PORT -U $VERTICA_USER -w $AWS_VERTICA_PASSWORD -d $VERTICA_DATABASE -f "
    else
        vsql="$VERTICA_VSQL -C -h $VERTICA_HOST -U $VERTICA_USER -w $VERTICA_PASSWORD -d $VERTICA_DATABASE -f "
    fi
        results_file="row_count_results.out"
        $vsql ./resources/row_count_reconciliation.sql -v VERTICA_SCHEMA="'$JOB_FILE_NAME'" -v JOB_NAME="'$JOB_NAME'" -o $results_file
        cat $results_file

        marker_text="<<<<"
        if grep -q $marker_text $results_file; then
            attachment=$(cat $results_file | grep $marker_text | tr -d ' ' | awk  'BEGIN { FS="|"; OFS="";} { print "- ",$1,".",$2; }')
            json=$(cat<<-EOM
            payload={
                "channel": "#ingest_alerts",
                "username": "webhookbot",
                "text": "JOB COMPLETED: $JOB_NAME, see <$BUILD_URL/consoleFull|jenkins log>",
                "icon_emoji": ":ingestee:",
                "attachments": [
                    {
                        "fallback": "row count reconciliation issues in this job",
                        "color": "danger",
                        "pretext": "*** source vs. Vertica row count reconciliation reported issues in below tables ***",
                        "title": "click for build $BUILD_NUMBER log",
                        "title_link": "$BUILD_URL/consoleFull#footer",
                        "text": "$attachment",
                        "footer": "LOAD_FROM_AWS: $LOAD_FROM_AWS"
                    }
                ]
            }
EOM
)
            curl -X POST --data-urlencode "$json" https://hooks.slack.com/services/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
        fi
fi
