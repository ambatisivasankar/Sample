#!/bin/bash
# This script is used to launch a squark job.
# it accepts and arg to determine whether this is prod or dev.
# Setting a dev or prod will determine which WAREHOUSE_DIR and VERTICA_CONNECTION_ID are to be used.
set -e

VERTICA_CONNECTION_ID="vertica_dev"
VERTICA_HOST="vertica-dev"
WH_DIR="/_wh_dev/"
HELP=NO
SKIP_HDFS_LOAD=NO
SKIP_VERTICA_LOAD=NO
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
            JOB_NAME=${i}
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

if [ -z ${JOB_NAME+x} ]; then
    echo "ERROR! : NO JOB NAME WAS FOUND! PLEASE SUPPLY A JOB NAME WITH THE COMMAND."
    echo " -- The job name will be the name of the bash file in the jobs directory in squark-classic."
    echo "Quitting..."
    exit 1
fi

cd squark-classic
source jobs/${JOB_NAME}.sh

export VERTICA_CONNECTION_ID=$VERTICA_CONNECTION_ID
export WAREHOUSE_DIR=$WH_DIR
export SQUARK_TYPE=$SQUARK_TYPE
export VERTICA_HOST=$VERTICA_HOST
export USE_AWS=$USE_AWS
export USE_HDFS=$USE_HDFS
export LOAD_FROM_AWS=$LOAD_FROM_AWS
export LOAD_FROM_HDFS=$LOAD_FROM_HDFS
export SQUARK_TEMP=$WAREHOUSE_DIR
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
echo " -- JOB_NAME: $JOB_NAME"
echo " -- SQUARK_TYPE: $SQUARK_TYPE"
echo "-------- CUTOVER DIRS INFO ----------"
echo " -- SQUARK_WAREHOUSE: $SQUARK_WAREHOUSE"
echo " -- SQUARK_TEMP: $SQUARK_TEMP"
echo " -- SQUARK_ARCHIVE: $SQUARK_ARCHIVE"
echo "====================================================="

if [ $SKIP_HDFS_LOAD == YES ]; then
    echo " --- SKIPPING LOADING DATA INTO HDFS!"
fi
if [ $SKIP_HDFS_LOAD == NO ]; then
    ./run.sh
fi

if [ $SKIP_VERTICA_LOAD == YES ]; then
    echo " --- SKIPPING LOADING DATA INTO VERTICA!"
fi
if [ $SKIP_VERTICA_LOAD == NO ]; then
    ./load_wh.sh ${JOB_NAME}
fi

# Do the cutover
# NOTE: Only run if neither skip hdfs or vertica options are given, or the --force-cutover option is given
if [[ ( -z $SKIP_HDFS_LOAD && -z $SKIP_VERTICA_LOAD ) || $FORCE_CUTOVER ]]; then
    if [ -z $SKIP_CUTOVER ]; then
        echo "Running the CUTOVER script..."
        $PYTHON_VENV/bin/python wh_dir_cutover.py $JOB_NAME
    fi
fi
