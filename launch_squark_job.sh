#!/usr/bin/env bash
###############################################################
## Launhch squark job
# This script is used to launch a squark job.
# it accepts and arg to determine whether this is prod or dev.
# Setting a dev or prod will determine which WAREHOUSE_DIR and VERTICA_CONNECTION_ID are to be used.

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x
set -e


# Load up some utility functions
# shellcheck source=./scripts/shell_utils.sh
source "${WORKSPACE}/scripts/shell_utils.sh"

printLineMsg before "Launching Squark Job"
HELP="NO"



for i in "$@"; do
    case $i in
        --dev|--develop)
            # shellcheck source=./scripts/environemnt_settings/env_dev.sh
            source "${ENV_SETTINGS_DIR}/env_dev.sh"
        ;;
        --qa)
            # shellcheck source=./scripts/environemnt_settings/env_qa.sh
            source "${ENV_SETTINGS_DIR}/env_qa.sh"
        ;;
        --prod|--production)
            # shellcheck source=./scripts/environemnt_settings/env_prod.sh
            source "${ENV_SETTINGS_DIR}/env_prod.sh"
        ;;
        -h|--help|help)
            HELP=YES
        ;;
        -v=*|--vertica-id=*)
            #VERTICA_CONNECTION_ID="${i#*=}"
            # Now set in (dev|qa|prod))
        ;;
        -s=*|--s3-id=*)
            #S3_CONNECTION_ID="${i#*=}"
            # Now set in  (dev|qa|prod)
        ;;
        --use-aws|--use-hdfs)
            # Do nothing
        ;;
        --load-from-aws|--load-from-hdfs)
            # Do nothing
        ;;
        --facing-schema=*)
            FACING_SCHEMA="${i#*=}"
        ;;
        --skip-export|--skip-hdfs-load)
            SKIP_EXPORT=1
        ;;
        --skip-load|--skip-vertica-load)
            SKIP_LOAD=1
        ;;
        --create-projections)
            CREATE_PROJECTIONS=1
        ;;
        --make_ddl_from_target)
            MAKE_DDL_FROM_TARGET=1
        ;;
        --skip-schema)
           SKIP_SCHEMA=1
        ;;
        --incr-load)
           SKIP_SCHEMA=1
           INCR_LOAD=1
        ;;
        --parquet)
            WRITE_FORMAT=parquet
        ;;
        --orc)
            WRITE_FORMAT=orc
        ;;
        *)
            # Unknown option -- assume to be job_name
            JOB_FILE_NAME=${i}
        ;;
    esac
done

if [ $HELP == "YES" ]; then
# shellcheck source=./scripts/launch_squark_job/launch_squark_job_help.sh
source "${LAUNCH_SQUARK_DIR}/launch_squark_job_help.sh"
fi

if [ -z ${schema_name+x} ]; then
    echo "Variable schema_name is not set and so assigning schema_name='$JOB_FILE_NAME'"
    export schema_name=${JOB_FILE_NAME}
else
    echo "schema_name= '${schema_name}'"
    echo "Jobname = '${JOB_FILE_NAME}'"
fi


export SKIP_EXPORT=${SKIP_EXPORT:-0}
export SKIP_LOAD=${SKIP_LOAD:-0}
export SKIP_SCHEMA=${SKIP_SCHEMA:-0}
export CREATE_PROJECTIONS=${CREATE_PROJECTIONS:-0}
export MAKE_DDL_FROM_TARGET=${MAKE_DDL_FROM_TARGET:-0}
export FACING_SCHEMA=${FACING_SCHEMA:+x}
export IS_INCREMENTAL_SCHEMA=${IS_INCREMENTAL_SCHEMA:-0}
export SKIP_SOURCE_ROW_COUNT=${SKIP_SOURCE_ROW_COUNT:-0}
export WRITE_FORMAT=${WRITE_FORMAT:-orc}

# Check if squark job name is available
# shellcheck source=./scripts/launch_squark_job/check_job_name_available.sh
source "${LAUNCH_SQUARK_DIR}/check_job_name_available.sh"

# Check if squark job name is available
# shellcheck source=./scripts/launch_squark_job/check_job_name_exists.sh
source "${LAUNCH_SQUARK_DIR}/check_job_name_exists.sh"

# Load variables from job file
# shellcheck source=.//squark-classic/jobs/${JOB_FILE_NAME}.sh
source "${WORKSPACE}/squark-classic/jobs/${JOB_FILE_NAME}.sh"

printLineMsg before "Running squark with the following values:"
echo "--- VERTICA_CONNECTION_ID: ${VERTICA_CONNECTION_ID}"
echo "--- VERTICA_HOST: ${AWS_VERTICA_HOST}"
echo "--- JOB_FILE_NAME: ${JOB_FILE_NAME}"
echo "--- SQUARK_TYPE: ${SQUARK_TYPE}"
printLine

printLine
if [ ${SKIP_EXPORT} -eq 0 ]; then
    echo "--- Exporting data to S3"
    "${WORKSPACE}"/squark-classic/run.sh
else
    echo "--- Skipping export to S3"
fi
printLine


printLine
if [ ${SKIP_LOAD} -eq 0 ]; then
    echo "--- Loading data to vertica"
    "${WORKSPACE}"/squark-classic/load_wh.sh ${JOB_FILE_NAME}
else
    echo "--- Skipping load to Vertica"
fi
printLine

printLine
if [[ ( ${SKIP_EXPORT} -eq 0 && ${SKIP_SOURCE_ROW_COUNT} -eq 1 ) ]]; then
    echo "--- Row count Reconcilliation"
    # shellcheck source=./scripts/launch_squark_job/row_count_reconciliation.sh
    source "${LAUNCH_SQUARK_DIR}"/row_count_reconciliation.sh ${JOB_FILE_NAME}
else
    echo "--- No row count reconciliation"
fi

printLineMsg after "Squark Job Done"
