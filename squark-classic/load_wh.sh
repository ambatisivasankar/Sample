# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail

slug=$1
tmpschema=_"$slug"
schema="$slug"
if [ ${SKIP_SCHEMA} -ne 1 ]
then
    # Drop the existing temp schema, if any.
    aws_vsql="${VERTICA_VSQL} -C -h ${AWS_VERTICA_HOST} -p ${AWS_VERTICA_PORT} -U ${VERTICA_USER} -m require -w ${AWS_VERTICA_PASSWORD} -d advana -c "
    $aws_vsql "drop schema if exists ${tmpschema} cascade;"
    RET_CODE=$?
    if [ ${RET_CODE} -ne 0 ]; then
      die "AN ERROR OCCURRED IN THE DROP SCHEMA... continuing"
    fi
    # Recreate the temp schema.
    $aws_vsql "create schema $tmpschema;"
    RET_CODE=$?
    if [ ${RET_CODE} -ne 0 ]; then
      die "AN ERROR OCCURRED IN THE CREATE SCHEMA... quitting!"
    fi
    
    echo " -- LAUNCHING COPYDDL..."
    "${PYTHON_VENV}"/bin/python "${WORKSPACE}"/squark-classic/copyddl.py
    
    # Load the tables from S3 into Vertica.
    echo " -- LAUNCHING DIRLOAD..."
    "${PYTHON_VENV}"/bin/python "${WORKSPACE}"/squark-classic/dirload.py "$tmpschema" "${WAREHOUSE_DIR}$slug"

    # Now rename the tmpschema to the real schema.
    $aws_vsql "alter schema $schema rename to _${schema}_archive;"
    RET_CODE=$?
    if [ ${RET_CODE} -ne 0 ]; then
      die "AN ERROR OCCURRED IN THE ALTER SCHEMA SCHEMA TO CREATE ARCHIVE... continuing!"
    fi
    $aws_vsql "alter schema $tmpschema rename to $schema;"
    RET_CODE=$?
    if [ ${RET_CODE} -ne 0 ]; then
      die "AN ERROR OCCURRED IN THE ALTER SCHEMA SCHEMA FROM TMP TO PROD... quitting!"
    fi
    $aws_vsql "drop schema if exists _${schema}_archive cascade;"
    RET_CODE=$?
    if [ ${RET_CODE} -ne 0 ]; then
      die "AN ERROR OCCURRED WHILE DROPPING ARCHIVE SCHEMA... continuing!"
    fi
    "${WORKSPACE}"/squark-classic/v_setperms.sh "${schema}"
else
    echo "Skipped the schema & table creation: proceeding to load into the table in the schema '$schema_name'"
    "${PYTHON_VENV}"/bin/python "${WORKSPACE}"/squark-classic/dirload.py "$schema_name" "${WAREHOUSE_DIR}$slug"  
fi



echo "IS_INCREMENTAL_SCHEMA: "$IS_INCREMENTAL_SCHEMA
echo "FACING_SCHEMA: " $FACING_SCHEMA

# TODO: is that best syntax for checking variables? FACING_SCHEMA just care if it is non-empty
if [[ ($IS_INCREMENTAL_SCHEMA = 1 && $FACING_SCHEMA) ]]; then
# path1, _haven_weekly got populated and then rotated in as haven_weekly
#  need to create _haven schema,
#  populate with views that are all SELECT * FROM haven_weekly.TABLE
#  rename _haven to haven

    echo "BEGIN INCREMENTALIZATIONITUDE..."
    facing_tmpschema=_"$FACING_SCHEMA"
    source_schema="$schema"

    # Drop the existing temp facing schema, if any.
    aws_vsql="$VERTICA_VSQL -C -h $AWS_VERTICA_HOST -p $AWS_VERTICA_PORT -U $VERTICA_USER -w $AWS_VERTICA_PASSWORD -d advana -m require -c "
    $aws_vsql "drop schema if exists $facing_tmpschema cascade;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE DROP TEMP FACING SCHEMA... continuing"
    fi
    # Recreate the temp facing schema.
    $aws_vsql "create schema $facing_tmpschema;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE CREATE FACING SCHEMA... quitting!"
            exit 1
    fi

    set -e
    # create the SELECT views in the facing schema
    echo " -- LAUNCHING CREATEVIEWS..."
    $PYTHON_VENV/bin/python "${WORKSPACE}"/squark-classic/createviews.py "${FACING_SCHEMA}" "$source_schema"
    set +e

    # Now rename the temp facing to the real facing schema.
    $aws_vsql "alter schema $FACING_SCHEMA rename to _${FACING_SCHEMA}_archive;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE ALTER FACING SCHEMA SCHEMA TO CREATE ARCHIVE... continuing!"
    fi
    $aws_vsql "alter schema $facing_tmpschema rename to $FACING_SCHEMA;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE ALTER FACING SCHEMA SCHEMA FROM TMP TO PROD... quitting!"
            exit 1
    fi
    $aws_vsql "drop schema if exists _${FACING_SCHEMA}_archive cascade;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED WHILE DROPPING FACING ARCHIVE SCHEMA... continuing!"
    fi

    "${WORKSPACE}"/squark-classic//v_setperms.sh "${FACING_SCHEMA}"

fi
