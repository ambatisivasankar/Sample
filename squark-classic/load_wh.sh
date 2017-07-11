
slug=$1
#./bootstrap.sh
#source "${SQUARK_ENV:-env.sh}"
#source "jobs/$slug.sh"
tmpschema=_"$slug"
schema="$slug"
# Drop the existing temp schema, if any.
if [ $LOAD_FROM_HDFS ]; then
    vsql="$VERTICA_VSQL -C -h $VERTICA_HOST -U $VERTICA_USER -w $VERTICA_PASSWORD -d $VERTICA_DATABASE -c"
    $vsql "drop schema if exists $tmpschema cascade;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE DROP SCHEMA... continuing"
    fi
    # Recreate the temp schema.
    $vsql "create schema $tmpschema;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE CREATE SCHEMA... quitting!"
            exit 1
    fi
fi
# DO THE SAME THING IS LOAD_FROM_AWS is true:
if [ $LOAD_FROM_AWS ]; then
    # Drop the existing temp schema, if any.
    aws_vsql="$VERTICA_VSQL -C -h $AWS_VERTICA_HOST -p $AWS_VERTICA_PORT -U $VERTICA_USER -w $AWS_VERTICA_PASSWORD -d advana -c"
    $aws_vsql "drop schema if exists $tmpschema cascade;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE DROP SCHEMA... continuing"
    fi
    # Recreate the temp schema.
    $aws_vsql "create schema $tmpschema;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE CREATE SCHEMA... quitting!"
            exit 1
    fi
fi


set -e
# USING PYTHON3 FOR NOW -- Need to update this to use the virtualenv environment soon.
# Copy ddls from the source system
#virt/bin/python copyddl.py
echo " -- LAUNCHING COPYDDL..."
$PYTHON_VENV/bin/python copyddl.py

# Load the tables from HDFS.
#virt/bin/python dirload.py "$tmpschema" "${SQUARK_SRC_DIR:-$SQUARK_TEMP}$slug"
echo " -- LAUNCHING DIRLOAD..."
$PYTHON_VENV/bin/python dirload.py "$tmpschema" "${WAREHOUSE_DIR}$slug"
set +e


# Now rename the tmpschema to the real schema.
if [ $LOAD_FROM_HDFS ]; then
    $vsql "alter schema $schema rename to _${schema}_archive;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE ALTER SCHEMA SCHEMA TO CREATE ARCHIVE... continuing!"
    fi
    $vsql "alter schema $tmpschema rename to $schema;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE ALTER SCHEMA SCHEMA FROM TMP TO PROD... quitting!"
            exit 1
    fi
    $vsql "drop schema if exists _${schema}_archive cascade;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED WHILE DROPPING ARCHIVE SCHEMA... continuing!"
    fi
fi
# DO THE SAME THING FOR AWS FOR THE TIME BEING -- UNTIL SUNSETTING.
if [ $LOAD_FROM_AWS ]; then
    # Now rename the tmpschema to the real schema.
    $aws_vsql "alter schema $schema rename to _${schema}_archive;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE ALTER SCHEMA SCHEMA TO CREATE ARCHIVE... continuing!"
    fi
    $aws_vsql "alter schema $tmpschema rename to $schema;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED IN THE ALTER SCHEMA SCHEMA FROM TMP TO PROD... quitting!"
            exit 1
    fi
    $aws_vsql "drop schema if exists _${schema}_archive cascade;"
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
            echo "AN ERROR OCCURRED WHILE DROPPING ARCHIVE SCHEMA... continuing!"
    fi
    #./v_setperms.sh $schema
fi
./v_setperms.sh $schema
