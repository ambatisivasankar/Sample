slug=$1
./bootstrap.sh
source env.sh
source "jobs/$slug.sh"
tmpschema=_"$slug"
schema="$slug"
oldschema="$slug"_old
vsql="/opt/vertica/bin/vsql -C -h $VERTICA_HOST -U $VERTICA_USER -w $VERTICA_PASSWORD -c"
# Drop the existing temp schema, if any.
$vsql "drop schema $tmpschema cascade;"
# Recreate the temp schema.
$vsql "create schema $tmpschema;"
# Load the tables from HDFS.
virt/bin/python dirload.py "$tmpschema" "$SQUARK_TEMP$slug"
# Now atomically rename the tmpschema to the real schema.
$vsql "drop schema $oldschema cascade;"
$vsql "alter schema $schema rename to $oldschema;"
$vsql "alter schema $tmpschema rename to $schema;"
$vsql "drop schema $oldschema cascade;"
./v_setperms.sh $schema
