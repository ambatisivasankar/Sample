set -x
schema=$1
vsql="/opt/vertica/bin/vsql -C -h $VERTICA_HOST -U $VERTICA_USER -m require -w $VERTICA_PASSWORD -c "
# Recreate the temp schema.
$vsql "create role $schema;" || true
$vsql "grant usage on schema $schema to $schema;" || true
$vsql "grant select on schema $schema to $schema;" || true
$vsql "grant select on all tables in schema $schema to $schema;" || true

$vsql "create role $(echo $schema)_admin;" || true
$vsql "grant usage on schema $schema to $(echo $schema)_admin;" || true
$vsql "grant all on schema $schema to $(echo $schema)_admin;" || true
$vsql "grant all on all functions in schema $schema to $(echo $schema)_admin;" || true
$vsql "grant all on all sequences in schema $schema to $(echo $schema)_admin;" || true
$vsql "grant all on all tables in schema $schema to $(echo $schema)_admin;" || true
$vsql "alter schema $schema default include privileges;" || true
