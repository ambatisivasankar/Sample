set -x
tmpschema=$1
if [ $LOAD_FROM_HDFS ]; then
    vsql="$VERTICA_VSQL -C -h $VERTICA_HOST -U $VERTICA_USER -w $VERTICA_PASSWORD -d $VERTICA_DATABASE -c "
    # Recreate the temp schema.
    $vsql "create role $tmpschema;" || true
    $vsql "grant usage on schema $tmpschema to $tmpschema;" || true
    $vsql "grant select on schema $tmpschema to $tmpschema;" || true
    $vsql "grant select on all tables in schema $tmpschema to $tmpschema;" || true
    
    $vsql "create role $(echo $tmpschema)_admin;" || true
    $vsql "grant usage on schema $tmpschema to $(echo $tmpschema)_admin;" || true
    $vsql "grant all on schema $tmpschema to $(echo $tmpschema)_admin;" || true
    $vsql "grant all on all functions in schema $tmpschema to $(echo $tmpschema)_admin;" || true
    $vsql "grant all on all sequences in schema $tmpschema to $(echo $tmpschema)_admin;" || true
    $vsql "grant all on all tables in schema $tmpschema to $(echo $tmpschema)_admin;" || true
    $vsql "alter schema $tmpschema default include privileges;" || true
    
    $vsql "create role ${tmpschema}_view;" || true
    $vsql "grant usage on schema ${tmpschema} to ${tmpschema}_view;" || true
    $vsql "grant select on schema ${tmpschema} to ${tmpschema}_view;" || true
    $vsql "grant select on all tables in schema ${tmpschema} to ${tmpschema}_view;" || true
fi

if [ $LOAD_FROM_AWS ]; then
    aws_vsql="$VERTICA_VSQL -C -h $AWS_VERTICA_HOST -p $AWS_VERTICA_PORT -U $VERTICA_USER -w $AWS_VERTICA_PASSWORD -d advana -c "
    # Recreate the temp schema.
    $aws_vsql "create role $tmpschema;" || true
    $aws_vsql "grant usage on schema $tmpschema to $tmpschema;" || true
    $aws_vsql "grant select on schema $tmpschema to $tmpschema;" || true
    $aws_vsql "grant select on all tables in schema $tmpschema to $tmpschema;" || true
    
    $aws_vsql "create role $(echo $tmpschema)_admin;" || true
    $aws_vsql "grant usage on schema $tmpschema to $(echo $tmpschema)_admin;" || true
    $aws_vsql "grant all on schema $tmpschema to $(echo $tmpschema)_admin;" || true
    $aws_vsql "grant all on all functions in schema $tmpschema to $(echo $tmpschema)_admin;" || true
    $aws_vsql "grant all on all sequences in schema $tmpschema to $(echo $tmpschema)_admin;" || true
    $aws_vsql "grant all on all tables in schema $tmpschema to $(echo $tmpschema)_admin;" || true
    $aws_vsql "alter schema $tmpschema default include privileges;" || true
    
    $aws_vsql "create role ${tmpschema}_view;" || true
    $aws_vsql "grant usage on schema ${tmpschema} to ${tmpschema}_view;" || true
    $aws_vsql "grant select on schema ${tmpschema} to ${tmpschema}_view;" || true
    $aws_vsql "grant select on all tables in schema ${tmpschema} to ${tmpschema}_view;" || true
fi
