slug=$1
./bootstrap.sh
source env.sh
tmpschema=_"$slug"
schema="$slug"
# Drop the existing temp schema, if any.
if [ $LOAD_FROM_HDFS ]; then
    vsql="/opt/vertica/bin/vsql -C -h $VERTICA_HOST -U $VERTICA_USER -m require -w $VERTICA_PASSWORD -c "
    $vsql "drop schema $tmpschema cascade;"
    # Recreate the temp schema.
    $vsql "create schema $tmpschema;"
    $vsql "create role $tmpschema;"
    $vsql "grant usage on schema $tmpschema to $schema;"
    $vsql "grant select on schema $tmpschema to $schema;"
    $vsql "grant select on all tables in schema $tmpschema to $schema;"
    
    $vsql "create role $(echo $schema)_admin;"
    $vsql "grant usage on schema $tmpschema to $(echo $schema)_admin;"
    $vsql "grant all on schema $tmpschema to $(echo $schema)_admin;"
    $vsql "grant all on all functions in schema $tmpschema to $(echo $schema)_admin;"
    $vsql "grant all on all sequences in schema $tmpschema to $(echo $schema)_admin;"
    $vsql "grant all on all tables in schema $tmpschema to $(echo $schema)_admin;"
    $vsql "alter schema $tmpschema default include privileges;"
    
    # Create the tables.
    
    # First acxiom. NOTE: if acxiom's output filename changes, this will need to be updated.
    $vsql "create table $tmpschema.acxiom(MAINTENANCE_KEY_10050 char(10), entity_id int);"
    # Then the others. 
    for path in $(hdfs dfs -ls /data/splinkr/raw_with_eid | awk '{print $8}' | grep -v acxiom); do  
      table="$(basename $path)"
      $vsql "create table $tmpschema.$table(_advana_id int, entity_id int);"
    done
    
    # Load the tables from HDFS.
    virt/bin/python dirload.py "$tmpschema" "$SPLINKR_EIDS"
    # Now rename the tmpschema to the real schema.
    $vsql "drop schema $schema cascade;"
    $vsql "alter schema $tmpschema rename to $schema;"
fi

if [ $LOAD_FROM_AWS ]; then
    vsql="$VERTICA_VSQL -C -h $AWS_VERTICA_HOST -p $AWS_VERTICA_PORT -U $VERTICA_USER -m require -w $AWS_VERTICA_PASSWORD -d advana -c "
    $vsql "drop schema $tmpschema cascade;"
    # Recreate the temp schema.
    $vsql "create schema $tmpschema;"
    $vsql "create role $tmpschema;"
    $vsql "grant usage on schema $tmpschema to $schema;"
    $vsql "grant select on schema $tmpschema to $schema;"
    $vsql "grant select on all tables in schema $tmpschema to $schema;"
    
    $vsql "create role $(echo $schema)_admin;"
    $vsql "grant usage on schema $tmpschema to $(echo $schema)_admin;"
    $vsql "grant all on schema $tmpschema to $(echo $schema)_admin;"
    $vsql "grant all on all functions in schema $tmpschema to $(echo $schema)_admin;"
    $vsql "grant all on all sequences in schema $tmpschema to $(echo $schema)_admin;"
    $vsql "grant all on all tables in schema $tmpschema to $(echo $schema)_admin;"
    $vsql "alter schema $tmpschema default include privileges;"
    
    # Create the tables.
    
    # First acxiom. NOTE: if acxiom's output filename changes, this will need to be updated.
    $vsql "create table $tmpschema.acxiom(MAINTENANCE_KEY_10050 char(10), entity_id int);"
    # Then the others. 
    for path in $(hdfs dfs -ls /data/splinkr/raw_with_eid | awk '{print $8}' | grep -v acxiom); do  
      table="$(basename $path)"
      $vsql "create table $tmpschema.$table(_advana_id int, entity_id int);"
    done
    
    # Load the tables from HDFS.
    virt/bin/python dirload.py "$tmpschema" "$SPLINKR_EIDS"
    # Now rename the tmpschema to the real schema.
    $vsql "drop schema $schema cascade;"
    $vsql "alter schema $tmpschema rename to $schema;"
fi
