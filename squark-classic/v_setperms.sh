# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
tmpschema=$1

aws_vsql="$VERTICA_VSQL -C -h $AWS_VERTICA_HOST -p $AWS_VERTICA_PORT -U $VERTICA_USER -m require -w $AWS_VERTICA_PASSWORD -d advana -c "
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

