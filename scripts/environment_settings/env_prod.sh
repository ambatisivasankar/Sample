# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x

export SQUARK_BUCKET=${SQUARK_PROD_BUCKET}
export AWS_VERTICA_HOST=${PROD_AWS_VERTICA_HOST}
export VERTICA_CONNECTION_ID=${PROD_VERTICA_CONNECTION_ID}
export SQUARK_TYPE=${PROD_SQUARK_TYPE}
export S3_CONNECTION_ID=${PROD_S3_CONNECTION_ID}
export JENKINS_URL=${PROD_JENKINS_URL}
