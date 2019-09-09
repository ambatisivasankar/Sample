# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x

printLineMsg before "Checking job file name available"
if [ -z ${JOB_FILE_NAME+x} ]; then
    die "Job name not passed to launch squark job "
fi
printLineMsg after "Job = ${JOB_FILE_NAME}"
