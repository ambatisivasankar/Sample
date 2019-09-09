# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x

printLineMsg before "Checking job file exists"
if [ ! -e "${WORKSPACE}/squark-classic/jobs/${JOB_FILE_NAME}.sh" ]; then
  die "Job name ${JOB_FILE_NAME}.sh does not exist"
fi
printLineMsg after "Job file exists"
