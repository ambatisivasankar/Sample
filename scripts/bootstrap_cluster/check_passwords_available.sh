###############################################################
## Check that Passwords are Available

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x


printLineMsg before "Checking password variables are set..."
if [ -z ${SQUARK_PASSWORD+x} ]; then
    die "Error! SQUARK_PASSWORD variable is not set. Exiting..."
fi
if [ -z ${VERTICA_PASSWORD+x} ] && [ -z ${AWS_VERTICA_PASSWORD+x} ]; then
    die "Error! Neither VERTICA_PASSWORD nor AWS_VERTICA_PASSWORD variables are set. Exiting..."
fi
printLineMsg after "Finished checking password variables..."
