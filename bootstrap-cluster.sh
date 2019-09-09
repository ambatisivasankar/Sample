## Bootstrapping script for setting up the environment for the cluster

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x

# Load up some utility functions
# shellcheck source=./scripts/shell_utils.sh
source "${WORKSPACE}/scripts/shell_utils.sh"

printLineMsg before "Bootstrapping environment"

# Need to make sure passwords are available
# shellcheck source=./scripts/bootstrap_cluster/check_passwords_available.sh
source "${BOOTSTRAP_DIR}/check_passwords_available.sh"

# Determine environment
printLineMsg before "Determining environment"
echo
for i in "$@"; do
    case $i in 
        dev|develop)
            ENV="dev"
            # shellcheck source=./scripts/environemnt_settings/env_dev.sh
            source "${ENV_SETTINGS_DIR}/env_dev.sh"
        ;;
        qa)
            ENV="qa"
            # shellcheck source=./scripts/environemnt_settings/env_qa.sh
            source "${ENV_SETTINGS_DIR}/env_qa.sh"
        ;;
        prod|production|master)
            ENV="master"
            # shellcheck source=./scripts/environemnt_settings/env_prod.sh
            source "${ENV_SETTINGS_DIR}/env_prod.sh"
        ;;
        *)
            die "Unexpected parameter"
        ;;
    esac
done
echo "Environment      = ${ENV}"
echo "SQUARK_BUCKET    = ${SQUARK_BUCKET}"
echo "AWS_VERTICA_HOST = ${AWS_VERTICA_HOST}"
printLineMsg after "Environment determined"

#Check if PY4JDBC_JAR exists
#If not exist, download the jar file from s3 and save to $(pwd)/squark-classic/${JARS}
# shellcheck source=./scripts/bootstrap_cluster/download_py4jdbc.sh
source "${BOOTSTRAP_DIR}/download_py4jdbc.sh"

#Check if MM's tls-ca-bundle.jks exists
#If not exist, download the cert file from MM's artifactory
# shellcheck source=./scripts/bootstrap_cluster/download_tls_bundle.sh
source "${BOOTSTRAP_DIR}/download_tls_bundle.sh"

#Check if MM's root.crt exists
#If not exist, download the cert file from MM's artifactory
# shellcheck source=./scripts/bootstrap_cluster/download_root_crt.sh
source "${BOOTSTRAP_DIR}/download_root_crt.sh"

printLineMsg before "Setting password file and setting to mode 600..."
echo "${SQUARK_PASSWORD}" > "${SQUARK_PASSWORD_FILE}"
chmod 600 "${SQUARK_PASSWORD_FILE}"
printLineMsg after "Password file set"

# shellcheck source=./scripts/bootstrap_cluster./setup_virtual_env.sh
source "${BOOTSTRAP_DIR}/setup_virtual_env.sh"

printLine after "Finished bootstrapping environment"
