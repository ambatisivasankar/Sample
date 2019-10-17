###############################################################
## Check that root.crt is downloaded

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x

#Check if MM's root.crt exists
#If not exist, download the cert file from MM's artifactory
printLineMsg before "Checking if root.crt exists"
if [ -e "${VERTICA_ROOTCRTPATH}" ]
then
    echo "root.crt exits"
else
    mkdir -p "${WORKSPACE}/.vsql"
    echo "Download tls-ca-bundle.pem/root.crt from MM artifactory"
    TLS_CA_BUNDLE_PEM="${WORKSPACE}/squark-classic/tls-ca-bundle.pem"
    TLS_CA_BUNDLE_PEM_URL="https://artifactory.awsmgmt.massmutual.com/artifactory/mm-certificates/mm-cert-bundle.pem.unix"
    curl --output "${TLS_CA_BUNDLE_PEM}" "${TLS_CA_BUNDLE_PEM_URL}"
    ln -fs "${TLS_CA_BUNDLE_PEM}" "${WORKSPACE}/.vsql/root.crt"
fi
printLine after "Finished checking root.crt"
