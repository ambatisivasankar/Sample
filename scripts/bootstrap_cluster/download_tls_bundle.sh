###############################################################
## Check that TLS bundle is downloaded

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x

#Check if MM's tls-ca-bundle.jks exists
#If not exist, download the cert file from MM's artifactory
printLineMsg before "Checking if tls-ca-bundle.jks exists"
if [ -e "${VERTICA_TRUSTSTOREPATH}" ]
then
    echo "tls-ca-bundle.jks exists"
else
    echo "Download tls-ca-bundle.jks from MM artifactory"
    TLS_CA_BUNDLE_JKS=${WORKSPACE}/squark-classic/tls-ca-bundle.jks
    TLS_CA_BUNDLE_JKS_URL=https://artifactory.awsmgmt.massmutual.com/artifactory/mm-certificates/mm-cert-bundle.jks
    curl --output "${TLS_CA_BUNDLE_JKS}" "${TLS_CA_BUNDLE_JKS_URL}"
fi
printLineMsg after "Finished checking tls-ca-bundle.jks"
