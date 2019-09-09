###############################################################
## Check that py4jdbc is downloaded
# If not, download the jar file from s3 and save to $WORKSPACE/squark-classic/$JARS

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x


#Check if PY4JDBC_JAR exists
printLineMsg before "Checking if py4jdbc jar exists"
if [ -e "${PY4JDBC_JAR}" ]
then
    echo "${JARS}:"
    ls -ltr "${WORKSPACE}"/squark-classic/*.jar
else
    echo "File ${PY4JDBC_JAR} not found - downloading from S3"
    if [ -e ./aws-credentials.sh ]
    then
        echo "aws-credentials.sh exists"
    else
        rm -rf ./aws-credentials.sh
        echo "Removed aws aws-credentials and creating it again"
        set -e
cat - > vertica.ini <<EOF
        [DEFAULT]
        host=${AWS_VERTICA_HOST}
        port=${AWS_VERTICA_PORT}
        user=${VERTICA_USER}
        password=${AWS_VERTICA_PASSWORD}
        database=${VERTICA_DATABASE}
EOF
    set-aws-credentials vertica.ini data-engineer-admin
    fi
    echo "Execute source ./aws-credentials.sh"
    source ./aws-credentials.sh
    S3_PY4JDBC_JAR_PATH="s3://${SQUARK_BUCKET}/jdbc/${JARS}"
    echo "Copy Jar file from AWS: ${S3_PY4JDBC_JAR_PATH}"
    aws s3 cp --no-progress "${S3_PY4JDBC_JAR_PATH}" "${PY4JDBC_JAR}"
    rm -r vertica.ini
    rm -rf ./aws-credentials.sh
    echo "Availble jars:"
    ls -ltr "${WORKSPACE}"/squark-classic/*.jar
fi
printLineMsg "Finished checking py4jdbc jar"
