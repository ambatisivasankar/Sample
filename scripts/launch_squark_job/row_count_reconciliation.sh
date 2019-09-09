###############################################################
## Row Reconcillation
# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x
set -e

JOB_FILE_NAME=$1

vsql="${VERTICA_VSQL} -C -h ${AWS_VERTICA_HOST} -p ${AWS_VERTICA_PORT} -U ${VERTICA_USER} -m require -w ${AWS_VERTICA_PASSWORD} -d ${VERTICA_DATABASE} -f "
RESULTS_FILE="row_count_results.out"
$vsql "${WORKSPACE}"/squark-classic/resources/row_count_reconciliation.sql -v VERTICA_SCHEMA="'${JOB_FILE_NAME}'" -o ${RESULTS_FILE}
cat ${RESULTS_FILE}

MARKER_TEXT="<<<<"
if grep -q ${MARKER_TEXT} ${RESULTS_FILE}; then
    attachment=$(cat ${RESULTS_FILE} | grep ${MARKER_TEXT} | tr -d ' ' | awk  'BEGIN { FS="|"; OFS="";} { print "- ",$1,".",$2; }')
    json=$(cat<<-EOM
    payload={
        "channel": "#ingest_alerts",
        "username": "webhookbot",
        "text": "JOB COMPLETED: $JOB_NAME, see <$BUILD_URL/consoleFull|jenkins log>",
        "icon_emoji": ":ingestee:",
        "attachments": [
            {
                "fallback": "row count reconciliation issues in this job",
                "color": "danger",
                "pretext": "*** source vs. Vertica row count reconciliation reported issues in below tables ***",
                "title": "click for build $BUILD_NUMBER log",
                "title_link": "$BUILD_URL/consoleFull#footer",
                "text": "$attachment",
            }
        ]
    }
EOM
)
    curl -X POST --data-urlencode "$json" https://hooks.slack.com/services/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
fi
