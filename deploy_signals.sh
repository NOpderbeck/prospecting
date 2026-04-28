#!/usr/bin/env bash
# deploy_signals.sh — Deploy the daily signal alerts Cloud Run job.
#
# Reuses the same service account, secrets, and project as deploy_penetration.sh.
# Run once to set up; re-run any time to redeploy after code changes.
#
# Prerequisites: gcloud authenticated as nick.opderbeck@you.com,
#                project set to you-sales-toolkit,
#                deploy_penetration.sh already run (secrets already exist).

set -euo pipefail

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION=us-central1
JOB_NAME=signals-report
IMAGE=gcr.io/${PROJECT_ID}/${JOB_NAME}
SA_EMAIL=penetration-report@${PROJECT_ID}.iam.gserviceaccount.com
BURST_STATE_BUCKET=${PROJECT_ID}-signals-state

# Schedule: every day at 8:00 AM Pacific
SCHEDULE="0 8 * * *"
TIMEZONE="America/Los_Angeles"

# TEST_OWNER_EMAIL — all owner tags resolve to this address.
# Remove or clear this env var when ready to go live with real owner tagging.
TEST_OWNER_EMAIL="nick.opderbeck@you.com"

echo "═══════════════════════════════════════════════"
echo "  Daily Signal Alerts — Cloud Run Setup"
echo "  Project : ${PROJECT_ID}"
echo "  Region  : ${REGION}"
echo "  Image   : ${IMAGE}"
echo "  Test tag: ${TEST_OWNER_EMAIL}"
echo "  State   : gs://${BURST_STATE_BUCKET}/${_GCS_BURST_BLOB:-burst_state.json}"
echo "═══════════════════════════════════════════════"
echo

# ── 0. Ensure GCS state bucket exists ────────────────────────────────────────
echo "▶ Ensuring GCS state bucket gs://${BURST_STATE_BUCKET}..."
if ! gsutil ls -b "gs://${BURST_STATE_BUCKET}" &>/dev/null; then
  gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${BURST_STATE_BUCKET}"
  echo "  Created gs://${BURST_STATE_BUCKET}"
else
  echo "  Already exists"
fi
gsutil iam ch "serviceAccount:${SA_EMAIL}:roles/storage.objectAdmin" \
  "gs://${BURST_STATE_BUCKET}"
echo "  Granted objectAdmin to ${SA_EMAIL}"

# ── 1. Build Docker image ─────────────────────────────────────────────────────
echo "▶ Building Docker image..."
gcloud builds submit "$(dirname "$0")" \
  --tag "${IMAGE}" \
  --project="${PROJECT_ID}" \
  --quiet

# ── 2. Deploy Cloud Run job ───────────────────────────────────────────────────
echo "▶ Deploying Cloud Run job '${JOB_NAME}'..."
gcloud run jobs deploy "${JOB_NAME}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --service-account "${SA_EMAIL}" \
  --command="python3" \
  --args="signals_run.py" \
  --set-secrets="SF_USERNAME=pen-sf-username:latest,SF_PASSWORD=pen-sf-password:latest,SF_SECURITY_TOKEN=pen-sf-token:latest,SLACK_BOT_TOKEN=pen-slack-bot-token:latest" \
  --set-env-vars="TEST_OWNER_EMAIL=${TEST_OWNER_EMAIL},BURST_STATE_BUCKET=${BURST_STATE_BUCKET}" \
  --max-retries 1 \
  --task-timeout 10m \
  --project="${PROJECT_ID}" \
  --quiet

# ── 3. Cloud Scheduler ────────────────────────────────────────────────────────
echo "▶ Setting up Cloud Scheduler (${SCHEDULE} ${TIMEZONE})..."

JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe signals-daily --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "  Updating existing scheduler job..."
  gcloud scheduler jobs update http signals-daily \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet
else
  gcloud scheduler jobs create http signals-daily \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet
fi

# ── Done ───────────────────────────────────────────────────────────────────────
echo
echo "═══════════════════════════════════════════════"
echo "  ✅ Setup complete!"
echo
echo "  Job runs every day at 8:00 AM Pacific."
echo "  Owner tags currently resolved to: ${TEST_OWNER_EMAIL}"
echo
echo "  Test immediately:"
echo "    gcloud run jobs execute ${JOB_NAME} --region ${REGION} --wait"
echo
echo "  Go live (remove test override):"
echo "    gcloud run jobs update ${JOB_NAME} --region ${REGION}"
echo "      --remove-env-vars=TEST_OWNER_EMAIL"
echo
echo "  View logs:"
echo "    gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}' --limit 50"
echo "═══════════════════════════════════════════════"
