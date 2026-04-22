#!/usr/bin/env bash
# deploy_gong_insights.sh — Deploy the weekly Gong product intelligence Cloud Run job.
#
# Reuses the service account and most secrets from deploy_penetration.sh.
# Run once to set up; re-run any time to redeploy after code changes.
#
# Prerequisites: gcloud authenticated as nick.opderbeck@you.com,
#                project set to you-sales-toolkit,
#                deploy_penetration.sh already run (SA + shared secrets exist).

set -euo pipefail

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION=us-central1
JOB_NAME=gong-insights-report
IMAGE=gcr.io/${PROJECT_ID}/${JOB_NAME}
SA_EMAIL=penetration-report@${PROJECT_ID}.iam.gserviceaccount.com

# Schedule: every Friday at 8:00 AM Pacific
SCHEDULE="0 8 * * 5"
TIMEZONE="America/Los_Angeles"

echo "═══════════════════════════════════════════════"
echo "  Weekly Gong Insights — Cloud Run Setup"
echo "  Project : ${PROJECT_ID}"
echo "  Region  : ${REGION}"
echo "  Image   : ${IMAGE}"
echo "  Schedule: Fridays 8:00 AM PT"
echo "═══════════════════════════════════════════════"
echo

# ── 1. Grant SA access to gong secrets (idempotent) ──────────────────────────
echo "▶ Ensuring SA has access to Gong secrets..."
for SECRET in gong-api-key gong-api-secret; do
  gcloud secrets add-iam-policy-binding "${SECRET}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/secretmanager.secretAccessor" \
    --project="${PROJECT_ID}" --quiet 2>/dev/null || true
done

# ── 2. Build Docker image ─────────────────────────────────────────────────────
echo "▶ Building Docker image..."
gcloud builds submit "$(dirname "$0")" \
  --tag "${IMAGE}" \
  --project="${PROJECT_ID}" \
  --quiet

# ── 3. Deploy Cloud Run job ───────────────────────────────────────────────────
echo "▶ Deploying Cloud Run job '${JOB_NAME}'..."
gcloud run jobs deploy "${JOB_NAME}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --service-account "${SA_EMAIL}" \
  --command="python3" \
  --args="gong_insights_run.py" \
  --set-secrets="\
GONG_API_KEY=gong-api-key:latest,\
GONG_API_SECRET=gong-api-secret:latest,\
ANTHROPIC_API_KEY=pen-anthropic-key:latest,\
SLACK_BOT_TOKEN=pen-slack-bot-token:latest,\
GOOGLE_TOKEN_JSON=gong-google-token:latest" \
  --max-retries 1 \
  --task-timeout 20m \
  --project="${PROJECT_ID}" \
  --quiet

# ── 4. Cloud Scheduler ────────────────────────────────────────────────────────
echo "▶ Setting up Cloud Scheduler (${SCHEDULE} ${TIMEZONE})..."

JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe gong-insights-weekly --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "  Updating existing scheduler job..."
  gcloud scheduler jobs update http gong-insights-weekly \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet
else
  gcloud scheduler jobs create http gong-insights-weekly \
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
echo "  Job runs every Friday at 8:00 AM Pacific."
echo
echo "  Test run now:"
echo "    gcloud run jobs execute ${JOB_NAME} --region ${REGION} --wait"
echo
echo "  View logs:"
echo "    gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}' --limit 50 --format='value(textPayload)'"
echo "═══════════════════════════════════════════════"
