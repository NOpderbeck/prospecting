#!/usr/bin/env bash
# deploy_ae_activity.sh — Deploy the weekly AE Activity Dashboard Cloud Run job.
#
# Reuses the same service account, secrets, and project as deploy_penetration.sh.
# Run once to set up; re-run any time to redeploy after code changes.
#
# Prerequisites: gcloud authenticated as nick.opderbeck@you.com,
#                project set to you-sales-toolkit,
#                deploy_penetration.sh already run (secrets already exist),
#                Gong secrets stored in Secret Manager (see step 0 below).

set -euo pipefail

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION=us-central1
JOB_NAME=ae-activity-report
IMAGE=gcr.io/${PROJECT_ID}/sales-toolkit
SA_EMAIL=penetration-report@${PROJECT_ID}.iam.gserviceaccount.com

# Schedule: every Monday at 8:00 AM Pacific
SCHEDULE="0 8 * * 1"
TIMEZONE="America/Los_Angeles"

echo "═══════════════════════════════════════════════"
echo "  AE Activity Dashboard — Cloud Run Setup"
echo "  Project : ${PROJECT_ID}"
echo "  Region  : ${REGION}"
echo "  Image   : ${IMAGE}"
echo "  Schedule: ${SCHEDULE} ${TIMEZONE}"
echo "═══════════════════════════════════════════════"
echo

# ── 0. Ensure Gong secrets exist ──────────────────────────────────────────
# Run these once manually if not already created:
#   printf "%s" "$GONG_API_KEY"    | gcloud secrets create gong-api-key    --data-file=- --project=${PROJECT_ID}
#   printf "%s" "$GONG_API_SECRET" | gcloud secrets create gong-api-secret --data-file=- --project=${PROJECT_ID}
#   gcloud secrets add-iam-policy-binding gong-api-key    --member="serviceAccount:${SA_EMAIL}" --role="roles/secretmanager.secretAccessor" --project=${PROJECT_ID}
#   gcloud secrets add-iam-policy-binding gong-api-secret --member="serviceAccount:${SA_EMAIL}" --role="roles/secretmanager.secretAccessor" --project=${PROJECT_ID}

# ── 1. Build Docker image ─────────────────────────────────────────────────
echo "▶ Building Docker image..."
gcloud builds submit "$(dirname "$0")" \
  --tag "${IMAGE}" \
  --project="${PROJECT_ID}" \
  --quiet

# ── 2. Deploy Cloud Run job ───────────────────────────────────────────────
echo "▶ Deploying Cloud Run job '${JOB_NAME}'..."
gcloud run jobs deploy "${JOB_NAME}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --service-account "${SA_EMAIL}" \
  --command="python3" \
  --args="ae_activity_run.py" \
  --set-secrets="SF_USERNAME=pen-sf-username:latest,SF_PASSWORD=pen-sf-password:latest,SF_SECURITY_TOKEN=pen-sf-token:latest,SLACK_BOT_TOKEN=pen-slack-bot-token:latest,GONG_API_KEY=gong-api-key:latest,GONG_API_SECRET=gong-api-secret:latest" \
  --max-retries 1 \
  --task-timeout 15m \
  --project="${PROJECT_ID}" \
  --quiet

# ── 3. Cloud Scheduler ────────────────────────────────────────────────────
echo "▶ Setting up Cloud Scheduler (${SCHEDULE} ${TIMEZONE})..."

JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe ae-activity-weekly --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "  Updating existing scheduler job..."
  gcloud scheduler jobs update http ae-activity-weekly \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet
else
  gcloud scheduler jobs create http ae-activity-weekly \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet
fi

# ── Done ──────────────────────────────────────────────────────────────────
echo
echo "═══════════════════════════════════════════════"
echo "  ✅ Setup complete!"
echo
echo "  Job runs every Monday at 8:00 AM Pacific."
echo "  Posts to #sales-leadership."
echo
echo "  Test immediately:"
echo "    gcloud run jobs execute ${JOB_NAME} --region ${REGION} --wait"
echo
echo "  View logs:"
echo "    gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}' --limit 50"
echo "═══════════════════════════════════════════════"
