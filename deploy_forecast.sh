#!/usr/bin/env bash
# deploy_forecast.sh — Deploy the Forecast Dashboard to Cloud Run with IAP.
#
# Deploys:
#   - Cloud Run Service  : forecast-dashboard  (FastAPI web server)
#   - Cloud Run Job      : forecast-refresh     (daily data refresh)
#   - Cloud Scheduler    : forecast-daily       (7:30 AM Pacific trigger)
#   - GCS Bucket         : ${PROJECT_ID}-forecast-data
#
# Prerequisites: gcloud authenticated as nick.opderbeck@you.com,
#                project set to you-sales-toolkit.
#
# chmod +x deploy_forecast.sh

set -euo pipefail

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION=us-central1
SERVICE_NAME=forecast-dashboard
JOB_NAME=forecast-refresh
IMAGE=gcr.io/${PROJECT_ID}/${SERVICE_NAME}
SA_EMAIL=penetration-report@${PROJECT_ID}.iam.gserviceaccount.com
DATA_BUCKET=${PROJECT_ID}-forecast-data

# Schedule: every day at 7:30 AM Pacific
SCHEDULE="30 7 * * *"
TIMEZONE="America/Los_Angeles"

echo "═══════════════════════════════════════════════"
echo "  Forecast Dashboard — Cloud Run Deploy"
echo "  Project : ${PROJECT_ID}"
echo "  Region  : ${REGION}"
echo "  Service : ${SERVICE_NAME}"
echo "  Job     : ${JOB_NAME}"
echo "  Image   : ${IMAGE}"
echo "  Bucket  : gs://${DATA_BUCKET}"
echo "═══════════════════════════════════════════════"
echo

# ── 0. Ensure GCS data bucket exists ─────────────────────────────────────────
echo "▶ Ensuring GCS data bucket gs://${DATA_BUCKET}..."
if ! gsutil ls -b "gs://${DATA_BUCKET}" &>/dev/null; then
  gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${DATA_BUCKET}"
  echo "  Created gs://${DATA_BUCKET}"
else
  echo "  Already exists"
fi
gsutil iam ch "serviceAccount:${SA_EMAIL}:roles/storage.objectAdmin" \
  "gs://${DATA_BUCKET}"
echo "  Granted objectAdmin to ${SA_EMAIL}"

# ── 1. Check Gong secrets (warn but don't fail) ───────────────────────────────
echo "▶ Checking Gong secrets in Secret Manager..."
for SECRET in gong-api-key gong-api-secret; do
  if gcloud secrets describe "${SECRET}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "  ✓ ${SECRET} found"
  else
    echo "  ⚠ WARNING: secret '${SECRET}' not found in Secret Manager — Gong integration will be disabled" >&2
  fi
done || true

# ── 2. Build Docker image ─────────────────────────────────────────────────────
echo "▶ Building Docker image (Dockerfile.forecast)..."
gcloud builds submit . \
  --config=<(cat <<'CBEOF'
steps:
- name: 'gcr.io/cloud-builders/docker'
  args: ['build', '-f', 'Dockerfile.forecast', '-t', '$_IMAGE', '.']
images: ['$_IMAGE']
CBEOF
) \
  --substitutions="_IMAGE=${IMAGE}" \
  --project="${PROJECT_ID}" \
  --quiet

# ── 3. Upload dashboard HTML to GCS ──────────────────────────────────────────
# HTML is served from GCS, not baked into the image — so UI changes deploy
# with a simple gsutil cp rather than a full Docker rebuild.
echo "▶ Uploading forecast_review.html to gs://${DATA_BUCKET}..."
HTML_PATH="$(dirname "$0")/reports/forecast_review.html"
if [[ ! -f "${HTML_PATH}" ]]; then
  echo "  ERROR: ${HTML_PATH} not found — cannot upload dashboard HTML" >&2
  exit 1
fi
gsutil -h "Content-Type:text/html" cp "${HTML_PATH}" "gs://${DATA_BUCKET}/forecast_review.html"
echo "  Uploaded forecast_review.html"

# ── 4. Deploy Cloud Run Service (web dashboard) ───────────────────────────────
echo "▶ Deploying Cloud Run service '${SERVICE_NAME}'..."
gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --service-account "${SA_EMAIL}" \
  --no-allow-unauthenticated \
  --set-secrets="SF_USERNAME=pen-sf-username:latest,SF_PASSWORD=pen-sf-password:latest,SF_SECURITY_TOKEN=pen-sf-token:latest" \
  --set-env-vars="GCS_BUCKET=${DATA_BUCKET}" \
  --min-instances 0 \
  --max-instances 2 \
  --memory 512Mi \
  --timeout 60s \
  --project="${PROJECT_ID}" \
  --quiet

# ── 5. Deploy Cloud Run Job (data refresh) ────────────────────────────────────
echo "▶ Deploying Cloud Run job '${JOB_NAME}'..."
gcloud run jobs deploy "${JOB_NAME}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --service-account "${SA_EMAIL}" \
  --command="python3" \
  --args="forecast_refresh.py" \
  --set-secrets="SF_USERNAME=pen-sf-username:latest,SF_PASSWORD=pen-sf-password:latest,SF_SECURITY_TOKEN=pen-sf-token:latest,GONG_API_KEY=gong-api-key:latest,GONG_API_SECRET=gong-api-secret:latest" \
  --set-env-vars="GCS_BUCKET=${DATA_BUCKET}" \
  --max-retries 1 \
  --task-timeout 15m \
  --project="${PROJECT_ID}" \
  --quiet

# ── 6. Cloud Scheduler ────────────────────────────────────────────────────────
echo "▶ Setting up Cloud Scheduler (${SCHEDULE} ${TIMEZONE})..."

JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe forecast-daily --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "  Updating existing scheduler job..."
  gcloud scheduler jobs update http forecast-daily \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet
else
  gcloud scheduler jobs create http forecast-daily \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet
fi
echo "  Scheduled: ${SCHEDULE} ${TIMEZONE}"

# ── 7. Grant run.invoker for IAP backend ──────────────────────────────────────
echo "▶ Granting roles/run.invoker to ${SA_EMAIL} on service..."
gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
  --region="${REGION}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.invoker" \
  --project="${PROJECT_ID}" \
  --quiet

# ── 8. IAP instructions ───────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ⚠️  MANUAL STEP REQUIRED: Enable IAP"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  1. Open: https://console.cloud.google.com/security/iap?project=${PROJECT_ID}"
echo "  2. Find '${SERVICE_NAME}' under Cloud Run"
echo "  3. Toggle IAP ON"
echo "  4. Add principal: forecast-users@you.com (or individual emails)"
echo "     Role: IAP-secured Web App User"
echo ""
echo "  Once IAP is on, the service URL is accessible only to @you.com users"
echo "  you explicitly grant access to."

# ── Done ──────────────────────────────────────────────────────────────────────
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --region "${REGION}" \
  --format='value(status.url)' \
  --project="${PROJECT_ID}")

echo ""
echo "═══════════════════════════════════════════════"
echo "  ✅ Deploy complete!"
echo ""
echo "  Service URL : ${SERVICE_URL}"
echo "  Data bucket : gs://${DATA_BUCKET}"
echo "  Refresh runs: daily at 7:30 AM Pacific"
echo ""
echo "  Seed initial data (run once before first scheduled refresh):"
echo "    gcloud run jobs execute ${JOB_NAME} --region ${REGION} --wait"
echo ""
echo "  Update dashboard HTML only (no Docker rebuild):"
echo "    gsutil -h Content-Type:text/html cp reports/forecast_review.html gs://${DATA_BUCKET}/forecast_review.html"
echo ""
echo "  View service logs:"
echo "    gcloud logging read 'resource.type=cloud_run_revision AND resource.labels.service_name=${SERVICE_NAME}' --limit 50"
echo ""
echo "  View refresh logs:"
echo "    gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}' --limit 50"
echo "═══════════════════════════════════════════════"
