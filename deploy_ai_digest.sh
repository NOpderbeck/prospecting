#!/usr/bin/env bash
# deploy_ai_digest.sh — Deploy the Daily AI Trends Digest Cloud Run job.
#
# Posts to #daily-digest every weekday morning at 7:00 AM Pacific.
# Collects signals from X (TwitterAPI.io), LinkedIn (RapidAPI), and You.com,
# then synthesizes a competitive AI market digest via Claude.
#
# Secrets required in Secret Manager (see setup notes at bottom):
#   digest-twitter-api-key      TWITTER_API_KEY
#   digest-linkedin-rapidapi-key LINKEDIN_RAPIDAPI_KEY
#   digest-youcom-api-key       YOUCOM_API_KEY
#   pen-anthropic-key           ANTHROPIC_API_KEY  (shared with other jobs)
#   pen-slack-bot-token         SLACK_BOT_TOKEN    (shared with other jobs)
#
# Run once to set up; re-run any time to redeploy after code changes.
#
# Prerequisites: gcloud authenticated as nick.opderbeck@you.com,
#                project set to you-sales-toolkit

set -euo pipefail

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION=us-central1
JOB_NAME=ai-digest
IMAGE=gcr.io/${PROJECT_ID}/sales-toolkit
SA_EMAIL=penetration-report@${PROJECT_ID}.iam.gserviceaccount.com

# Schedule: Mon–Fri at 7:00 AM Pacific
SCHEDULE="0 7 * * 1-5"
TIMEZONE="America/Los_Angeles"

echo "═══════════════════════════════════════════════"
echo "  Daily AI Trends Digest — Cloud Run Setup"
echo "  Project : ${PROJECT_ID}"
echo "  Region  : ${REGION}"
echo "  Image   : ${IMAGE}"
echo "  Schedule: ${SCHEDULE} ${TIMEZONE}"
echo "═══════════════════════════════════════════════"
echo

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
  --args="ai_digest_run.py" \
  --set-secrets="ANTHROPIC_API_KEY=pen-anthropic-key:latest,SLACK_BOT_TOKEN=pen-slack-bot-token:latest,TWITTER_API_KEY=digest-twitter-api-key:latest,LINKEDIN_RAPIDAPI_KEY=digest-linkedin-rapidapi-key:latest,YOUCOM_API_KEY=digest-youcom-api-key:latest" \
  --set-env-vars="SLACK_CHANNEL_AI_DIGEST=#daily-digest" \
  --max-retries 1 \
  --task-timeout 10m \
  --project="${PROJECT_ID}" \
  --quiet

# ── 3. Cloud Scheduler ────────────────────────────────────────────────────
echo "▶ Setting up Cloud Scheduler (${SCHEDULE} ${TIMEZONE})..."

JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe ai-digest-daily --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "  Updating existing scheduler job..."
  gcloud scheduler jobs update http ai-digest-daily \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet
else
  gcloud scheduler jobs create http ai-digest-daily \
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
echo "  Job runs Mon–Fri at 7:00 AM Pacific."
echo "  Posts to #daily-digest."
echo
echo "  ── Secret Manager setup (first time only) ──"
echo
echo "  1. Twitter API key:"
echo "    printf \"%s\" \"\$TWITTER_API_KEY\" | gcloud secrets create digest-twitter-api-key --data-file=- --project=${PROJECT_ID}"
echo "    gcloud secrets add-iam-policy-binding digest-twitter-api-key \\"
echo "      --member=\"serviceAccount:${SA_EMAIL}\" --role=\"roles/secretmanager.secretAccessor\" \\"
echo "      --project=${PROJECT_ID}"
echo
echo "  2. LinkedIn RapidAPI key:"
echo "    printf \"%s\" \"\$LINKEDIN_RAPIDAPI_KEY\" | gcloud secrets create digest-linkedin-rapidapi-key --data-file=- --project=${PROJECT_ID}"
echo "    gcloud secrets add-iam-policy-binding digest-linkedin-rapidapi-key \\"
echo "      --member=\"serviceAccount:${SA_EMAIL}\" --role=\"roles/secretmanager.secretAccessor\" \\"
echo "      --project=${PROJECT_ID}"
echo
echo "  3. You.com API key (if not already stored):"
echo "    printf \"%s\" \"\$YOUCOM_API_KEY\" | gcloud secrets create digest-youcom-api-key --data-file=- --project=${PROJECT_ID}"
echo "    gcloud secrets add-iam-policy-binding digest-youcom-api-key \\"
echo "      --member=\"serviceAccount:${SA_EMAIL}\" --role=\"roles/secretmanager.secretAccessor\" \\"
echo "      --project=${PROJECT_ID}"
echo
echo "  ── Test immediately ──"
echo "    gcloud run jobs execute ${JOB_NAME} --region ${REGION} --wait"
echo
echo "  ── Dry run locally ──"
echo "    python ai_digest_run.py --dry-run"
echo
echo "  ── View logs ──"
echo "    gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}' --limit 50"
echo "═══════════════════════════════════════════════"
