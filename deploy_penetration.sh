#!/usr/bin/env bash
# deploy_penetration.sh — One-time setup + deploy for the Cloud Run penetration job.
#
# Run once to set up, then redeploy any time by running again.
# Prerequisites:
#   gcloud CLI authenticated: gcloud auth login
#   Project set:              gcloud config set project <your-project-id>
#   APIs enabled:             script enables them if not already

set -euo pipefail

# ── Config ─────────────────────────────────────────────────────────────────────
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION=us-central1
JOB_NAME=penetration-report
IMAGE=gcr.io/${PROJECT_ID}/${JOB_NAME}
SA_NAME=penetration-report
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
DRIVE_FOLDER_ID=1gujAtCzSVHZQtNbvH33Js0k-Oqjtr7V8

# Schedule: every Monday at 6:00 AM Pacific
SCHEDULE="0 6 * * 1"
TIMEZONE="America/Los_Angeles"

echo "═══════════════════════════════════════════════"
echo "  Penetration Report — Cloud Run Setup"
echo "  Project : ${PROJECT_ID}"
echo "  Region  : ${REGION}"
echo "  Image   : ${IMAGE}"
echo "═══════════════════════════════════════════════"
echo

# ── 1. Enable APIs ─────────────────────────────────────────────────────────────
echo "▶ Enabling required APIs..."
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  cloudbuild.googleapis.com \
  drive.googleapis.com \
  docs.googleapis.com \
  --quiet

# ── 2. Service account ─────────────────────────────────────────────────────────
echo "▶ Creating service account ${SA_EMAIL}..."
gcloud iam service-accounts create "${SA_NAME}" \
  --display-name="Penetration Report Job" \
  --project="${PROJECT_ID}" 2>/dev/null \
  || echo "  (already exists, continuing)"

# ── 3. Secrets ─────────────────────────────────────────────────────────────────
# Load values from local .env if present; otherwise prompt
DOTENV_FILE="$(dirname "$0")/.env"
source_dotenv() {
  local key="$1"
  local val=""
  if [[ -f "${DOTENV_FILE}" ]]; then
    val=$(grep -E "^${key}=" "${DOTENV_FILE}" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"'"'" || true)
  fi
  if [[ -z "${val}" ]]; then
    read -r -s -p "  Enter ${key}: " val; echo
  fi
  echo "${val}"
}

create_or_update_secret() {
  local name="$1"
  local value="$2"
  if gcloud secrets describe "${name}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "  Updating secret ${name}..."
    echo -n "${value}" | gcloud secrets versions add "${name}" --data-file=- --project="${PROJECT_ID}" --quiet
  else
    echo "  Creating secret ${name}..."
    echo -n "${value}" | gcloud secrets create "${name}" --data-file=- --project="${PROJECT_ID}" --quiet
  fi
  # Grant SA read access
  gcloud secrets add-iam-policy-binding "${name}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/secretmanager.secretAccessor" \
    --project="${PROJECT_ID}" --quiet
}

echo "▶ Storing secrets in Secret Manager..."
SF_USERNAME=$(source_dotenv SF_USERNAME)
SF_PASSWORD=$(source_dotenv SF_PASSWORD)
SF_SECURITY_TOKEN=$(source_dotenv SF_SECURITY_TOKEN)
SLACK_BOT_TOKEN=$(source_dotenv SLACK_BOT_TOKEN)

create_or_update_secret "pen-sf-username"      "${SF_USERNAME}"
create_or_update_secret "pen-sf-password"      "${SF_PASSWORD}"
create_or_update_secret "pen-sf-token"         "${SF_SECURITY_TOKEN}"
create_or_update_secret "pen-slack-bot-token"  "${SLACK_BOT_TOKEN}"

# ── 4. Grant SA Drive folder access ────────────────────────────────────────────
echo
echo "▶ Next manual step — share the Drive folder with the service account:"
echo "  Folder  : https://drive.google.com/drive/folders/${DRIVE_FOLDER_ID}"
echo "  Add     : ${SA_EMAIL}  (Contributor role)"
echo "  Press Enter once done..."
read -r

# ── 5. Build Docker image ─────────────────────────────────────────────────────
echo "▶ Building Docker image..."
gcloud builds submit "$(dirname "$0")" \
  --tag "${IMAGE}" \
  --project="${PROJECT_ID}" \
  --quiet

# ── 6. Deploy Cloud Run job ────────────────────────────────────────────────────
echo "▶ Deploying Cloud Run job '${JOB_NAME}'..."
gcloud run jobs deploy "${JOB_NAME}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --service-account "${SA_EMAIL}" \
  --set-secrets="SF_USERNAME=pen-sf-username:latest,SF_PASSWORD=pen-sf-password:latest,SF_SECURITY_TOKEN=pen-sf-token:latest,SLACK_BOT_TOKEN=pen-slack-bot-token:latest" \
  --max-retries 1 \
  --task-timeout 30m \
  --project="${PROJECT_ID}" \
  --quiet

# ── 7. Cloud Scheduler ────────────────────────────────────────────────────────
echo "▶ Setting up Cloud Scheduler (${SCHEDULE} ${TIMEZONE})..."

SCHEDULER_SA_EMAIL="${SA_EMAIL}"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SCHEDULER_SA_EMAIL}" \
  --role="roles/run.invoker" \
  --quiet

JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe penetration-weekly --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "  Updating existing scheduler job..."
  gcloud scheduler jobs update http penetration-weekly \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SCHEDULER_SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet
else
  gcloud scheduler jobs create http penetration-weekly \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SCHEDULER_SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet
fi

# ── Done ───────────────────────────────────────────────────────────────────────
echo
echo "═══════════════════════════════════════════════"
echo "  ✅ Setup complete!"
echo
echo "  Job runs every Monday at 6:00 AM Pacific."
echo
echo "  Test immediately:"
echo "    gcloud run jobs execute ${JOB_NAME} --region ${REGION} --wait"
echo
echo "  View logs:"
echo "    gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}' --limit 50"
echo "═══════════════════════════════════════════════"
