# Prospecting Toolkit

A personal sales prospecting toolkit that connects to Salesforce, Slack, Google, and You.com to automate company research, account context gathering, and meeting prep — powered by Claude (Anthropic).

---

## Scripts

### `research.py` — Company Research
External company research via You.com, synthesized into a structured report by Claude.

```bash
python research.py <company> [--output-dir reports/] [--verbose]
```

**Output:** `reports/<company-slug>_YYYY-MM-DD.md`

---

### `score.py` — Fit Score
Scores a company on weighted dimensions for You.com Search API / RAG fit.

```bash
python score.py <company> [--research-file path/to/report.md] [--url https://company.com] [--output-dir reports/] [--verbose]
```

**Output:** `reports/<company-slug>_YYYY-MM-DD_fit_score.md`

---

### `lookup.py` — Internal Account Lookup
Searches all internal systems (Salesforce, Slack, Gmail, Google Drive) for references to a company and synthesizes a relationship summary.

```bash
python lookup.py <company> [--domain company.com] [--news] [--email] [--output-dir reports/] [--verbose]
```

**Output:** `reports/<date>/<company-slug>_<date>_internal_context.md`

---

### `meeting_prep.py` — Meeting Preparation Briefs
Pulls upcoming Google Calendar events, researches attendees via You.com + Slack + Drive, and generates a pre-meeting brief.

```bash
python meeting_prep.py [--days 1-5] [--email] [--output-dir reports/] [--verbose]
```

- `--days N` — look ahead N days (default: 1, max: 5)
- `--email` — email the brief as HTML after generating

**Output:** `reports/meeting_prep/meeting_prep_YYYY-MM-DD_dN.md`

Runs automatically each weekday at 7:00 AM via launchd (see [Scheduled Job](#scheduled-job)).

---

### `ask.py` — Query Reports with Natural Language
Ask Claude questions about existing account reports without re-running any connectors.

```bash
python ask.py <company> [question] [--type lookup|context|research|score] [--list] [--all]
```

**Examples:**
```bash
python ask.py salesforce "Who is our main champion?"
python ask.py brex --list
python ask.py stripe "What are the blockers?" --type lookup
```

---

### `server.py` — Web UI
FastAPI web server providing a browser interface for all scripts.

```bash
python server.py
# → http://localhost:8000
```

| Route | Description |
|---|---|
| `GET /` | Dashboard — all accounts with report counts and latest dates |
| `GET /run` | Run any script with live streaming output |
| `GET /history/<slug>` | Browse all reports for an account |
| `GET /report/<slug>/<file>` | Render a report as HTML |
| `GET /ask` | Natural language Q&A over reports |
| `GET /run/stream` | SSE endpoint — streams subprocess output in real time |

---

### `generate_it_doc.py` — IT Admin Document
Generates a Word document (.docx) for IT Admin / security review of the Slack app.

```bash
python generate_it_doc.py
```

**Output:** `reports/ClaudeResearchApp_IT_Admin_Response.docx`

---

## Setup

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in the values you need:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | ✅ | Anthropic API key (`sk-ant-...`) |
| `YOUCOM_API_KEY` | Optional | You.com Search API key |
| `SF_USERNAME` | Optional | Salesforce username |
| `SF_PASSWORD` | Optional | Salesforce password |
| `SF_SECURITY_TOKEN` | Optional | Salesforce security token |
| `SF_DOMAIN` | Optional | `login` (default) or `test` for sandbox |
| `SLACK_USER_TOKEN` | Optional | Slack user token (`xoxp-...`) — requires `search:read` scope |
| `GOOGLE_CREDENTIALS_FILE` | Optional | Path to Google OAuth credentials JSON |
| `SMTP_USER` | Optional | Gmail address (for `--email` flag) |
| `SMTP_PASSWORD` | Optional | Gmail app password |
| `EMAIL_TO` | Optional | Recipient email address (for `--email` flag) |
| `MY_EMAIL_DOMAIN` | Optional | Override auto-detected user domain |
| `INTERNAL_MEETING_KEYWORDS` | Optional | Comma-separated title phrases to classify as internal meetings |
| `INTERNAL_EMAILS` | Optional | Comma-separated personal email addresses to treat as internal |

All connectors are optional — scripts degrade gracefully and skip unconfigured ones.

### 3. Google OAuth (first run)

Google-connected scripts (`lookup.py`, `meeting_prep.py`) require OAuth on first run:

```bash
python lookup.py "test company"     # Opens browser → saves .credentials/google_token.json
python meeting_prep.py              # Opens browser → saves .credentials/google_token_meeting.json
```

Tokens are cached and reused on subsequent runs.

---

## Directory Structure

```
Prospecting/
├── research.py              # Company research
├── score.py                 # Fit score
├── lookup.py                # Internal account lookup
├── ask.py                   # Q&A over reports
├── meeting_prep.py          # Meeting prep briefs
├── context.py               # Connector library (imported by lookup.py)
├── server.py                # FastAPI web UI
├── generate_it_doc.py       # IT admin document
├── run_meeting_prep.sh      # Launchd wrapper script
├── requirements.txt
├── .env.example
│
├── .credentials/            # Google OAuth tokens (git-ignored)
│   ├── google_credentials.json
│   ├── google_token.json
│   └── google_token_meeting.json
│
├── reports/                 # All generated reports (git-ignored)
│   ├── <company-slug>/
│   │   ├── YYYY-MM-DD_internal_context.md
│   │   ├── YYYY-MM-DD_fit_score.md
│   │   └── ...
│   └── meeting_prep/
│       └── meeting_prep_YYYY-MM-DD_d1.md
│
├── logs/                    # Launchd logs (git-ignored)
│
├── templates/               # Jinja2 HTML templates (web UI)
│   ├── base.html
│   ├── dashboard.html
│   ├── run.html
│   ├── report.html
│   ├── history.html
│   └── ask.html
│
└── static/
    └── style.css
```

---

## Scheduled Job

`meeting_prep.py` runs automatically each weekday morning via macOS launchd.

**Wrapper script:** `run_meeting_prep.sh`
- Skips weekends automatically
- Logs output to `logs/meeting_prep_YYYY-MM-DD.log`
- Runs with `--days 1 --email`

**LaunchAgent:** `~/Library/LaunchAgents/com.you.meeting-prep.plist`
- Fires at 7:00 AM daily
- Passes stdout/stderr to `logs/launchd_stdout.log` and `logs/launchd_stderr.log`

**Management commands:**

```bash
# Check if loaded
launchctl list | grep meeting

# Reload after editing plist
launchctl unload ~/Library/LaunchAgents/com.you.meeting-prep.plist
launchctl load   ~/Library/LaunchAgents/com.you.meeting-prep.plist

# Run manually
bash run_meeting_prep.sh

# View today's log
cat logs/meeting_prep_$(date +%Y-%m-%d).log
```

---

## Architecture

```
User / Web UI / LaunchAgent
        │
        ▼
   CLI scripts
        │
   ┌────┴────────────────────────────┐
   │    Connectors (all optional)    │
   │  Salesforce · Slack · Google   │
   │  Gmail · Drive · You.com       │
   └────────────────┬───────────────┘
                    │
                    ▼
          Claude (claude-sonnet-4-6)
          Synthesizes → Markdown report
                    │
                    ▼
            reports/ directory
            (also emailable as HTML)
```

All scripts follow the same pattern:
1. Gather data from configured connectors
2. Build a structured prompt
3. Stream Claude's response line-by-line
4. Write the result to `reports/` as Markdown

---

## Dependencies

| Package | Purpose |
|---|---|
| `anthropic` | Claude API (streaming) |
| `fastapi` + `uvicorn` | Web server |
| `jinja2` | HTML templates |
| `requests` | HTTP client (You.com API) |
| `python-dotenv` | `.env` loading |
| `simple-salesforce` | Salesforce connector |
| `slack-sdk` | Slack connector |
| `google-api-python-client` + auth | Google Calendar / Drive / Gmail |
| `notion-client` | Notion connector (optional) |
| `markdown` | Render reports as HTML |
| `watchfiles` | Dev server auto-reload |
