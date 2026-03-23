"""
server.py — Web frontend for the Prospecting Toolkit.

Serves a browser UI with:
  • Dashboard    — quick overview of all accounts with reports on file
  • Run          — launch any script with live streaming output (SSE)
  • History      — browse all reports for a company
  • Report       — view a single report rendered as HTML
  • Ask          — natural-language Q&A against a report via Claude

Usage:
    python server.py            # listens on http://localhost:8000
    python server.py --port 8080
"""

import os
import sys
import json
import time
import uuid
import shutil
import argparse
import asyncio
import subprocess
from datetime import date
from pathlib import Path

import markdown as md_lib
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

import db as db_module
from context import slugify
from ask import find_reports, load_reports, ask_claude

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

# In-memory conversation store for multi-turn Ask sessions.
# Keyed by conv_id (UUID string); value holds the report text and message
# history so follow-up questions don't need to reload reports.
_conversations: dict[str, dict] = {}

load_dotenv(override=True)

BASE_DIR = Path(__file__).parent
REPORTS_DIR = BASE_DIR / "reports"
DB_PATH = BASE_DIR / "prospecting.db"

app = FastAPI(title="P0")
db_module.init_db(DB_PATH)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

SCRIPT_META = {
    "lookup": {
        "label": "Account Lookup",
        "script": "lookup.py",
        "description": "Pull all internal signals (Salesforce, Slack, email) and generate a relationship summary.",
        "color": "blue",
        "icon": "🔍",
    },
    "research": {
        "label": "Company Research",
        "script": "research.py",
        "description": "External research via You.com — company overview, AI strategy, recent news.",
        "color": "green",
        "icon": "🌐",
    },
    "score": {
        "label": "Fit Score",
        "script": "score.py",
        "description": "Score the account on weighted dimensions for You.com Web Search API fit.",
        "color": "orange",
        "icon": "📊",
    },
    "meeting_prep": {
        "label": "Meeting Prep",
        "script": "meeting_prep.py",
        "description": "Pull upcoming calendar events and generate attendee research briefs.",
        "color": "teal",
        "icon": "📅",
        "no_company": True,  # does not take a company positional arg
    },
    "bulk_score": {
        "label": "Bulk Fit Score",
        "script": "bulk_score.py",
        "description": "Score all prospects from a Google Sheet and write results back.",
        "color": "purple",
        "icon": "📋",
        "no_company": True,
    },
    "prospect": {
        "label": "Prospect Research",
        "script": "prospect.py",
        "description": "Research a company, find the right execs, pull verified LinkedIn profiles, and generate personalized outreach intros.",
        "color": "indigo",
        "icon": "🎯",
    },
}

REPORT_TYPE_ORDER = ["lookup", "context", "research", "score"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_all_accounts() -> list[dict]:
    """Return a sorted list of account dicts from the reports/ directory."""
    accounts = []
    if not REPORTS_DIR.exists():
        return accounts

    # Include meeting_prep at the top if it has reports
    mp_dir = REPORTS_DIR / "meeting_prep"
    if mp_dir.exists():
        mp_files = sorted(mp_dir.glob("*.md"))
        if mp_files:
            accounts.append({
                "slug": "meeting_prep",
                "display": "Meeting Prep",
                "file_count": len(mp_files),
                "latest_date": mp_files[-1].stem.split("_")[0],
                "report_types": ["meeting_prep"],
                "is_meeting_prep": True,
            })

    SKIP_DIRS = {"meeting_prep"}
    for entry in sorted(REPORTS_DIR.iterdir()):
        # Skip non-dirs, meeting_prep, and old date-named folders (YYYY-MM-DD)
        if not entry.is_dir():
            continue
        if entry.name in SKIP_DIRS:
            continue
        # Old date-subfolders look like "2026-03-04" — skip them
        if len(entry.name) == 10 and entry.name[4] == "-" and entry.name[7] == "-":
            continue
        slug = entry.name
        files = sorted(entry.glob("*.md"))
        if not files:
            continue
        latest = files[-1]
        report_types = sorted({f.stem.split("_", 1)[1] for f in files if "_" in f.stem})
        accounts.append({
            "slug": slug,
            "display": slug.replace("-", " ").title(),
            "file_count": len(files),
            "latest_date": latest.stem.split("_")[0],
            "report_types": report_types,
        })
    return accounts


def _normalize_domain(raw: str) -> str | None:
    """Strip protocol, www, path, and whitespace from a domain entry."""
    import re
    d = raw.strip().lower()
    d = re.sub(r'^https?://', '', d)
    d = re.sub(r'^www\.', '', d)
    d = d.split('/')[0]  # drop any path
    return d or None


def get_account_reports(slug: str) -> list[dict]:
    """Return all report metadata for a given account slug."""
    company_dir = REPORTS_DIR / slug
    if not company_dir.exists():
        return []
    reports = []
    for f in sorted(company_dir.glob("*.md"), reverse=True):
        stem = f.stem  # e.g. 2026-03-04_lookup
        parts = stem.split("_", 1)
        report_date = parts[0] if parts else "?"
        report_type = parts[1] if len(parts) > 1 else "unknown"
        reports.append({
            "filename": f.name,
            "date": report_date,
            "type": report_type,
            "path": str(f.relative_to(BASE_DIR)),
        })
    return reports


def render_markdown(text: str) -> str:
    """Convert markdown text to safe HTML."""
    return md_lib.markdown(
        text,
        extensions=["tables", "fenced_code", "nl2br"],
    )


# ---------------------------------------------------------------------------
# Routes — Pages
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    accounts = get_all_accounts()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "accounts": accounts,
        "script_meta": SCRIPT_META,
        "today": date.today().isoformat(),
    })


@app.get("/accounts", response_class=HTMLResponse)
async def accounts_page(request: Request):
    accounts = [a for a in get_all_accounts() if not a.get("is_meeting_prep")]
    return templates.TemplateResponse("accounts.html", {
        "request": request,
        "accounts": accounts,
        "active": "accounts",
    })


@app.get("/account/{slug}", response_class=HTMLResponse)
async def account_detail_page(request: Request, slug: str):
    if slug == "meeting_prep":
        raise HTTPException(status_code=404, detail="Not found")
    reports = get_account_reports(slug)
    display = slug.replace("-", " ").title()
    meta    = db_module.get_account_meta(DB_PATH, slug)
    actions = db_module.get_action_items(DB_PATH, slug)
    return templates.TemplateResponse("account.html", {
        "request": request,
        "slug": slug,
        "display": display,
        "reports": reports,
        "meta": meta,
        "actions": actions,
        "active": "accounts",
    })


@app.get("/run", response_class=HTMLResponse)
async def run_page(request: Request, company: str = "", script: str = "lookup"):
    return templates.TemplateResponse("run.html", {
        "request": request,
        "script_meta": SCRIPT_META,
        "prefill_company": company,
        "prefill_script": script,
    })


@app.get("/history/{slug}", response_class=HTMLResponse)
async def history_page(request: Request, slug: str):
    reports = get_account_reports(slug)
    return templates.TemplateResponse("history.html", {
        "request": request,
        "slug": slug,
        "display": slug.replace("-", " ").replace("_", " ").title(),
        "is_meeting_prep": slug == "meeting_prep",
        "reports": reports,
        "script_meta": SCRIPT_META,
    })


@app.get("/report/{slug}/{filename}", response_class=HTMLResponse)
async def report_page(request: Request, slug: str, filename: str):
    filepath = REPORTS_DIR / slug / filename
    if not filepath.exists():
        return HTMLResponse("<h2>Report not found.</h2>", status_code=404)
    raw = filepath.read_text(encoding="utf-8")
    html_body = render_markdown(raw)
    stem = Path(filename).stem
    parts = stem.split("_", 1)
    report_date = parts[0] if parts else "?"
    report_type = parts[1] if len(parts) > 1 else "unknown"
    return templates.TemplateResponse("report.html", {
        "request": request,
        "slug": slug,
        "display": slug.replace("-", " ").title(),
        "filename": filename,
        "report_date": report_date,
        "report_type": report_type,
        "html_body": html_body,
        "raw": raw,
    })


@app.delete("/report/{slug}/{filename}")
async def delete_report(slug: str, filename: str):
    filepath = (REPORTS_DIR / slug / filename).resolve()
    # Security: ensure the resolved path stays inside REPORTS_DIR
    if not str(filepath).startswith(str(REPORTS_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="Report not found")
    filepath.unlink()
    return Response(content="", status_code=200)


@app.delete("/account/{slug}")
async def delete_account(slug: str):
    acct_dir = (REPORTS_DIR / slug).resolve()
    # Security: slug must be a direct child of REPORTS_DIR (no traversal)
    if acct_dir.parent != REPORTS_DIR.resolve():
        raise HTTPException(status_code=400, detail="Invalid slug")
    if not acct_dir.exists():
        raise HTTPException(status_code=404, detail="Account not found")
    shutil.rmtree(acct_dir)
    return Response(content="", status_code=200)


# ---------------------------------------------------------------------------
# Routes — Account metadata
# ---------------------------------------------------------------------------

@app.post("/account/{slug}/meta/refresh", response_class=HTMLResponse)
async def refresh_meta(request: Request, slug: str):
    """Re-pull SF URLs from Salesforce; auto-fill Slack channel if blank."""
    from context import pull_salesforce
    config = {
        "sf_username":       os.getenv("SF_USERNAME"),
        "sf_password":       os.getenv("SF_PASSWORD"),
        "sf_security_token": os.getenv("SF_SECURITY_TOKEN"),
        "sf_domain":         os.getenv("SF_DOMAIN", "login"),
        "db_path":           str(DB_PATH),
    }
    display_name = slug.replace("-", " ").title()
    domain = (db_module.get_account_meta(DB_PATH, slug) or {}).get("domain") or ""
    # Run blocking SF call in a thread — wrap in try/except so any unhandled
    # exception becomes a graceful error result rather than a 500.
    try:
        result = await asyncio.to_thread(pull_salesforce, display_name, config, False, domain)
    except Exception as exc:
        print(f"  [refresh] Unexpected error: {exc}")
        result = {"error": str(exc)}

    # Log the outcome to the server console so errors are always visible
    if result.get("error"):
        print(f"  [refresh] SF error for '{slug}': {result['error']}")
    elif result.get("account") is None:
        print(f"  [refresh] SF: no account found matching '{display_name}'")
    else:
        acct_name = (result.get("account") or {}).get("Name", "?")
        print(f"  [refresh] SF OK — matched: {acct_name}")

    # Auto-fill Slack channel if still blank
    meta = db_module.get_account_meta(DB_PATH, slug) or {}
    if not meta.get("slack_channel"):
        db_module.upsert_account_meta(DB_PATH, slug, slack_channel=f"#internal-{slug}")

    meta = db_module.get_account_meta(DB_PATH, slug) or {}

    # Brief status badge for the panel
    if result.get("error"):
        refresh_msg = f"⚠ SF: {result['error']}"
    elif result.get("account") is None:
        refresh_msg = f"⚠ No SF account for '{display_name}'"
    else:
        refresh_msg = "✓ Refreshed"

    return templates.TemplateResponse("_meta_panel.html", {
        "request": request,
        "slug": slug,
        "meta": meta,
        "refresh_msg": refresh_msg,
    })


@app.get("/account/{slug}/meta", response_class=HTMLResponse)
async def get_meta_panel(request: Request, slug: str):
    meta = db_module.get_account_meta(DB_PATH, slug)
    return templates.TemplateResponse("_meta_panel.html", {
        "request": request,
        "slug": slug,
        "meta": meta,
    })


@app.get("/account/{slug}/meta/edit", response_class=HTMLResponse)
async def edit_meta_form(request: Request, slug: str):
    meta = db_module.get_account_meta(DB_PATH, slug)
    # Pre-fill slack channel suggestion if not set
    if not meta.get("slack_channel"):
        meta["slack_channel"] = f"#internal-{slug}"
    return templates.TemplateResponse("_meta_form.html", {
        "request": request,
        "slug": slug,
        "meta": meta,
    })


@app.post("/account/{slug}/meta", response_class=HTMLResponse)
async def save_account_meta(request: Request, slug: str):
    form = await request.form()
    db_module.upsert_account_meta(
        DB_PATH, slug,
        domain             = _normalize_domain(form.get("domain", "")),
        sf_account_url     = form.get("sf_account_url", "").strip() or None,
        sf_opportunity_url = form.get("sf_opportunity_url", "").strip() or None,
        slack_channel      = form.get("slack_channel", "").strip() or None,
        notes              = form.get("notes", "").strip() or None,
    )
    meta = db_module.get_account_meta(DB_PATH, slug)
    return templates.TemplateResponse("_meta_panel.html", {
        "request": request,
        "slug": slug,
        "meta": meta,
    })


# ---------------------------------------------------------------------------
# Routes — Action items
# ---------------------------------------------------------------------------

@app.post("/account/{slug}/actions", response_class=HTMLResponse)
async def add_action(
    request: Request,
    slug: str,
    text: str = Form(...),
    source_report: str = Form(""),
):
    db_module.add_action_item(DB_PATH, slug, text, source_report or None)
    actions = db_module.get_action_items(DB_PATH, slug)
    return templates.TemplateResponse("_action_list.html", {
        "request": request,
        "slug": slug,
        "actions": actions,
    })


@app.patch("/account/{slug}/actions/{item_id}", response_class=HTMLResponse)
async def toggle_action(request: Request, slug: str, item_id: int):
    item = db_module.toggle_action_item(DB_PATH, item_id)
    # Use _task_row.html (a <tr>) when called from the /tasks page;
    # use _action_row.html (a <li>) when called from an account detail page.
    current_url = request.headers.get("HX-Current-URL", "")
    template = "_task_row.html" if "/tasks" in current_url else "_action_row.html"
    return templates.TemplateResponse(template, {
        "request": request,
        "slug": slug,
        "item": item,
    })


@app.delete("/account/{slug}/actions/{item_id}")
async def delete_action(slug: str, item_id: int):
    db_module.delete_action_item(DB_PATH, item_id)
    return Response(content="", status_code=200)


def _extract_action_items_via_claude(
    report_texts: list[str],
    existing_items: list[str],
    api_key: str,
) -> list[str]:
    """
    Call Claude to extract action items from reports, skipping anything
    already captured (including completed tasks).  Returns a list of new
    item strings, or [] on any failure.
    """
    import anthropic, json as _json

    existing_block = (
        "\n".join(f"  - {t}" for t in existing_items)
        if existing_items
        else "  (none yet)"
    )
    combined_reports = "\n\n---\n\n".join(report_texts)

    prompt = f"""You are reviewing internal sales prospecting reports to extract action items and follow-up tasks.

ALREADY CAPTURED TASKS — do NOT recreate these, even if phrased differently or previously completed:
{existing_block}

REPORTS TO ANALYSE:
{combined_reports}

Instructions:
- Extract every specific, actionable follow-up mentioned in the reports
- Skip anything already covered by the list above (be strict — semantic duplicates count)
- Be concise: max 12 words per item; start with a verb
- Skip vague catch-alls like "follow up" with no further detail
- Return ONLY a valid JSON array of strings, e.g. ["Schedule technical deep-dive", "Send pricing to CFO"]
- If there are no new items, return exactly: []"""

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    try:
        raw = msg.content[0].text.strip()
        # Strip markdown code fences if Claude wraps the JSON
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        items = _json.loads(raw.strip())
        return [s.strip() for s in items if isinstance(s, str) and s.strip()]
    except Exception as exc:
        print(f"  [extract] JSON parse error: {exc} — raw: {msg.content[0].text[:200]}")
        return []


@app.post("/account/{slug}/actions/extract", response_class=HTMLResponse)
async def extract_actions_from_reports(request: Request, slug: str):
    """
    Scan the account's reports with Claude, extract action items, and add any
    that are not already in the DB (active or completed).
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    extract_msg = ""
    extract_ok  = False

    account_dir  = REPORTS_DIR / slug
    report_files = sorted(
        account_dir.glob("*.md") if account_dir.exists() else [],
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )[:6]  # Cap at 6 most-recent reports to stay within token budget

    if not report_files:
        extract_msg = "⚠ No reports found — run a Lookup first."
    elif not api_key:
        extract_msg = "⚠ ANTHROPIC_API_KEY not set."
    else:
        # Load reports; truncate each to keep the total prompt manageable
        report_texts = []
        for f in report_files:
            try:
                text = f.read_text(encoding="utf-8")[:4000]
                report_texts.append(f"### {f.stem}\n{text}")
            except Exception:
                continue

        # Fetch ALL existing items (active + completed) for deduplication
        all_items     = db_module.get_all_action_items_for_dedup(DB_PATH, slug)
        existing_texts = [a["text"] for a in all_items]

        try:
            new_items = await asyncio.to_thread(
                _extract_action_items_via_claude,
                report_texts,
                existing_texts,
                api_key,
            )
        except Exception as exc:
            print(f"  [extract] Thread error for '{slug}': {exc}")
            new_items  = []
            extract_msg = f"⚠ Extraction error: {exc}"

        if not extract_msg:
            for text in new_items:
                db_module.add_action_item(
                    DB_PATH, slug, text, source_report="auto_extracted"
                )
            n = len(new_items)
            r = len(report_files)
            if n:
                extract_msg = (
                    f"✓ Added {n} new action item{'s' if n != 1 else ''} "
                    f"from {r} report{'s' if r != 1 else ''}."
                )
                extract_ok = True
            else:
                extract_msg = (
                    f"No new action items found across "
                    f"{r} report{'s' if r != 1 else ''}."
                )
            print(f"  [extract] '{slug}': {extract_msg}")

    actions = db_module.get_action_items(DB_PATH, slug)
    return templates.TemplateResponse("_action_list.html", {
        "request":    request,
        "slug":       slug,
        "actions":    actions,
        "extract_msg": extract_msg,
        "extract_ok":  extract_ok,
    })


# ---------------------------------------------------------------------------
# Routes — Slack Status
# ---------------------------------------------------------------------------

def _generate_slack_status_via_claude(
    report_text: str,
    company: str,
    api_key: str,
) -> str:
    """Call Claude to synthesise a concise Slack status from the most recent lookup report."""
    import anthropic
    from datetime import date as _date

    today = _date.today().strftime("%-m/%-d")

    prompt = f"""You are a sales rep writing a brief internal Slack status update for the account "{company}".

Using the report below, generate a concise weekly status. Focus on activity from the past 7 days. Be direct and factual — no fluff.

Output EXACTLY this format (plain text, no markdown formatting characters like # or **):

Status update: {today}

1. Commercial Update
* [bullet]
* [bullet]

2. Open Items / Risk
* [bullet]
* [bullet]

3. Goals for the Week
* [bullet]
* [bullet]

Guidelines:
- Commercial Update: deal stage, pricing, contract progress, recent meetings or proposals
- Open Items / Risk: blockers, competitive threats, unanswered questions, pending decisions
- Goals for the Week: specific next actions and owner if known
- 3–5 bullets per section; each bullet is 1–2 lines max
- Only include items with real substance — skip generic filler
- Do NOT add any preamble or explanation outside the format above

REPORT:
{report_text}"""

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()


@app.post("/account/{slug}/slack-status", response_class=HTMLResponse)
async def generate_slack_status(request: Request, slug: str):
    """Generate a Slack-ready status update from the most recent lookup report."""
    import html as _html

    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    account_dir = REPORTS_DIR / slug

    # Prefer most-recent lookup; fall back to any report
    report_file = None
    if account_dir.exists():
        lookup_files = sorted(account_dir.glob("*_lookup.md"), reverse=True)
        report_file = lookup_files[0] if lookup_files else None
        if not report_file:
            all_files = sorted(account_dir.glob("*.md"), reverse=True)
            report_file = all_files[0] if all_files else None

    def _err(msg: str) -> HTMLResponse:
        return HTMLResponse(
            f'<div class="card" style="padding:16px;margin-bottom:24px;'
            f'border-left:3px solid var(--red);">'
            f'<p style="color:var(--red);">{msg}</p></div>'
        )

    if not report_file:
        return _err("⚠ No reports found — run a Lookup first.")
    if not api_key:
        return _err("⚠ ANTHROPIC_API_KEY not set.")

    display_name = slug.replace("-", " ").title()
    try:
        report_text = report_file.read_text(encoding="utf-8")[:8000]
        status_text = await asyncio.to_thread(
            _generate_slack_status_via_claude,
            report_text,
            display_name,
            api_key,
        )
    except Exception as exc:
        print(f"  [slack-status] Error for '{slug}': {exc}")
        return _err(f"⚠ Error generating status: {exc}")

    status_escaped = _html.escape(status_text)
    source_label   = report_file.name

    return HTMLResponse(f"""
<div class="card" id="slack-status-card"
     style="padding:20px;margin-bottom:24px;border-left:4px solid #4A90D9;">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;gap:12px;">
    <div>
      <span style="font-weight:600;font-size:14px;">💬 Slack Status</span>
      <span style="color:var(--muted);font-size:12px;margin-left:8px;">from {source_label}</span>
    </div>
    <div style="display:flex;gap:8px;">
      <button class="btn btn-ghost btn-sm" id="slack-copy-btn" onclick="copySlackStatus()">Copy</button>
      <button class="btn btn-ghost btn-sm" style="color:var(--muted);"
              onclick="document.getElementById('slack-status-card').remove()">✕</button>
    </div>
  </div>
  <pre id="slack-status-text"
       style="white-space:pre-wrap;font-family:inherit;font-size:13px;line-height:1.7;margin:0;color:var(--text);">{status_escaped}</pre>
</div>
<script>
(function() {{
  var _statusText = {json.dumps(status_text)};
  window.copySlackStatus = function() {{
    navigator.clipboard.writeText(_statusText).then(function() {{
      var btn = document.getElementById('slack-copy-btn');
      btn.textContent = '✓ Copied!';
      setTimeout(function() {{ btn.textContent = 'Copy'; }}, 2000);
    }});
  }};
}})();
</script>
""")


# ---------------------------------------------------------------------------
# Routes — Tasks (cross-account)
# ---------------------------------------------------------------------------

@app.get("/tasks", response_class=HTMLResponse)
async def tasks_page(request: Request):
    items      = db_module.get_all_action_items_all_accounts(DB_PATH)
    open_count = sum(1 for i in items if not i.get("completed"))
    return templates.TemplateResponse("tasks.html", {
        "request":    request,
        "items":      items,
        "open_count": open_count,
        "active":     "tasks",
    })


@app.get("/ask", response_class=HTMLResponse)
async def ask_page(request: Request, company: str = "", report_type: str = ""):
    accounts = get_all_accounts()
    return templates.TemplateResponse("ask.html", {
        "request": request,
        "accounts": accounts,
        "prefill_company": company,
        "prefill_type": report_type,
    })


# ---------------------------------------------------------------------------
# Routes — Actions
# ---------------------------------------------------------------------------

@app.post("/ask/query", response_class=HTMLResponse)
async def ask_query(
    request: Request,
    company: str = Form(...),
    question: str = Form(...),
    report_type: str = Form(""),
    use_all: str = Form(""),
    conv_id: str = Form(""),
):
    api_key = os.getenv("ANTHROPIC_API_KEY", "")

    # ── Resume or start a conversation ──────────────────────────────────────
    if conv_id and conv_id in _conversations:
        conv = _conversations[conv_id]
        report_text  = conv["report_text"]
        source_label = conv["source_label"]
        prior_msgs   = conv["messages"]
    else:
        slug  = slugify(company.strip())
        paths = find_reports(slug, str(REPORTS_DIR), report_type)
        if not paths:
            return HTMLResponse(
                f'<div class="chat-turn answer-error">'
                f'No reports found for <strong>{company}</strong>. Run a script first.'
                f'</div>'
            )
        paths_to_load = paths if use_all else [paths[-1]]
        report_text  = load_reports(paths_to_load)
        source_label = "all reports" if use_all else paths[-1].name
        prior_msgs   = []
        conv_id      = str(uuid.uuid4())

    # ── Call Claude ──────────────────────────────────────────────────────────
    answer = await asyncio.to_thread(
        ask_claude, report_text, question, api_key, prior_msgs or None
    )

    # ── Persist turn into conversation store ────────────────────────────────
    if prior_msgs:
        # Follow-up: append new pair to existing history
        _conversations[conv_id]["messages"].extend([
            {"role": "user",      "content": question},
            {"role": "assistant", "content": answer},
        ])
    else:
        # First turn: store report text + full first exchange
        _conversations[conv_id] = {
            "report_text":  report_text,
            "source_label": source_label,
            "messages": [
                {"role": "user",      "content": f"{report_text}\n\n---\n\nQuestion: {question}"},
                {"role": "assistant", "content": answer},
            ],
        }

    answer_html  = render_markdown(answer)
    is_first_str = "true" if not prior_msgs else "false"

    return templates.TemplateResponse("_chat_turn.html", {
        "request":      request,
        "question":     question,
        "answer_html":  answer_html,
        "conv_id":      conv_id,
        "source_label": source_label,
        "is_first":     not prior_msgs,
    })


@app.get("/run/stream")
async def run_stream(
    company: str = "",
    script: str = "lookup",
    domain: str = "",
    news: str = "",
    url: str = "",
    verbose: str = "",
    days: str = "1",
    email: str = "",
    sheet_url: str = "",
    limit: str = "",
):
    """SSE endpoint — streams subprocess stdout/stderr line by line."""

    script_info = SCRIPT_META.get(script)
    if not script_info:
        async def error_gen():
            yield f"data: Unknown script: {script}\n\n"
            yield "data: [DONE]\n\n"
        return StreamingResponse(error_gen(), media_type="text/event-stream")

    # Build command
    cmd = [sys.executable, str(BASE_DIR / script_info["script"])]

    if script == "meeting_prep":
        # No company positional — uses --days instead
        try:
            days_int = max(1, min(14, int(days)))
        except ValueError:
            days_int = 1
        cmd += ["--days", str(days_int)]
        if email == "true":
            cmd.append("--email")
    elif script == "bulk_score":
        if not sheet_url:
            async def error_gen():
                yield f'data: {json.dumps({"line": "ERROR: Google Sheet URL is required", "done": True, "exit_code": 1})}\n\n'
            return StreamingResponse(error_gen(), media_type="text/event-stream")
        cmd += ["--sheet-url", sheet_url]
        if limit and limit != "0":
            cmd += ["--limit", limit]
    else:
        if not company:
            async def error_gen():
                yield f'data: {json.dumps({"line": "ERROR: company name is required", "done": True, "exit_code": 1})}\n\n'
            return StreamingResponse(error_gen(), media_type="text/event-stream")
        cmd.append(company)
        if domain:
            cmd += ["--domain", domain]
        if news == "true" and script == "lookup":
            cmd.append("--news")
        if url and script == "score":
            cmd += ["--url", url]
        if script == "prospect" and limit and limit != "0":
            cmd += ["--count", limit]

    if verbose == "true":
        cmd.append("--verbose")

    async def event_gen():
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(BASE_DIR),
                # Force line-by-line flushing — without this Python buffers
                # output in large blocks when stdout is a pipe, causing the
                # terminal to appear frozen until the script exits.
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            async for raw_line in proc.stdout:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
                payload = json.dumps({"line": line})
                yield f"data: {payload}\n\n"
            await proc.wait()
            exit_code = proc.returncode

            # After a successful run, find the newest report file written in
            # the last 2 minutes so we can surface a direct link in the UI.
            report_url = None
            if exit_code == 0:
                if script == "meeting_prep":
                    check_dir = REPORTS_DIR / "meeting_prep"
                elif script == "bulk_score":
                    check_dir = None  # multiple reports — no single link
                else:
                    check_dir = REPORTS_DIR / slugify(company)
                if check_dir and check_dir.exists():
                    candidates = sorted(
                        check_dir.glob("*.md"),
                        key=lambda f: f.stat().st_mtime,
                        reverse=True,
                    )
                    if candidates and (time.time() - candidates[0].stat().st_mtime) < 120:
                        rel = candidates[0].relative_to(REPORTS_DIR)
                        report_url = f"/report/{rel.parts[0]}/{rel.parts[1]}"

            yield f"data: {json.dumps({'done': True, 'exit_code': exit_code, 'report_url': report_url})}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'line': f'ERROR: {exc}', 'done': True, 'exit_code': 1})}\n\n"

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Prospecting Toolkit web server")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", 8000)), help="Port to listen on (default: 8000, or $PORT)")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)")
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable auto-reload (recommended when running under launchd)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    import uvicorn
    args = parse_args()
    print(f"Starting P0 at http://{args.host}:{args.port}")
    uvicorn.run("server:app", host=args.host, port=args.port, reload=not args.no_reload)
