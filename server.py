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

load_dotenv(override=True)

BASE_DIR = Path(__file__).parent
REPORTS_DIR = BASE_DIR / "reports"
DB_PATH = BASE_DIR / "prospecting.db"

app = FastAPI(title="Prospecting Toolkit")
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
        "description": "Score the account on weighted dimensions for You.com Search API / RAG fit.",
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

    # Run blocking SF call in a thread — wrap in try/except so any unhandled
    # exception becomes a graceful error result rather than a 500.
    try:
        result = await asyncio.to_thread(pull_salesforce, display_name, config, False)
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
    return templates.TemplateResponse("_action_row.html", {
        "request": request,
        "slug": slug,
        "item": item,
    })


@app.delete("/account/{slug}/actions/{item_id}")
async def delete_action(slug: str, item_id: int):
    db_module.delete_action_item(DB_PATH, item_id)
    return Response(content="", status_code=200)


# ---------------------------------------------------------------------------
# Routes — Tasks (cross-account)
# ---------------------------------------------------------------------------

@app.get("/tasks", response_class=HTMLResponse)
async def tasks_page(request: Request):
    items = db_module.get_all_open_action_items(DB_PATH)
    return templates.TemplateResponse("tasks.html", {
        "request": request,
        "items": items,
        "active": "tasks",
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
):
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    slug = slugify(company.strip())
    paths = find_reports(slug, str(REPORTS_DIR), report_type)

    if not paths:
        return HTMLResponse(
            f'<div class="answer-error">No reports found for <strong>{company}</strong>.'
            f' Run a script first.</div>'
        )

    paths_to_load = paths if use_all else [paths[-1]]
    report_text = load_reports(paths_to_load)
    source_label = "all reports" if use_all else paths[-1].name

    answer = ask_claude(report_text, question, api_key)
    answer_html = render_markdown(answer)

    return HTMLResponse(f"""
        <div class="answer-meta">
            Answered from: <span class="answer-source">{source_label}</span>
        </div>
        <div class="answer-body">{answer_html}</div>
    """)


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
                else:
                    check_dir = REPORTS_DIR / slugify(company)
                if check_dir.exists():
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
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on (default: 8000)")
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
    print(f"Starting Prospecting Toolkit at http://{args.host}:{args.port}")
    uvicorn.run("server:app", host=args.host, port=args.port, reload=not args.no_reload)
