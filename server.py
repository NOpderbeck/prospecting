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
import argparse
import asyncio
import subprocess
from datetime import date
from pathlib import Path

import markdown as md_lib
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

from context import slugify
from ask import find_reports, load_reports, ask_claude

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

load_dotenv(override=True)

BASE_DIR = Path(__file__).parent
REPORTS_DIR = BASE_DIR / "reports"

app = FastAPI(title="Prospecting Toolkit")
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
    "context": {
        "label": "Internal Context",
        "script": "context.py",
        "description": "Deep-dive into Salesforce history, Slack threads, and Google Drive for an account.",
        "color": "purple",
        "icon": "📁",
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
        "display": slug.replace("-", " ").title(),
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
    company: str,
    script: str = "lookup",
    domain: str = "",
    news: str = "",
    url: str = "",
    verbose: str = "",
):
    """SSE endpoint — streams subprocess stdout/stderr line by line."""

    script_info = SCRIPT_META.get(script)
    if not script_info:
        async def error_gen():
            yield f"data: Unknown script: {script}\n\n"
            yield "data: [DONE]\n\n"
        return StreamingResponse(error_gen(), media_type="text/event-stream")

    # Build command
    cmd = [sys.executable, str(BASE_DIR / script_info["script"]), company]
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
            )
            async for raw_line in proc.stdout:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
                # SSE: escape any embedded newlines, send as data field
                payload = json.dumps({"line": line})
                yield f"data: {payload}\n\n"
            await proc.wait()
            exit_code = proc.returncode
            yield f"data: {json.dumps({'done': True, 'exit_code': exit_code})}\n\n"
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
    return parser.parse_args()


if __name__ == "__main__":
    import uvicorn
    args = parse_args()
    print(f"Starting Prospecting Toolkit at http://{args.host}:{args.port}")
    uvicorn.run("server:app", host=args.host, port=args.port, reload=True)
