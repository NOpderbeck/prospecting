"""
gong_insights_run.py — Weekly Gong Product Intelligence Report

Pulls all Gong call transcripts from the past N days, uses Claude to extract
product intelligence, publishes a Google Doc, and posts a summary to Slack.

Usage:
    python gong_insights_run.py                          # full run
    python gong_insights_run.py --days 14                # extend window
    python gong_insights_run.py --from-cache reports/gong_insights_cache_2026-04-22.json
    python gong_insights_run.py --from-cache reports/gong_insights_cache_2026-04-22.json --dry-run
    python gong_insights_run.py --skip-publish           # analysis only, no Drive/Slack

Environment variables:
    GONG_API_KEY, GONG_API_SECRET   — Gong REST API credentials
    ANTHROPIC_API_KEY               — Claude API key
    GOOGLE_CREDENTIALS_FILE         — path to OAuth credentials JSON
    SLACK_BOT_TOKEN                 — Slack bot token (needs chat:write scope)
"""

import argparse
import base64
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
import anthropic
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GONG_BASE             = "https://us-64844.api.gong.io"
MIN_DURATION_SECONDS  = 120
GONG_RATE_SLEEP       = 0.4

DRIVE_FOLDER_ID       = "1HwwN52Gnsl1t0C5urtiB7xHwOnBOqk2p"
GOOGLE_TOKEN_PATH     = os.path.join(".credentials", "google_token_gong_insights.json")
GOOGLE_SCOPES         = ["https://www.googleapis.com/auth/drive",
                         "https://www.googleapis.com/auth/documents"]
SLACK_CHANNEL         = "C0B01T4505N"   # #weekly-gong-insights — ID is rename-safe


# Patterns that identify internal-only calls by title when no CRM account is linked
_INTERNAL_TITLE_RE = re.compile(
    r'\b(team\s+weekly|team\s+sync|internal|all.?hands|stand.?up|standup|'
    r'retrospective|retro|onboarding|training|interview|hiring|'
    r'kickoff\s+internal|company\s+update|culture|offsite)\b',
    re.IGNORECASE,
)


def _is_likely_external(call: dict) -> bool:
    """Return True if the call appears to be with an external party."""
    meta         = call.get("metaData") or {}
    account_name = (meta.get("primaryAccount") or {}).get("name") or ""
    if account_name:
        return True
    title = (call.get("title") or "").strip()
    if _INTERNAL_TITLE_RE.search(title):
        return False
    return True  # no CRM account but title doesn't look internal — include


# ---------------------------------------------------------------------------
# CLI & Config
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Extract product intelligence from Gong call transcripts.",
        epilog="Example: python gong_insights_run.py --days 7 --verbose",
    )
    p.add_argument("--days",        type=int, default=7,
                   help="Days to look back (default: 7)")
    p.add_argument("--max-calls",   type=int, default=75,
                   help="Max calls to analyze (default: 75)")
    p.add_argument("--output-dir",  default="reports",
                   help="Output directory (default: reports/)")
    p.add_argument("--verbose",     action="store_true",
                   help="Print API response details")
    p.add_argument("--from-cache",  metavar="FILE",
                   help="Skip Gong + Claude; load analysis from cache JSON")
    p.add_argument("--dry-run",     action="store_true",
                   help="Print Slack message; skip Google Doc creation and Slack post")
    p.add_argument("--skip-publish", action="store_true",
                   help="Save analysis + cache but skip Google Doc and Slack")
    return p.parse_args()


def load_config():
    load_dotenv(override=True)
    required = {
        "gong_api_key":    os.getenv("GONG_API_KEY"),
        "gong_api_secret": os.getenv("GONG_API_SECRET"),
        "anthropic_api_key": os.getenv("ANTHROPIC_API_KEY"),
    }
    missing = [k for k, v in required.items() if not v or v.startswith("your_")]
    if missing:
        print(f"ERROR: Missing config: {', '.join(missing)}")
        sys.exit(1)
    return {
        **required,
        "google_creds_file":   os.getenv("GOOGLE_CREDENTIALS_FILE", ""),
        "slack_bot_token":     os.getenv("SLACK_BOT_TOKEN", ""),
        "sf_username":         os.getenv("SF_USERNAME", ""),
        "sf_password":         os.getenv("SF_PASSWORD", ""),
        "sf_security_token":   os.getenv("SF_SECURITY_TOKEN", ""),
        "sf_domain":           os.getenv("SF_DOMAIN", "login"),
    }


# ---------------------------------------------------------------------------
# Gong API
# ---------------------------------------------------------------------------

def _gong_headers(config: dict) -> dict:
    token = base64.b64encode(
        f"{config['gong_api_key']}:{config['gong_api_secret']}".encode()
    ).decode()
    return {"Authorization": f"Basic {token}"}


def fetch_calls(config: dict, from_dt: datetime, to_dt: datetime,
                max_calls: int, verbose: bool) -> list[dict]:
    url = f"{GONG_BASE}/v2/calls"
    headers = _gong_headers(config)
    calls: list[dict] = []
    cursor = None
    from_str = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str   = to_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"  Window: {from_str} → {to_str}")

    while True:
        params: dict = {"fromDateTime": from_str, "toDateTime": to_str}
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if verbose:
            print(f"  API page: totalRecords={data.get('records', {}).get('totalRecords', '?')}")

        for call in data.get("calls", []):
            if not _is_likely_external(call):
                continue
            if (call.get("duration") or 0) >= MIN_DURATION_SECONDS:
                calls.append(call)
            if len(calls) >= max_calls:
                print(f"  Reached --max-calls cap ({max_calls})")
                return calls

        cursor = data.get("records", {}).get("cursor")
        if not cursor:
            break
        time.sleep(GONG_RATE_SLEEP)

    print(f"  {len(calls)} qualifying calls (≥{MIN_DURATION_SECONDS}s)")
    return calls


def fetch_transcripts(config: dict, call_ids: list[str],
                      verbose: bool) -> dict[str, list]:
    if not call_ids:
        return {}

    url = f"{GONG_BASE}/v2/calls/transcript"
    headers = {**_gong_headers(config), "Content-Type": "application/json"}
    result: dict[str, list] = {}

    for i in range(0, len(call_ids), 100):
        batch = call_ids[i: i + 100]
        resp = requests.post(url, headers=headers,
                             json={"filter": {"callIds": batch}}, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("callTranscripts", []):
            cid = item.get("callId")
            if cid:
                result[cid] = item.get("transcript", [])

        if verbose:
            print(f"  Transcript batch {i//100 + 1}: {len(result)} received so far")

        if i + 100 < len(call_ids):
            time.sleep(GONG_RATE_SLEEP)

    return result


def format_transcript(call: dict, transcript_parts: list) -> str:
    speaker_map: dict[str, str] = {}
    for party in call.get("parties", []):
        pid = party.get("speakerId") or party.get("id") or ""
        name = (party.get("name") or "Unknown").strip()
        affiliation = (party.get("affiliation") or "").capitalize()
        if pid:
            speaker_map[pid] = f"{name} ({affiliation})" if affiliation else name

    meta = call.get("metaData") or {}
    account_name = (meta.get("primaryAccount") or {}).get("name") or ""
    deal_stage   = (meta.get("primaryOpportunity") or {}).get("stage") or ""

    call_id  = call.get("id") or ""
    call_url = f"https://us-64844.app.gong.io/call?id={call_id}" if call_id else ""

    lines = [
        f"## {call.get('title') or 'Untitled Call'}",
        f"- Date: {(call.get('started') or '')[:10] or 'unknown'}",
        f"- Duration: {(call.get('duration') or 0) // 60} min",
        f"- Account: {account_name or 'unknown'}",
        f"- Deal Stage: {deal_stage or 'unknown'}",
        f"- URL: {call_url}",
        "",
        "### Transcript",
    ]

    sentences: list[tuple[float, str, str]] = []
    for part in transcript_parts:
        speaker = speaker_map.get(part.get("speakerId") or "", "Unknown")
        for s in part.get("sentences", []):
            text = (s.get("text") or "").strip()
            if text:
                sentences.append((s.get("start", 0) / 1000.0, speaker, text))

    sentences.sort(key=lambda x: x[0])
    current_speaker: str | None = None
    for _, speaker, text in sentences:
        if speaker != current_speaker:
            lines.append(f"\n**{speaker}:** {text}")
            current_speaker = speaker
        else:
            lines.append(text)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Claude analysis
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a product intelligence analyst extracting actionable insights from
enterprise software sales call transcripts. Your output directly informs
product roadmap decisions, competitive positioning, and GTM strategy.

Rules:
- Use DIRECT QUOTES from transcripts wherever possible
- Do NOT hallucinate — only extract what is explicitly stated or clearly implied
- Note the account name and call date with every finding
- Ignore small talk, scheduling logistics, and pleasantries
- Flag when the same theme appears across multiple accounts"""


ANALYSIS_PROMPT = """\
Analyze the following Gong call transcripts (past {days} days, {n} calls).

{transcripts}

---

Produce a structured product intelligence report with EXACTLY these sections:

## Executive Summary
3–5 bullets. Highest-impact insights for Product leadership. Lead with what
Product should do differently because of this week's calls.

## Product Gaps (Ranked by Frequency & Impact)
For each gap theme:
- **[Theme]** — {n_placeholder} mentions | Impact: High / Medium / Low
  - Quote: "exact customer words" — [Account], [Date]
  - Normalized need: [one-sentence description]
  - Accounts: [comma-separated list of all accounts that raised this gap]

Rank descending by (frequency × impact).

## Capability Wins
Where customers reacted positively to existing capabilities. For each:
- **[Capability]**
  - Quote: "exact customer words" — [Account], [Date]
  - Why it mattered: [complete sentence, e.g. "It mattered because..." or "This resonated because..."]

## Feature Opportunities
Cluster related requests into themes. For each:
- **[Theme]** — signal: Early / Repeated / Strong demand
  - Quote: "exact customer words" — [Account], [Date]
  - Why it matters: [context]

## Objection Analysis
For each objection type:
- **[Objection]** — {n_placeholder} occurrences | Stage: [funnel stage]
  - Root cause: [one-sentence classification]
  - Mitigation: [suggested product or GTM response]
  - Quote: "exact customer words" — [Account], [Date]
  - Accounts: [comma-separated list]

## Competitive Landscape
For each competitor mentioned:
- **[Competitor]** — {n_placeholder} mentions
  - Context: evaluation / replacement / comparison / dissatisfaction
  - Outcome: win / loss / unknown
  - Differentiators customers cited

## Raw Evidence Appendix
| Category | Quote | Account | Date | Call URL |
|----------|-------|---------|------|----------|

Include ALL extracted quotes with source info.

---
Main sections (Executive Summary through Competitive Landscape): max 2,000 words.
Every insight must cite the account it came from. Cover all accounts represented in the transcripts — do not omit any.
IMPORTANT: Always use the exact account name from the "Account:" field in the transcript header (e.g. "OWKIN", "Mutiny HQ"). Never substitute a company name mentioned in conversation — the header field is the source of truth.
Use bullet points throughout. No filler.\
"""


def analyze_with_claude(config: dict, transcript_texts: list[str],
                        days: int, verbose: bool) -> str:
    client = anthropic.Anthropic(api_key=config["anthropic_api_key"])
    combined = "\n\n---\n\n".join(transcript_texts)

    if len(combined) > 450_000:
        print(f"  Trimming combined transcript: {len(combined):,} → 450,000 chars")
        combined = combined[:450_000] + "\n\n[... additional transcripts truncated ...]"

    print(f"  Sending {len(combined):,} chars across {len(transcript_texts)} transcripts to Claude ...")

    prompt = ANALYSIS_PROMPT.format(
        days=days,
        n=len(transcript_texts),
        n_placeholder="N",
        transcripts=combined,
    )

    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        system=[{"type": "text", "text": SYSTEM_PROMPT,
                 "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": prompt}],
    )

    if verbose:
        u = resp.usage
        cache_read = getattr(u, "cache_read_input_tokens", 0)
        cache_create = getattr(u, "cache_creation_input_tokens", 0)
        print(f"  Claude tokens — input: {u.input_tokens:,} | output: {u.output_tokens:,} "
              f"| cache_read: {cache_read:,} | cache_created: {cache_create:,}")

    return resp.content[0].text


# ---------------------------------------------------------------------------
# Cache: save / load
# ---------------------------------------------------------------------------

def count_unique_accounts(calls: list[dict]) -> int:
    """Count unique non-empty account names across all calls."""
    names = set()
    for call in calls:
        name = ((call.get("metaData") or {}).get("primaryAccount") or {}).get("name") or ""
        if name.strip():
            names.add(name.strip().lower())
    return len(names)


def count_accounts_from_analysis(analysis: str) -> int:
    """
    Count unique accounts across the full analysis:
    1. Account column of the Raw Evidence Appendix table
    2. Inline citation patterns '— AccountName, YYYY-MM-DD' throughout the body
    Normalises by stripping parentheticals, lowercasing, and collapsing
    qualifier suffixes ('eval', 'call', 'ceo', 'cto', etc.) so 'CrewAI CEO'
    and 'CrewAI (Joao)' both reduce to 'crewai'.
    """
    QUALIFIER_WORDS = {"eval", "call", "ceo", "cto", "vp", "svp", "cso",
                       "founder", "team", "group"}
    raw: set[str] = set()

    # Source 1: appendix table Account column
    section = _extract_section(analysis, "Raw Evidence Appendix")
    for line in section.split("\n"):
        if not line.startswith("|"):
            continue
        parts = [p.strip() for p in line.strip("|").split("|")]
        if len(parts) >= 3:
            acct = re.sub(r'\s*\(.*?\)', '', parts[2]).strip().lower()
            if acct and acct not in ("account", "") and re.match(r'^[a-z]', acct):
                raw.add(acct)

    # Source 2: inline '— Account, YYYY-MM-DD' citations in body
    # Restrict account name to ≤4 words and no embedded quotes/dashes
    for m in re.finditer(
        r'—\s*([A-Z][A-Za-z0-9 /]{1,35}?),?\s*\d{4}-\d{2}-\d{2}', analysis
    ):
        acct_raw = m.group(1).strip()
        # Skip if the matched text contains quote chars or another em-dash
        if any(c in acct_raw for c in ('"', '"', '"', '—', '–')):
            continue
        acct = re.sub(r'\s*\(.*?\)', '', acct_raw).strip().lower()
        if acct:
            raw.add(acct)

    # Normalise: drop trailing qualifier words
    def _strip_qualifiers(name: str) -> str:
        words = name.split()
        while words and words[-1].rstrip('.') in QUALIFIER_WORDS:
            words = words[:-1]
        return " ".join(words)

    normalised = {_strip_qualifiers(a) for a in raw if a}
    normalised.discard("")

    # Deduplicate: if 'crewai call' and 'crewai' both exist, keep the shorter
    final: set[str] = set()
    for name in sorted(normalised, key=len):
        if not any(name.startswith(existing + " ") for existing in final):
            final.add(name)

    return len(final)


def build_owner_map_from_calls(calls: list[dict]) -> dict[str, str]:
    """Build {account_name_lower: owner_first_name} from Gong call metadata.

    Uses primaryUserId matched against call parties — same data Gong shows
    on its own account record, so it's always correct (no LIKE ambiguity).
    """
    owner_map: dict[str, str] = {}
    for call in calls:
        meta = call.get("metaData") or {}
        acct = ((meta.get("primaryAccount") or {}).get("name") or "").strip()
        if not acct:
            continue
        primary_uid = meta.get("primaryUserId") or ""
        for party in call.get("parties", []):
            uid = party.get("userId") or party.get("id") or ""
            if uid and uid == primary_uid:
                name = (party.get("name") or "").strip()
                if name:
                    owner_map[acct.lower()] = name.split()[0]
                break
    return owner_map


def save_cache(out_dir: Path, today: str, analysis: str,
               calls_count: int, accounts_count: int, days: int,
               owner_map: dict[str, str] | None = None) -> Path:
    cache = {
        "analysis":       analysis,
        "calls_count":    calls_count,
        "accounts_count": accounts_count,
        "date":           today,
        "days":           days,
        "owner_map":      owner_map or {},
    }
    cache_path = out_dir / f"gong_insights_cache_{today}.json"
    cache_path.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    print(f"  Cache saved → {cache_path}")
    return cache_path


def load_cache(cache_file: str) -> dict:
    p = Path(cache_file)
    if not p.exists():
        print(f"ERROR: cache file not found: {cache_file}")
        sys.exit(1)
    data = json.loads(p.read_text(encoding="utf-8"))
    required = {"analysis", "calls_count", "date", "days"}
    missing = required - set(data.keys())
    if missing:
        print(f"ERROR: cache file missing fields: {missing}")
        sys.exit(1)
    if "accounts_count" not in data:
        data["accounts_count"] = count_accounts_from_analysis(data["analysis"])
    if "owner_map" not in data:
        data["owner_map"] = {}
    print(f"  Loaded cache: {data['calls_count']} calls, "
          f"{data['accounts_count']} accounts, date={data['date']}, days={data['days']}")
    return data


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------

def _extract_section(analysis: str, heading: str) -> str:
    """Return the raw text of a ## section."""
    m = re.search(
        rf'##\s+{re.escape(heading)}.*?\n(.*?)(?=\n##\s|\Z)',
        analysis, re.DOTALL | re.IGNORECASE
    )
    return m.group(1) if m else ""


_BOLD_ITEM = re.compile(r'^(?:- )?\*\*(?:\d+\.\s+)?(.+?)\*\*')


def _top_items(section: str, max_items: int = 4) -> list[str]:
    """Bold-labeled top-level bullets (handles '- **label**' and '**N. label**' formats)."""
    items = []
    for line in section.split("\n"):
        if line.startswith((" ", "\t")):
            continue
        m = _BOLD_ITEM.match(line.strip())
        if m:
            items.append(m.group(1))
        if len(items) >= max_items:
            break
    return items


def _extract_counts(section: str, max_items: int = 4) -> list[tuple[str, int]]:
    """Bold-labeled bullets with count, sorted descending. Returns (label, count) list."""
    items = []
    for line in section.split("\n"):
        if line.startswith((" ", "\t")):
            continue
        m_label = _BOLD_ITEM.match(line.strip())
        if not m_label:
            continue
        m_count = re.search(r'(\d+)\s+(?:mention|occurrence)', line)
        count = int(m_count.group(1)) if m_count else 0
        items.append((m_label.group(1), count))
    items.sort(key=lambda x: -x[1])
    return items[:max_items]


def _extract_gaps_brief(section: str, max_items: int = 4) -> list[str]:
    """Gap items as '• Theme ×N [Impact]' — no descriptions."""
    results = []
    for line in section.split("\n"):
        if line.startswith((" ", "\t")):
            continue
        m = _BOLD_ITEM.match(line.strip())
        if not m:
            continue
        label = m.group(1)
        m_count  = re.search(r'(\d+)\s+mention', line)
        m_impact = re.search(r'Impact:\s*(\w+)', line)
        count_str  = f" ×{m_count.group(1)}" if m_count else ""
        impact_str = f" [{m_impact.group(1)}]" if m_impact else ""
        results.append(f"• *{label}*{count_str}{impact_str}")
        if len(results) >= max_items:
            break
    return results


def _extract_objections_brief(section: str, max_items: int = 3) -> list[str]:
    """Objection items as '• Theme  _stage_' — no root cause / description."""
    results = []
    for line in section.split("\n"):
        if line.startswith((" ", "\t")):
            continue
        m = _BOLD_ITEM.match(line.strip())
        if not m:
            continue
        label = m.group(1).strip('"')
        m_stage = re.search(r'Stage:\s*([^|]+)', line)
        stage_str = f"  _{m_stage.group(1).strip()}_" if m_stage else ""
        results.append(f"• *{label}*{stage_str}")
        if len(results) >= max_items:
            break
    return results


def _competitor_bar_chart(section: str, max_items: int = 5, bar_width: int = 10) -> list[str]:
    """Render competitors as Unicode bar chart lines, sorted descending by count."""
    items = _extract_counts(section, max_items)
    if not items:
        return []
    max_count = max(c for _, c in items) or 1
    lines = []
    for label, count in items:
        bars = round((count / max_count) * bar_width)
        bar = "█" * bars + "░" * (bar_width - bars)
        lines.append(f"`{bar}` *{label}* ×{count}" if count else f"`{'░' * bar_width}` *{label}*")
    return lines


def build_slack_message(analysis: str, calls_count: int, accounts_count: int,
                        days: int, today: str, doc_url: str) -> str:
    end   = date.fromisoformat(today)
    start = end - timedelta(days=days - 1)
    date_range = f"{start.strftime('%b %-d')}–{end.strftime('%-d, %Y')}"

    wins_lines = [f"• {w}" for w in _top_items(_extract_section(analysis, "Capability Wins"), 3)] or ["—"]
    gap_lines  = _extract_gaps_brief(_extract_section(analysis, "Product Gaps"), 4) or ["—"]
    obj_lines  = _extract_objections_brief(_extract_section(analysis, "Objection Analysis"), 3) or ["—"]
    comp_lines = _competitor_bar_chart(_extract_section(analysis, "Competitive Landscape"), 5) or ["—"]

    lines = [
        f"*📊 Gong Insights — {date_range}*  _{calls_count} calls · {accounts_count} accounts_",
        "",
        "*✅ What's Landing*",
        *wins_lines,
        "",
        "*🔧 Top Gaps*",
        *gap_lines,
        "",
        "*🚧 Objections*",
        *obj_lines,
        "",
        "*🏁 Competitor mentions*",
        *comp_lines,
        "",
        f"📄 *<{doc_url}|Full Report>*",
    ]
    return "\n".join(lines)


def post_to_slack(token: str, message: str, dry_run: bool = False):
    if dry_run:
        print("\n── DRY RUN: Slack message (not posted) ──────────────────")
        print(message)
        print("──────────────────────────────────────────────────────────\n")
        return

    if not token:
        print("  ⚠️  No SLACK_BOT_TOKEN — skipping Slack post")
        return

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"channel": SLACK_CHANNEL, "text": message,
              "mrkdwn": True, "unfurl_links": False},
        timeout=10,
    )
    result = resp.json()
    if result.get("ok"):
        print(f"  ✅ Posted to {SLACK_CHANNEL}")
    else:
        print(f"  ⚠️  Slack post failed: {result.get('error')}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Google Auth
# ---------------------------------------------------------------------------

def get_google_creds(creds_file: str):
    sa_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if sa_key and os.path.exists(sa_key):
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_file(
            sa_key, scopes=GOOGLE_SCOPES)

    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request as GoogleRequest

    # Cloud Run: token JSON injected via Secret Manager env var
    token_json = os.getenv("GOOGLE_TOKEN_JSON", "")
    if token_json:
        creds = Credentials.from_authorized_user_info(
            json.loads(token_json), GOOGLE_SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())
        if creds and creds.valid:
            return creds

    creds = None
    if os.path.exists(GOOGLE_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_PATH, GOOGLE_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())
        else:
            if not creds_file or not os.path.exists(creds_file):
                print(f"ERROR: Google credentials file not found: {creds_file}")
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, GOOGLE_SCOPES)
            creds = flow.run_local_server(port=0)
        write_path = GOOGLE_TOKEN_PATH
        try:
            os.makedirs(os.path.dirname(write_path), exist_ok=True)
            Path(write_path).write_text(creds.to_json())
        except OSError:
            write_path = "/tmp/google_token_gong_insights.json"
            Path(write_path).write_text(creds.to_json())

    if creds:
        return creds

    try:
        import google.auth
        creds, _ = google.auth.default(scopes=GOOGLE_SCOPES)
        return creds
    except Exception:
        pass

    print("ERROR: No Google credentials found")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Google Docs helpers  (same pattern as penetration_publish.py)
# ---------------------------------------------------------------------------

def _batch_update(docs_svc, doc_id: str, requests_list: list):
    from googleapiclient.errors import HttpError
    delay = 15
    for attempt in range(6):
        try:
            docs_svc.documents().batchUpdate(
                documentId=doc_id, body={"requests": requests_list}
            ).execute()
            return
        except HttpError as e:
            if e.resp.status == 429 and attempt < 5:
                print(f"  Rate limited — retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2
            else:
                raise


def _doc_end(docs_svc, doc_id: str) -> int:
    d = docs_svc.documents().get(documentId=doc_id).execute()
    return d["body"]["content"][-1]["endIndex"] - 1


def _utf16_len(s: str) -> int:
    """Length in UTF-16 code units (what Google Docs API uses for positions)."""
    return sum(1 if ord(c) < 0x10000 else 2 for c in s)


def _py_to_utf16_offset(text: str, py_offset: int) -> int:
    """Convert a Python character offset to a UTF-16 code-unit offset."""
    return _utf16_len(text[:py_offset])


def _append_segments(docs_svc, doc_id: str, segments: list):
    """
    Append tuples to the doc. Each tuple is:
      (text, named_style, bold)                    — whole-line bold
      (text, named_style, bold, inline_formats)    — inline_formats: [(rel_start, rel_end, fmt)]
                                                     where fmt = 'bold' or a URL string
    """
    if not segments:
        return
    # Normalise to 4-tuples
    normalized = [s if len(s) == 4 else (*s, []) for s in segments]

    insert_pos = _doc_end(docs_svc, doc_id)
    full_text  = "\n".join(t for t, *_ in normalized) + "\n"
    _batch_update(docs_svc, doc_id,
                  [{"insertText": {"location": {"index": insert_pos}, "text": full_text}}])

    style_reqs = []
    pos = insert_pos
    for text, style, bold, inline_fmts in normalized:
        end = pos + _utf16_len(text) + 1
        if style != "NORMAL_TEXT":
            style_reqs.append({"updateParagraphStyle": {
                "range": {"startIndex": pos, "endIndex": end},
                "paragraphStyle": {"namedStyleType": style},
                "fields": "namedStyleType",
            }})
        if bold and text:
            style_reqs.append({"updateTextStyle": {
                "range": {"startIndex": pos, "endIndex": pos + _utf16_len(text)},
                "textStyle": {"bold": True},
                "fields": "bold",
            }})
        for rel_s, rel_e, fmt in inline_fmts:
            rng = {"startIndex": pos + _py_to_utf16_offset(text, rel_s),
                   "endIndex":   pos + _py_to_utf16_offset(text, rel_e)}
            if fmt == "bold":
                style_reqs.append({"updateTextStyle": {
                    "range": rng, "textStyle": {"bold": True}, "fields": "bold",
                }})
            elif fmt == "italic":
                style_reqs.append({"updateTextStyle": {
                    "range": rng, "textStyle": {"italic": True}, "fields": "italic",
                }})
            elif fmt == "highlight":
                style_reqs.append({"updateTextStyle": {
                    "range": rng,
                    "textStyle": {"backgroundColor": {"color": {"rgbColor": {
                        "red": 0.741, "green": 0.843, "blue": 0.933}}}},
                    "fields": "backgroundColor",
                }})
            else:
                style_reqs.append({"updateTextStyle": {
                    "range": rng, "textStyle": {"link": {"url": fmt}}, "fields": "link",
                }})
        pos = end

    for i in range(0, len(style_reqs), 200):
        _batch_update(docs_svc, doc_id, style_reqs[i:i+200])


def _append_table(docs_svc, doc_id: str, headers: list, rows: list):
    """Append a table. Cells that are URLs become '→ Link' hyperlinks."""
    if not rows:
        return

    insert_pos = _doc_end(docs_svc, doc_id)
    n_cols     = len(headers)
    _batch_update(docs_svc, doc_id, [{"insertTable": {
        "rows": len(rows) + 1,
        "columns": n_cols,
        "location": {"index": insert_pos},
    }}])

    d = docs_svc.documents().get(documentId=doc_id).execute()
    table_elem = next(
        (e["table"] for e in reversed(d["body"]["content"]) if "table" in e), None)
    if not table_elem:
        return

    # Build cell data: (index, display_text, bold, url)
    cell_data = []
    for ci, h in enumerate(headers):
        cell = table_elem["tableRows"][0]["tableCells"][ci]
        cell_data.append((cell["content"][0]["startIndex"], h, True, None))

    for ri, row in enumerate(rows, start=1):
        for ci, val in enumerate(row):
            cell = table_elem["tableRows"][ri]["tableCells"][ci]
            idx  = cell["content"][0]["startIndex"]
            val  = str(val)
            if val.startswith("http"):
                cell_data.append((idx, "→ Link", False, val))
            else:
                cell_data.append((idx, val, False, None))

    cell_data.sort(key=lambda x: -x[0])

    cell_reqs = []
    for idx, text, bold, url in cell_data:
        cell_reqs.append({"insertText": {"location": {"index": idx}, "text": text}})
        if bold:
            cell_reqs.append({"updateTextStyle": {
                "range": {"startIndex": idx, "endIndex": idx + len(text)},
                "textStyle": {"bold": True}, "fields": "bold",
            }})
        if url:
            cell_reqs.append({"updateTextStyle": {
                "range": {"startIndex": idx, "endIndex": idx + len(text)},
                "textStyle": {"link": {"url": url}}, "fields": "link",
            }})

    for i in range(0, len(cell_reqs), 200):
        _batch_update(docs_svc, doc_id, cell_reqs[i:i+200])


# ---------------------------------------------------------------------------
# Markdown → Google Doc renderer
# ---------------------------------------------------------------------------

def _strip_bold(text: str) -> str:
    return re.sub(r'\*\*(.+?)\*\*', r'\1', text)


def _parse_appendix_urls(analysis: str) -> dict[tuple[str, str], str]:
    """
    Parse the Raw Evidence Appendix table into {(account_key, date): call_url}.
    account_key is lowercased with parentheticals stripped.
    """
    section = _extract_section(analysis, "Raw Evidence Appendix")
    urls: dict[tuple[str, str], str] = {}
    for line in section.split("\n"):
        if not line.startswith("|"):
            continue
        parts = [p.strip() for p in line.strip("|").split("|")]
        if len(parts) < 5:
            continue
        account_raw, date, url = parts[2], parts[3], parts[4]
        if not url.startswith("http") or not date:
            continue
        # Normalise: strip parentheticals, lowercase, first token only
        account_key = re.sub(r'\s*\(.*?\)', '', account_raw).strip().lower()
        urls[(account_key, date)] = url
    return urls


def _parse_inline_formats(text: str, url_dict: dict) -> tuple[str, list]:
    """
    Strip ** markers and compute inline bold ranges.
    Also detects '— Account, YYYY-MM-DD' citations and hyperlinks them
    when a matching URL exists in url_dict.
    Returns (plain_text, [(rel_start, rel_end, fmt)]).
    """
    formats: list[tuple[int, int, str]] = []

    # Walk character-by-character, stripping ** and recording bold ranges
    result: list[str] = []
    i, bold_start = 0, None
    while i < len(text):
        if text[i:i+2] == "**":
            if bold_start is None:
                bold_start = len(result)
            else:
                formats.append((bold_start, len(result), "bold"))
                bold_start = None
            i += 2
        else:
            result.append(text[i])
            i += 1
    plain = "".join(result)

    # Citation links: "— AccountName [(Person)] , YYYY-MM-DD" anywhere in line
    # Uses finditer so mid-line dates (e.g. Capability Wins) are also caught.
    _ACCT_QUALIFIERS = {"eval", "call", "ceo", "cto", "vp", "svp", "cso", "team"}
    if url_dict:
        for m in re.finditer(
            r'—\s*([A-Z][A-Za-z0-9() /]{1,40}?),?\s*(\d{4}-\d{2}-\d{2})', plain
        ):
            raw_acct = re.sub(r'\s*\(.*?\)', '', m.group(1)).strip().lower()
            # Strip trailing qualifier words to match appendix key (e.g. "hawking eval" → "hawking")
            words = raw_acct.split()
            while words and words[-1] in _ACCT_QUALIFIERS:
                words = words[:-1]
            account_key = " ".join(words)
            date = m.group(2).strip()
            url = url_dict.get((account_key, date)) or url_dict.get((raw_acct, date))
            if url:
                formats.append((m.start(), m.end(), url))

    return plain, formats


def render_markdown_to_doc(docs_svc, doc_id: str, markdown_text: str,
                           url_dict: dict | None = None,
                           owner_map: dict[str, str] | None = None):
    """
    Parse Claude's markdown output and write it into the Google Doc.

    - Skips the Raw Evidence Appendix section (call links are embedded inline).
    - Applies inline bold and citation hyperlinks via url_dict.
    - Bullets use inline bold only (not whole-line bold), so Exec Summary
      reads as normal weight text with bolded lead phrases.
    """
    H1, H2, N = "HEADING_1", "HEADING_2", "NORMAL_TEXT"
    url_dict  = url_dict  or {}
    owner_map = owner_map or {}

    SECTION_EMOJIS = {
        "executive summary":       "📋",
        "product gaps":            "🔧",
        "feature opportunities":   "💡",
        "competitive landscape":   "🏁",
        "objection analysis":      "🚧",
        "capability wins":         "✅",
        "raw evidence appendix":   "📎",
    }

    # Sections that use structured theme entries (highlight + label-bold formatting)
    DATA_SECTIONS = {
        "product gaps", "feature opportunities", "competitive landscape",
        "objection analysis", "capability wins",
    }
    current_section: str = ""
    # Buffer sub-items so we can emit in desired order: norm → quote → blocking
    pending_entry: dict = {}

    def flush_entry():
        nonlocal pending_entry
        if not pending_entry:
            return
        # Fallback: derive "Mentioned by" from last citation if no explicit Accounts line
        quotes = pending_entry.get("quotes", [])
        if quotes and "accounts" not in pending_entry:
            last_c = quotes[-1][1]  # c_tuple from (q_tuple, c_tuple)
            if last_c:
                cit_text = last_c[0]
                acct = re.sub(r'\s*\(.*?\)', '', cit_text.split(',')[0]).strip()
                # strip leading em-dash if present
                acct = re.sub(r'^—\s*', '', acct).strip()
                if acct:
                    label = "Mentioned by: "
                    acct_display = _annotate_accounts(acct, owner_map)
                    pending_entry["accounts"] = (
                        label + acct_display, N, False,
                        [(0, len(label) - 1, "bold")],
                    )
        # Root cause and mitigation as separate plain lines (Objection Analysis)
        rc  = pending_entry.get("root_cause")
        mit = pending_entry.get("mitigation")
        if rc:
            add(rc[0], N, False, rc[1])
        if mit:
            add(mit[0], N, False, mit[1])

        add_norm      = pending_entry.get("norm")
        add_why       = pending_entry.get("why")
        add_why_after = pending_entry.get("why_after")
        quotes        = pending_entry.get("quotes", [])
        extras        = pending_entry.get("extra", [])
        accounts      = pending_entry.get("accounts")
        if add_norm:
            add(*add_norm)
        if add_why:
            add(*add_why)
        for q_tuple, c_tuple in quotes:
            add(*q_tuple)
            if c_tuple:
                add(*c_tuple)
        if add_why_after:
            _wt, _ws, _wb, _wf = add_why_after
            # Ensure complete sentence: capitalize and add terminal period
            if _wt and not _wt[0].isupper():
                _wt = _wt[0].upper() + _wt[1:]
            if _wt and _wt[-1] not in ".!?":
                _wt = _wt + "."
            add(_wt, _ws, _wb, _wf)
        for txt, style, bold, fmts in extras:
            prefix = "• "
            shifted = [(s + len(prefix), e + len(prefix), f) for s, e, f in fmts]
            add(prefix + txt, style, bold, shifted)
        if accounts:
            add(*accounts)
        pending_entry.clear()

    lines = markdown_text.split("\n")
    segments: list = []
    table_headers: list[str] | None = None
    table_rows: list[list[str]] = []
    in_table = False
    skip_appendix = False  # set True once we hit the Raw Evidence Appendix heading
    last_style = None       # track previous segment style to suppress redundant blank lines

    def flush_segments():
        nonlocal segments
        if segments:
            _append_segments(docs_svc, doc_id, segments)
            segments = []

    def flush_table():
        nonlocal table_headers, table_rows, in_table
        if table_headers and table_rows:
            _append_table(docs_svc, doc_id, table_headers, table_rows)
        table_headers = None
        table_rows = []
        in_table = False

    def seg(text, style=N, bold=False, inline=None):
        return (text, style, bold, inline or [])

    def add(text, style=N, bold=False, inline=None):
        """Append a segment, suppressing blank lines after headings or consecutive blanks."""
        nonlocal last_style, segments
        is_blank = not text.strip()
        if is_blank:
            if last_style in (H1, H2, None) or last_style == "":
                return   # skip blank immediately after heading or at doc start
        segments.append(seg(text, style, bold, inline))
        last_style = "" if is_blank else style

    for line in lines:
        # ── Skip Raw Evidence Appendix entirely ──────────────────────────────
        if re.match(r'^##\s+Raw Evidence Appendix', line, re.IGNORECASE):
            flush_segments()
            skip_appendix = True
            continue
        if skip_appendix:
            continue

        # ── Table rows ───────────────────────────────────────────────────────
        if line.startswith("|"):
            stripped = line.strip("|").strip()
            if all(set(cell.strip()) <= set("-: ") for cell in stripped.split("|")):
                in_table = True
                continue
            parts = [c.strip() for c in line.strip("|").split("|")]
            if table_headers is None:
                flush_segments()
                table_headers = parts
            else:
                table_rows.append(parts)
            in_table = True
            continue

        if in_table:
            flush_table()

        # ── Horizontal rule ──────────────────────────────────────────────────
        if line.strip() in ("---", "***", "___"):
            continue

        # ── H1 (skip duplicate: only render the first one) ───────────────────
        if line.startswith("# ") and not line.startswith("## "):
            add(_strip_bold(line[2:].strip()), H1)
            continue

        # ── H2 ───────────────────────────────────────────────────────────────
        if line.startswith("## "):
            heading = _strip_bold(line[3:].strip())
            current_section = next(
                (k for k in DATA_SECTIONS if k in heading.lower()), ""
            )
            emoji = next((e for k, e in SECTION_EMOJIS.items() if k in heading.lower()), "")
            add(f"{emoji}  {heading}" if emoji else heading, H2)
            continue

        # ── H3 ───────────────────────────────────────────────────────────────
        if line.startswith("### "):
            plain, fmts = _parse_inline_formats(line[4:].strip(), url_dict)
            add(plain, N, False, fmts)
            continue

        # Matches "— Account [(person)], YYYY-MM-DD" at end of line.
        # [^—]+ prevents matching a mid-quote em-dash as the citation start.
        _CIT_RE = re.compile(r'(\s*—\s*[^—]+?,\s*\d{4}-\d{2}-\d{2})\s*$')

        def _handle_data_subitem(raw: str) -> bool:
            """Handle a sub-item line within a DATA_SECTIONS entry.
            Returns True if handled, False if unknown (caller should flush+render)."""
            if re.match(r'^Quotes?:\s*', raw, re.IGNORECASE):
                stripped = re.sub(r'^Quotes?:\s*', '', raw, flags=re.IGNORECASE)
                plain, fmts = _parse_inline_formats(stripped, url_dict)
                url = next((f for _, _, f in fmts
                            if isinstance(f, str) and f.startswith("http")), None)
                cit_m = _CIT_RE.search(plain)
                if cit_m:
                    quote_body = plain[:cit_m.start()]
                    citation   = re.sub(r'^—\s*', '', cit_m.group(1).strip())  # strip leading em-dash
                else:
                    quote_body = plain
                    citation   = None
                q_m    = re.search(r'[\u201c\u0022].*[\u201d\u0022]', quote_body)
                q_fmts = [(q_m.start(), q_m.end(), "italic")] if q_m else [(0, len(quote_body), "italic")]
                c_tuple = None
                if citation:
                    c_fmts  = [(0, len(citation), url)] if url else []
                    c_tuple = (citation, N, False, c_fmts)
                pending_entry.setdefault("quotes", []).append(
                    ((quote_body, N, False, q_fmts), c_tuple)
                )
                return True
            if re.match(r'^Normalized need:\s*', raw, re.IGNORECASE):
                stripped = re.sub(r'^Normalized need:\s*', '', raw, flags=re.IGNORECASE)
                plain, fmts = _parse_inline_formats(stripped, url_dict)
                pending_entry["norm"] = (plain, N, False, fmts)
                return True
            if re.match(r'^Why it matter', raw, re.IGNORECASE):
                stripped = re.sub(r'^Why it matter[^:]*:\s*', '', raw, flags=re.IGNORECASE)
                plain, fmts = _parse_inline_formats(stripped, url_dict)
                # Capability Wins: description goes AFTER the quote
                key = "why_after" if current_section == "capability wins" else "why"
                pending_entry[key] = (plain, N, False, fmts)
                return True
            if re.match(r'^Accounts?:\s*', raw, re.IGNORECASE):
                accts = re.sub(r'^Accounts?:\s*', '', raw, flags=re.IGNORECASE).strip()
                label = "Mentioned by: "
                accts_display = _annotate_accounts(accts, owner_map)
                pending_entry["accounts"] = (
                    label + accts_display, N, False,
                    [(0, len(label) - 1, "bold")],
                )
                flush_entry()
                return True
            if re.match(r'^Root cause:\s*', raw, re.IGNORECASE):
                stripped = re.sub(r'^Root cause:\s*', '', raw, flags=re.IGNORECASE)
                plain, fmts = _parse_inline_formats(stripped, url_dict)
                pending_entry["root_cause"] = (plain, fmts)
                return True
            if re.match(r'^Mitigation:\s*', raw, re.IGNORECASE):
                stripped = re.sub(r'^Mitigation:\s*', '', raw, flags=re.IGNORECASE)
                plain, fmts = _parse_inline_formats(stripped, url_dict)
                pending_entry["mitigation"] = (plain, fmts)
                return True
            if re.match(r'^Blocking deal', raw, re.IGNORECASE):
                flush_entry()
                return True
            # Generic "Label: value" sub-item (Context:, Outcome:, Differentiators cited:,
            # etc.) — strip label, render as plain bullet
            if re.match(r'^[A-Za-z][^—\n]{0,60}:\s*\S', raw):
                stripped = re.sub(r'^[A-Za-z][^:]{0,60}:\s*', '', raw)
                plain, fmts = _parse_inline_formats(stripped, url_dict)
                pending_entry.setdefault("extra", []).append((plain, N, False, fmts))
                return True
            return False

        # ── Numbered list item (Claude sometimes uses "1. **Theme**" in data sections) ──
        m_num = re.match(r'^\d+\.\s+', line)
        if m_num and current_section in DATA_SECTIONS:
            raw = line[m_num.end():].strip()
            if not _handle_data_subitem(raw):
                plain, fmts = _parse_inline_formats(raw, url_dict)
                flush_entry()
                sep = " — "
                idx = plain.find(sep)
                display = (plain[:idx].upper() + sep + plain[idx + len(sep):]) \
                          if idx >= 0 else plain.upper()
                add(display, N, False, fmts)
            continue

        # ── Top-level bullet ─────────────────────────────────────────────────
        if re.match(r'^- ', line):
            raw = line[2:].strip()
            if current_section in DATA_SECTIONS:
                # Sub-item patterns can appear at top level if Claude skips indentation
                if _handle_data_subitem(raw):
                    continue
                plain, fmts = _parse_inline_formats(raw, url_dict)
                flush_entry()

                # Capability Wins: split old single-line format into separate paragraphs.
                # Handles both:
                #   **Title** — "quote" — Account, YYYY-MM-DD — description
                #   **Title** — "quote" — description (no date citation)
                _split_done = False
                if current_section == "capability wins":
                    _cw_ti = plain.find(" \u2014 ")
                    if _cw_ti > 0:
                        _title   = plain[:_cw_ti]
                        _rest    = plain[_cw_ti + 3:]   # skip " — "
                        _qm      = re.search(r'["\u201c](.*?)["\u201d]', _rest)
                        if _qm:
                            _q_body  = _rest[_qm.start():_qm.end()]
                            _after_q = _rest[_qm.end():].strip()
                            _url     = next((f for _, _, f in fmts
                                             if isinstance(f, str)
                                             and f.startswith("http")), None)
                            # Try to extract Account, YYYY-MM-DD citation
                            _cit_m   = re.match(
                                r'^\u2014?\s*([^\u2014]+,\s*\d{4}-\d{2}-\d{2})'
                                r'\s*(?:\u2014\s*)?',
                                _after_q)
                            if _cit_m:
                                _cit  = _cit_m.group(1).strip()
                                _desc = _after_q[_cit_m.end():].strip()
                                _cf   = [(0, len(_cit), _url)] if _url else []
                                pending_entry.setdefault("quotes", []).append(
                                    ((_q_body, N, False, [(0, len(_q_body), "italic")]),
                                     (_cit, N, False, _cf))
                                )
                            else:
                                # No date citation — everything after quote is description
                                _desc = re.sub(r'^\u2014\s*', '', _after_q)
                                pending_entry.setdefault("quotes", []).append(
                                    ((_q_body, N, False, [(0, len(_q_body), "italic")]),
                                     None)
                                )
                            if _desc:
                                pending_entry["why_after"] = (_desc, N, False, [])
                            add(_title.upper(), N, True, [])
                            _split_done = True

                if not _split_done:
                    # Generic header: uppercase label before ' — '
                    sep = " \u2014 "
                    idx = plain.find(sep)
                    display = (plain[:idx].upper() + sep + plain[idx + len(sep):]) \
                              if idx >= 0 else plain.upper()
                    add(display, N, False, fmts)
            else:
                plain, fmts = _parse_inline_formats(raw, url_dict)
                prefix = "• "
                shifted = [(s + len(prefix), e + len(prefix), f) for s, e, f in fmts]
                add(prefix + plain, N, False, shifted)
            continue

        # ── Indented sub-bullet ──────────────────────────────────────────────
        if re.match(r'^\s{2,}- ', line):
            raw = re.sub(r'^\s+-\s+', '', line).strip()
            if current_section in DATA_SECTIONS:
                if not _handle_data_subitem(raw):
                    flush_entry()
                    plain, fmts = _parse_inline_formats(raw, url_dict)
                    label_m = re.match(r'^([A-Za-z][^:?]*[:?])', plain)
                    if label_m:
                        fmts = list(fmts)
                        fmts.insert(0, (0, label_m.end(), "bold"))
                    add(plain, N, False, fmts)
            else:
                flush_entry()
                plain, fmts = _parse_inline_formats(raw, url_dict)
                prefix = "    ◦ "
                shifted = [(s + len(prefix), e + len(prefix), f) for s, e, f in fmts]
                add(prefix + plain, N, False, shifted)
            continue

        flush_entry()

        # ── Full-line bold ───────────────────────────────────────────────────
        m = re.match(r'^\*\*(.+?)\*\*\s*$', line.strip())
        if m:
            add(m.group(1), N, True)
            continue

        # ── Empty line ───────────────────────────────────────────────────────
        if not line.strip():
            add("", N)
            continue

        # ── Plain text ───────────────────────────────────────────────────────
        plain, fmts = _parse_inline_formats(line, url_dict)
        add(plain, N, False, fmts)

    flush_entry()
    if in_table:
        flush_table()
    else:
        flush_segments()


# ---------------------------------------------------------------------------
# Table of Contents
# ---------------------------------------------------------------------------

def add_table_of_contents(docs_svc, doc_id: str,
                          account_names: list[str] | None = None):
    """
    Read the rendered doc, collect all HEADING_2 paragraphs, and insert a
    linked TOC block immediately before the first H2.
    """
    d = docs_svc.documents().get(documentId=doc_id).execute()

    sections = []
    for elem in d["body"]["content"]:
        if "paragraph" not in elem:
            continue
        para  = elem["paragraph"]
        style = para.get("paragraphStyle", {}).get("namedStyleType", "")
        if style != "HEADING_2":
            continue
        heading_id = para.get("paragraphStyle", {}).get("headingId", "")
        raw_text = "".join(
            r.get("textRun", {}).get("content", "")
            for r in para.get("elements", [])
        ).strip()
        # Strip leading emoji / non-letter chars and trailing parentheticals
        clean = re.sub(r'^[^A-Za-z0-9]+', '', raw_text).strip()
        clean = re.sub(r'\s*\(.*?\)\s*$', '', clean).strip()
        if clean:
            sections.append((clean, heading_id, elem.get("startIndex", 0)))

    if not sections:
        return

    insert_pos = sections[0][2]

    # Insert all TOC lines as plain text (+ a blank line after)
    toc_text = "\n".join(t for t, _, _ in sections) + "\n\n"
    _batch_update(docs_svc, doc_id, [
        {"insertText": {"location": {"index": insert_pos}, "text": toc_text}}
    ])

    # Apply styles to each TOC line: smaller font, no inter-paragraph spacing, hyperlink
    style_reqs = []
    pos = insert_pos
    for clean, heading_id, _ in sections:
        line_len = _utf16_len(clean)
        end = pos + line_len
        # Compact paragraph spacing (no gap between lines)
        style_reqs.append({"updateParagraphStyle": {
            "range": {"startIndex": pos, "endIndex": end + 1},
            "paragraphStyle": {
                "spaceAbove": {"magnitude": 0, "unit": "PT"},
                "spaceBelow": {"magnitude": 0, "unit": "PT"},
            },
            "fields": "spaceAbove,spaceBelow",
        }})
        # Smaller font size (10pt)
        style_reqs.append({"updateTextStyle": {
            "range": {"startIndex": pos, "endIndex": end},
            "textStyle": {"fontSize": {"magnitude": 10, "unit": "PT"}},
            "fields": "fontSize",
        }})
        if heading_id:
            style_reqs.append({"updateTextStyle": {
                "range": {"startIndex": pos, "endIndex": end},
                "textStyle": {"link": {"headingId": heading_id}},
                "fields": "link",
            }})
        pos += line_len + 1  # +1 for the newline

    if style_reqs:
        _batch_update(docs_svc, doc_id, style_reqs)

    # Insert "Accounts mentioned" section immediately after the TOC block
    if account_names:
        names_str = ", ".join(sorted(set(account_names), key=str.lower))
        label     = "Prospects/Customers mentioned in this report: "
        body      = names_str
        acct_text = f"{label}{body}\n"
        # Position is right after the TOC text we already inserted
        acct_pos  = insert_pos + _utf16_len(toc_text)
        _batch_update(docs_svc, doc_id, [
            {"insertText": {"location": {"index": acct_pos}, "text": acct_text}}
        ])
        # Style: bold+italic label, italic body, 10pt, no spacing gap
        label_start = acct_pos              # no leading \n
        label_end   = label_start + _utf16_len(label)
        body_end    = label_end + _utf16_len(body)
        # Zero out spacing on the blank paragraph before this line (the gap after TOC)
        blank_para_pos = max(1, acct_pos - 1)
        _batch_update(docs_svc, doc_id, [
            {"updateParagraphStyle": {
                "range": {"startIndex": blank_para_pos, "endIndex": acct_pos},
                "paragraphStyle": {
                    "spaceAbove": {"magnitude": 0, "unit": "PT"},
                    "spaceBelow": {"magnitude": 0, "unit": "PT"},
                },
                "fields": "spaceAbove,spaceBelow",
            }},
            # Whole line: 10pt italic, no spacing
            {"updateTextStyle": {
                "range": {"startIndex": label_start, "endIndex": body_end},
                "textStyle": {"italic": True,
                              "fontSize": {"magnitude": 10, "unit": "PT"}},
                "fields": "italic,fontSize",
            }},
            # Label portion: also bold
            {"updateTextStyle": {
                "range": {"startIndex": label_start, "endIndex": label_end},
                "textStyle": {"bold": True},
                "fields": "bold",
            }},
            {"updateParagraphStyle": {
                "range": {"startIndex": label_start, "endIndex": body_end + 1},
                "paragraphStyle": {
                    "spaceAbove": {"magnitude": 0, "unit": "PT"},
                    "spaceBelow": {"magnitude": 0, "unit": "PT"},
                },
                "fields": "spaceAbove,spaceBelow",
            }},
        ])


# ---------------------------------------------------------------------------
# Google Drive: find or create doc
# ---------------------------------------------------------------------------

def get_or_create_doc(docs_svc, drive_svc, title: str) -> str:
    existing = drive_svc.files().list(
        q=(f"name='{title}' and '{DRIVE_FOLDER_ID}' in parents "
           f"and mimeType='application/vnd.google-apps.document' and trashed=false"),
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        fields="files(id)",
    ).execute().get("files", [])

    if existing:
        doc_id = existing[0]["id"]
        print("  Overwriting existing doc ...")
        d   = docs_svc.documents().get(documentId=doc_id).execute()
        end = d["body"]["content"][-1]["endIndex"]
        if end > 2:
            _batch_update(docs_svc, doc_id,
                [{"deleteContentRange": {"range": {"startIndex": 1, "endIndex": end - 1}}}])
    else:
        doc = drive_svc.files().create(
            body={
                "name":     title,
                "mimeType": "application/vnd.google-apps.document",
                "parents":  [DRIVE_FOLDER_ID],
            },
            supportsAllDrives=True,
            fields="id",
        ).execute()
        doc_id = doc["id"]
        # Share with anyone at you.com
        try:
            drive_svc.permissions().create(
                fileId=doc_id,
                body={"type": "domain", "role": "reader", "domain": "you.com"},
                supportsAllDrives=True,
                fields="id",
            ).execute()
        except Exception:
            pass

    return doc_id


_SECTION_ORDER = [
    "executive summary",
    "product gaps",
    "capability wins",
    "feature opportunities",
    "objection analysis",
    "competitive landscape",
    "raw evidence appendix",
]


def _reorder_analysis_sections(text: str) -> str:
    """Enforce canonical section order regardless of what Claude output."""
    chunks: dict[str, str] = {}
    current_key: str | None = None
    current_lines: list[str] = []

    for line in text.splitlines(keepends=True):
        if line.startswith("## "):
            if current_key is not None:
                chunks[current_key] = "".join(current_lines)
            current_key = line[3:].strip().lower()
            current_lines = [line]
        else:
            if current_key is None:
                current_key = "__preamble__"
            current_lines.append(line)

    if current_key is not None:
        chunks[current_key] = "".join(current_lines)

    ordered: list[str] = []
    if "__preamble__" in chunks:
        ordered.append(chunks.pop("__preamble__"))

    for name in _SECTION_ORDER:
        for key in list(chunks.keys()):
            if key == name or key.startswith(name):
                ordered.append(chunks.pop(key))
                break

    # Append any remaining sections not in the canonical list
    ordered.extend(chunks.values())
    return "".join(ordered)


def fetch_sf_owner_map(config: dict, account_names: list[str]) -> dict[str, str]:
    """Return {input_name_lower: owner_first_name} from Salesforce.

    Tries exact match first; falls back to LIKE for names that didn't match.
    Degrades gracefully — returns empty dict if SF not configured or unavailable.
    """
    if not (config.get("sf_username") and config.get("sf_password")):
        return {}
    if not account_names:
        return {}
    try:
        from simple_salesforce import Salesforce
    except ImportError:
        return {}
    try:
        sf = Salesforce(
            username=config["sf_username"],
            password=config["sf_password"],
            security_token=config.get("sf_security_token", ""),
            domain=config.get("sf_domain", "login"),
        )
    except Exception:
        return {}

    def _esc(v: str) -> str:
        return v.replace("'", "\\'")

    def _first_name(full: str) -> str:
        return full.split()[0] if full else full

    owner_map: dict[str, str] = {}  # input_name_lower → owner_first_name

    # Step 1: exact IN query
    names_in = ", ".join(f"'{_esc(n)}'" for n in account_names)
    try:
        records = sf.query(
            f"SELECT Name, Owner.Name FROM Account WHERE Name IN ({names_in})"
        ).get("records", [])
    except Exception:
        records = []

    matched_inputs: set[str] = set()
    for rec in records:
        sf_name = (rec.get("Name") or "").strip()
        owner   = ((rec.get("Owner") or {}).get("Name") or "").strip()
        if not (sf_name and owner):
            continue
        # Map back to input name(s) that match this SF name
        for inp in account_names:
            if inp.lower() == sf_name.lower():
                owner_map[inp.lower()] = _first_name(owner)
                matched_inputs.add(inp.lower())

    # Step 2: LIKE fallback for unmatched names — pick best-fit when multiple results
    import difflib
    unmatched = [n for n in account_names if n.lower() not in matched_inputs]
    for inp in unmatched:
        try:
            rows = sf.query(
                f"SELECT Name, Owner.Name FROM Account "
                f"WHERE Name LIKE '%{_esc(inp)}%' LIMIT 10"
            ).get("records", [])
        except Exception:
            continue
        if not rows:
            continue
        best = max(
            rows,
            key=lambda r: difflib.SequenceMatcher(
                None, inp.lower(), (r.get("Name") or "").lower()
            ).ratio(),
        )
        owner = ((best.get("Owner") or {}).get("Name") or "").strip()
        if owner:
            owner_map[inp.lower()] = _first_name(owner)

    return owner_map


def _annotate_accounts(accts_str: str, owner_map: dict[str, str]) -> str:
    """Append (OwnerFirstName) after each account name that has an SF owner."""
    if not owner_map:
        return accts_str
    parts = [a.strip() for a in accts_str.split(",") if a.strip()]
    annotated = []
    for acct in parts:
        owner = owner_map.get(acct.lower())
        annotated.append(f"{acct} ({owner})" if owner else acct)
    return ", ".join(annotated)


def publish_to_google_drive(config: dict, title: str,
                            analysis: str, calls_count: int, accounts_count: int,
                            today: str, days: int,
                            owner_map: dict[str, str] | None = None) -> str:
    from googleapiclient.discovery import build

    creds     = get_google_creds(config["google_creds_file"])
    docs_svc  = build("docs",  "v1", credentials=creds)
    drive_svc = build("drive", "v3", credentials=creds)

    doc_id = get_or_create_doc(docs_svc, drive_svc, title)
    print(f"  Rendering report into doc {doc_id} ...")

    end   = date.fromisoformat(today)
    start = end - timedelta(days=days - 1)
    date_range = f"{start.strftime('%b %-d')}–{end.strftime('%-d, %Y')}"

    # Build url_dict from appendix before stripping it from the rendered output
    url_dict = _parse_appendix_urls(analysis)
    print(f"  Citation URLs indexed: {len(url_dict)}")

    # Strip any H1 title Claude adds to its own output (would duplicate our header)
    clean_analysis = re.sub(r'^#\s+[^\n]+\n', '', analysis, count=1).lstrip("\n")
    clean_analysis = _reorder_analysis_sections(clean_analysis)

    header_md = (
        f"# Gong Product Intelligence — {date_range}\n"
        f"Period: Last {days} days  |  Generated: {today}  |  "
        f"Calls analyzed: {calls_count}  |  Accounts represented: {accounts_count}\n\n"
        "---\n\n"
    )
    render_markdown_to_doc(docs_svc, doc_id, header_md + clean_analysis, url_dict,
                           owner_map=owner_map or {})

    # Collect account names from citations for the "Prospects/Customers" line
    _cited: set[str] = set()
    for line in clean_analysis.splitlines():
        for m in re.finditer(r'—\s*([A-Z][A-Za-z0-9 /]{1,30}?)(?:\s*\([^)]*\))?,\s*\d{4}-\d{2}-\d{2}',
                             line):
            name = m.group(1).strip()
            # Strip trailing qualifier words (eval, call, CEO, etc.)
            _QUAL = {"eval", "call", "ceo", "cto", "vp", "svp", "cso", "team"}
            words = name.split()
            while words and words[-1].lower() in _QUAL:
                words = words[:-1]
            if words:
                _cited.add(" ".join(words))
        for m2 in re.finditer(r'Accounts?:\s*([^\n]+)', line, re.IGNORECASE):
            for a in m2.group(1).split(","):
                a = a.strip()
                if a:
                    _cited.add(a)
    cited_accounts = sorted(_cited, key=str.lower)

    add_table_of_contents(docs_svc, doc_id, account_names=cited_accounts)

    return f"https://docs.google.com/document/d/{doc_id}/edit"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    args   = parse_args()
    config = load_config()

    print()
    print("=" * 60)
    print("  Gong Product Intelligence Report")
    print("=" * 60)
    print()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(exist_ok=True)

    # ── Phase 1: Get analysis (fresh or from cache) ──────────────────────────
    gong_owner_map: dict[str, str] = {}   # exact SF account_name_lower → owner first name

    if args.from_cache:
        print(f"[1/3] Loading from cache: {args.from_cache}")
        cache = load_cache(args.from_cache)
        analysis        = cache["analysis"]
        calls_count     = cache["calls_count"]
        accounts_count  = cache["accounts_count"]
        today           = cache["date"]
        days            = cache["days"]
        gong_owner_map  = cache.get("owner_map") or {}
    else:
        days  = args.days
        today = date.today().isoformat()

        now     = datetime.now(timezone.utc)
        from_dt = now - timedelta(days=days)

        print(f"[1/4] Fetching calls (last {days} days) ...")
        calls = fetch_calls(config, from_dt, now, args.max_calls, args.verbose)
        if not calls:
            print(f"No qualifying calls found in the past {days} days.")
            sys.exit(0)

        print(f"\n[2/4] Fetching transcripts for {len(calls)} calls ...")
        call_ids = [c["id"] for c in calls if c.get("id")]
        transcripts_by_id = fetch_transcripts(config, call_ids, args.verbose)
        print(f"  Transcripts received: {len(transcripts_by_id)}")

        print("\n[3/4] Formatting transcripts ...")
        calls_by_id = {c["id"]: c for c in calls if c.get("id")}
        transcript_texts: list[str] = []
        skipped = 0
        for cid, parts in transcripts_by_id.items():
            if not parts:
                skipped += 1
                continue
            transcript_texts.append(format_transcript(calls_by_id.get(cid, {"id": cid}), parts))

        if skipped:
            print(f"  Skipped {skipped} calls with empty transcripts")
        print(f"  Ready to analyze: {len(transcript_texts)} transcripts")

        if not transcript_texts:
            print("No transcript content found — nothing to analyze.")
            sys.exit(0)

        # Build owner map from Gong call metadata (exact SF account name → AE first name)
        gong_owner_map = build_owner_map_from_calls(calls)

        print("\n[4/4] Running Claude analysis ...")
        analysis       = analyze_with_claude(config, transcript_texts, days, args.verbose)
        calls_count    = len(transcript_texts)
        # Prefer analysis-derived count (more reliable than sparse CRM metadata)
        accounts_count = max(
            count_unique_accounts(calls),
            count_accounts_from_analysis(analysis),
        )

        # Save markdown + cache
        md_path = out_dir / f"gong_insights_{today}.md"
        header = (f"# Gong Product Intelligence Report\n"
                  f"**Period:** Last {days} days  |  **Generated:** {today}  |  "
                  f"**Calls analyzed:** {calls_count}  |  **Accounts:** {accounts_count}\n\n---\n\n")
        md_path.write_text(header + analysis, encoding="utf-8")
        print(f"\n  Markdown saved → {md_path}")

        cache_path = save_cache(out_dir, today, analysis, calls_count, accounts_count, days,
                                owner_map=gong_owner_map)
        print(f"  Re-run with: python gong_insights_run.py --from-cache {cache_path}")

    # ── Phase 2: Publish ─────────────────────────────────────────────────────
    if args.skip_publish:
        print("\n--skip-publish set — done.")
        return

    # Resolve owner map: Gong data is primary (exact SF names, always correct).
    # SF LIKE lookup fills gaps for abbreviated names Claude uses that don't match exactly.
    _acct_set: set[str] = set()
    for line in analysis.splitlines():
        m = re.match(r'^\s*-?\s*Accounts?:\s*(.+)', line, re.IGNORECASE)
        if m:
            for a in m.group(1).split(","):
                if a.strip():
                    _acct_set.add(a.strip())
        for cit in re.findall(r'—\s*([^,—\n(]+?)(?:\s*\([^)]*\))?,\s*\d{4}-\d{2}-\d{2}', line):
            cit = cit.strip()
            if cit:
                _acct_set.add(cit)

    # Names not already covered by Gong exact-match map → SF LIKE fallback
    unresolved = [n for n in _acct_set if n.lower() not in gong_owner_map]
    sf_map = fetch_sf_owner_map(config, unresolved) if unresolved else {}

    # Merge: Gong data takes precedence over SF LIKE results
    owner_map = {**sf_map, **gong_owner_map}
    if owner_map:
        print(f"  Owner map: {len(owner_map)} accounts ({len(gong_owner_map)} from Gong, "
              f"{len(sf_map)} from SF fallback)")

    end_date   = date.fromisoformat(today)
    start_date = end_date - timedelta(days=days - 1)
    date_range = f"{start_date.strftime('%b %-d')}–{end_date.strftime('%-d, %Y')}"
    doc_title  = f"Gong Product Intelligence — {date_range}"

    if args.dry_run:
        doc_url = "https://docs.google.com/document/d/PREVIEW"
        print("\n[Publish] Dry run — skipping Google Doc creation")
    else:
        print(f"\n[Publish] Creating Google Doc: '{doc_title}' ...")
        doc_url = publish_to_google_drive(
            config, doc_title, analysis, calls_count, accounts_count, today, days,
            owner_map=owner_map)
        print(f"  ✅ Doc created → {doc_url}")

    slack_msg = build_slack_message(analysis, calls_count, accounts_count, days, today, doc_url)

    if args.dry_run:
        post_to_slack("", slack_msg, dry_run=True)
    elif config["slack_bot_token"]:
        print(f"\n[Publish] Posting to {SLACK_CHANNEL} ...")
        post_to_slack(config["slack_bot_token"], slack_msg)
    else:
        print("\n  ⚠️  No SLACK_BOT_TOKEN — skipping Slack post")

    print()


if __name__ == "__main__":
    main()
