"""
forecast_run.py — Weekly Q2 Forecast Report

Pulls Q2 pipeline from Salesforce for Nick's team, enriches key deal changes
with Granola meeting notes and Slack context, generates a Claude narrative,
and outputs to terminal + a new Google Doc.

Usage:
    python3 forecast_run.py [--no-doc] [--days N]

Env vars:
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN
    GRANOLA_API_KEY
    SLACK_USER_TOKEN        — xoxp- token (search:read scope)
    ANTHROPIC_API_KEY
    GOOGLE_CREDENTIALS_FILE — path to OAuth credentials JSON
    TEAM_QUOTA              — Q2 quota in dollars (e.g. 2000000)
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import requests

ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")

Q2_START = "2026-04-01"
Q2_END   = "2026-06-30"

TEAM_MEMBERS = {
    "Nick Opderbeck":          "nick.opderbeck@you.com",
    "David Wacker":            "david.wacker@you.com",
    "Ryan Reed":               "ryan.reed@you.com",
    "Ryan Allred":             "ryan.allred@you.com",
    "Andrew Miller-McKeever":  "andrew.miller-mckeever@you.com",
}

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
]

FORECAST_FOLDER_ID = "1Jh5ft2H7XkZCcInbykWBrCiXfHnj_gCY"

GOOGLE_TOKEN_FILE = os.path.join(
    os.path.dirname(__file__), ".credentials", "google_token_forecast.json"
)


# ── Formatters ───────────────────────────────────────────────────────────────

def fmt_amt(amount) -> str:
    if not amount:
        return "PAYG"
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.0f}K"
    return f"${int(amount):,}"

def fmt_date(d: str) -> str:
    try:
        return date.fromisoformat(d[:10]).strftime("%-m/%-d/%Y")
    except Exception:
        return d or "—"

def pct(part, whole) -> str:
    if not whole:
        return "—%"
    return f"{part / whole * 100:.0f}%"


# ── Salesforce ────────────────────────────────────────────────────────────────

def connect_sf():
    from simple_salesforce import Salesforce
    return Salesforce(
        username=os.environ["SF_USERNAME"],
        password=os.environ["SF_PASSWORD"],
        security_token=os.environ["SF_SECURITY_TOKEN"],
    )

def soql(sf, query: str) -> list:
    try:
        return sf.query_all(query.strip()).get("records", [])
    except Exception as e:
        print(f"  SOQL error: {e}", file=sys.stderr)
        return []

def resolve_team_ids(sf) -> dict[str, str]:
    """Returns {name: sf_user_id}"""
    result = {}
    for name in TEAM_MEMBERS:
        rows = soql(sf, f"SELECT Id FROM User WHERE Name = '{name}' AND IsActive = true LIMIT 1")
        if rows:
            result[name] = rows[0]["Id"]
        else:
            print(f"  ⚠️  User not found: {name}", file=sys.stderr)
    return result

def fetch_q2_all_open(sf, ids_str: str) -> list[dict]:
    """Fetch all open Q2 opps regardless of forecast category — used for diagnostics and bucketing."""
    return soql(sf, f"""
        SELECT Id, Name, AccountId, Account.Name, Amount, CloseDate,
               StageName, ForecastCategoryName, OwnerId, Owner.Name
        FROM Opportunity
        WHERE IsClosed = false
        AND CloseDate >= {Q2_START}
        AND CloseDate <= {Q2_END}
        AND OwnerId IN ('{ids_str}')
        ORDER BY Amount DESC NULLS LAST
    """)

def fetch_q2_opps(sf, ids_str: str, category: str) -> list[dict]:
    return soql(sf, f"""
        SELECT Id, Name, AccountId, Account.Name, Amount, CloseDate,
               StageName, ForecastCategoryName, OwnerId, Owner.Name
        FROM Opportunity
        WHERE IsClosed = false
        AND ForecastCategoryName = '{category}'
        AND CloseDate >= {Q2_START}
        AND CloseDate <= {Q2_END}
        AND OwnerId IN ('{ids_str}')
        ORDER BY Amount DESC NULLS LAST
    """)

def fetch_q2_closed_won(sf, ids_str: str) -> list[dict]:
    return soql(sf, f"""
        SELECT Id, Name, AccountId, Account.Name, Amount, CloseDate,
               StageName, OwnerId, Owner.Name
        FROM Opportunity
        WHERE IsWon = true
        AND CloseDate >= {Q2_START}
        AND CloseDate <= {Q2_END}
        AND OwnerId IN ('{ids_str}')
        ORDER BY CloseDate DESC
    """)

def fetch_field_history(sf, ids_str: str, days: int = 7) -> list[dict]:
    """Recent field changes on team opps — stage, close date, forecast category, amount."""
    since = (date.today() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    return soql(sf, f"""
        SELECT OpportunityId, Opportunity.Name, Opportunity.Account.Name,
               Opportunity.Owner.Name, Opportunity.Amount,
               Field, OldValue, NewValue, CreatedDate, CreatedBy.Name
        FROM OpportunityFieldHistory
        WHERE Opportunity.OwnerId IN ('{ids_str}')
        AND Field IN ('StageName', 'CloseDate', 'ForecastCategoryName', 'Amount')
        AND CreatedDate >= {since}
        ORDER BY CreatedDate DESC
    """)


# ── Granola ───────────────────────────────────────────────────────────────────

def fetch_granola_notes(api_key: str, days: int = 14) -> list[dict]:
    since = (date.today() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    try:
        resp = requests.get(
            "https://public-api.granola.ai/v1/notes",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"created_after": since, "page_size": 30},
            timeout=15,
        )
        if resp.status_code == 200:
            notes = resp.json().get("notes", [])
            print(f"  Granola: {len(notes)} note(s) in last {days}d")
            return notes
    except Exception as e:
        print(f"  Granola error: {e}", file=sys.stderr)
    return []

def fetch_granola_detail(api_key: str, note_id: str) -> dict | None:
    try:
        resp = requests.get(
            f"https://public-api.granola.ai/v1/notes/{note_id}",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"  Granola detail error: {e}", file=sys.stderr)
    return None

def _keywords(account_name: str) -> list[str]:
    stopwords = {"inc", "llc", "corp", "corporation", "ltd", "co", "the", "and", "&"}
    words = [w for w in account_name.split() if w.lower() not in stopwords]
    return [account_name] + ([words[0]] if words else [])

def match_notes(all_notes: list[dict], account_name: str, ae_email: str) -> list[dict]:
    keywords = _keywords(account_name)
    matched = []
    for note in all_notes:
        title = (note.get("title") or "").lower()
        emails = [(a.get("email") or "").lower() for a in (note.get("attendees") or [])]
        if any(kw.lower() in title for kw in keywords) or ae_email.lower() in emails:
            matched.append(note)
    return matched

def get_note_summaries(api_key: str, stubs: list[dict]) -> list[str]:
    summaries = []
    for stub in stubs[:3]:
        detail = fetch_granola_detail(api_key, stub["id"])
        if detail:
            title   = detail.get("title") or ""
            created = (detail.get("created_at") or "")[:10]
            summary = (detail.get("summary_markdown") or "").strip()[:800]
            if summary:
                summaries.append(f"[{created}] {title}\n{summary}")
    return summaries


# ── Slack ─────────────────────────────────────────────────────────────────────

def fetch_slack_messages(user_token: str, account_name: str, days: int = 14) -> list[str]:
    if not user_token:
        return []
    keywords = _keywords(account_name)
    after    = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        resp = requests.get(
            "https://slack.com/api/search.messages",
            headers={"Authorization": f"Bearer {user_token}"},
            params={"query": f'"{keywords[0]}" after:{after}', "count": 10, "sort": "timestamp"},
            timeout=15,
        )
        data = resp.json()
        if not data.get("ok"):
            return []
        matches = (data.get("messages") or {}).get("matches") or []
        return [re.sub(r'<[^>]+>', '', m.get("text") or "").strip()[:300] for m in matches[:5] if m.get("text")]
    except Exception as e:
        print(f"  Slack error for '{account_name}': {e}", file=sys.stderr)
        return []


# ── Claude ────────────────────────────────────────────────────────────────────

def _deal_ctx(opp: dict) -> str:
    acct  = (opp.get("Account") or {}).get("Name") or "?"
    owner = (opp.get("Owner") or {}).get("Name") or "?"
    return (
        f"Deal: {opp.get('Name','?')} ({acct})\n"
        f"  Owner: {owner} | Stage: {opp.get('StageName','?')} | "
        f"Amount: {fmt_amt(opp.get('Amount'))} | Close: {fmt_date(opp.get('CloseDate',''))}"
    )

def generate_changes_narrative(
    history: list[dict],
    granola_notes: list[dict],
    granola_key: str,
    slack_token: str,
    client,
) -> list[str]:
    """
    Turn field history into plain-English bullet points, enriched with
    Granola + Slack context for the most material changes.
    """
    today_str = date.today().strftime("%B %-d, %Y")

    # Group history by opp, classify each change
    by_opp: dict[str, list] = defaultdict(list)
    for h in history:
        by_opp[h["OpportunityId"]].append(h)

    # Build raw change descriptions
    raw_changes: list[dict] = []
    for opp_id, events in by_opp.items():
        opp_name  = (events[0].get("Opportunity") or {}).get("Name") or "?"
        acct_name = ((events[0].get("Opportunity") or {}).get("Account") or {}).get("Name") or "?"
        owner     = ((events[0].get("Opportunity") or {}).get("Owner") or {}).get("Name") or "?"
        amount    = (events[0].get("Opportunity") or {}).get("Amount")

        for e in events:
            field = e.get("Field")
            old   = e.get("OldValue") or ""
            new   = e.get("NewValue") or ""
            when  = (e.get("CreatedDate") or "")[:10]

            if field == "StageName":
                raw_changes.append({
                    "type": "stage", "opp_id": opp_id, "account": acct_name,
                    "owner": owner, "amount": amount, "date": when,
                    "old": old, "new": new,
                    "closed_won":  new == "Closed Won",
                    "closed_lost": new == "Closed Lost",
                })
            elif field == "CloseDate":
                try:
                    old_d = date.fromisoformat(old[:10])
                    new_d = date.fromisoformat(new[:10])
                    pushed = new_d > old_d
                    raw_changes.append({
                        "type": "close_date", "opp_id": opp_id, "account": acct_name,
                        "owner": owner, "amount": amount, "date": when,
                        "old": fmt_date(old), "new": fmt_date(new), "pushed": pushed,
                    })
                except Exception:
                    pass
            elif field == "ForecastCategoryName":
                raw_changes.append({
                    "type": "forecast_cat", "opp_id": opp_id, "account": acct_name,
                    "owner": owner, "amount": amount, "date": when,
                    "old": old, "new": new,
                })
            elif field == "Amount":
                raw_changes.append({
                    "type": "amount", "opp_id": opp_id, "account": acct_name,
                    "owner": owner, "amount": amount, "date": when,
                    "old": old, "new": new,
                })

    if not raw_changes:
        return ["No material deal changes detected in the last 7 days."]

    # Fetch Granola + Slack context for the top changed accounts (up to 5)
    seen_accounts: set[str] = set()
    context_blocks: list[str] = []
    for c in raw_changes:
        acct = c["account"]
        if acct in seen_accounts or len(seen_accounts) >= 5:
            continue
        seen_accounts.add(acct)
        ae_email = ""
        for name, email in TEAM_MEMBERS.items():
            if name == c["owner"]:
                ae_email = email
                break
        stubs    = match_notes(granola_notes, acct, ae_email)
        summaries = get_note_summaries(granola_key, stubs) if stubs else []
        slack    = fetch_slack_messages(slack_token, acct)

        if summaries or slack:
            block = f"Context for {acct}:"
            for s in summaries[:2]:
                block += f"\n  MEETING NOTE: {s[:400]}"
            for m in slack[:2]:
                block += f"\n  SLACK: {m[:200]}"
            context_blocks.append(block)

    # Serialize raw changes for prompt
    changes_text = "\n".join(
        f"- [{c['type'].upper()}] {c['account']} (owned by {c['owner']}): "
        + (f"Closed Won, {fmt_amt(c.get('amount'))}" if c.get("closed_won") else
           f"Closed Lost" if c.get("closed_lost") else
           f"{c.get('old','')} → {c.get('new','')}")
        for c in raw_changes
    )
    context_text = "\n\n".join(context_blocks) if context_blocks else "No additional context available."

    prompt = f"""You are writing the "Key Changes This Week" section of a weekly sales forecast report for a VP of Sales. Today is {today_str}.

Here are the deal changes that occurred in the last 7 days:
{changes_text}

Here is additional context from meeting notes and Slack:
{context_text}

Write 4–8 concise bullet points summarising the most important changes. Each bullet should:
- Open with a relevant emoji followed by a verb (Closed, Lost, Advanced, Pushed, etc.)
  Use: ✅ for closed won, ❌ for closed lost, 📅 for close date pushes, 📈 for stage advances, ⚠️ for risks
- Name the account and owner
- Include the dollar amount where non-zero (use PAYG for $0 deals)
- Reference specific context from meeting notes or Slack where available
- Be direct and specific — avoid filler phrases

Prioritize: closed won/lost first, then close date pushes on large deals, then stage advances.
Return plain bullet points only — one per line starting with the emoji. No headers, no markdown."""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        bullets = [
            line.strip() for line in msg.content[0].text.strip().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        return bullets if bullets else [msg.content[0].text.strip()]
    except Exception as e:
        print(f"  Claude changes error: {e}", file=sys.stderr)
        return [f"• {c['account']}: {c.get('old','')} → {c.get('new','')}" for c in raw_changes[:8]]


def generate_deal_narratives(
    opps: list[dict],
    granola_notes: list[dict],
    granola_key: str,
    slack_token: str,
    client,
    label: str,
) -> dict[str, str]:
    """1-sentence narrative per deal, keyed by opp Id."""
    if not opps:
        return {}

    today_str = date.today().strftime("%B %-d, %Y")
    deal_blocks: list[str] = []

    for opp in opps:
        acct     = (opp.get("Account") or {}).get("Name") or "?"
        owner    = (opp.get("Owner") or {}).get("Name") or "?"
        ae_email = TEAM_MEMBERS.get(owner, "")
        stubs    = match_notes(granola_notes, acct, ae_email)
        summaries = get_note_summaries(granola_key, stubs) if stubs else []
        slack    = fetch_slack_messages(slack_token, acct)

        block = _deal_ctx(opp)
        if summaries:
            block += f"\n  Meeting notes: {summaries[0][:400]}"
        if slack:
            block += f"\n  Slack: {slack[0][:200]}"
        if not summaries and not slack:
            block += "\n  No recent meeting notes or Slack activity."
        deal_blocks.append(block)

    opp_ids   = [o.get("Id") for o in opps]
    acct_names = [(o.get("Account") or {}).get("Name") or "?" for o in opps]

    prompt = f"""You are writing deal summaries for the {label} section of a weekly forecast report. Today is {today_str}.

{chr(10).join(deal_blocks)}

Write one sentence per deal. Reference meeting content or Slack where available. If no activity, say the deal is dark and note the risk.
Return ONLY a JSON object: {{"{acct_names[0]}": "sentence", ...}}
Use exact account names as keys: {acct_names}
No markdown, no extra keys."""

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        raw   = msg.content[0].text.strip()
        start = raw.find("{"); end = raw.rfind("}") + 1
        if start != -1 and end > start:
            raw = raw[start:end]
        result = json.loads(raw)
        return {o.get("Id"): result.get((o.get("Account") or {}).get("Name") or "?", "(no summary)") for o in opps}
    except Exception as e:
        print(f"  Claude narrative error ({label}): {e}", file=sys.stderr)
        return {o.get("Id"): "(unavailable)" for o in opps}


# ── Google Docs ───────────────────────────────────────────────────────────────

def get_google_creds():
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request

    creds_file = os.environ.get("GOOGLE_CREDENTIALS_FILE",
                                os.path.join(os.path.dirname(__file__), ".credentials", "google_credentials.json"))
    creds = None
    if os.path.exists(GOOGLE_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_FILE, GOOGLE_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, GOOGLE_SCOPES)
            creds = flow.run_local_server(port=0)
        with open(GOOGLE_TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return creds


class DocBuilder:
    """
    Accumulates text segments with explicit styling.
    Uses only NORMAL_TEXT paragraphs + updateTextStyle to avoid
    Google Docs named-style spacing inconsistencies.
    """

    def __init__(self):
        # each segment: {text, bold, size (pt), indent (pt)}
        self.segments: list[dict] = []

    def add(self, text: str, bold: bool = False, size: int = 11, indent: int = 0):
        if not text.endswith("\n"):
            text += "\n"
        self.segments.append({"text": text, "bold": bold, "size": size, "indent": indent})

    def blank(self):
        self.segments.append({"text": "\n", "bold": False, "size": 11, "indent": 0})

    def build_requests(self) -> tuple[str, list[dict]]:
        full_text = "".join(s["text"] for s in self.segments)
        requests  = []
        index     = 1  # Google Docs body starts at index 1

        for seg in self.segments:
            text        = seg["text"]
            end         = index + len(text)
            content_end = end - 1   # exclude the trailing \n

            # Force NORMAL_TEXT + zero spacing on every paragraph to prevent bleed
            requests.append({
                "updateParagraphStyle": {
                    "range": {"startIndex": index, "endIndex": end},
                    "paragraphStyle": {
                        "namedStyleType": "NORMAL_TEXT",
                        "spaceAbove": {"magnitude": 2, "unit": "PT"},
                        "spaceBelow": {"magnitude": 2, "unit": "PT"},
                        "indentStart": {"magnitude": seg["indent"], "unit": "PT"},
                    },
                    "fields": "namedStyleType,spaceAbove,spaceBelow,indentStart",
                }
            })

            # Apply explicit text style to the content characters (not the \n)
            if content_end > index:
                requests.append({
                    "updateTextStyle": {
                        "range": {"startIndex": index, "endIndex": content_end},
                        "textStyle": {
                            "bold": seg["bold"],
                            "fontSize": {"magnitude": seg["size"], "unit": "PT"},
                            "weightedFontFamily": {"fontFamily": "Arial"},
                        },
                        "fields": "bold,fontSize,weightedFontFamily",
                    }
                })

            index = end

        return full_text, requests


def _find_existing_doc(drive_service, title: str) -> str | None:
    """Return the doc ID if a doc with this title already exists in the forecast folder."""
    safe_title = title.replace("'", "\\'")
    resp = drive_service.files().list(
        q=(
            f"name='{safe_title}'"
            f" and '{FORECAST_FOLDER_ID}' in parents"
            f" and mimeType='application/vnd.google-apps.document'"
            f" and trashed=false"
        ),
        fields="files(id)",
        pageSize=1,
    ).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def _clear_doc(docs_service, doc_id: str):
    """Delete all body content from an existing Google Doc, then reset residual styles."""
    doc       = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc["body"]["content"][-1]["endIndex"]
    if end_index > 2:
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={"requests": [{"deleteContentRange": {
                "range": {"startIndex": 1, "endIndex": end_index - 1}
            }}]},
        ).execute()
    # After deletion a single paragraph mark remains at index 1 with the old
    # run's text style (bold, large font).  Reset it so the new insertText
    # doesn't inherit stale formatting.
    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": [
            {
                "updateTextStyle": {
                    "range": {"startIndex": 1, "endIndex": 2},
                    "textStyle": {
                        "bold": False,
                        "fontSize": {"magnitude": 11, "unit": "PT"},
                        "weightedFontFamily": {"fontFamily": "Arial"},
                    },
                    "fields": "bold,fontSize,weightedFontFamily",
                }
            },
            {
                "updateParagraphStyle": {
                    "range": {"startIndex": 1, "endIndex": 2},
                    "paragraphStyle": {
                        "namedStyleType": "NORMAL_TEXT",
                        "spaceAbove": {"magnitude": 0, "unit": "PT"},
                        "spaceBelow": {"magnitude": 0, "unit": "PT"},
                    },
                    "fields": "namedStyleType,spaceAbove,spaceBelow",
                }
            },
        ]},
    ).execute()


def create_google_doc(title: str, builder: DocBuilder) -> tuple[str, bool]:
    """
    Creates or updates a Google Doc in the forecast folder.
    Returns (url, created_new: bool).
    """
    from googleapiclient.discovery import build

    creds         = get_google_creds()
    docs_service  = build("docs", "v1", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)

    existing_id = _find_existing_doc(drive_service, title)

    if existing_id:
        doc_id      = existing_id
        created_new = False
        _clear_doc(docs_service, doc_id)
    else:
        file_meta = {
            "name":     title,
            "mimeType": "application/vnd.google-apps.document",
            "parents":  [FORECAST_FOLDER_ID],
        }
        file        = drive_service.files().create(body=file_meta, fields="id").execute()
        doc_id      = file["id"]
        created_new = True

    # Populate content
    full_text, fmt_requests = builder.build_requests()
    requests = [{"insertText": {"location": {"index": 1}, "text": full_text}}]
    requests.extend(fmt_requests)

    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": requests},
    ).execute()

    return f"https://docs.google.com/document/d/{doc_id}/edit", created_new


# ── Report builder ────────────────────────────────────────────────────────────

def _stage_emoji(stage: str) -> str:
    s = stage.lower()
    if "closed won" in s:                    return "✅"
    if "agreement" in s or "submit" in s:    return "🔥"
    if "proof" in s or "tech eval" in s:     return "🟡"
    return "🔵"

def _stage_short(stage: str) -> str:
    """Format '3 - Workshop' → 'Stage 3 - Workshop'."""
    m = re.match(r'^(\d+)\s*-\s*(.+)', stage.strip())
    if m:
        return f"Stage {m.group(1)} - {m.group(2).strip()}"
    return stage.strip()

def _stage_num(stage: str) -> int:
    """Return the leading stage number, or 0 if not present."""
    m = re.match(r'^(\d+)', stage.strip())
    return int(m.group(1)) if m else 0

def _deal_line(opp: dict, suffix: str = "") -> str:
    """Single-line deal summary for the forecast overview (no leading spaces)."""
    acct   = (opp.get("Account") or {}).get("Name") or "?"
    amount = opp.get("Amount")
    stage  = opp.get("StageName") or ""
    label  = suffix or _stage_short(stage)
    return f"{acct}  —  {fmt_amt(amount)}  ({label})"


def build_report(
    quota: float,
    closed_won: list[dict],
    commit:     list[dict],
    best_case:  list[dict],
    pipeline:   list[dict],
    changes_bullets: list[str],
    date_str: str,
    gong_commit:    float = 0,
    gong_best_case: float = 0,
    gong_pipeline:  float = 0,
) -> DocBuilder:

    cw_total  = sum((o.get("Amount") or 0) for o in closed_won)
    cm_total  = sum((o.get("Amount") or 0) for o in commit)
    bc_total  = sum((o.get("Amount") or 0) for o in best_case)
    pip_total = sum((o.get("Amount") or 0) for o in pipeline)

    # Use Gong override if set, else fall back to SFDC roll-up
    commit_cw  = (gong_commit or cm_total) + cw_total
    bc_display = gong_best_case or bc_total
    pip_display = gong_pipeline or pip_total

    doc = DocBuilder()

    # ── Title ──────────────────────────────────────────────────────────────
    doc.add(f"Q2 2026 Forecast  |  Week of {date_str}", bold=True, size=20)
    doc.blank()

    # ── Forecast Overview (waterfall) ──────────────────────────────────────
    doc.add("Forecast Overview", bold=True, size=14)
    doc.add(f"Quota   {fmt_amt(quota)}", size=11)
    doc.blank()

    # Commit + Closed Won
    doc.add(f"Commit + Closed Won   {fmt_amt(commit_cw)}   ({pct(commit_cw, quota)} to quota)", bold=True, size=12)
    for o in closed_won:
        doc.add(_deal_line(o, "Closed Won"), size=11, indent=18)
    for o in commit:
        doc.add(_deal_line(o), size=11, indent=18)
    if not closed_won and not commit:
        doc.add("None", size=11, indent=18)
    doc.blank()

    # Best Case
    doc.add(f"Best Case   {fmt_amt(bc_display)}   ({pct(bc_display, quota)} to quota)", bold=True, size=12)
    doc.add("Includes all Commit + Closed Won, plus:", size=11, indent=18)
    for o in best_case:
        doc.add(_deal_line(o), size=11, indent=18)
    doc.blank()

    # Pipeline — top-line + Stage 3+ deals
    pip_stage3 = [o for o in pipeline if _stage_num(o.get("StageName") or "") >= 3]
    doc.add(f"Pipeline   {fmt_amt(pip_display)}   ({pct(pip_display, quota)} to quota)", bold=True, size=12)
    for o in pip_stage3:
        doc.add(_deal_line(o), size=11, indent=18)
    doc.blank()

    # ── Key Changes ────────────────────────────────────────────────────────
    doc.add("Key Changes This Week", bold=True, size=14)

    won     = [b for b in changes_bullets if b.startswith("✅")]
    lost    = [b for b in changes_bullets if b.startswith("❌")]
    notable = [b for b in changes_bullets if not b.startswith("✅") and not b.startswith("❌")]

    for group in [won, lost, notable]:
        if group:
            for bullet in group:
                doc.add(bullet, size=11)
            doc.blank()

    return doc


# ── Terminal preview ──────────────────────────────────────────────────────────

def print_report(
    quota, closed_won, commit, best_case, pipeline,
    changes_bullets, date_str,
    gong_commit: float = 0, gong_best_case: float = 0, gong_pipeline: float = 0,
):
    cw_total  = sum((o.get("Amount") or 0) for o in closed_won)
    cm_total  = sum((o.get("Amount") or 0) for o in commit)
    bc_total  = sum((o.get("Amount") or 0) for o in best_case)
    pip_total = sum((o.get("Amount") or 0) for o in pipeline)

    commit_cw   = (gong_commit or cm_total) + cw_total
    bc_display  = gong_best_case or bc_total
    pip_display = gong_pipeline or pip_total
    bar = "─" * 60

    print(f"\n{'═'*60}")
    print(f"  Q2 2026 Forecast  |  Week of {date_str}")
    print(f"{'═'*60}")
    print(f"  {'Quota:':<22} {fmt_amt(quota):>12}")
    print(f"  {'Commit + Closed Won:':<22} {fmt_amt(commit_cw):>12}  ({pct(commit_cw, quota)} to quota)")
    print(f"  {'Best Case:':<22} {fmt_amt(bc_display):>12}  ({pct(bc_display, quota)} to quota)")
    print(f"  {'Pipeline:':<22} {fmt_amt(pip_display):>12}  ({pct(pip_display, quota)} to quota)")

    print(f"\n{bar}")
    print(f"  KEY CHANGES THIS WEEK")
    print(bar)
    won     = [b for b in changes_bullets if b.startswith("✅")]
    lost    = [b for b in changes_bullets if b.startswith("❌")]
    notable = [b for b in changes_bullets if not b.startswith("✅") and not b.startswith("❌")]
    for group in [won, lost, notable]:
        if group:
            for b in group:
                print(f"  {b}")
            print()

    print(f"\n{'═'*60}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-doc",  action="store_true", help="Skip Google Doc creation")
    parser.add_argument("--days",    type=int, default=7,  help="Days back for field history (default: 7)")
    parser.add_argument("--date",    default=None,          help="Override report date string")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv(ENV_PATH, override=True)

    today_str       = args.date or date.today().strftime("%B %-d, %Y")
    quota           = float(os.getenv("TEAM_QUOTA", "0") or 0)
    gong_commit     = float(os.getenv("CURRENT_QUARTER_COMMIT", "0") or 0)
    gong_best_case  = float(os.getenv("CURRENT_QUARTER_BEST_CASE", "0") or 0)
    gong_pipeline   = float(os.getenv("CURRENT_QUARTER_PIPELINE", "0") or 0)
    granola_key     = os.getenv("GRANOLA_API_KEY", "")
    slack_token     = os.getenv("SLACK_USER_TOKEN", "")
    anthropic_key   = os.getenv("ANTHROPIC_API_KEY", "")

    print(f"📊 Q2 Forecast Report — {today_str}")
    if not quota:
        print("  ⚠️  TEAM_QUOTA not set in .env — quota will show as PAYG", file=sys.stderr)

    # ── Salesforce ─────────────────────────────────────────────────────────
    print("Connecting to Salesforce...")
    sf = connect_sf()

    print("Resolving team members...")
    team_id_map = resolve_team_ids(sf)
    ids_str     = "', '".join(team_id_map.values())
    print(f"  {', '.join(team_id_map.keys())}")

    print("Fetching Q2 pipeline...")
    closed_won  = fetch_q2_closed_won(sf, ids_str)
    all_open    = fetch_q2_all_open(sf, ids_str)

    # Show every distinct ForecastCategoryName so mismatches are visible
    from collections import Counter
    cat_counts = Counter(o.get("ForecastCategoryName") or "None" for o in all_open)
    print(f"  Open Q2 forecast categories: { dict(cat_counts) }")

    commit    = [o for o in all_open if (o.get("ForecastCategoryName") or "") == "Commit"]
    best_case = [o for o in all_open if (o.get("ForecastCategoryName") or "") == "Best Case"]
    pipeline  = [o for o in all_open if (o.get("ForecastCategoryName") or "") == "Pipeline"]
    print(f"  CW={len(closed_won)}  Commit={len(commit)}  Best Case={len(best_case)}  Pipeline={len(pipeline)}")

    print(f"Fetching field history (last {args.days}d)...")
    history = fetch_field_history(sf, ids_str, days=args.days)
    print(f"  {len(history)} field change(s)")

    # ── Granola ────────────────────────────────────────────────────────────
    granola_notes: list[dict] = []
    if granola_key:
        print("Fetching Granola notes...")
        granola_notes = fetch_granola_notes(granola_key, days=14)
    else:
        print("  ⚠️  No GRANOLA_API_KEY", file=sys.stderr)

    # ── Claude ─────────────────────────────────────────────────────────────
    if not anthropic_key:
        print("  ⚠️  No ANTHROPIC_API_KEY", file=sys.stderr)
        changes_bullets = ["(Claude unavailable — no API key)"]
    else:
        import anthropic
        client = anthropic.Anthropic(api_key=anthropic_key)

        print("Generating key changes narrative...")
        changes_bullets = generate_changes_narrative(
            history, granola_notes, granola_key, slack_token, client
        )

    # ── Output ─────────────────────────────────────────────────────────────
    print_report(quota, closed_won, commit, best_case, pipeline,
                 changes_bullets, today_str,
                 gong_commit, gong_best_case, gong_pipeline)

    if not args.no_doc:
        print("Creating / updating Google Doc...")
        try:
            doc_builder = build_report(
                quota, closed_won, commit, best_case, pipeline,
                changes_bullets, today_str,
                gong_commit, gong_best_case, gong_pipeline,
            )
            title             = f"Q2 2026 Forecast — Week of {today_str}"
            doc_url, is_new   = create_google_doc(title, doc_builder)
            action            = "created" if is_new else "updated"
            print(f"\n✅ Google Doc {action}: {doc_url}\n")
        except Exception as e:
            print(f"  ⚠️  Google Doc failed: {e}", file=sys.stderr)
    else:
        print("(--no-doc: skipping Google Doc)")


if __name__ == "__main__":
    main()
