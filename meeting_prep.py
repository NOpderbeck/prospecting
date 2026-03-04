"""
meeting_prep.py — Meeting Preparation Brief Generator

Looks at Google Calendar for the next N days, identifies external meetings,
researches external attendees (LinkedIn titles/roles via You.com), pulls
internal context from Slack and Google Drive, searches for existing account
plan documents, and generates a comprehensive meeting prep brief in markdown.

Usage:
    python meeting_prep.py
    python meeting_prep.py --days 5
    python meeting_prep.py --days 3 --output-dir my_reports --verbose
    python meeting_prep.py --email          # also sends HTML email

Requirements:
    pip install google-api-python-client google-auth-oauthlib google-auth-httplib2
    pip install slack-sdk requests anthropic python-dotenv markdown

Email setup (.env):
    EMAIL_TO=you@example.com         # recipient (defaults to SF_USERNAME)
    SMTP_USER=you@gmail.com          # sender Gmail address
    SMTP_PASSWORD=xxxx xxxx xxxx     # Gmail App Password (myaccount.google.com/apppasswords)
    SMTP_HOST=smtp.gmail.com         # optional, default: smtp.gmail.com
    SMTP_PORT=587                    # optional, default: 587

Note: First run will open a browser for Google OAuth (one-time). Uses a
      separate token file from context.py so both scripts work independently.
"""

import os
import re
import sys
import time
import argparse
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date, datetime, timedelta, timezone

import requests
import anthropic
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Google OAuth scopes — includes Calendar (new) + Drive + Gmail
# Uses a separate token file so context.py is not affected
# ---------------------------------------------------------------------------

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/gmail.readonly",
]
GOOGLE_TOKEN_PATH = os.path.join(".credentials", "google_token_meeting.json")


# ---------------------------------------------------------------------------
# CLI & Config
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate meeting prep briefs from upcoming Google Calendar events.",
        epilog="Example: python meeting_prep.py --days 3 --verbose",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        metavar="N",
        help="Number of days to look ahead (1-5, default: 1)",
    )
    parser.add_argument(
        "--output-dir",
        default="reports",
        help="Directory to save reports (default: reports/)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed connector response info",
    )
    parser.add_argument(
        "--email",
        action="store_true",
        help="Convert output to HTML and email it (requires SMTP_USER and SMTP_PASSWORD in .env)",
    )
    return parser.parse_args()


def load_config():
    load_dotenv(override=True)
    config = {
        "anthropic_api_key":       os.getenv("ANTHROPIC_API_KEY"),
        "youcom_api_key":          os.getenv("YOUCOM_API_KEY"),
        "slack_user_token":        os.getenv("SLACK_USER_TOKEN"),
        "sf_username":             os.getenv("SF_USERNAME", ""),
        "google_credentials_file": os.getenv("GOOGLE_CREDENTIALS_FILE"),
        # Email (optional — only needed with --email flag)
        "smtp_host":               os.getenv("SMTP_HOST", "smtp.gmail.com"),
        "smtp_port":               int(os.getenv("SMTP_PORT", "587")),
        "smtp_user":               os.getenv("SMTP_USER", ""),
        "smtp_password":           os.getenv("SMTP_PASSWORD", ""),
        "email_to":                os.getenv("EMAIL_TO", ""),
    }
    if not config["anthropic_api_key"] or config["anthropic_api_key"].startswith("your_"):
        print("ERROR: Missing or placeholder ANTHROPIC_API_KEY in .env")
        sys.exit(1)
    if not config["google_credentials_file"]:
        print("ERROR: GOOGLE_CREDENTIALS_FILE not set — Google Calendar access is required.")
        sys.exit(1)
    return config


def get_my_domain(config: dict) -> str:
    """Derive user's email domain from SF_USERNAME or MY_EMAIL_DOMAIN env var."""
    sf_user = config.get("sf_username", "")
    if "@" in sf_user:
        return sf_user.split("@")[1].lower()
    my_domain = os.getenv("MY_EMAIL_DOMAIN", "")
    if my_domain:
        return my_domain.lower()
    print("ERROR: Cannot determine your email domain. Set SF_USERNAME or MY_EMAIL_DOMAIN in .env")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


def domain_to_company_name(domain: str) -> str:
    """Best-effort company name from domain. e.g. 'acme-corp.com' → 'Acme Corp'"""
    stop_tlds = {"com", "co", "org", "net", "io", "ai", "us", "uk", "ca", "gov", "edu", "app"}
    parts = domain.lower().replace("-", " ").split(".")
    name_parts = [p for p in parts if p not in stop_tlds and p]
    if not name_parts:
        return domain
    return " ".join(p.capitalize() for p in name_parts)


def format_datetime_display(dt_str: str) -> str:
    """Format ISO datetime string for human display."""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%A, %B %-d at %-I:%M %p %Z").strip()
    except Exception:
        return dt_str


def compute_duration(start_str: str, end_str: str) -> str:
    """Return human-readable duration between two ISO datetime strings."""
    try:
        start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        mins = int((end - start).total_seconds() / 60)
        if mins < 60:
            return f"{mins} minutes"
        hours, rem = divmod(mins, 60)
        return f"{hours}h {rem}m" if rem else f"{hours} hour{'s' if hours > 1 else ''}"
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Google OAuth (Calendar + Drive + Gmail)
# ---------------------------------------------------------------------------

def get_google_creds(config: dict, verbose: bool):
    """Return valid Google credentials, prompting for OAuth if needed."""
    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request as GoogleRequest
    except ImportError:
        print(
            "ERROR: Google libraries not installed. Run:\n"
            "  pip install google-api-python-client google-auth-oauthlib google-auth-httplib2"
        )
        sys.exit(1)

    creds = None
    if os.path.exists(GOOGLE_TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_PATH, GOOGLE_SCOPES)
        except Exception:
            creds = None  # Corrupted or scope-mismatched token — re-auth

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(GoogleRequest())
                if verbose:
                    print("    [verbose] Google token refreshed")
            except Exception:
                creds = None  # Refresh failed — need fresh auth

        if not creds:
            creds_file = config["google_credentials_file"]
            if not os.path.exists(creds_file):
                print(
                    f"ERROR: Google credentials file not found at '{creds_file}'.\n"
                    "Download credentials.json from Google Cloud Console → "
                    "APIs & Services → Credentials → OAuth 2.0 Client ID (Desktop app)."
                )
                sys.exit(1)
            print("  Google OAuth: opening browser for authorization (one-time only)...")
            print("  Note: This script needs 'calendar.readonly' scope in addition to Drive/Gmail.")
            try:
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, GOOGLE_SCOPES)
                creds = flow.run_local_server(port=0)
            except Exception as e:
                print(f"ERROR: Google OAuth failed: {e}")
                sys.exit(1)

        os.makedirs(".credentials", exist_ok=True)
        try:
            with open(GOOGLE_TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
            if verbose:
                print(f"    [verbose] Google token saved to {GOOGLE_TOKEN_PATH}")
        except Exception:
            pass  # Non-fatal

    return creds


# ---------------------------------------------------------------------------
# Google Calendar
# ---------------------------------------------------------------------------

def fetch_calendar_events(creds, days: int, verbose: bool) -> list:
    """Fetch calendar events for today through today + N days."""
    try:
        from googleapiclient.discovery import build
    except ImportError:
        print("ERROR: googleapiclient not installed.")
        sys.exit(1)

    svc = build("calendar", "v3", credentials=creds)
    now = datetime.now(timezone.utc)
    time_min = now.replace(hour=0, minute=0, second=0, microsecond=0)
    time_max = time_min + timedelta(days=days)

    try:
        result = svc.events().list(
            calendarId="primary",
            timeMin=time_min.isoformat(),
            timeMax=time_max.isoformat(),
            singleEvents=True,
            orderBy="startTime",
            maxResults=50,
        ).execute()
        events = result.get("items", [])
        if verbose:
            print(f"    [verbose] Calendar: {len(events)} total events in window")
        return events
    except Exception as e:
        print(f"  WARN  Calendar fetch error: {e}")
        return []


def is_external_meeting(event: dict, my_domain: str) -> bool:
    """Return True if the event has at least one non-resource external attendee."""
    if event.get("status") == "cancelled":
        return False
    # Skip all-day events (have 'date' key, not 'dateTime')
    start = event.get("start", {})
    if "date" in start and "dateTime" not in start:
        return False
    attendees = event.get("attendees", [])
    if not attendees:
        return False
    for att in attendees:
        email = att.get("email", "").lower()
        if "@" not in email:
            continue
        domain = email.split("@")[1]
        if domain == my_domain:
            continue
        if domain.endswith("calendar.google.com") or domain.endswith("resource.calendar.google.com"):
            continue
        if att.get("responseStatus") == "declined":
            continue
        return True
    return False


def get_external_attendees(event: dict, my_domain: str) -> list:
    """Extract external attendees, deduped by email."""
    result = []
    seen = set()
    for att in event.get("attendees", []):
        email = att.get("email", "").lower()
        if not email or "@" not in email:
            continue
        domain = email.split("@")[1]
        if domain == my_domain:
            continue
        if domain.endswith("calendar.google.com"):
            continue
        if att.get("responseStatus") == "declined":
            continue
        if email not in seen:
            seen.add(email)
            result.append({
                "email": email,
                "display_name": att.get("displayName", ""),
                "domain": domain,
            })
    return result


# ---------------------------------------------------------------------------
# You.com Search
# ---------------------------------------------------------------------------

def search_youcom(query: str, api_key: str, num_results: int = 5, verbose: bool = False) -> dict:
    """Call You.com search API and return the raw response."""
    if not api_key or api_key.startswith("your_"):
        return {"results": {"web": []}, "error": "not_configured"}

    url = "https://api.you.com/v1/search"
    headers = {"X-API-Key": api_key, "Accept": "application/json"}
    params = {"query": query, "num_web_results": num_results}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if verbose:
            hits = data.get("results", {}).get("web", [])
            print(f"    [verbose] You.com '{query[:50]}' → {len(hits)} results")
        return data
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else "?"
        if verbose:
            print(f"    [verbose] You.com HTTP {status} for '{query[:50]}'")
        return {"results": {"web": []}, "error": f"http_{status}"}
    except Exception as e:
        if verbose:
            print(f"    [verbose] You.com error: {e}")
        return {"results": {"web": []}, "error": str(e)}


def extract_snippets(data: dict, max_chars: int = 1000) -> str:
    """Concatenate title + snippet text from You.com web results."""
    web_results = data.get("results", {}).get("web", [])
    parts = []
    for hit in web_results:
        title = hit.get("title", "")
        snippets = hit.get("snippets", [])
        if isinstance(snippets, str):
            snippets = [snippets]
        description = hit.get("description", "")
        text = " ".join(snippets) if snippets else description
        if text:
            header = f"**{title}**" if title else ""
            parts.append(f"{header}\n{text.strip()}".strip())
    combined = "\n\n".join(parts)
    return combined[:max_chars] if len(combined) > max_chars else combined


# ---------------------------------------------------------------------------
# Attendee Research (LinkedIn / public profile via You.com)
# ---------------------------------------------------------------------------

def research_attendee(name: str, company: str, email: str, api_key: str, verbose: bool) -> dict:
    """
    Search You.com for the attendee's title/role from LinkedIn or other
    public sources. Falls back to email-derived name if display_name is empty.
    Includes the email domain as a disambiguator for generic company names.
    """
    if not name:
        # Derive a name from the email local part
        local = email.split("@")[0]
        name = local.replace(".", " ").replace("_", " ").replace("-", " ").title()

    domain = email.split("@")[1] if "@" in email else ""
    domain_qualifier = f" {domain}" if domain else ""

    snippets = ""

    # Primary: LinkedIn-focused search with domain disambiguator
    q1 = f'"{name}" "{company}"{domain_qualifier} LinkedIn title role'
    data1 = search_youcom(q1, api_key, num_results=3, verbose=verbose)
    snippets = extract_snippets(data1, max_chars=700)
    time.sleep(0.3)

    # Fallback: broader public search
    if not snippets or len(snippets) < 100:
        q2 = f'"{name}" {company}{domain_qualifier} executive director manager position'
        data2 = search_youcom(q2, api_key, num_results=3, verbose=verbose)
        snippets2 = extract_snippets(data2, max_chars=700)
        if len(snippets2) > len(snippets):
            snippets = snippets2
        time.sleep(0.3)

    return {
        "name": name,
        "email": email,
        "company": company,
        "domain": email.split("@")[1] if "@" in email else "",
        "research_snippets": snippets or f"No public profile information found for {name} at {company}.",
    }


# ---------------------------------------------------------------------------
# Slack Context
# ---------------------------------------------------------------------------

def pull_slack_context(company: str, config: dict, verbose: bool) -> str:
    """Search Slack for internal messages mentioning the company."""
    token = config.get("slack_user_token", "")
    if not token or token.startswith("xoxp-your") or token.startswith("your_"):
        return "_Slack: not configured._"

    try:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError
    except ImportError:
        return "_Slack: `slack-sdk` not installed. Run: pip install slack-sdk_"

    client = WebClient(token=token)
    try:
        response = client.search_messages(
            query=f'"{company}"',
            count=10,
            sort="timestamp",
            sort_dir="desc",
        )
    except SlackApiError as e:
        error_code = e.response.get("error", "unknown")
        return f"_Slack search error: `{error_code}`_"
    except Exception as e:
        return f"_Slack connection error: {e}_"

    matches = response.get("messages", {}).get("matches", [])
    if verbose:
        print(f"    [verbose] Slack: {len(matches)} messages for '{company}'")

    if not matches:
        return f"_No Slack messages found mentioning '{company}'._"

    lines = [f"**Slack** ({len(matches)} messages mentioning '{company}')"]
    for m in matches[:8]:
        ts_raw = m.get("ts", "0")
        try:
            ts_display = datetime.fromtimestamp(float(ts_raw)).strftime("%Y-%m-%d")
        except Exception:
            ts_display = ts_raw
        channel = (m.get("channel") or {}).get("name", "unknown")
        username = m.get("username") or m.get("user", "unknown")
        text = (m.get("text") or "").strip()[:220]
        permalink = m.get("permalink", "")
        lines.append(f"\n[{ts_display}] #{channel} — {username}")
        lines.append(f"> {text}")
        if permalink:
            lines.append(f"  [→ View]({permalink})")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Google Drive Context
# ---------------------------------------------------------------------------

def pull_drive_context(company: str, creds, verbose: bool) -> dict:
    """
    Search Google Drive for documents mentioning the company.
    Flags any document that looks like an account plan.
    Returns dict with: files, account_plan (file dict or None), formatted (str).
    """
    try:
        from googleapiclient.discovery import build
    except ImportError:
        return {"files": [], "account_plan": None, "formatted": "_Drive: libraries not installed._"}

    try:
        drive_svc = build("drive", "v3", credentials=creds)
    except Exception as e:
        return {"files": [], "account_plan": None, "formatted": f"_Drive init error: {e}_"}

    files = []
    account_plan = None

    try:
        result = drive_svc.files().list(
            q=f'fullText contains "{company}"',
            fields="files(id,name,mimeType,webViewLink,modifiedTime)",
            pageSize=15,
            orderBy="modifiedTime desc",
        ).execute()
        files = result.get("files", [])
        if verbose:
            print(f"    [verbose] Drive: {len(files)} files for '{company}'")
    except Exception as e:
        if verbose:
            print(f"    [verbose] Drive error: {e}")
        return {"files": [], "account_plan": None, "formatted": f"_Drive search error: {e}_"}

    # Identify account plan documents
    plan_keywords = ["account plan", "account_plan", "acct plan", "qbr", "business plan", "strategic plan"]
    for f in files:
        name_lower = (f.get("name") or "").lower()
        if any(kw in name_lower for kw in plan_keywords):
            account_plan = f
            break

    if not files:
        return {
            "files": [],
            "account_plan": None,
            "formatted": f"_No Google Drive files found mentioning '{company}'._",
        }

    lines = [f"**Google Drive** ({len(files)} files mentioning '{company}')"]
    if account_plan:
        lines.append(f"\n⭐ Account Plan found: [{account_plan.get('name')}]({account_plan.get('webViewLink', '')})")

    for f in files:
        mime_short = (f.get("mimeType") or "").split(".")[-1]
        modified = (f.get("modifiedTime") or "")[:10]
        name = f.get("name", "Untitled")
        link = f.get("webViewLink", "")
        flag = " ⭐" if f is account_plan else ""
        entry = f"- [{name}]({link})" if link else f"- {name}"
        lines.append(f"{entry} — `{mime_short}` — {modified}{flag}")

    return {
        "files": files,
        "account_plan": account_plan,
        "formatted": "\n".join(lines),
    }


# ---------------------------------------------------------------------------
# Recent News Search
# ---------------------------------------------------------------------------

def search_recent_news(company: str, domain: str, api_key: str, verbose: bool) -> str:
    """Search You.com for recent news and AI/data context about the company."""
    if not api_key or api_key.startswith("your_"):
        return "_You.com not configured — news search skipped._"

    # Include the domain as an additional disambiguator for generic company names
    # e.g. "Factory" alone matches Fox Factory; "Factory factory.ai" narrows correctly
    domain_qualifier = f" {domain}" if domain else ""

    queries = [
        {
            "label": "News & Press",
            "query": f'"{company}"{domain_qualifier} news earnings press release announcement 2025 2026',
        },
        {
            "label": "AI & Data Strategy",
            "query": f'"{company}"{domain_qualifier} generative AI artificial intelligence data transformation strategy 2025 2026',
        },
    ]

    all_snippets = []
    for q in queries:
        data = search_youcom(q["query"], api_key, num_results=5, verbose=verbose)
        snippet = extract_snippets(data, max_chars=800)
        if snippet:
            all_snippets.append(f"**{q['label']}**\n{snippet}")
        time.sleep(0.35)

    if not all_snippets:
        return f"_No recent news found for '{company}'._"

    combined = "\n\n---\n\n".join(all_snippets)
    return combined[:1800] if len(combined) > 1800 else combined


# ---------------------------------------------------------------------------
# Claude Synthesis
# ---------------------------------------------------------------------------

def build_meeting_prompt(
    meeting: dict,
    external_attendees: list,
    attendee_research: list,
    slack_context: str,
    drive_context: str,
    news_context: str,
    orgs: list,
) -> str:
    """Build the Claude synthesis prompt for a single meeting."""
    start = meeting.get("start", {})
    end = meeting.get("end", {})
    start_str = start.get("dateTime", start.get("date", ""))
    end_str = end.get("dateTime", end.get("date", ""))
    title = meeting.get("summary", "(No title)")
    location = meeting.get("location", "N/A")
    description = (meeting.get("description") or "").strip()[:600]
    duration = compute_duration(start_str, end_str)
    time_display = format_datetime_display(start_str) if start_str else "TBD"
    org_names = ", ".join(orgs) if orgs else "Unknown Organization"

    # Build attendee list
    attendee_lines = []
    for att in external_attendees:
        name = att.get("display_name") or att["email"].split("@")[0].title()
        attendee_lines.append(f"- {name} <{att['email']}> ({att['domain']})")
    attendee_block = "\n".join(attendee_lines) if attendee_lines else "- (no external attendees identified)"

    # Build attendee research block
    research_block = ""
    for ar in attendee_research:
        research_block += f"\n### {ar['name']} ({ar['email']})\n"
        research_block += f"Company: {ar['company']}\n"
        research_block += f"Public research snippets:\n{ar['research_snippets']}\n"

    return f"""You are preparing a meeting prep brief for a sales representative at You.com (an AI-powered search company). You need to synthesize information about an upcoming external meeting into a detailed, actionable brief.

=== MEETING DETAILS ===
Title: {title}
Date/Time: {time_display}
Duration: {duration}
Location/Link: {location}
Calendar Description: {description or "(none)"}

=== EXTERNAL ATTENDEES ===
{attendee_block}

=== ATTENDEE RESEARCH (from LinkedIn / public sources) ===
{research_block if research_block.strip() else "No research data available."}

=== INTERNAL CONTEXT — SLACK ===
{slack_context}

=== INTERNAL CONTEXT — GOOGLE DRIVE ===
{drive_context}

=== RECENT NEWS & DEVELOPMENTS — {org_names.upper()} ===
{news_context}

---

Using ONLY the information above, write a comprehensive meeting prep brief in clean markdown. Follow this exact structure:

## Meeting Overview
State the meeting title, date/time, duration, location/link, and a one-sentence description of what this meeting is about based on the available context. List all external attendees in a table: Name | Company | Email | Title/Role (extract titles from the research snippets where found; write "Unknown" if not found).

## About {org_names}
Write 2-4 sentences describing what {org_names} does, their market position, and any relevant context. Base this on the research snippets and news. If information is limited, say so.

## Attendee Profiles
For each external attendee, write a short paragraph covering their role/title (extracted from research), seniority level, and any notable context relevant to this meeting. If no title is found in the research, acknowledge it and note where to verify (e.g., LinkedIn).

## Our Relationship with {org_names}
Summarize what the internal Slack messages and Drive documents reveal about the existing relationship. Note the tone (active prospect, existing customer, dormant, new), any prior meetings or discussions, and any open items or commitments. If an Account Plan was found in Drive, highlight it prominently with a link and note its last modified date.

## Recent News & Talking Points
List 4-6 recent developments about {org_names} from the news research that could serve as conversation starters or demonstrate preparation. Format as bullet points with approximate dates where available.

## Strategic Prep Recommendations
Write 5-7 specific, actionable bullet points covering:
- The primary objective for this meeting based on the relationship context
- Key questions to ask each attendee based on their role
- Topics to raise that connect You.com's capabilities to their likely priorities
- Any sensitivities or things to avoid based on the relationship history
- What you want the attendee to agree to or commit to by end of meeting

---

Be specific and practical. Ground all claims in the provided data. Where information is missing, acknowledge it briefly and suggest a concrete action to fill the gap before the meeting (e.g., "Check their LinkedIn before the call"). Do NOT fabricate names, titles, or facts not present in the research.
"""


def synthesize_meeting_prep(meeting_data: dict, api_key: str, verbose: bool) -> str:
    """Call Claude to generate the meeting prep brief."""
    client = anthropic.Anthropic(api_key=api_key)
    prompt = build_meeting_prompt(**meeting_data)

    if verbose:
        print(f"    [verbose] Meeting prep prompt: {len(prompt):,} characters")

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=(
                "You are a senior sales executive assistant preparing detailed, actionable meeting "
                "briefs for B2B sales reps at You.com. Your briefs are specific, grounded in provided "
                "data, and practical. You extract titles and roles from research snippets wherever "
                "possible. You write in clear, professional prose and explicitly acknowledge when "
                "information is unavailable rather than speculating."
            ),
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text

    except anthropic.AuthenticationError:
        print("ERROR: Anthropic API key is invalid.")
        sys.exit(1)
    except anthropic.RateLimitError:
        print("ERROR: Anthropic API rate limit exceeded.")
        sys.exit(1)
    except anthropic.APIError as e:
        print(f"ERROR: Anthropic API error: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_meeting_prep(briefs: list, days: int, output_dir: str) -> str:
    """Write all meeting prep briefs to a date-stamped subfolder."""
    today_str = date.today().strftime("%Y-%m-%d")
    dated_dir = os.path.join(output_dir, today_str)
    os.makedirs(dated_dir, exist_ok=True)
    filename = f"meeting_prep_d{days}.md"
    filepath = os.path.join(dated_dir, filename)

    header = (
        f"# Meeting Prep Brief\n\n"
        f"_Generated: {today_str} | Looking ahead: {days} day{'s' if days > 1 else ''}_\n\n"
        f"---\n\n"
    )

    if not briefs:
        content = header + "_No external meetings found in this window._\n"
    else:
        sections = []
        for i, brief in enumerate(briefs, 1):
            meeting_title = brief["meeting"].get("summary", "(No title)")
            start = brief["meeting"].get("start", {})
            start_str = start.get("dateTime", "")
            time_display = format_datetime_display(start_str) if start_str else ""
            sections.append(
                f"# Meeting {i} of {len(briefs)}: {meeting_title}\n"
                f"_{time_display}_\n\n"
                f"{brief['content'].strip()}\n"
            )
        content = header + "\n\n---\n\n".join(sections) + "\n"

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


# ---------------------------------------------------------------------------
# Email (HTML conversion + SMTP send)
# ---------------------------------------------------------------------------

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{subject}</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    font-size: 15px;
    line-height: 1.6;
    color: #1a1a1a;
    background: #f5f5f5;
    margin: 0;
    padding: 24px 16px;
  }}
  .wrapper {{
    max-width: 760px;
    margin: 0 auto;
    background: #ffffff;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
  }}
  .header {{
    background: #0f0f0f;
    color: #ffffff;
    padding: 24px 32px;
  }}
  .header h1 {{
    margin: 0 0 4px 0;
    font-size: 20px;
    font-weight: 600;
    letter-spacing: -0.3px;
  }}
  .header p {{
    margin: 0;
    font-size: 13px;
    color: #aaaaaa;
  }}
  .body {{
    padding: 28px 32px;
  }}
  h1 {{ font-size: 22px; font-weight: 700; color: #0f0f0f; margin: 32px 0 12px; border-bottom: 2px solid #e8e8e8; padding-bottom: 8px; }}
  h2 {{ font-size: 17px; font-weight: 600; color: #1a1a1a; margin: 24px 0 8px; }}
  h3 {{ font-size: 15px; font-weight: 600; color: #333333; margin: 20px 0 6px; }}
  p {{ margin: 0 0 12px; }}
  ul, ol {{ margin: 0 0 12px; padding-left: 24px; }}
  li {{ margin-bottom: 6px; }}
  a {{ color: #0066cc; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  blockquote {{
    margin: 12px 0;
    padding: 10px 16px;
    background: #fff8e6;
    border-left: 4px solid #f5a623;
    border-radius: 0 4px 4px 0;
    color: #5a4000;
    font-size: 14px;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    margin: 12px 0 16px;
    font-size: 14px;
  }}
  th {{
    background: #f0f0f0;
    text-align: left;
    padding: 9px 12px;
    font-weight: 600;
    border: 1px solid #ddd;
    color: #333;
  }}
  td {{
    padding: 8px 12px;
    border: 1px solid #e0e0e0;
    vertical-align: top;
  }}
  tr:nth-child(even) td {{ background: #fafafa; }}
  code {{
    background: #f0f0f0;
    border-radius: 3px;
    padding: 1px 5px;
    font-family: 'SF Mono', 'Menlo', monospace;
    font-size: 13px;
    color: #c7254e;
  }}
  pre {{
    background: #f6f6f6;
    border-radius: 4px;
    padding: 14px 16px;
    overflow-x: auto;
    font-size: 13px;
    border: 1px solid #e4e4e4;
  }}
  pre code {{ background: none; padding: 0; color: inherit; }}
  hr {{ border: none; border-top: 1px solid #e8e8e8; margin: 24px 0; }}
  .footer {{
    text-align: center;
    font-size: 12px;
    color: #999;
    padding: 16px 32px 24px;
    border-top: 1px solid #efefef;
  }}
  /* Checkbox list items */
  li input[type=checkbox] {{ margin-right: 6px; }}
</style>
</head>
<body>
<div class="wrapper">
  <div class="header">
    <h1>Meeting Prep Brief</h1>
    <p>{subtitle}</p>
  </div>
  <div class="body">
    {body}
  </div>
  <div class="footer">
    Generated by meeting_prep.py &mdash; You.com
  </div>
</div>
</body>
</html>
"""


def markdown_to_html(md_content: str) -> str:
    """Convert markdown string to HTML. Requires: pip install markdown"""
    try:
        import markdown as md_lib
    except ImportError:
        print("  WARN  `markdown` library not installed — install with: pip install markdown")
        print("        Falling back to plain-text email body.")
        return f"<pre>{md_content}</pre>"

    # Convert GFM-style task list checkboxes before passing to markdown
    md_prepped = re.sub(r"^- \[ \] ", "- ☐ ", md_content, flags=re.MULTILINE)
    md_prepped = re.sub(r"^- \[x\] ", "- ☑ ", md_prepped, flags=re.MULTILINE)

    return md_lib.markdown(
        md_prepped,
        extensions=["tables", "fenced_code", "nl2br", "sane_lists"],
    )


def send_email(markdown_content: str, subject: str, config: dict, verbose: bool) -> bool:
    """Convert markdown to HTML and send via SMTP. Returns True on success."""
    smtp_user = config.get("smtp_user", "")
    smtp_password = config.get("smtp_password", "")
    email_to = config.get("email_to", "") or config.get("sf_username", "")

    if not smtp_user or not smtp_password:
        print("  SKIP  Email: SMTP_USER or SMTP_PASSWORD not set in .env")
        return False
    if not email_to:
        print("  SKIP  Email: no recipient — set EMAIL_TO in .env")
        return False

    today_str = date.today().strftime("%B %-d, %Y")
    subtitle = f"{today_str}"

    body_html = markdown_to_html(markdown_content)

    full_html = HTML_TEMPLATE.format(
        subject=subject,
        subtitle=subtitle,
        body=body_html,
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = email_to
    msg.attach(MIMEText(markdown_content, "plain", "utf-8"))
    msg.attach(MIMEText(full_html, "html", "utf-8"))

    try:
        if verbose:
            print(f"    [verbose] SMTP: {config['smtp_host']}:{config['smtp_port']} → {email_to}")
        with smtplib.SMTP(config["smtp_host"], config["smtp_port"]) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, email_to, msg.as_string())
        return True
    except smtplib.SMTPAuthenticationError:
        print("  ERROR Email: SMTP authentication failed.")
        print("        For Gmail, use an App Password: myaccount.google.com/apppasswords")
        return False
    except Exception as e:
        print(f"  ERROR Email: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    if not 1 <= args.days <= 5:
        print("ERROR: --days must be between 1 and 5")
        sys.exit(1)

    config = load_config()
    my_domain = get_my_domain(config)
    youcom_key = config.get("youcom_api_key", "")

    print(f"\nMeeting Prep — Next {args.days} day{'s' if args.days > 1 else ''}")
    print(f"Your domain: @{my_domain}")
    print("=" * 60)

    # -----------------------------------------------------------------
    # [1/5] Google OAuth & Calendar fetch
    # -----------------------------------------------------------------
    print("\n[1/5] Connecting to Google Calendar...")
    creds = get_google_creds(config, args.verbose)
    events = fetch_calendar_events(creds, args.days, args.verbose)
    external_meetings = [e for e in events if is_external_meeting(e, my_domain)]

    print(f"  {len(events)} total events  |  {len(external_meetings)} with external attendees")

    if not external_meetings:
        print("\n  No external meetings found in this window. Exiting without report.")
        return

    for i, ev in enumerate(external_meetings, 1):
        start_str = ev.get("start", {}).get("dateTime", "")
        print(f"  {i}. {ev.get('summary', '(No title)')} — {format_datetime_display(start_str)}")

    # -----------------------------------------------------------------
    # [2/5] Research external attendees via You.com
    # -----------------------------------------------------------------
    print(f"\n[2/5] Researching external attendees...")

    if not youcom_key or youcom_key.startswith("your_"):
        print("  SKIP  You.com not configured — attendee and news research will be skipped")

    all_meeting_data = []

    for meeting in external_meetings:
        title = meeting.get("summary", "(No title)")
        ext_attendees = get_external_attendees(meeting, my_domain)

        # Map email domain → company name
        domain_to_org = {}
        for att in ext_attendees:
            d = att["domain"]
            if d not in domain_to_org:
                domain_to_org[d] = domain_to_company_name(d)

        orgs = list(dict.fromkeys(domain_to_org.values()))  # unique, order-preserving
        print(f"\n  → {title}")
        print(f"    Orgs: {', '.join(orgs) or 'none detected'}")
        print(f"    External attendees: {len(ext_attendees)}")

        # Research each attendee
        attendee_research = []
        for att in ext_attendees:
            name = att.get("display_name", "")
            company = domain_to_org.get(att["domain"], att["domain"])
            label = name or att["email"]
            if args.verbose:
                print(f"    Researching: {label} @ {company}")
            else:
                print(f"    Researching: {label}")
            result = research_attendee(name, company, att["email"], youcom_key, args.verbose)
            attendee_research.append(result)

        all_meeting_data.append({
            "meeting": meeting,
            "ext_attendees": ext_attendees,
            "attendee_research": attendee_research,
            "orgs": orgs,
            "domain_to_org": domain_to_org,
        })

    # -----------------------------------------------------------------
    # [3/5] Pull internal context (Slack + Drive) per unique org
    # -----------------------------------------------------------------
    print(f"\n[3/5] Pulling internal context (Slack + Drive)...")

    # De-duplicate orgs across all meetings to avoid redundant searches
    all_orgs = list(dict.fromkeys(
        org
        for md in all_meeting_data
        for org in md["orgs"]
    ))

    # Build org → domain mapping for search disambiguation
    org_to_domain = {}
    for md in all_meeting_data:
        for domain, org in md["domain_to_org"].items():
            if org not in org_to_domain:
                org_to_domain[org] = domain

    org_contexts = {}
    for org in all_orgs:
        print(f"  Searching: {org}")
        slack_ctx = pull_slack_context(org, config, args.verbose)
        drive_ctx = pull_drive_context(org, creds, args.verbose)

        if org_contexts and args.verbose:
            time.sleep(0.2)

        org_contexts[org] = {
            "slack": slack_ctx,
            "drive": drive_ctx,
        }

    # -----------------------------------------------------------------
    # [4/5] Search for recent news per unique org
    # -----------------------------------------------------------------
    print(f"\n[4/5] Searching for recent news...")

    if youcom_key and not youcom_key.startswith("your_"):
        for org in all_orgs:
            domain = org_to_domain.get(org, "")
            print(f"  News: {org}" + (f" ({domain})" if domain else ""))
            news_ctx = search_recent_news(org, domain, youcom_key, args.verbose)
            org_contexts[org]["news"] = news_ctx
    else:
        for org in all_orgs:
            org_contexts[org]["news"] = "_You.com not configured — news search skipped._"

    # -----------------------------------------------------------------
    # [5/5] Synthesize with Claude + write output
    # -----------------------------------------------------------------
    print(f"\n[5/5] Generating meeting briefs with Claude...")

    briefs = []
    for md in all_meeting_data:
        meeting = md["meeting"]
        title = meeting.get("summary", "(No title)")
        orgs = md["orgs"]
        print(f"  Synthesizing: {title}...")

        # Combine context for all orgs in this meeting
        combined_slack = "\n\n".join(
            org_contexts[org]["slack"]
            for org in orgs
            if org in org_contexts
        )
        combined_drive = "\n\n".join(
            org_contexts[org]["drive"]["formatted"]
            for org in orgs
            if org in org_contexts
        )
        combined_news = "\n\n".join(
            org_contexts[org].get("news", "")
            for org in orgs
            if org in org_contexts
        )

        meeting_data = {
            "meeting": meeting,
            "external_attendees": md["ext_attendees"],
            "attendee_research": md["attendee_research"],
            "slack_context": combined_slack or "_No Slack context available._",
            "drive_context": combined_drive or "_No Drive documents found._",
            "news_context": combined_news or "_No news data available._",
            "orgs": orgs,
        }

        brief_content = synthesize_meeting_prep(meeting_data, config["anthropic_api_key"], args.verbose)
        briefs.append({"meeting": meeting, "content": brief_content})

    filepath = write_meeting_prep(briefs, args.days, args.output_dir)

    print(f"\nDone! Meeting prep saved to: {filepath}")

    if args.email:
        print("\nSending email...")
        n = len(briefs)
        orgs_label = ", ".join(
            dict.fromkeys(org for md in all_meeting_data for org in md["orgs"])
        )
        subject = (
            f"Meeting Prep: {date.today().strftime('%b %-d')} — "
            f"{n} meeting{'s' if n != 1 else ''}"
            + (f" ({orgs_label})" if orgs_label else "")
        )
        with open(filepath, encoding="utf-8") as f:
            md_content = f.read()
        ok = send_email(md_content, subject, config, args.verbose)
        if ok:
            print(f"  Email sent to: {config.get('email_to') or config.get('sf_username')}")

    print("=" * 60)


if __name__ == "__main__":
    main()
