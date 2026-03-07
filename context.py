"""
context.py — Internal Account Context Report

Pulls internal account history from Salesforce CRM, Slack, Gmail,
Google Drive, and Notion, then synthesizes a structured sales rep
briefing using Claude.

All connectors are optional — unconfigured ones are skipped with a
note in the report. Only ANTHROPIC_API_KEY is required.

Usage:
    python context.py "Acme Corp"
    python context.py "Acme Corp" --output-dir my_reports --verbose
"""

import os
import re
import sys
import argparse
from datetime import date
from pathlib import Path

import anthropic
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Connector registry — defines which env vars each connector requires
# ---------------------------------------------------------------------------

CONNECTORS = [
    {
        "key": "salesforce",
        "label": "Salesforce CRM",
        "env_vars": ["SF_USERNAME", "SF_PASSWORD", "SF_SECURITY_TOKEN"],
    },
    {
        "key": "slack",
        "label": "Slack",
        "env_vars": ["SLACK_USER_TOKEN"],
    },
    {
        "key": "google",
        "label": "Gmail & Google Drive",
        "env_vars": ["GOOGLE_CREDENTIALS_FILE"],
    },
]


# ---------------------------------------------------------------------------
# CLI & Config
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Pull internal account history and generate a context report.",
        epilog='Example: python context.py "Acme Corp" --verbose',
    )
    parser.add_argument("company", help="Company name to look up")
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
    return parser.parse_args()


def load_config():
    load_dotenv(override=True)
    config = {
        # Required
        "anthropic_api_key":      os.getenv("ANTHROPIC_API_KEY"),
        # Salesforce (optional)
        "sf_username":            os.getenv("SF_USERNAME"),
        "sf_password":            os.getenv("SF_PASSWORD"),
        "sf_security_token":      os.getenv("SF_SECURITY_TOKEN"),
        "sf_domain":              os.getenv("SF_DOMAIN", "login"),
        # Slack (optional)
        "slack_user_token":       os.getenv("SLACK_USER_TOKEN"),
        # Google (optional)
        "google_credentials_file": os.getenv("GOOGLE_CREDENTIALS_FILE"),
        # Drive false-positive exclusions (optional)
        "drive_exclude_files":     os.getenv("DRIVE_EXCLUDE_FILES", ""),
        # SQLite DB path (for auto-populating account metadata)
        "db_path": str(Path(__file__).parent / "prospecting.db"),
    }
    # Only the Anthropic key is required — everything else is optional
    if not config["anthropic_api_key"] or config["anthropic_api_key"].startswith("your_"):
        print("ERROR: Missing or placeholder value for ANTHROPIC_API_KEY")
        print("Edit .env and set ANTHROPIC_API_KEY.")
        sys.exit(1)
    return config


def is_connector_configured(connector_def: dict, config: dict) -> bool:
    """Returns True only if every env var for this connector is set and non-placeholder."""
    for env_var in connector_def["env_vars"]:
        val = config.get(env_var.lower())
        if not val or str(val).startswith("your_") or str(val).startswith("xoxp-your"):
            return False
    return True


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def slugify(company: str) -> str:
    slug = company.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


def _soql_escape(value: str) -> str:
    """Escape single quotes in SOQL string literals."""
    return value.replace("'", "\\'")


def _best_account_match(accounts: list, company: str, domain: str = "") -> dict:
    """Pick the account whose Name/Website best matches the search term.

    Scoring (higher = better):
      4 — domain matches the SF account's Website field
      3 — exact name match (case-insensitive)
      2 — name starts with the search term
      1 — search term appears as a whole word inside the name
      0 — substring match (the SOQL LIKE fallback)
    """
    import re
    term = company.strip().lower()
    dom  = domain.strip().lower()

    def score(acct):
        if dom:
            website = (acct.get("Website") or "").lower()
            # Strip protocol/www for comparison
            website = re.sub(r'^https?://', '', website)
            website = re.sub(r'^www\.', '', website)
            if website.startswith(dom) or f".{dom}" in website:
                return 4
        name = (acct.get("Name") or "").strip().lower()
        if name == term:
            return 3
        if name.startswith(term):
            return 2
        if re.search(r'\b' + re.escape(term) + r'\b', name):
            return 1
        return 0

    return max(accounts, key=score)


# ---------------------------------------------------------------------------
# Salesforce Connector
# ---------------------------------------------------------------------------

def pull_salesforce(company: str, config: dict, verbose: bool, domain: str = "") -> dict:
    """
    Returns a dict with: account, opportunities, contacts, activities,
    events, formatted_text. On failure: {"error": ..., "formatted_text": ...}
    """
    try:
        from simple_salesforce import Salesforce, SalesforceAuthenticationFailed
    except ImportError:
        return {
            "error": "library_not_installed",
            "formatted_text": (
                "_Salesforce connector: `simple-salesforce` library not installed. "
                "Run: `pip install simple-salesforce`_"
            ),
        }

    try:
        sf = Salesforce(
            username=config["sf_username"],
            password=config["sf_password"],
            security_token=config["sf_security_token"],
            domain=config["sf_domain"],
        )
    except SalesforceAuthenticationFailed as e:
        return {
            "error": "auth_failed",
            "formatted_text": f"_Salesforce authentication failed: {e}_",
        }
    except Exception as e:
        return {
            "error": "connection_failed",
            "formatted_text": f"_Salesforce connection error: {e}_",
        }

    company_safe = _soql_escape(company)
    _acct_fields = (
        "SELECT Id, Name, Industry, Website, AnnualRevenue, "
        "NumberOfEmployees, Description, OwnerId, Owner.Name FROM Account"
    )

    # Step 1: Find the Account.
    # When a domain is provided, query by Website first — it's a more reliable
    # identifier than a fuzzy name match.  Fall back to name if domain yields nothing.
    accounts: list[dict] = []

    if domain:
        domain_safe = _soql_escape(domain)
        try:
            dom_result = sf.query(
                f"{_acct_fields} WHERE Website LIKE '%{domain_safe}%' LIMIT 5"
            )
            accounts = dom_result.get("records", [])
            if verbose and accounts:
                print(f"    [verbose] SF domain search found {len(accounts)} account(s)")
        except Exception:
            pass  # domain query is best-effort; fall through to name search

    if not accounts:
        # No domain, or domain search returned nothing — search by name
        try:
            acct_result = sf.query(
                f"{_acct_fields} WHERE Name LIKE '%{company_safe}%' LIMIT 5"
            )
            accounts = acct_result.get("records", [])
        except Exception as e:
            return {"error": "query_failed", "formatted_text": f"_Salesforce account query failed: {e}_"}

        # If we got name results AND have a domain, also merge any domain matches
        # (catches cases where the name search found a different account than the
        # domain search would, so _best_account_match can pick the better one).
        if accounts and domain:
            domain_safe = _soql_escape(domain)
            try:
                dom_result = sf.query(
                    f"{_acct_fields} WHERE Website LIKE '%{domain_safe}%' LIMIT 5"
                )
                existing_ids = {a["Id"] for a in accounts}
                accounts += [a for a in dom_result.get("records", []) if a["Id"] not in existing_ids]
            except Exception:
                pass

    if not accounts:
        return {
            "account": None,
            "opportunities": [],
            "contacts": [],
            "activities": [],
            "events": [],
            "formatted_text": f"_No Salesforce account found matching '{company}'._",
        }

    account = _best_account_match(accounts, company, domain)
    extra_note = f" ({len(accounts)} accounts matched; using best)" if len(accounts) > 1 else ""
    account_ids_str = ", ".join(f"'{a['Id']}'" for a in accounts)

    if verbose:
        print(f"    [verbose] SF Account: {account.get('Name')}{extra_note}")

    # Step 2: Opportunities
    opps = []
    try:
        opps = sf.query(
            f"SELECT Id, Name, StageName, Amount, CloseDate, Probability, "
            f"Owner.Name, LeadSource, IsClosed, IsWon "
            f"FROM Opportunity WHERE AccountId IN ({account_ids_str}) "
            f"ORDER BY CloseDate DESC LIMIT 20"
        ).get("records", [])
        if verbose:
            print(f"    [verbose] SF Opportunities: {len(opps)}")
    except Exception as e:
        if verbose:
            print(f"    [verbose] SF Opportunity query error: {e}")

    # Step 3: Contacts
    contacts = []
    try:
        contacts = sf.query(
            f"SELECT Id, FirstName, LastName, Title, Email, Phone, Department "
            f"FROM Contact WHERE AccountId IN ({account_ids_str}) LIMIT 20"
        ).get("records", [])
        if verbose:
            print(f"    [verbose] SF Contacts: {len(contacts)}")
    except Exception as e:
        if verbose:
            print(f"    [verbose] SF Contact query error: {e}")

    # Step 4: Tasks (recent activities)
    activities = []
    try:
        activities = sf.query(
            f"SELECT Id, Subject, Status, Priority, Description, ActivityDate, Owner.Name "
            f"FROM Task WHERE WhatId IN ({account_ids_str}) "
            f"ORDER BY ActivityDate DESC NULLS LAST LIMIT 30"
        ).get("records", [])
        if verbose:
            print(f"    [verbose] SF Tasks: {len(activities)}")
    except Exception as e:
        if verbose:
            print(f"    [verbose] SF Task query error: {e}")

    # Step 5: Events (meetings / calls)
    events = []
    try:
        events = sf.query(
            f"SELECT Id, Subject, Description, StartDateTime, Owner.Name "
            f"FROM Event WHERE WhatId IN ({account_ids_str}) "
            f"ORDER BY StartDateTime DESC NULLS LAST LIMIT 20"
        ).get("records", [])
        if verbose:
            print(f"    [verbose] SF Events: {len(events)}")
    except Exception as e:
        if verbose:
            print(f"    [verbose] SF Event query error: {e}")

    formatted_text = _format_salesforce(account, opps, contacts, activities, events, extra_note)

    # Auto-persist SF URLs to the local DB (best-effort, never fails the script)
    if config.get("db_path"):
        try:
            import db as db_module
            sf_base = f"https://{sf.sf_instance}"
            open_opps = [o for o in opps if not o.get("IsClosed")]
            best_opp = open_opps[0] if open_opps else (opps[0] if opps else None)
            db_module.upsert_account_meta(
                config["db_path"],
                slugify(company),
                sf_account_url     = f"{sf_base}/{account['Id']}",
                sf_opportunity_url = (f"{sf_base}/{best_opp['Id']}" if best_opp else None),
            )
        except Exception:
            pass  # never fail the script over a DB write

    sf_account_url = f"https://{sf.sf_instance}/{account['Id']}"

    return {
        "account": account,
        "opportunities": opps,
        "contacts": contacts,
        "activities": activities,
        "events": events,
        "formatted_text": formatted_text,
        "sf_account_url": sf_account_url,
    }


def _format_salesforce(account, opps, contacts, activities, events, note) -> str:
    lines = []

    # Account overview
    lines.append(f"### Account: {account.get('Name', 'Unknown')}{note}")
    lines.append(f"- Industry: {account.get('Industry') or 'N/A'}")
    lines.append(f"- Website: {account.get('Website') or 'N/A'}")
    rev = account.get("AnnualRevenue")
    lines.append(f"- Annual Revenue: ${rev:,.0f}" if rev else "- Annual Revenue: N/A")
    emp = account.get("NumberOfEmployees")
    lines.append(f"- Employees: {emp:,}" if emp else "- Employees: N/A")
    owner_name = (account.get("Owner") or {}).get("Name", "N/A")
    lines.append(f"- Account Owner: {owner_name}")
    desc = (account.get("Description") or "").strip()
    if desc:
        lines.append(f"- Description: {desc[:300]}")

    # Opportunities table
    lines.append("\n### Opportunities")
    if opps:
        lines.append("| Name | Stage | Amount | Close Date | Owner | Result |")
        lines.append("|------|-------|--------|------------|-------|--------|")
        for o in opps:
            amt = f"${o.get('Amount') or 0:,.0f}"
            if o.get("IsWon"):
                result = "Won ✓"
            elif o.get("IsClosed"):
                result = "Lost ✗"
            else:
                result = f"Open ({o.get('Probability', '?')}%)"
            owner_name = (o.get("Owner") or {}).get("Name", "N/A")
            name = (o.get("Name") or "")[:45]
            lines.append(
                f"| {name} | {o.get('StageName', '')} | {amt} "
                f"| {o.get('CloseDate', '')} | {owner_name} | {result} |"
            )
    else:
        lines.append("No opportunities found.")

    # Contacts
    lines.append("\n### Contacts")
    if contacts:
        for c in contacts:
            name = f"{c.get('FirstName', '')} {c.get('LastName', '')}".strip()
            dept = c.get("Department")
            dept_str = f" ({dept})" if dept else ""
            lines.append(
                f"- **{name}**{dept_str} | {c.get('Title') or 'N/A'} "
                f"| {c.get('Email') or 'N/A'} | {c.get('Phone') or 'N/A'}"
            )
    else:
        lines.append("No contacts found.")

    # Recent Tasks
    lines.append("\n### Recent Tasks / Activities")
    if activities:
        for t in activities:
            owner_name = (t.get("Owner") or {}).get("Name", "N/A")
            date_str = t.get("ActivityDate") or "no date"
            subj = t.get("Subject") or "(no subject)"
            status = t.get("Status") or ""
            lines.append(f"- [{date_str}] **{subj}** — {status} (Owner: {owner_name})")
    else:
        lines.append("No recent tasks found.")

    # Recent Events
    lines.append("\n### Recent Events / Meetings")
    if events:
        for e in events:
            owner_name = (e.get("Owner") or {}).get("Name", "N/A")
            dt = (e.get("StartDateTime") or "")[:10]
            subj = e.get("Subject") or "(no subject)"
            lines.append(f"- [{dt}] **{subj}** (Owner: {owner_name})")
    else:
        lines.append("No recent events found.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Slack Connector
# ---------------------------------------------------------------------------

def pull_slack(company: str, config: dict, verbose: bool) -> dict:
    """
    Returns dict with: messages, formatted_text.
    Requires a USER token (xoxp-...) with search:read scope.
    Bot tokens (xoxb-) cannot call search.messages.
    """
    try:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError
    except ImportError:
        return {
            "error": "library_not_installed",
            "formatted_text": (
                "_Slack connector: `slack-sdk` library not installed. "
                "Run: `pip install slack-sdk`_"
            ),
        }

    client = WebClient(token=config["slack_user_token"])

    try:
        response = client.search_messages(
            query=f'"{company}"',
            count=20,
            sort="timestamp",
            sort_dir="desc",
        )
    except SlackApiError as e:
        error_code = e.response.get("error", "unknown")
        if error_code in ("invalid_auth", "not_authed", "token_revoked", "token_expired"):
            return {
                "error": "auth_failed",
                "formatted_text": (
                    f"_Slack authentication failed: `{error_code}`. "
                    "Ensure SLACK_USER_TOKEN is a user token (xoxp-...) with search:read scope._"
                ),
            }
        if error_code == "missing_scope":
            return {
                "error": "missing_scope",
                "formatted_text": (
                    "_Slack connector: token is missing the `search:read` scope, "
                    "or is a bot token (xoxb-) which cannot search messages. "
                    "Use a user token (xoxp-...) with search:read scope._"
                ),
            }
        return {
            "error": f"slack_api_{error_code}",
            "formatted_text": f"_Slack API error: `{error_code}`_",
        }
    except Exception as e:
        return {"error": "connection_failed", "formatted_text": f"_Slack connection error: {e}_"}

    matches = response.get("messages", {}).get("matches", [])
    if verbose:
        print(f"    [verbose] Slack: {len(matches)} messages found")

    if not matches:
        return {
            "messages": [],
            "formatted_text": f"_No Slack messages found mentioning '{company}'._",
        }

    messages = []
    for m in matches:
        ts_raw = m.get("ts", "0")
        try:
            from datetime import datetime
            ts_display = datetime.fromtimestamp(float(ts_raw)).strftime("%Y-%m-%d %H:%M")
        except (ValueError, OSError, OverflowError):
            ts_display = ts_raw

        channel_name = (m.get("channel") or {}).get("name", "unknown-channel")
        username = m.get("username") or m.get("user", "unknown")
        text = (m.get("text") or "").strip()
        if len(text) > 250:
            text = text[:247] + "..."
        permalink = m.get("permalink", "")

        messages.append({
            "timestamp": ts_display,
            "channel": channel_name,
            "username": username,
            "text": text,
            "permalink": permalink,
        })

    return {"messages": messages, "formatted_text": _format_slack(messages)}


def _format_slack(messages: list) -> str:
    lines = [f"### Slack Messages ({len(messages)} found, most recent first)"]
    for m in messages:
        lines.append(f"\n**[{m['timestamp']}] #{m['channel']} — {m['username']}**")
        lines.append(f"> {m['text']}")
        if m["permalink"]:
            lines.append(f"  [→ View in Slack]({m['permalink']})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Google (Gmail + Drive) Connector
# ---------------------------------------------------------------------------

def _extract_gmail_body(payload: dict, max_chars: int = 800) -> str:
    """
    Recursively extract plain-text body from a Gmail message payload.
    Falls back to HTML (stripped) if no plain-text part exists.
    Returns a truncated string, or empty string if nothing found.
    """
    import base64

    def decode(data: str) -> str:
        try:
            return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
        except Exception:
            return ""

    def strip_html(html: str) -> str:
        """Very lightweight HTML tag stripper."""
        return re.sub(r"<[^>]+>", " ", html)

    def walk(part: dict) -> str:
        mime = part.get("mimeType", "")
        body_data = part.get("body", {}).get("data", "")

        if mime == "text/plain" and body_data:
            return decode(body_data)
        if mime == "text/html" and body_data:
            return strip_html(decode(body_data))

        # Recurse into multipart
        for sub in part.get("parts", []):
            result = walk(sub)
            if result.strip():
                return result
        return ""

    text = walk(payload).strip()
    # Collapse whitespace runs and truncate
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    if len(text) > max_chars:
        text = text[:max_chars].rsplit(" ", 1)[0] + " …"
    return text


def pull_google(company: str, config: dict, verbose: bool) -> dict:
    """
    Returns dict with: emails, drive_files, formatted_text.
    Uses OAuth2 — opens browser on first run, caches token in .credentials/.
    """
    try:
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request as GoogleRequest
    except ImportError:
        return {
            "error": "library_not_installed",
            "formatted_text": (
                "_Google connector: required libraries not installed. Run: "
                "`pip install google-api-python-client google-auth-oauthlib google-auth-httplib2`_"
            ),
        }

    SCOPES = [
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    TOKEN_PATH = os.path.join(".credentials", "google_token.json")
    creds_file = config["google_credentials_file"]

    # Load or refresh credentials
    creds = None
    if os.path.exists(TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        except Exception:
            creds = None  # Corrupted token — will re-auth

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(GoogleRequest())
            except Exception:
                creds = None  # Refresh failed — need fresh auth

        if not creds:
            if not os.path.exists(creds_file):
                return {
                    "error": "credentials_file_missing",
                    "formatted_text": (
                        f"_Google connector: credentials file not found at `{creds_file}`. "
                        "Download `credentials.json` from Google Cloud Console "
                        "(APIs & Services → Credentials → Create OAuth 2.0 Client ID → Desktop app)._"
                    ),
                }
            try:
                print("  Google OAuth: opening browser for authorization (one-time only)...")
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
                creds = flow.run_local_server(port=0)
            except Exception as e:
                return {"error": "oauth_failed", "formatted_text": f"_Google OAuth failed: {e}_"}

        # Save token for future runs
        os.makedirs(".credentials", exist_ok=True)
        try:
            with open(TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
            if verbose:
                print(f"    [verbose] Google token saved to {TOKEN_PATH}")
        except Exception:
            pass  # Non-fatal

    try:
        gmail_svc = build("gmail", "v1", credentials=creds)
        drive_svc = build("drive", "v3", credentials=creds)
    except Exception as e:
        return {"error": "service_build_failed", "formatted_text": f"_Google service init failed: {e}_"}

    # Gmail search
    emails = []
    try:
        list_result = gmail_svc.users().messages().list(
            userId="me", q=f'"{company}"', maxResults=10
        ).execute()
        msg_ids = [m["id"] for m in list_result.get("messages", [])]

        if verbose:
            print(f"    [verbose] Gmail: {len(msg_ids)} messages found")

        for msg_id in msg_ids:
            try:
                msg = gmail_svc.users().messages().get(
                    userId="me",
                    id=msg_id,
                    format="full",
                ).execute()
                payload = msg.get("payload", {})
                headers = {
                    h["name"]: h["value"]
                    for h in payload.get("headers", [])
                }
                body = _extract_gmail_body(payload)
                emails.append({
                    "subject": headers.get("Subject", "(no subject)"),
                    "from": headers.get("From", "unknown"),
                    "date": headers.get("Date", ""),
                    "body": body,
                })
            except Exception:
                continue
    except Exception as e:
        if verbose:
            print(f"    [verbose] Gmail error: {e}")

    # Drive search
    drive_files = []
    try:
        drive_result = drive_svc.files().list(
            q=f'fullText contains "{company}"',
            fields="files(id,name,mimeType,webViewLink,modifiedTime)",
            pageSize=10,
            orderBy="modifiedTime desc",
        ).execute()
        drive_files = drive_result.get("files", [])

        if verbose:
            print(f"    [verbose] Drive: {len(drive_files)} files found")
    except Exception as e:
        if verbose:
            print(f"    [verbose] Drive error: {e}")

    # Filter out known false-positive files
    exclude_raw = config.get("drive_exclude_files", "") or ""
    exclude_names = [n.strip().lower() for n in exclude_raw.split(",") if n.strip()]
    if exclude_names:
        before = len(drive_files)
        drive_files = [f for f in drive_files if f.get("name", "").lower() not in exclude_names]
        if verbose and len(drive_files) < before:
            print(f"    [verbose] Drive: excluded {before - len(drive_files)} file(s) by DRIVE_EXCLUDE_FILES")

    if not emails and not drive_files:
        return {
            "emails": [],
            "drive_files": [],
            "formatted_text": f"_No Gmail messages or Drive files found mentioning '{company}'._",
        }

    return {
        "emails": emails,
        "drive_files": drive_files,
        "formatted_text": _format_google(emails, drive_files),
    }


def _format_google(emails: list, drive_files: list) -> str:
    lines = []

    lines.append(f"### Gmail ({len(emails)} emails found)")
    if emails:
        for e in emails:
            lines.append(f"\n**Subject:** {e['subject']}")
            lines.append(f"**From:** {e['from']}  |  **Date:** {e['date']}")
            body = (e.get("body") or "").strip()
            if body:
                lines.append(f"**Body:**\n> {body.replace(chr(10), chr(10) + '> ')}")
            else:
                lines.append("_Body not available._")
    else:
        lines.append("No emails found.")

    lines.append(f"\n### Google Drive ({len(drive_files)} files found)")
    if drive_files:
        for f in drive_files:
            mime_parts = (f.get("mimeType") or "").split(".")
            mime_short = mime_parts[-1] if mime_parts else "file"
            modified = (f.get("modifiedTime") or "")[:10]
            link = f.get("webViewLink", "")
            name = f.get("name", "Untitled")
            if link:
                lines.append(f"- [{name}]({link}) — `{mime_short}` — modified {modified}")
            else:
                lines.append(f"- {name} — `{mime_short}` — modified {modified}")
    else:
        lines.append("No Drive files found.")

    return "\n".join(lines)



# ---------------------------------------------------------------------------
# Claude Synthesis
# ---------------------------------------------------------------------------

def build_context_prompt(company: str, connector_data: dict) -> str:
    CONNECTOR_LABELS = {
        "salesforce": "SALESFORCE CRM DATA",
        "slack":      "SLACK MESSAGES",
        "google":     "GMAIL & GOOGLE DRIVE DATA",
    }

    data_block = ""
    for key, label in CONNECTOR_LABELS.items():
        data_block += f"\n{'=' * 60}\n{label}\n{'=' * 60}\n"
        if key in connector_data:
            data_block += connector_data[key].get("formatted_text", "[No data returned]")
        else:
            data_block += "[Connector not configured — skipped]"
        data_block += "\n"

    return f"""Below is internal account history for "{company}" pulled from connected internal systems. Using ONLY this information, write a structured Internal Account Context report for a sales representative preparing for outreach.

{data_block}

---

Write the report using EXACTLY this markdown structure. Ground every claim in the data above. If a section has no data (connector not configured or no results found), write a brief note — do not speculate or fabricate.

## CRM Summary (Salesforce)

Describe the account: industry, size, annual revenue, website, account owner. Then summarize the opportunity history: total number of deals, overall win/loss pattern, largest deal sizes, most recent deal activity, and current open opportunities if any. Note any deal velocity trends (e.g. "multiple stalled deals in Negotiation stage").

## Key Contacts

For each contact found in the CRM, list: name, title, department, email, and phone. Note the most senior contacts and flag anyone who appears in both CRM activities and Slack/email discussions.

## Internal Discussions (Slack)

Summarize the key themes from internal Slack messages mentioning this account. What has the team been discussing? Are there concerns, urgency signals, action items, or notable context? Quote specific phrases where helpful.

## Email History (Gmail)

Summarize the email threads found. Note the direction (inbound vs outbound where inferable from the From field), recency, and topic patterns. Flag any threads that suggest active negotiation, objections, or pending follow-ups.

## Relevant Documents (Google Drive)

List the documents found. For each: note the name, type, and last-modified date. Highlight any proposals, contracts, presentations, or meeting notes that would be useful context before reaching out.

## Sales Rep Briefing

Write 4–5 sentences synthesizing the full picture: (1) the overall relationship status and history — are we an active, dormant, or new prospect? (2) the most promising next action to take and the best angle to lead with, (3) who the best person to contact is and why, and (4) any known sensitivities, prior commitments, lost deal reasons, or things NOT to do based on the history above.

---

Do NOT invent or assume any data. Do NOT add a Sources section.
"""


def synthesize_with_claude(company: str, connector_data: dict, api_key: str, verbose: bool) -> str:
    print("  Calling Claude to synthesize internal context...")
    client = anthropic.Anthropic(api_key=api_key)
    prompt = build_context_prompt(company, connector_data)

    if verbose:
        print(f"  [verbose] Context prompt: {len(prompt):,} characters")

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=(
                "You are a senior sales analyst synthesizing internal account history for a B2B "
                "sales representative preparing for outreach. You write clear, professional, and "
                "actionable briefings grounded exclusively in the provided internal data. When a "
                "data source is absent or returned no results, you acknowledge that explicitly "
                "rather than speculating or filling gaps with assumptions."
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

def write_context_report(company: str, report_body: str, output_dir: str) -> str:
    slug = slugify(company)
    today = date.today().strftime("%Y-%m-%d")
    company_dir = os.path.join(output_dir, slug)
    os.makedirs(company_dir, exist_ok=True)
    filename = f"{today}_context.md"
    filepath = os.path.join(company_dir, filename)

    content = (
        f"# {company} — Internal Account Context\n\n"
        f"_Generated: {today}_\n\n"
        f"---\n\n"
        f"{report_body.strip()}\n"
    )

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


def _count_records(result: dict) -> str:
    """Return a human-readable summary of records retrieved."""
    parts = []
    for k, v in result.items():
        if isinstance(v, list) and k != "formatted_text":
            if v:
                parts.append(f"{len(v)} {k}")
    return ", ".join(parts) if parts else "data retrieved"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    config = load_config()

    company = args.company.strip()
    print(f"\nBuilding internal context: {company}")
    print("=" * 50)

    # -----------------------------------------------------------------
    # [1/3] Pull data from all configured connectors
    # -----------------------------------------------------------------
    print(f"\n[1/3] Pulling data from internal systems...")

    connector_data = {}
    for connector in CONNECTORS:
        key = connector["key"]
        label = connector["label"]

        if not is_connector_configured(connector, config):
            print(f"  SKIP  {label} (not configured)")
            continue

        print(f"  ···   {label}...")

        if key == "salesforce":
            result = pull_salesforce(company, config, args.verbose)
        elif key == "slack":
            result = pull_slack(company, config, args.verbose)
        elif key == "google":
            result = pull_google(company, config, args.verbose)
        else:
            continue

        if result.get("error"):
            print(f"  WARN  {label}: {result['error']}")
        else:
            summary = _count_records(result)
            print(f"  OK    {label}: {summary}")

        connector_data[key] = result

    if not connector_data:
        print(
            "\n  NOTE: No connectors configured — report will acknowledge all sources as unavailable.\n"
            "  Add credentials to .env to pull live data."
        )

    # -----------------------------------------------------------------
    # [2/3] Report data summary
    # -----------------------------------------------------------------
    print("\n[2/3] Formatting connector data...")
    active = len(connector_data)
    total = len(CONNECTORS)
    total_chars = sum(len(v.get("formatted_text", "")) for v in connector_data.values())
    print(f"  {active}/{total} connectors active | {total_chars:,} characters of context")

    # -----------------------------------------------------------------
    # [3/3] Synthesize with Claude and write report
    # -----------------------------------------------------------------
    print("\n[3/3] Synthesizing with Claude...")
    report_body = synthesize_with_claude(company, connector_data, config["anthropic_api_key"], args.verbose)

    filepath = write_context_report(company, report_body, args.output_dir)

    print(f"\nDone! Context report saved to: {filepath}")
    print("=" * 50)


if __name__ == "__main__":
    main()
