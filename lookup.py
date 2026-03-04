"""
lookup.py — Ad-hoc Internal Account Lookup

Searches all connected internal systems (Salesforce, Slack, Gmail, Google Drive)
for every reference to a company and synthesizes a concise summary using Claude.
Useful when you receive an email about an account you're unfamiliar with and need
to quickly understand past discussions, contacts, and open threads.

Usage:
    python lookup.py "Factory"
    python lookup.py "Factory" --domain factory.ai
    python lookup.py "Factory" --domain factory.ai --news
    python lookup.py "Acme Corp" --verbose

Requirements:
    Same connectors as context.py (.env credentials).
    Optional: YOUCOM_API_KEY for external news (--news flag).
"""

import os
import sys
import time
import argparse
from datetime import date

import requests
import anthropic
from dotenv import load_dotenv

# Reuse all connector functions from context.py — no duplication needed
from context import (
    pull_salesforce,
    pull_slack,
    pull_google,
    slugify,
    is_connector_configured,
    CONNECTORS,
)

# Reuse email functions from meeting_prep.py — no duplication needed
from meeting_prep import send_email, markdown_to_html


# ---------------------------------------------------------------------------
# CLI & Config
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Look up all internal references to an account and summarize.",
        epilog='Example: python lookup.py "Factory" --domain factory.ai --news',
    )
    parser.add_argument("company", help="Company name to look up")
    parser.add_argument(
        "--domain",
        default="",
        metavar="DOMAIN",
        help="Company domain (e.g. factory.ai) to disambiguate searches",
    )
    parser.add_argument(
        "--news",
        action="store_true",
        help="Also search You.com for recent external news (requires YOUCOM_API_KEY in .env)",
    )
    parser.add_argument(
        "--output-dir",
        default="reports",
        help="Base directory for reports (default: reports/). Output goes into a YYYY-MM-DD subfolder.",
    )
    parser.add_argument(
        "--email",
        action="store_true",
        help="Convert output to HTML and email it (requires SMTP_USER and SMTP_PASSWORD in .env)",
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
        "anthropic_api_key":       os.getenv("ANTHROPIC_API_KEY"),
        # Salesforce (optional)
        "sf_username":             os.getenv("SF_USERNAME"),
        "sf_password":             os.getenv("SF_PASSWORD"),
        "sf_security_token":       os.getenv("SF_SECURITY_TOKEN"),
        "sf_domain":               os.getenv("SF_DOMAIN", "login"),
        # Slack (optional)
        "slack_user_token":        os.getenv("SLACK_USER_TOKEN"),
        # Google (optional)
        "google_credentials_file": os.getenv("GOOGLE_CREDENTIALS_FILE"),
        # You.com (optional, --news only)
        "youcom_api_key":          os.getenv("YOUCOM_API_KEY"),
        # Email (optional, --email only)
        "smtp_host":               os.getenv("SMTP_HOST", "smtp.gmail.com"),
        "smtp_port":               int(os.getenv("SMTP_PORT", "587")),
        "smtp_user":               os.getenv("SMTP_USER", ""),
        "smtp_password":           os.getenv("SMTP_PASSWORD", ""),
        "email_to":                os.getenv("EMAIL_TO", ""),
    }
    if not config["anthropic_api_key"] or config["anthropic_api_key"].startswith("your_"):
        print("ERROR: Missing or placeholder ANTHROPIC_API_KEY in .env")
        sys.exit(1)
    return config


# ---------------------------------------------------------------------------
# You.com News Search (optional, --news flag)
# ---------------------------------------------------------------------------

def search_news(company: str, domain: str, youcom_key: str, verbose: bool) -> str:
    """Search You.com for recent news about the company. Returns formatted markdown string."""
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

    sections = []
    for q in queries:
        if verbose:
            print(f"    [verbose] You.com: {q['query']}")
        try:
            resp = requests.get(
                "https://api.you.com/v1/search",
                headers={"X-API-Key": youcom_key},
                params={"query": q["query"], "count": 5},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            hits = data.get("results", {}).get("web", [])
            if hits:
                snippets = []
                for h in hits:
                    title = h.get("title", "")
                    snippet = h.get("description", h.get("snippet", ""))
                    url = h.get("url", "")
                    if title or snippet:
                        line = f"- **{title}**: {snippet}"
                        if url:
                            line += f" [→]({url})"
                        snippets.append(line)
                if snippets:
                    sections.append(f"**{q['label']}**\n" + "\n".join(snippets))
            else:
                sections.append(f"**{q['label']}**: _No results found._")
        except Exception as e:
            if verbose:
                print(f"    [verbose] You.com error ({q['label']}): {e}")
            sections.append(f"**{q['label']}**: _Search error: {e}_")

    return "\n\n".join(sections) if sections else "_No news results returned._"


# ---------------------------------------------------------------------------
# LinkedIn Research (key contacts only)
# ---------------------------------------------------------------------------

# Title keywords that indicate a contact is senior / strategically relevant.
# Contacts matching at least one keyword are eligible for LinkedIn lookup.
SENIOR_TITLE_KEYWORDS = [
    "partner", "principal", "director", "managing director",
    "vp ", "vice president", "president",
    "ceo", "cto", "cfo", "coo", "cio", "ciso", "cmo", "cpo", "chief",
    "head of", " ai", "artificial intelligence", "machine learning",
    "data science", "analytics",
]


def get_key_crm_contacts(contacts: list, max_contacts: int = 8) -> list:
    """
    Filter CRM contacts to the most senior / AI-relevant ones.
    Scores each contact by how many SENIOR_TITLE_KEYWORDS appear in their title,
    returns up to max_contacts sorted by score descending.
    """
    scored = []
    for c in contacts:
        title = (c.get("Title") or "").lower()
        score = sum(1 for kw in SENIOR_TITLE_KEYWORDS if kw in title)
        if score > 0:
            scored.append((score, c))
    scored.sort(key=lambda x: -x[0])
    return [c for _, c in scored[:max_contacts]]


def find_linkedin_url(name: str, company: str, youcom_key: str, verbose: bool) -> str:
    """
    Search You.com for a person's LinkedIn profile.
    Returns the first linkedin.com/in/ URL found, or empty string.
    """
    query = f'"{name}" {company} LinkedIn profile'
    try:
        resp = requests.get(
            "https://api.you.com/v1/search",
            headers={"X-API-Key": youcom_key},
            params={"query": query, "count": 5},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        for hit in data.get("results", {}).get("web", []):
            url = hit.get("url", "")
            if "linkedin.com/in/" in url:
                if verbose:
                    print(f"    [verbose] LinkedIn found: {name} → {url}")
                return url
    except Exception as e:
        if verbose:
            print(f"    [verbose] LinkedIn search error for {name}: {e}")
    return ""


def research_key_contacts(
    contacts: list, company: str, youcom_key: str, verbose: bool
) -> dict:
    """
    Run LinkedIn lookups for key CRM contacts.
    Returns dict mapping {full_name: linkedin_url} for contacts where a URL was found.
    """
    key_contacts = get_key_crm_contacts(contacts)
    if not key_contacts:
        return {}

    print(f"  Researching LinkedIn for {len(key_contacts)} key contacts...")
    linkedin_urls = {}
    for i, c in enumerate(key_contacts):
        name = f"{c.get('FirstName', '')} {c.get('LastName', '')}".strip()
        if not name:
            continue
        url = find_linkedin_url(name, company, youcom_key, verbose)
        if url:
            linkedin_urls[name] = url
        if i < len(key_contacts) - 1:
            time.sleep(0.3)  # avoid hammering the API

    found = len(linkedin_urls)
    print(f"  OK    LinkedIn: {found}/{len(key_contacts)} profiles found")
    return linkedin_urls


# ---------------------------------------------------------------------------
# Claude Synthesis
# ---------------------------------------------------------------------------

def build_lookup_prompt(company: str, connector_data: dict, news_text: str, linkedin_urls: dict) -> str:
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

    if news_text:
        data_block += f"\n{'=' * 60}\nEXTERNAL NEWS (You.com)\n{'=' * 60}\n{news_text}\n"

    linkedin_block = ""
    if linkedin_urls:
        lines = ["The following LinkedIn profile URLs were found for key contacts."]
        lines.append("In the Key People section, hyperlink each person's name as [Name](url) wherever their URL appears below.")
        for name, url in linkedin_urls.items():
            lines.append(f"- {name}: {url}")
        linkedin_block = (
            f"\n{'=' * 60}\nLINKEDIN PROFILES\n{'=' * 60}\n"
            + "\n".join(lines) + "\n"
        )
        data_block += linkedin_block

    return f"""Below is all internal (and optionally external) data found for the account "{company}". Synthesize a concise account lookup summary for a sales representative who has little or no familiarity with this account.

{data_block}

---

Write the report using EXACTLY this markdown structure. Be specific and ground every claim in the data above. If a section has no supporting data, acknowledge it in one sentence — do not speculate or fabricate.

## Account Snapshot
Describe who this company is: industry, size, revenue, website, and current Salesforce account owner. State clearly whether this appears to be an active customer, an active prospect, a dormant account, or a net-new name based on the CRM data.

## Relationship History
Summarize the arc of our relationship with this account in **reverse chronological order (most recent first)**. Cover: key deals (won/lost/open with amounts and dates), notable meetings or events logged in CRM, relevant email threads, and Slack discussions. End with a one-sentence assessment of the trajectory — growing, stalled, or picking back up?

## Key People
List every person we have on file at this account: CRM contacts (name, title, email), plus anyone who appears in Slack threads or email. Flag the most senior contacts and note anyone who appears across multiple sources as particularly relevant.

## Open Threads & Unresolved Items
Call out anything that appears unresolved or pending: open opportunities, overdue tasks, unanswered emails, or Slack threads that trailed off without a clear outcome. These are the highest-priority items to address before reaching out.

## Key Themes
What topics, concerns, or interests come up repeatedly across Slack, email, CRM notes, and news? (e.g. pricing pressure, a specific product use case, a competitor mention, a champion who changed roles, interest in AI/data, etc.) Keep this to the 3–5 most prominent themes.

## Recommended Next Steps
3–5 specific, actionable steps for a rep picking this account up with fresh eyes. Name who to contact, what angle to lead with, and what to avoid based on the history. Be direct.

---

Do NOT invent or assume any data. Do NOT add a Sources section. If a connector returned no data, note it briefly in the relevant section and move on.
"""


def synthesize_with_claude(
    company: str,
    connector_data: dict,
    news_text: str,
    linkedin_urls: dict,
    api_key: str,
    verbose: bool,
) -> str:
    client = anthropic.Anthropic(api_key=api_key)
    prompt = build_lookup_prompt(company, connector_data, news_text, linkedin_urls)

    if verbose:
        print(f"    [verbose] Lookup prompt: {len(prompt):,} characters")

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=(
                "You are a senior sales analyst synthesizing internal account data for a B2B "
                "sales representative who is unfamiliar with an account. You write concise, "
                "actionable briefings grounded exclusively in the provided data. Acknowledge "
                "data gaps honestly rather than speculating or filling them with assumptions."
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

def write_report(company: str, report_body: str, output_dir: str) -> str:
    today = date.today().strftime("%Y-%m-%d")
    dated_dir = os.path.join(output_dir, today)
    os.makedirs(dated_dir, exist_ok=True)

    slug = slugify(company)
    filename = f"{slug}_lookup.md"
    filepath = os.path.join(dated_dir, filename)

    content = (
        f"# {company} — Account Lookup\n\n"
        f"_Generated: {today}_\n\n"
        f"---\n\n"
        f"{report_body.strip()}\n"
    )

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


def _count_records(result: dict) -> str:
    parts = []
    for k, v in result.items():
        if isinstance(v, list) and k not in ("formatted_text",):
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
    domain = args.domain.strip()

    print(f"\nAccount Lookup: {company}" + (f"  [{domain}]" if domain else ""))
    print("=" * 60)

    # -----------------------------------------------------------------
    # [1/3] Pull internal data from all configured connectors
    # -----------------------------------------------------------------
    print("\n[1/3] Searching internal systems...")

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
    # [2/4] LinkedIn research for key contacts
    # -----------------------------------------------------------------
    linkedin_urls = {}
    youcom_key = config.get("youcom_api_key", "")
    if youcom_key and not youcom_key.startswith("your_"):
        crm_contacts = connector_data.get("salesforce", {}).get("contacts", [])
        if crm_contacts:
            print("\n[2/4] Researching LinkedIn for key contacts...")
            linkedin_urls = research_key_contacts(crm_contacts, company, youcom_key, args.verbose)
        else:
            print("\n[2/4] Skipping LinkedIn research (no CRM contacts found)")
    else:
        print("\n[2/4] Skipping LinkedIn research (YOUCOM_API_KEY not configured)")

    # -----------------------------------------------------------------
    # [3/4] Optional: You.com news search
    # -----------------------------------------------------------------
    news_text = ""
    if args.news:
        print("\n[3/4] Searching external news (You.com)...")
        if youcom_key and not youcom_key.startswith("your_"):
            news_text = search_news(company, domain, youcom_key, args.verbose)
            print("  OK    News search complete")
        else:
            print("  SKIP  YOUCOM_API_KEY not configured in .env")
    else:
        print("\n[3/4] Skipping external news  (add --news to include)")

    # -----------------------------------------------------------------
    # [4/4] Synthesize with Claude and write report
    # -----------------------------------------------------------------
    total_chars = sum(len(v.get("formatted_text", "")) for v in connector_data.values())
    print(f"\n[4/4] Synthesizing with Claude ({total_chars:,} chars of context)...")

    report_body = synthesize_with_claude(
        company, connector_data, news_text, linkedin_urls, config["anthropic_api_key"], args.verbose
    )

    filepath = write_report(company, report_body, args.output_dir)

    print(f"\nDone! Report saved to: {filepath}")

    if args.email:
        print("\nSending email...")
        subject = f"Account Lookup: {company} — {date.today().strftime('%b %-d, %Y')}"
        with open(filepath, encoding="utf-8") as f:
            md_content = f.read()
        ok = send_email(md_content, subject, config, args.verbose)
        if ok:
            print(f"  Email sent to: {config.get('email_to') or config.get('sf_username')}")

    print("=" * 60)


if __name__ == "__main__":
    main()
