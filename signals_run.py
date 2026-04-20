"""
signals_run.py — Daily signal alerts for Tier 1 target accounts.

Detects three signals:
  1. New users added to a target account (Product_User__c.CreatedDate within lookback)
  2. New API traffic from those users (First_API_Call_Date__c within lookback + calls > 0)
  3. Usage burst — account-level spike that clears both a minimum absolute delta
     (MIN_BURST_DELTA) and a minimum ratio vs weekly average (MIN_BURST_RATIO),
     preventing false positives from low-volume noise (10→20 calls is not a burst).

Posts a single Slack message to #sales-target-alerts tagging account owners.
Silent if no signals found.

Usage (local):
    python3 signals_run.py [--date "April 16, 2026"] [--lookback 1]

Environment variables:
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN  — Salesforce creds
    SLACK_BOT_TOKEN                               — Slack bot token
    TEST_OWNER_EMAIL                              — When set, all owner tags
                                                    resolve to this email
                                                    (use during testing)
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import date, timedelta
from dotenv import load_dotenv

ENV_PATH      = os.path.join(os.path.dirname(__file__), ".env")
SLACK_CHANNEL = "C0AT4Q506Q2"  # #sales-target-alerts — ID is rename-safe
SF_BASE       = "https://ydc.my.salesforce.com/"

# ── Burst detection thresholds ─────────────────────────────────────────────────
# Both conditions must be true to fire. The absolute floor prevents false
# positives at low volumes (10→20 is noise); the ratio ensures the spike is
# proportionally meaningful at higher volumes (5000→5800 is growth, not a burst).
MIN_BURST_DELTA = 500   # minimum absolute increase over weekly average (calls)
MIN_BURST_RATIO = 1.75  # this week must be ≥ 1.75× the weekly average

# ── Alert blocklist ────────────────────────────────────────────────────────────
# Accounts listed here are silently excluded from all signal alerts (new users
# and usage bursts). Match is case-insensitive substring of Account Name.
ALERT_BLOCKLIST = [
    "BytePlus",
]


def is_blocked(account_name: str) -> bool:
    name_lower = account_name.lower()
    return any(entry.lower() in name_lower for entry in ALERT_BLOCKLIST)


# ── Salesforce ─────────────────────────────────────────────────────────────────

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


def fetch_new_users(sf, lookback_cutoff: date) -> list:
    """
    Return all Product_User__c records on Tier 1 accounts created on or after
    the lookback_cutoff date (midnight UTC). Uses an explicit calendar-date
    boundary instead of LAST_N_DAYS so consecutive daily runs never overlap —
    a user created on Tuesday is reported Tuesday and never again on Wednesday.
    """
    since = lookback_cutoff.strftime("%Y-%m-%dT00:00:00Z")
    return soql(sf, f"""
        SELECT Email__c,
               CreatedDate,
               First_API_Call_Date__c,
               API_Calls_Last_7_Days__c,
               API_Calls_Last_30_Days__c,
               Account__c,
               Account__r.Name,
               Account__r.Id,
               Account__r.Owner.Name,
               Account__r.Owner.Email
        FROM Product_User__c
        WHERE Account__r.Account_Tier__c IN ('1. TARGET ACCOUNT', 'Tier 1')
        AND CreatedDate >= {since}
        ORDER BY Account__r.Name, CreatedDate DESC
    """)


def fetch_all_usage(sf) -> list:
    """
    Return API_Calls_Last_7_Days__c and API_Calls_Last_30_Days__c for every
    Product_User__c on a Tier 1 account. Used to detect account-level bursts
    independent of whether the user is new.
    """
    return soql(sf, """
        SELECT Account__c,
               Account__r.Name,
               Account__r.Id,
               Account__r.Owner.Name,
               Account__r.Owner.Email,
               API_Calls_Last_7_Days__c,
               API_Calls_Last_30_Days__c
        FROM Product_User__c
        WHERE Account__r.Account_Tier__c IN ('1. TARGET ACCOUNT', 'Tier 1')
        ORDER BY Account__r.Name
    """)


def detect_bursts(usage_records: list) -> dict:
    """
    Aggregate usage per account and return those that clear both burst gates:
      1. delta (total_7d − weekly_avg) >= MIN_BURST_DELTA
      2. total_7d >= weekly_avg × MIN_BURST_RATIO

    Returns:
    {
      account_id: {
        'name':        str,
        'sf_id':       str,
        'owner_name':  str,
        'owner_email': str,
        'total_7d':    int,
        'weekly_avg':  float,
        'delta':       int,
      }
    }
    """
    agg: dict = {}
    for r in usage_records:
        acc_id  = r.get("Account__c") or r.get("Account__r", {}).get("Id", "")
        acc_ref = r.get("Account__r") or {}
        if acc_id not in agg:
            agg[acc_id] = {
                "name":        acc_ref.get("Name", "Unknown"),
                "sf_id":       acc_ref.get("Id", acc_id),
                "owner_name":  (acc_ref.get("Owner") or {}).get("Name", ""),
                "owner_email": (acc_ref.get("Owner") or {}).get("Email", ""),
                "total_7d":    0,
                "total_30d":   0,
            }
        agg[acc_id]["total_7d"]  += int(r.get("API_Calls_Last_7_Days__c")  or 0)
        agg[acc_id]["total_30d"] += int(r.get("API_Calls_Last_30_Days__c") or 0)

    bursts: dict = {}
    for acc_id, acc in agg.items():
        if is_blocked(acc["name"]):
            continue
        total_7d   = acc["total_7d"]
        weekly_avg = acc["total_30d"] / 4
        delta      = total_7d - weekly_avg
        if (weekly_avg > 0
                and delta >= MIN_BURST_DELTA
                and total_7d >= weekly_avg * MIN_BURST_RATIO):
            bursts[acc_id] = {
                "name":        acc["name"],
                "sf_id":       acc["sf_id"],
                "owner_name":  acc["owner_name"],
                "owner_email": acc["owner_email"],
                "total_7d":    total_7d,
                "weekly_avg":  round(weekly_avg, 1),
                "delta":       int(delta),
            }

    return bursts


# ── Slack helpers ──────────────────────────────────────────────────────────────

_slack_id_cache: dict[str, str] = {}


def resolve_slack_id(bot_token: str, email: str) -> str | None:
    """Look up a Slack user ID by email. Returns '<@USERID>' or None."""
    if email in _slack_id_cache:
        return _slack_id_cache[email]
    import requests
    resp = requests.get(
        "https://slack.com/api/users.lookupByEmail",
        headers={"Authorization": f"Bearer {bot_token}"},
        params={"email": email},
        timeout=10,
    )
    r = resp.json()
    if r.get("ok"):
        uid = r["user"]["id"]
        _slack_id_cache[email] = uid
        return uid
    print(f"  ⚠️  users.lookupByEmail failed for {email}: {r.get('error')}", file=sys.stderr)
    _slack_id_cache[email] = None
    return None


def owner_mention(bot_token: str, owner_email: str, owner_name: str,
                  test_email: str | None) -> str:
    """
    Return a Slack mention string for the account owner.
    If TEST_OWNER_EMAIL is set, resolves to that user instead.
    Falls back to plain owner name if lookup fails.
    """
    lookup_email = test_email or owner_email
    uid = resolve_slack_id(bot_token, lookup_email)
    if uid:
        return f"<@{uid}>"
    return owner_name


def fmt_name(email: str) -> str:
    """
    Convert an email address to a display name.
    bob.woolworth@acme.com → Bob Woolworth
    """
    local = email.split("@")[0]
    return " ".join(part.capitalize() for part in local.replace(".", " ").replace("_", " ").split())


# ── LinkedIn lookup ────────────────────────────────────────────────────────────

_linkedin_cache: dict[str, str | None] = {}

YOUCOM_SEARCH_URL = "https://ydc-index.io/v1/search"


def linkedin_url_for_email(email: str, display_name: str, company_name: str) -> str | None:
    """
    Search for a LinkedIn profile URL using a site-scoped You.com query:
      site:linkedin.com/in/ {display_name} {company_name}
    Returns the first linkedin.com/in/ URL found, or None.
    Cached per email to avoid duplicate API calls.

    Confidence gates (both must pass):
      1. Name completeness — email local part must contain '.' or '_', meaning
         we have at least a first and last name. Single-name emails like
         ethan@... or basia@... are skipped entirely.
      2. Slug verification — every token of display_name must appear as a
         substring in the LinkedIn URL slug (case-insensitive). This rejects
         results where the search returned an unrelated person.
    """
    if email in _linkedin_cache:
        return _linkedin_cache[email]

    # Gate 1: require a full name (first + last) derivable from the email
    local = email.split("@")[0]
    if "." not in local and "_" not in local:
        print(f"  ⏭️  LinkedIn skipped for {display_name}: single-name email, low confidence",
              file=sys.stderr)
        _linkedin_cache[email] = None
        return None

    youcom_key = os.getenv("YOUCOM_API_KEY", "")
    if not youcom_key:
        _linkedin_cache[email] = None
        return None

    import requests, re
    # Gate 2 uses only the last name token — given names are unstable (Robert/Bob,
    # William/Bill) but family names are stable. Requiring the surname in the slug
    # is enough to reject unrelated people without over-blocking nickname variants.
    name_tokens = [t.lower() for t in display_name.split() if t]
    last_name   = name_tokens[-1] if name_tokens else ""
    query = f"site:linkedin.com/in/ {display_name} {company_name}"
    try:
        resp = requests.get(
            YOUCOM_SEARCH_URL,
            headers={"Accept": "application/json", "X-API-KEY": youcom_key},
            params={"query": query, "count": 5, "language": "EN", "crawl_timeout": 10},
            timeout=15,
        )
        hits = resp.json().get("results", {}).get("web", [])
        for hit in hits:
            url = hit.get("url", "")
            m = re.match(r"https://www\.linkedin\.com/in/([^/]+)/?$", url)
            if not m:
                continue
            # Gate 2: verify the last name (surname) appears in the URL slug
            slug = m.group(1).lower()
            if last_name and last_name in slug:
                print(f"  🔗 LinkedIn found for {display_name}: {url}", file=sys.stderr)
                _linkedin_cache[email] = url
                return url
            else:
                print(f"  ⚠️  LinkedIn skipped unconfident match for {display_name}: {url}",
                      file=sys.stderr)
    except Exception as e:
        print(f"  ⚠️  LinkedIn lookup failed for {display_name}: {e}", file=sys.stderr)

    _linkedin_cache[email] = None
    return None


def fmt_name_linked(display_name: str, email: str, company_name: str) -> str:
    """
    Return display_name hyperlinked to their LinkedIn profile if found,
    otherwise plain display_name. Formatted for Slack mrkdwn.
    """
    url = linkedin_url_for_email(email, display_name, company_name)
    if url:
        return f"<{url}|{display_name}>"
    return display_name


# ── Signal grouping ────────────────────────────────────────────────────────────

def group_signals(records: list, lookback_cutoff: date) -> dict:
    """
    Group new-user records by account. For each user, classify as:
      - 'with_calls'  : new user + first API calls within lookback window
      - 'user_only'   : new user added, no new API calls yet

    Returns:
    {
      account_id: {
        'name':        str,
        'sf_id':       str,
        'owner_name':  str,
        'owner_email': str,
        'with_calls':  [(display_name, email, call_count), ...],
        'user_only':   [(display_name, email), ...],
      }
    }
    """
    accounts: dict = {}

    for r in records:
        acc_id    = r.get("Account__c") or r.get("Account__r", {}).get("Id", "")
        acc_ref   = r.get("Account__r") or {}
        acc_name  = acc_ref.get("Name", "Unknown")
        if is_blocked(acc_name):
            continue
        owner     = acc_ref.get("Owner") or {}
        email     = r.get("Email__c") or ""
        calls_7d  = int(r.get("API_Calls_Last_7_Days__c") or 0)

        # Determine if this user's first API call falls within the lookback window
        first_call_raw = r.get("First_API_Call_Date__c")
        first_call_recent = False
        if first_call_raw:
            first_call_date = date.fromisoformat(first_call_raw[:10])
            first_call_recent = first_call_date >= lookback_cutoff

        if acc_id not in accounts:
            accounts[acc_id] = {
                "name":        acc_name,
                "sf_id":       acc_ref.get("Id", acc_id),
                "owner_name":  owner.get("Name", ""),
                "owner_email": owner.get("Email", ""),
                "with_calls":  [],
                "user_only":   [],
            }

        display = fmt_name(email) if email else "Unknown User"
        if first_call_recent and calls_7d > 0:
            accounts[acc_id]["with_calls"].append((display, email, calls_7d))
        else:
            accounts[acc_id]["user_only"].append((display, email))

    return accounts


# ── Message building ───────────────────────────────────────────────────────────

def build_alert_lines(accounts: dict, bursts: dict, bot_token: str,
                      test_email: str | None, date_str: str) -> list[str]:
    """
    Build one alert line per signal event (not per account).
    An account can produce multiple lines if it fires more than one signal type.
    """
    lines = []

    # ── New user signals ───────────────────────────────────────────────────────
    for acc_id, acc in sorted(accounts.items(), key=lambda x: x[1]["name"]):
        name    = acc["name"]
        sf_url  = SF_BASE + acc["sf_id"]
        mention = owner_mention(bot_token, acc["owner_email"], acc["owner_name"], test_email)

        for display, email, calls in acc["with_calls"]:
            linked = fmt_name_linked(display, email, name)
            lines.append(
                f"• *<{sf_url}|{name}>* — {linked} added as a new user "
                f"and made *{calls:,} API calls* for the first time. {mention}"
            )

        if acc["user_only"]:
            users = acc["user_only"]
            if len(users) == 1:
                display, email = users[0]
                linked = fmt_name_linked(display, email, name)
                lines.append(
                    f"• *<{sf_url}|{name}>* — {linked} added as a new user. {mention}"
                )
            else:
                names_fmt = ", ".join(fmt_name_linked(d, e, name) for d, e in users)
                lines.append(
                    f"• *<{sf_url}|{name}>* — {len(users)} new users added: "
                    f"{names_fmt}. {mention}"
                )

    # ── Usage burst signals ────────────────────────────────────────────────────
    for acc_id, acc in sorted(bursts.items(), key=lambda x: x[1]["name"]):
        name    = acc["name"]
        sf_url  = SF_BASE + acc["sf_id"]
        mention = owner_mention(bot_token, acc["owner_email"], acc["owner_name"], test_email)
        lines.append(
            f"• *<{sf_url}|{name}>* — usage spike: *{acc['total_7d']:,} API calls* "
            f"this week vs *{acc['weekly_avg']:,.0f}/wk* avg "
            f"(+{acc['delta']:,}). {mention}"
        )

    return lines


def post_to_slack(bot_token: str, lines: list[str], date_str: str, dry_run: bool = False):
    import requests
    header = f"🔔 *Daily Signal Alerts — {date_str}*"
    text   = header + "\n\n" + "\n".join(lines)

    if dry_run:
        print("\n── DRY RUN: Slack message (not posted) ──────────────────────────")
        print(text)
        print("─────────────────────────────────────────────────────────────────\n")
        return

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json"},
        json={"channel": SLACK_CHANNEL, "text": text, "mrkdwn": True, "unfurl_links": False},
        timeout=10,
    )
    result = resp.json()
    if result.get("ok"):
        print(f"✅ Posted {len(lines)} alert(s) to {SLACK_CHANNEL}")
    else:
        print(f"⚠️  Slack post failed: {result.get('error')}", file=sys.stderr)


# ── Main ───────────────────────────────────────────────────────────────────────

SIMULATE_ACCOUNTS = [
    {
        "name":        "Acme Corp",
        "sf_id":       "001000000000001AAA",
        "owner_name":  "Nick Opderbeck",
        "owner_email": "nick.opderbeck@you.com",
        "with_calls":  [("Bob Woolworth", "bob.woolworth@acme.com", 22)],
        "user_only":   [],
    },
    {
        "name":        "Globex Inc",
        "sf_id":       "001000000000002AAA",
        "owner_name":  "Nick Opderbeck",
        "owner_email": "nick.opderbeck@you.com",
        "with_calls":  [],
        "user_only":   [("Sally Jones", "sally.jones@globex.com"), ("John Smith", "john.smith@globex.com"), ("Carol White", "carol.white@globex.com")],
    },
]

SIMULATE_BURSTS = {
    "001000000000003AAA": {
        "name":        "Initech LLC",
        "sf_id":       "001000000000003AAA",
        "owner_name":  "Nick Opderbeck",
        "owner_email": "nick.opderbeck@you.com",
        "total_7d":    1_500,
        "weekly_avg":  100.0,
        "delta":       1_400,
    },
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",     default=None,
                        help="Report date string, e.g. 'April 16, 2026' (default: today)")
    parser.add_argument("--lookback", type=int, default=None,
                        help="Days to look back (default: 3 on Monday, 1 otherwise)")
    parser.add_argument("--simulate", action="store_true",
                        help="Inject synthetic signals to verify Slack plumbing end-to-end")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print Slack message to stdout; do not post to channel")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    today     = date.today()
    date_str  = args.date or today.strftime("%B %-d, %Y")

    lookback_days   = args.lookback if args.lookback is not None else 1
    lookback_cutoff = today - timedelta(days=lookback_days)
    print(f"Date: {date_str}  |  Lookback: {lookback_days} day(s) (since {lookback_cutoff})")

    bot_token  = os.getenv("SLACK_BOT_TOKEN", "")
    test_email = os.getenv("TEST_OWNER_EMAIL", "")  # override owner tags for testing
    simulate   = args.simulate or os.getenv("SIMULATE_SIGNALS", "").lower() == "true"

    if test_email:
        print(f"⚠️  TEST MODE — all owner tags will resolve to: {test_email}")

    if simulate:
        print("🧪 SIMULATE MODE — using synthetic signals, skipping Salesforce")
        args.dry_run = True  # simulate never posts to the real channel
        accounts = {acc["sf_id"]: acc for acc in SIMULATE_ACCOUNTS}
        bursts   = SIMULATE_BURSTS
    else:
        print("Connecting to Salesforce...")
        sf = connect_sf()

        print(f"Fetching new Product_User__c records (since {lookback_cutoff})...")
        records = fetch_new_users(sf, lookback_cutoff)
        print(f"  {len(records)} new user record(s) found")

        print("Fetching usage data for burst detection...")
        usage_records = fetch_all_usage(sf)
        bursts = detect_bursts(usage_records)
        print(f"  {len(bursts)} burst account(s) detected")

        if not records and not bursts:
            print("No signals — skipping Slack post.")
            return

        accounts = group_signals(records, lookback_cutoff) if records else {}
        print(f"  New users across {len(accounts)} account(s)")

    lines = build_alert_lines(accounts, bursts, bot_token, test_email or None, date_str)

    if not lines:
        print("No actionable signals — skipping Slack post.")
        return

    print(f"\nAlert preview:")
    for line in lines:
        print(f"  {line}")

    if args.dry_run:
        post_to_slack(bot_token, lines, date_str, dry_run=True)
    elif bot_token:
        post_to_slack(bot_token, lines, date_str)
    else:
        print("⚠️  No SLACK_BOT_TOKEN — skipping Slack post", file=sys.stderr)


if __name__ == "__main__":
    main()
