"""
signals_run.py — Daily signal alerts for Tier 1 target accounts.

Detects two signals:
  1. New users added to a target account (Product_User__c.CreatedDate within lookback)
  2. New API traffic from those users (First_API_Call_Date__c within lookback + calls > 0)

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

ENV_PATH     = os.path.join(os.path.dirname(__file__), ".env")
SLACK_CHANNEL = "#sales-target-alerts"
SF_BASE       = "https://ydc.my.salesforce.com/"


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


def fetch_new_users(sf, lookback: int) -> list:
    """
    Return all Product_User__c records on Tier 1 accounts created within
    the lookback window. Includes usage signals to detect first-ever API calls.
    """
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
        AND CreatedDate >= LAST_N_DAYS:{lookback}
        ORDER BY Account__r.Name, CreatedDate DESC
    """)


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
        'with_calls':  [(display_name, call_count), ...],
        'user_only':   [display_name, ...],
      }
    }
    """
    accounts: dict = {}

    for r in records:
        acc_id    = r.get("Account__c") or r.get("Account__r", {}).get("Id", "")
        acc_ref   = r.get("Account__r") or {}
        acc_name  = acc_ref.get("Name", "Unknown")
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
            accounts[acc_id]["with_calls"].append((display, calls_7d))
        else:
            accounts[acc_id]["user_only"].append(display)

    return accounts


# ── Message building ───────────────────────────────────────────────────────────

def build_alert_lines(accounts: dict, bot_token: str, test_email: str | None,
                      date_str: str) -> list[str]:
    """
    Build one alert line per signal event (not per account).
    An account can produce multiple lines if it has both types of signals.
    """
    lines = []

    for acc_id, acc in sorted(accounts.items(), key=lambda x: x[1]["name"]):
        name    = acc["name"]
        sf_url  = SF_BASE + acc["sf_id"]
        mention = owner_mention(bot_token, acc["owner_email"], acc["owner_name"], test_email)

        # ── New user(s) with first API calls ──────────────────────────────────
        for display, calls in acc["with_calls"]:
            calls_fmt = f"{calls:,}"
            lines.append(
                f"• *<{sf_url}|{name}>* — `{display}` added as a new user "
                f"and made *{calls_fmt} API calls* for the first time. {mention}"
            )

        # ── New user(s) added, no calls yet ───────────────────────────────────
        if acc["user_only"]:
            users = acc["user_only"]
            if len(users) == 1:
                lines.append(
                    f"• *<{sf_url}|{name}>* — `{users[0]}` added as a new user. {mention}"
                )
            else:
                names_fmt = ", ".join(f"`{u}`" for u in users)
                lines.append(
                    f"• *<{sf_url}|{name}>* — {len(users)} new users added: {names_fmt}. {mention}"
                )

    return lines


def post_to_slack(bot_token: str, lines: list[str], date_str: str):
    import requests
    header = f"🔔 *Daily Signal Alerts — {date_str}*"
    text   = header + "\n\n" + "\n".join(lines)

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
        "with_calls":  [("Bob Woolworth", 22)],
        "user_only":   [],
    },
    {
        "name":        "Globex Inc",
        "sf_id":       "001000000000002AAA",
        "owner_name":  "Nick Opderbeck",
        "owner_email": "nick.opderbeck@you.com",
        "with_calls":  [],
        "user_only":   ["Sally Jones", "John Smith", "Carol White"],
    },
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",     default=None,
                        help="Report date string, e.g. 'April 16, 2026' (default: today)")
    parser.add_argument("--lookback", type=int, default=None,
                        help="Days to look back (default: 3 on Monday, 1 otherwise)")
    parser.add_argument("--simulate", action="store_true",
                        help="Inject synthetic signals to verify Slack plumbing end-to-end")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    today     = date.today()
    date_str  = args.date or today.strftime("%B %-d, %Y")

    # Monday (weekday 0) looks back 3 days to catch Fri/Sat/Sun signals
    if args.lookback is not None:
        lookback = args.lookback
    else:
        lookback = 3 if today.weekday() == 0 else 1

    lookback_cutoff = today - timedelta(days=lookback)
    print(f"Date: {date_str}  |  Lookback: {lookback} day(s) (since {lookback_cutoff})")

    bot_token  = os.getenv("SLACK_BOT_TOKEN", "")
    test_email = os.getenv("TEST_OWNER_EMAIL", "")  # override owner tags for testing
    simulate   = args.simulate or os.getenv("SIMULATE_SIGNALS", "").lower() == "true"

    if test_email:
        print(f"⚠️  TEST MODE — all owner tags will resolve to: {test_email}")

    if simulate:
        print("🧪 SIMULATE MODE — using synthetic signals, skipping Salesforce")
        accounts = {acc["sf_id"]: acc for acc in SIMULATE_ACCOUNTS}
    else:
        print("Connecting to Salesforce...")
        sf = connect_sf()

        print(f"Fetching new Product_User__c records (last {lookback} day(s))...")
        records = fetch_new_users(sf, lookback)
        print(f"  {len(records)} new user record(s) found")

        if not records:
            print("No signals — skipping Slack post.")
            return

        accounts = group_signals(records, lookback_cutoff)
        print(f"  Across {len(accounts)} account(s)")

    lines = build_alert_lines(accounts, bot_token, test_email or None, date_str)

    if not lines:
        print("No actionable signals — skipping Slack post.")
        return

    print(f"\nAlert preview:")
    for line in lines:
        print(f"  {line}")

    if bot_token:
        post_to_slack(bot_token, lines, date_str)
    else:
        print("⚠️  No SLACK_BOT_TOKEN — skipping Slack post", file=sys.stderr)


if __name__ == "__main__":
    main()
