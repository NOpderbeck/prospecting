#!/usr/bin/env python3
"""
hygiene_check.py — Salesforce Pipeline Hygiene Check
Runs every Tuesday at 12pm PT via Cloud Scheduler → Cloud Run Job.

Checks:
  1. Overdue close dates  — open opps where CloseDate < TODAY
  2. Early stage, closing soon — 1-Discovery closing ≤14d, 2-Qualification closing ≤45d

Posts a formatted Slack message to #team-dynamite (C0AGPFVCSSX).
"""

import os
import sys
import argparse
import requests
from datetime import date
from simple_salesforce import Salesforce, SalesforceExpiredSession

# ── Config ────────────────────────────────────────────────────────────────────

SLACK_CHANNEL   = "C0AGPFVCSSX"   # #team-dynamite
SF_BASE_URL     = "https://ydc.lightning.force.com/lightning/r/Opportunity/{id}/view"

TEAM = {
    "005Vq000009j4ezIAA": ("Andrew Miller-McKeever", "U0A4M1BAR08"),
    "005fo000000d1nSAAQ": ("David Wacker",            "U08P2MM9H8B"),
    "005Vq000008PlLhIAK": ("Nick Opderbeck",          "U09SR4ENM3J"),
    "005Vq00000DH5AHIA1": ("Ryan Allred",             "U0AGX0V6MK4"),
    "005Vq00000CQghSIAT": ("Ryan Reed",               "U0AE1PYFWG7"),
}

OWNER_IDS_SQL = "('" + "', '".join(TEAM.keys()) + "')"

STAGE_1 = {"1 - Discovery"}
STAGE_2 = {"2 - Qualification"}

# ── Salesforce ────────────────────────────────────────────────────────────────

_sf_client = None

def _get_sf():
    global _sf_client
    if _sf_client is None:
        _sf_client = Salesforce(
            username=os.environ["SF_USERNAME"],
            password=os.environ["SF_PASSWORD"],
            security_token=os.environ["SF_SECURITY_TOKEN"],
        )
    return _sf_client

def sf_query(soql: str) -> list[dict]:
    try:
        return _get_sf().query_all(soql)["records"]
    except SalesforceExpiredSession:
        global _sf_client
        _sf_client = None
        return _get_sf().query_all(soql)["records"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def days_to(close_date: str) -> int:
    return (date.fromisoformat(close_date) - date.today()).days

def fmt_date(close_date: str) -> str:
    return date.fromisoformat(close_date).strftime("%b %-d, %Y")

def urgency_emoji(d: int) -> str:
    if d <= 7:  return " 🔴"
    if d <= 21: return " 🟠"
    return ""

def opp_link(opp: dict) -> str:
    url = SF_BASE_URL.format(id=opp["Id"])
    acct = opp["Account"]["Name"]
    return f"*<{url}|{acct}>*"

def owner_tag(opp: dict) -> str:
    _, slack_id = TEAM.get(opp["OwnerId"], ("Unknown", None))
    return f"<@{slack_id}>" if slack_id else "unknown"

def row_overdue(opp: dict) -> str:
    d = abs(days_to(opp["CloseDate"]))
    return (
        f"{opp_link(opp)} · {opp['StageName']} · "
        f"_due {fmt_date(opp['CloseDate'])} · {d}d overdue_ · {owner_tag(opp)}"
    )

def row_closing(opp: dict) -> str:
    d = days_to(opp["CloseDate"])
    return (
        f"{opp_link(opp)} · {opp['StageName']} · "
        f"_closes {date.fromisoformat(opp['CloseDate']).strftime('%b %-d')} ({d}d)_"
        f"{urgency_emoji(d)} · {owner_tag(opp)}"
    )


# ── Core logic ────────────────────────────────────────────────────────────────

def run_hygiene() -> str:
    today = date.today()

    # Query A: all overdue open opps
    overdue_raw = sf_query(f"""
        SELECT Id, Name, StageName, Amount, CloseDate, Account.Name, OwnerId
        FROM Opportunity
        WHERE OwnerId IN {OWNER_IDS_SQL}
        AND IsClosed = false
        AND CloseDate < TODAY
        ORDER BY CloseDate ASC
    """)

    # Query B: early stage open opps closing within 90 days
    early_raw = sf_query(f"""
        SELECT Id, Name, StageName, Amount, CloseDate, Account.Name, OwnerId
        FROM Opportunity
        WHERE OwnerId IN {OWNER_IDS_SQL}
        AND IsClosed = false
        AND CloseDate >= TODAY
        AND CloseDate <= NEXT_N_DAYS:90
        AND StageName IN ('1 - Discovery', '2 - Qualification')
        ORDER BY CloseDate ASC
    """)

    # Bucket 1: overdue (all)
    seen = set()
    overdue = []
    for o in overdue_raw:
        if o["Id"] not in seen:
            seen.add(o["Id"])
            overdue.append(o)

    # Bucket 2: early stage closing soon — Discovery ≤14d, Qualification ≤45d
    early = []
    for o in early_raw:
        if o["Id"] in seen:
            continue
        d = days_to(o["CloseDate"])
        if o["StageName"] in STAGE_1 and d <= 14:
            seen.add(o["Id"])
            early.append(o)
        elif o["StageName"] in STAGE_2 and d <= 45:
            seen.add(o["Id"])
            early.append(o)

    total = len(overdue) + len(early)
    today_str = today.strftime("%b %-d, %Y")

    # ── Build message ──────────────────────────────────────────────────────────
    lines = [
        f"🔍 *Pipeline Hygiene — Full Team · {today_str}*",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # Overdue section
    lines.append(f"🚨 *OVERDUE ({len(overdue)})*")
    if overdue:
        for o in overdue:
            lines.append(row_overdue(o))
    else:
        lines.append("✅ None")

    lines.append("")

    # Early stage section
    lines.append(f"⚠️ *EARLY STAGE, CLOSING SOON ({len(early)})*")
    if early:
        for o in early:
            lines.append(row_closing(o))
    else:
        lines.append("✅ None")

    lines.append("")
    lines.append(f"📊 *SUMMARY* · {len(overdue)} overdue · {len(early)} closing soon · {total} total opps flagged")

    return "\n".join(lines)


# ── Slack ─────────────────────────────────────────────────────────────────────

def post_to_slack(message: str, dry_run: bool = False) -> None:
    if dry_run:
        print("🧪 DRY RUN — not posted to Slack\n")
        print(message)
        return

    token = os.environ.get("SLACK_BOT_TOKEN", "")
    if not token:
        print("ERROR: SLACK_BOT_TOKEN not set — cannot post to Slack", file=sys.stderr)
        sys.exit(1)

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"channel": SLACK_CHANNEL, "text": message, "mrkdwn": True},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        print(f"ERROR: Slack API error — {data.get('error')}", file=sys.stderr)
        sys.exit(1)

    print(f"✅ Posted to #team-dynamite")


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Salesforce pipeline hygiene check")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print output to stdout without posting to Slack")
    args = parser.parse_args()

    print("Running hygiene check...")
    message = run_hygiene()
    post_to_slack(message, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
