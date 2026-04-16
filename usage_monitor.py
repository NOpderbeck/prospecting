#!/usr/bin/env python3
"""
usage_monitor.py — Daily API usage signal detection across all priority accounts.

Queries all Tier 1 and Tier 2.A accounts org-wide, detects six categories of
usage signal (new activity, growth, multi-threading, expansion, sales gaps, risk),
groups results by account owner, and sends each rep a Slack DM with only their
flagged accounts.

Usage:
    python usage_monitor.py                        # Full org scan → Slack DMs
    python usage_monitor.py --dry-run              # Print to console, no Slack
    python usage_monitor.py --owner sarah@you.com  # Single rep only
    python usage_monitor.py --account "Toggle AI"  # Named account(s), comma-separated
"""

import os
import sys
import json
import subprocess
import argparse
import logging
import requests
from datetime import date, timedelta
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv


# ── Config ─────────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
ENV_PATH = SCRIPT_DIR / ".env"
REPORTS_DIR = SCRIPT_DIR / "reports"
SF_ORG = "ydc"
TODAY = date.today()
TODAY_STR = TODAY.isoformat()

# Signal thresholds (mirrors ydc-usage-monitor SKILL.md)
GROWTH_SPIKE_MULT    = 1.5       # total_7d > weekly_avg × 1.5
GROWTH_NOTABLE_CALLS = 10_000   # total_30d > 10,000
SALES_GAP_MIN        = 100      # total_30d > 100
RISK_DROP_MULT       = 0.5      # total_7d < weekly_avg × 0.5
RISK_DROP_MIN_30D    = 500      # AND total_30d > 500
RISK_SINGLE_MIN      = 1_000   # active_users_30d == 1 AND total_30d > 1,000
RISK_STALLED_MAX_7D  = 100      # new_users_30d > 0 AND total_7d < 100
RISK_STALLED_MAX_30D = 500      # AND total_30d < 500
EXPANSION_MIN_USERS  = 3        # active_users_30d >= 3
EXPANSION_ON_ACTIVE  = 1_000   # new_users_30d > 0 AND total_30d > 1,000
MAX_ACCOUNTS         = 500      # Safety cap on org-wide scan

# Accounts to permanently exclude from signal detection (e.g. known false positives)
EXCLUDED_ACCOUNTS: set = {
    "BytePlus",
}

TIER_VALUES = ("1. TARGET ACCOUNT", "2.A", "Tier 1", "Tier 2")
BUCKET_ORDER = ["🔴 ACT NOW", "🟠 ACT THIS WEEK", "🟡 EXPAND", "🔵 MONITOR", "⚫ NO SIGNAL"]
BUCKET_LABELS = {
    "🔴 ACT NOW":        "🔴 ACT NOW",
    "🟠 ACT THIS WEEK":  "🟠 ACT THIS WEEK",
    "🟡 EXPAND":         "🟡 EXPAND",
    "🔵 MONITOR":        "🔵 MONITOR",
    "⚫ NO SIGNAL":      "⚫ NO SIGNAL",
}


# ── Salesforce helpers ─────────────────────────────────────────────────────────

def soql(query: str) -> list[dict]:
    """Run a SOQL query via sf CLI and return records."""
    result = subprocess.run(
        ["sf", "data", "query", "--target-org", SF_ORG, "--json", "-q", query],
        capture_output=True, text=True
    )
    try:
        data = json.loads(result.stdout)
        return data.get("result", {}).get("records", [])
    except Exception as e:
        logging.warning(f"SOQL parse error: {e}")
        return []


def soql_batched(template: str, ids: list[str], batch_size: int = 200) -> list[dict]:
    """Run a SOQL query with an IN clause, batching large ID lists."""
    all_records = []
    for i in range(0, len(ids), batch_size):
        batch = ids[i:i + batch_size]
        id_list = "', '".join(batch)
        all_records.extend(soql(template.replace("{ID_LIST}", id_list)))
    return all_records


# ── Step 1: Fetch accounts ─────────────────────────────────────────────────────

def fetch_accounts(owner_email: str = None, account_names: list[str] = None) -> list[dict]:
    """Fetch priority accounts. Filters by owner email or account names if provided."""
    if account_names:
        all_accounts = []
        for name in account_names:
            safe = name.replace("'", "\\'")
            records = soql(f"""
                SELECT Id, Name, Account_Tier__c, Account_Score__c,
                       Total_Revenue_Closed_Won__c, Count_of_Open_Opportunities__c,
                       OwnerId, Owner.Name, Owner.Email
                FROM Account
                WHERE Name LIKE '%{safe}%'
                LIMIT 5
            """)
            all_accounts.extend(records)
        return all_accounts

    owner_clause = f"AND Owner.Email = '{owner_email}'" if owner_email else ""
    return soql(f"""
        SELECT Id, Name, Account_Tier__c, Account_Score__c,
               Total_Revenue_Closed_Won__c, Count_of_Open_Opportunities__c,
               OwnerId, Owner.Name, Owner.Email
        FROM Account
        WHERE Account_Tier__c IN ('1. TARGET ACCOUNT', '2.A', 'Tier 1', 'Tier 2')
        {owner_clause}
        ORDER BY Account_Score__c DESC NULLS LAST
        LIMIT {MAX_ACCOUNTS}
    """)


# ── Step 2: Bulk usage + activity fetch ────────────────────────────────────────

def fetch_usage_bulk(account_ids: list[str]) -> dict[str, list[dict]]:
    """Return Product_User__c records grouped by account ID."""
    records = soql_batched("""
        SELECT Account__c, Email__c,
               API_Calls_Last_7_Days__c, API_Calls_Last_30_Days__c,
               API_Calls_per_User_All_Time__c,
               First_API_Call_Date__c, Last_API_Call_Date__c
        FROM Product_User__c
        WHERE Account__c IN ('{ID_LIST}')
        ORDER BY API_Calls_Last_30_Days__c DESC NULLS LAST
    """, account_ids)

    by_account = defaultdict(list)
    for r in records:
        by_account[r["Account__c"]].append(r)
    return by_account


def fetch_activity_bulk(account_ids: list[str]) -> set[str]:
    """Return set of account IDs that have had a Task in the last 30 days."""
    records = soql_batched("""
        SELECT AccountId FROM Task
        WHERE AccountId IN ('{ID_LIST}')
        AND ActivityDate >= LAST_N_DAYS:30
    """, account_ids)
    return {r["AccountId"] for r in records}


def fetch_recently_closed_bulk(account_ids: list[str]) -> set[str]:
    """Return set of account IDs with a Closed Won opp in the last 60 days.
    Used to suppress signals that are normal for brand-new customers."""
    records = soql_batched("""
        SELECT AccountId FROM Opportunity
        WHERE AccountId IN ('{ID_LIST}')
        AND StageName = 'Closed Won'
        AND CloseDate >= LAST_N_DAYS:60
    """, account_ids)
    return {r["AccountId"] for r in records}


# ── Step 3: Compute per-account metrics ────────────────────────────────────────

def compute_metrics(users: list[dict]):
    if not users:
        return None

    def val(r, key):
        return r.get(key) or 0

    total_7d      = sum(val(r, "API_Calls_Last_7_Days__c") for r in users)
    total_30d     = sum(val(r, "API_Calls_Last_30_Days__c") for r in users)
    total_alltime = sum(val(r, "API_Calls_per_User_All_Time__c") for r in users)
    weekly_avg    = total_30d / 4

    active_users_30d = sum(1 for r in users if val(r, "API_Calls_Last_30_Days__c") > 0)
    active_emails    = [r["Email__c"] for r in users if val(r, "API_Calls_Last_30_Days__c") > 0 and r.get("Email__c")]

    first_dates = [r["First_API_Call_Date__c"][:10] for r in users if r.get("First_API_Call_Date__c")]
    last_dates  = [r["Last_API_Call_Date__c"][:10]  for r in users if r.get("Last_API_Call_Date__c")]

    first_call_ever = min(first_dates) if first_dates else None
    last_call_date  = max(last_dates)  if last_dates  else None

    days_dark = (TODAY - date.fromisoformat(last_call_date)).days if last_call_date else None

    cutoff_30d   = (TODAY - timedelta(days=30)).isoformat()
    new_users_30d = sum(1 for r in users if r.get("First_API_Call_Date__c") and r["First_API_Call_Date__c"][:10] >= cutoff_30d)

    return {
        "total_7d":        total_7d,
        "total_30d":       total_30d,
        "total_alltime":   total_alltime,
        "weekly_avg":      weekly_avg,
        "active_users_30d": active_users_30d,
        "active_emails":   active_emails,
        "first_call_ever": first_call_ever,
        "last_call_date":  last_call_date,
        "days_dark":       days_dark,
        "new_users_30d":   new_users_30d,
    }


# ── Step 4: Signal detection ───────────────────────────────────────────────────

def detect_signals(m: dict, account: dict, has_recent_activity: bool, recently_closed: bool = False) -> list[dict]:
    signals = []
    t7   = m["total_7d"]
    t30  = m["total_30d"]
    avg  = m["weekly_avg"]
    au   = m["active_users_30d"]
    nu   = m["new_users_30d"]
    dark = m["days_dark"]
    first = m["first_call_ever"]
    cutoff = (TODAY - timedelta(days=30)).isoformat()

    def sig(stype, sub, detail, action):
        signals.append({"type": stype, "sub": sub, "detail": detail, "action": action})

    # 1. NEW ACTIVITY
    if first and first >= cutoff and m["total_alltime"] > 0:
        sig("NEW_ACTIVITY", "first_activation", f"First API call: {first}", "Reach out")
    elif t7 > 0 and dark is not None and dark > 30:
        sig("NEW_ACTIVITY", "re_activation", f"Re-activated after {dark} days dark", "Reach out")

    # 2. GROWTH
    if avg > 0 and t7 > avg * GROWTH_SPIKE_MULT:
        pct = int((t7 / avg - 1) * 100)
        sig("GROWTH", "spike", f"{t7:,.0f} calls this week vs {avg:,.0f} avg (+{pct}%)", "Expand")
    if t30 > GROWTH_NOTABLE_CALLS:
        sig("GROWTH", "notable", f"{t30:,.0f} calls/30d (production-level)", "Expand")

    # 3. MULTI-THREADING
    if au >= 2:
        email_list = ", ".join(m["active_emails"][:3])
        sig("MULTI_THREADING", "multi_user", f"{au} active users: {email_list}", "Sell")
        if nu > 0:
            sig("MULTI_THREADING", "new_joins", f"{nu} new user(s) joined active account", "Sell")

    # 4. EXPANSION
    if au >= EXPANSION_MIN_USERS:
        sig("EXPANSION", "broad", f"{au} active users in 30d", "Sell")
    if nu > 0 and t30 > EXPANSION_ON_ACTIVE:
        sig("EXPANSION", "new_on_established", f"New user on account with {t30:,.0f} monthly calls", "Sell")

    # 5. SALES GAPS
    open_opps = int(account.get("Count_of_Open_Opportunities__c") or 0)
    if t30 > SALES_GAP_MIN and open_opps == 0:
        sig("SALES_GAP", "no_opp", f"{t30:,.0f} calls/30d — no open opportunity", "Pipeline")
    if t30 > SALES_GAP_MIN and not has_recent_activity and not recently_closed:
        sig("SALES_GAP", "no_activity", f"{t30:,.0f} calls/30d — no sales activity in 30 days", "Pipeline")

    # 6. RISK
    if avg > 0 and t7 < avg * RISK_DROP_MULT and t30 > RISK_DROP_MIN_30D:
        pct = int((1 - t7 / avg) * 100)
        sig("RISK", "drop", f"Usage fell {pct}% below avg ({t7:,.0f} vs {avg:,.0f})", "Intervene")
    is_customer = (account.get("Total_Revenue_Closed_Won__c") or 0) > 0
    if au == 1 and t30 > RISK_SINGLE_MIN and not is_customer:
        sig("RISK", "single_threaded", f"Only 1 active user on {t30:,.0f} monthly calls — could indicate narrow adoption pre-close", "Intervene")
    if nu > 0 and t7 < RISK_STALLED_MAX_7D and t30 < RISK_STALLED_MAX_30D and not is_customer:
        sig("RISK", "stalled", f"New user but activity stalled ({t7:,.0f} calls this week)", "Intervene")

    return signals


# ── Step 5: Priority Lens ──────────────────────────────────────────────────────

def priority_bucket(account: dict, signals: list[dict], has_recent_activity: bool) -> str:
    if not signals:
        return "⚫ NO SIGNAL"

    tier       = account.get("Account_Tier__c") or ""
    is_t1      = tier in ("1. TARGET ACCOUNT", "Tier 1")
    is_t2a     = tier in ("2.A", "Tier 2")
    is_customer = (account.get("Total_Revenue_Closed_Won__c") or 0) > 0
    types      = {s["type"] for s in signals}
    subs       = {s["sub"] for s in signals}

    # 🔴 ACT NOW — time-sensitive prospect moments or at-risk customers
    if not is_customer and "NEW_ACTIVITY" in types:
        return "🔴 ACT NOW"
    if is_customer and "RISK" in types:
        return "🔴 ACT NOW"
    if is_customer and "SALES_GAP" in types and not has_recent_activity:
        return "🔴 ACT NOW"

    # 🟠 ACT THIS WEEK — prospect momentum or neglected customer gaps
    if not is_customer and is_t1 and types & {"GROWTH", "MULTI_THREADING"}:
        return "🟠 ACT THIS WEEK"
    if is_customer and "SALES_GAP" in types and has_recent_activity:
        return "🟠 ACT THIS WEEK"
    if not is_customer and "stalled" in subs:
        return "🟠 ACT THIS WEEK"
    if not is_customer and is_t2a and "NEW_ACTIVITY" in types:
        return "🟠 ACT THIS WEEK"

    # 🟡 EXPAND — healthy growth signals, no urgency
    if is_customer and types & {"GROWTH", "MULTI_THREADING", "EXPANSION"}:
        return "🟡 EXPAND"
    if not is_customer and "EXPANSION" in types:
        return "🟡 EXPAND"

    # 🔵 MONITOR — anything else with a signal
    return "🔵 MONITOR"


# ── Step 6: Format Slack message ───────────────────────────────────────────────

def customer_label(account: dict) -> str:
    return "💰 Customer" if (account.get("Total_Revenue_Closed_Won__c") or 0) > 0 else "🔍 Prospect"


def contextualize_action(signal: dict, is_customer: bool) -> str:
    """Return action label adjusted for customer vs prospect context."""
    stype = signal["type"]
    if stype == "SALES_GAP":
        return "Expand" if is_customer else "Pipeline"
    if stype == "RISK":
        sub = signal.get("sub", "")
        if is_customer and sub in ("drop", "single_threaded"):
            return "⚠️ Churn Risk"
        if is_customer and sub == "stalled":
            return "Re-engage"
        return "Intervene"
    if stype == "GROWTH":
        return "Expand"
    if stype in ("MULTI_THREADING", "EXPANSION"):
        return "Sell" if not is_customer else "Expand"
    if stype == "NEW_ACTIVITY":
        return "Reach out"
    return signal["action"]


def format_account_block(account: dict, signals: list[dict], m: dict) -> str:
    name       = account["Name"]
    tier       = account.get("Account_Tier__c") or "No tier"
    score      = account.get("Account_Score__c")
    score_str  = f"Score {int(score)}" if score else "No score"
    open_opps  = int(account.get("Count_of_Open_Opportunities__c") or 0)
    opp_str    = f"{open_opps} open opp(s)" if open_opps else "No open opps"
    is_cust    = (account.get("Total_Revenue_Closed_Won__c") or 0) > 0
    cust_label = customer_label(account)

    sig_names = ", ".join(dict.fromkeys(s["type"].replace("_", " ").title() for s in signals))
    actions   = list(dict.fromkeys(contextualize_action(s, is_cust) for s in signals))

    if m:
        usage = (f"{m['total_7d']:,.0f} calls (7d) · "
                 f"{m['total_30d']:,.0f} (30d) · "
                 f"{m['active_users_30d']} active user(s)")
    else:
        usage = "No usage data"

    lines = [
        f"*{name}* · {tier} · {score_str} · {cust_label}",
        f"Signal: {sig_names}",
        f"Usage: {usage}",
    ]
    for s in signals:
        lines.append(f"  • {s['detail']}")
    lines.append(f"Opp: {opp_str}")
    lines.append(f"→ *{' / '.join(actions)}*")
    return "\n".join(lines)


def format_slack_message(owner_name: str, items: list[dict]) -> str:
    by_bucket = defaultdict(list)
    for item in items:
        by_bucket[item["bucket"]].append(item)

    first_name = owner_name.split()[0]
    flagged_count = sum(len(v) for k, v in by_bucket.items() if k != "⚫ NO SIGNAL")

    lines = [
        f"📊 *Usage Monitor — {TODAY_STR}*",
        f"Hey {first_name} — {flagged_count} account(s) need attention today.",
        "",
    ]

    for bucket in BUCKET_ORDER:
        items_in_bucket = by_bucket.get(bucket, [])
        if not items_in_bucket or bucket == "⚫ NO SIGNAL":
            continue
        lines += [
            "━" * 40,
            f"*{BUCKET_LABELS[bucket]}* ({len(items_in_bucket)})",
            "━" * 40,
        ]
        for item in sorted(items_in_bucket, key=lambda x: -(x["account"].get("Account_Score__c") or 0)):
            lines.append("")
            lines.append(format_account_block(item["account"], item["signals"], item["metrics"]))
        lines.append("")

    no_signal = by_bucket.get("⚫ NO SIGNAL", [])
    if no_signal:
        lines += [
            "━" * 40,
            f"*⚫ NO SIGNAL* ({len(no_signal)}) — no action needed",
            ", ".join(i["account"]["Name"] for i in no_signal),
        ]

    return "\n".join(lines)


# ── Step 7: Slack delivery ─────────────────────────────────────────────────────

def slack_lookup_user(email: str, token: str):
    resp = requests.get(
        "https://slack.com/api/users.lookupByEmail",
        headers={"Authorization": f"Bearer {token}"},
        params={"email": email},
        timeout=10,
    )
    data = resp.json()
    return data["user"]["id"] if data.get("ok") else None


def slack_send_dm(user_id: str, text: str, token: str) -> bool:
    # Open DM channel
    resp = requests.post(
        "https://slack.com/api/conversations.open",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"users": user_id},
        timeout=10,
    )
    channel_id = resp.json().get("channel", {}).get("id")
    if not channel_id:
        return False
    # Post message
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"channel": channel_id, "text": text, "mrkdwn": True},
        timeout=10,
    )
    return resp.json().get("ok", False)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Usage Monitor — daily signal scan")
    parser.add_argument("--dry-run",  action="store_true", help="Print to console, no Slack DMs")
    parser.add_argument("--owner",    help="Filter to one rep by email (e.g. sarah@you.com)")
    parser.add_argument("--account",  help="Named account(s), comma-separated")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)
    slack_token = os.getenv("SLACK_USER_TOKEN", "")
    REPORTS_DIR.mkdir(exist_ok=True)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger()

    # ── Fetch accounts ──────────────────────────────────────────────────────────
    account_names = [a.strip() for a in args.account.split(",")] if args.account else None
    log.info("Fetching accounts...")
    accounts = fetch_accounts(owner_email=args.owner, account_names=account_names)

    if not accounts:
        log.info("No accounts found.")
        return

    log.info(f"Found {len(accounts)} accounts. Fetching usage + activity in bulk...")

    # ── Bulk data fetch ─────────────────────────────────────────────────────────
    account_ids        = [a["Id"] for a in accounts]
    usage_by_account   = fetch_usage_bulk(account_ids)
    active_accounts    = fetch_activity_bulk(account_ids)
    recently_closed    = fetch_recently_closed_bulk(account_ids)

    # ── Signal detection ────────────────────────────────────────────────────────
    log.info("Running signal detection...")
    results = []
    for account in accounts:
        if account.get("Name") in EXCLUDED_ACCOUNTS:
            log.info(f"  Skipping excluded account: {account['Name']}")
            continue
        acc_id  = account["Id"]
        users   = usage_by_account.get(acc_id, [])
        metrics = compute_metrics(users)
        has_activity = acc_id in active_accounts

        is_recently_closed = acc_id in recently_closed
        signals = detect_signals(metrics, account, has_activity, is_recently_closed) if metrics else []
        bucket  = priority_bucket(account, signals, has_activity)

        owner_obj  = account.get("Owner") or {}
        results.append({
            "account":     account,
            "metrics":     metrics,
            "signals":     signals,
            "bucket":      bucket,
            "owner_id":    account.get("OwnerId", ""),
            "owner_name":  owner_obj.get("Name", "Unknown"),
            "owner_email": owner_obj.get("Email", ""),
        })

    # ── Group by owner ──────────────────────────────────────────────────────────
    by_owner = defaultdict(list)
    for r in results:
        key = (r["owner_id"], r["owner_name"], r["owner_email"])
        by_owner[key].append(r)

    total_flagged = sum(1 for r in results if r["bucket"] != "⚫ NO SIGNAL")
    log.info(f"\n{'='*60}")
    log.info(f"USAGE MONITOR — {TODAY_STR}")
    log.info(f"Scanned {len(accounts)} accounts · {total_flagged} with signals · {len(by_owner)} rep(s)")
    log.info(f"{'='*60}\n")

    # ── Deliver per-rep ─────────────────────────────────────────────────────────
    for (owner_id, owner_name, owner_email), items in sorted(by_owner.items(), key=lambda x: x[0][1]):
        flagged = [i for i in items if i["bucket"] != "⚫ NO SIGNAL"]
        if not flagged:
            log.info(f"  {owner_name} — no signals, skipping")
            continue

        message = format_slack_message(owner_name, items)

        if args.dry_run:
            log.info(f"\n{'─'*60}")
            log.info(f"TO: {owner_name} <{owner_email}>")
            log.info(f"{'─'*60}")
            log.info(message)
        else:
            if not slack_token:
                log.error("SLACK_USER_TOKEN not set in .env — use --dry-run to test")
                sys.exit(1)
            slack_uid = slack_lookup_user(owner_email, slack_token)
            if slack_uid:
                ok = slack_send_dm(slack_uid, message, slack_token)
                log.info(f"  {'✓' if ok else '✗'} {owner_name} ({owner_email}) — {len(flagged)} flagged")
            else:
                log.warning(f"  ✗ Could not find Slack user for {owner_email}")

    # ── Save summary report ─────────────────────────────────────────────────────
    report_path = REPORTS_DIR / f"usage_monitor_{TODAY_STR}.json"
    with open(report_path, "w") as f:
        json.dump([{
            "account": r["account"]["Name"],
            "tier":    r["account"].get("Account_Tier__c"),
            "owner":   r["owner_name"],
            "bucket":  r["bucket"],
            "signals": [s["type"] for s in r["signals"]],
        } for r in results], f, indent=2)
    log.info(f"\nReport saved: {report_path}")


if __name__ == "__main__":
    main()
