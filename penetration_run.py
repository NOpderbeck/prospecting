"""
penetration_run.py — Pull Salesforce data, compute penetration scores,
write pen_results.json, then invoke penetration_publish.py.

Designed for Cloud Run scheduled execution (no MCP, no Claude agent).

Usage (local):
    python3 penetration_run.py [--date "April 22, 2026"] [--results-file /tmp/pen_results.json]

Environment variables (via .env or Cloud Run secrets):
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN
    SLACK_BOT_TOKEN, GOOGLE_CREDENTIALS_FILE
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import date
from dotenv import load_dotenv

ENV_PATH     = os.path.join(os.path.dirname(__file__), ".env")
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PUBLISH_SCRIPT = os.path.join(SCRIPT_DIR, "penetration_publish.py")


# ── Salesforce connection ──────────────────────────────────────────────────────

def connect_sf():
    from simple_salesforce import Salesforce
    return Salesforce(
        username=os.environ["SF_USERNAME"],
        password=os.environ["SF_PASSWORD"],
        security_token=os.environ["SF_SECURITY_TOKEN"],
    )


def soql(sf, query: str) -> list:
    try:
        result = sf.query_all(query.strip())
        return result.get("records", [])
    except Exception as e:
        print(f"  SOQL error: {e}", file=sys.stderr)
        return []


# ── Fetch Tier 1 accounts ──────────────────────────────────────────────────────

def fetch_accounts(sf) -> list:
    return soql(sf, """
        SELECT Id, Name, Account_Tier__c, Account_Score__c,
               Total_Revenue_Closed_Won__c, Count_of_Open_Opportunities__c,
               Owner.Name, Owner.Email
        FROM Account
        WHERE Account_Tier__c IN ('1. TARGET ACCOUNT', 'Tier 1')
        ORDER BY Account_Score__c DESC NULLS LAST
    """)


# ── Enrich a single account ────────────────────────────────────────────────────

def enrich_account(sf, acc: dict) -> dict:
    acc_id = acc["Id"]
    today  = date.today()

    # ── 2a. Product usage ──────────────────────────────────────────────────────
    usage_rows = soql(sf, f"""
        SELECT API_Calls_Last_7_Days__c, API_Calls_Last_30_Days__c,
               API_Calls_per_User_All_Time__c, Last_API_Call_Date__c
        FROM Product_User__c
        WHERE Account__c = '{acc_id}'
        ORDER BY API_Calls_Last_30_Days__c DESC NULLS LAST
    """)

    total_7d   = sum(r.get("API_Calls_Last_7_Days__c")  or 0 for r in usage_rows)
    total_30d  = sum(r.get("API_Calls_Last_30_Days__c") or 0 for r in usage_rows)
    weekly_avg = total_30d / 4
    active_30  = sum(1 for r in usage_rows if (r.get("API_Calls_Last_30_Days__c") or 0) > 0)
    all_time   = sum(r.get("API_Calls_per_User_All_Time__c") or 0 for r in usage_rows)

    raw_dates = [r.get("Last_API_Call_Date__c") for r in usage_rows if r.get("Last_API_Call_Date__c")]
    last_call_dates = [d[:10] for d in raw_dates]
    last_call = max(last_call_dates) if last_call_dates else None

    days_since_usage = (today - date.fromisoformat(last_call)).days if last_call else None

    usage_present = total_30d > 0
    usage_multi   = active_30 >= 2
    usage_growing = (
        (total_7d - weekly_avg) / weekly_avg * 100 > 20
        if weekly_avg > 0 else False
    )
    usage_dormant = (days_since_usage is not None and days_since_usage > 30) or total_30d == 0

    # ── 2b. Recent Closed Lost opportunities ──────────────────────────────────
    closed_lost_rows = soql(sf, f"""
        SELECT Id, Name, CloseDate, Amount
        FROM Opportunity
        WHERE AccountId = '{acc_id}'
        AND StageName = 'Closed Lost'
        AND CloseDate >= LAST_N_DAYS:180
        ORDER BY CloseDate DESC
        LIMIT 1
    """)

    # ── 2c. Sales tasks (last 30d) ─────────────────────────────────────────────
    task_rows = soql(sf, f"""
        SELECT Type, Subject, ActivityDate, WhoId, Who.Name, Status
        FROM Task
        WHERE AccountId = '{acc_id}'
        AND ActivityDate >= LAST_N_DAYS:30
        AND Status != 'Not Started'
        ORDER BY ActivityDate DESC
    """)

    # ── 2d. Meetings / events (last 30d) ───────────────────────────────────────
    event_rows = soql(sf, f"""
        SELECT Subject, ActivityDateTime, WhoId, Who.Name
        FROM Event
        WHERE AccountId = '{acc_id}'
        AND ActivityDateTime >= LAST_N_DAYS:30
        ORDER BY ActivityDateTime DESC
    """)

    # ── Compute activity metrics ───────────────────────────────────────────────
    emails    = sum(1 for t in task_rows if t.get("Type") == "Email")
    calls_    = sum(1 for t in task_rows if t.get("Type") == "Call")
    mtgs      = len(event_rows)
    has_mtg   = mtgs > 0
    has_reply = any(
        t.get("Type") == "Email"
        and t.get("Status") == "Completed"
        and "Re:" in (t.get("Subject") or "")
        for t in task_rows
    )

    who_ids: set = set()
    for t in task_rows:
        if t.get("WhoId"):
            who_ids.add(t["WhoId"])
    for e in event_rows:
        if e.get("WhoId"):
            who_ids.add(e["WhoId"])
    contacts = len(who_ids)

    act_dates = []
    for t in task_rows:
        if t.get("ActivityDate"):
            act_dates.append(t["ActivityDate"][:10])
    for e in event_rows:
        if e.get("ActivityDateTime"):
            act_dates.append(e["ActivityDateTime"][:10])
    last_act = max(act_dates) if act_dates else None

    days_since_act = (today - date.fromisoformat(last_act)).days if last_act else None

    act_present    = bool(task_rows or event_rows)
    activity_stale = (days_since_act is None) or (days_since_act > 30)
    multi_sales    = contacts >= 2

    # ── Step 4: Score ──────────────────────────────────────────────────────────
    # Usage score (0–5)
    if total_30d == 0 and all_time == 0:
        us = 0
    elif usage_multi and total_30d >= 1000:
        us = 5
    elif total_30d >= 10000 or usage_growing:
        us = 4
    elif total_30d >= 1000:
        us = 3
    elif total_30d >= 500 and active_30 == 1:
        us = 2
    elif 0 < total_30d < 500 and active_30 == 1:
        us = 1
    else:
        us = 0

    # Activity score (0–5)
    if not act_present:
        as_ = 0
    elif has_mtg and multi_sales:
        as_ = 5
    elif has_mtg or has_reply:
        as_ = 4
    elif (emails + calls_) >= 3 and days_since_act is not None and days_since_act <= 14:
        as_ = 3
    elif calls_ > 0 and contacts == 1:
        as_ = 2
    elif emails > 0 and not has_mtg and contacts == 1:
        as_ = 1
    else:
        as_ = 0

    # Coverage multiplier (1.0–1.5)
    if has_mtg and multi_sales:
        cov = 1.5
    elif has_reply or has_mtg:
        cov = 1.2
    else:
        cov = 1.0

    pen = round((us + as_) * cov, 1)

    # ── Step 5: Classify ───────────────────────────────────────────────────────
    prior_usage = all_time > 0

    # Check if a recent Closed Lost explains dormant usage — if the deal closed
    # within 90 days of the last API call (or within 90 days of today if no
    # last_call), the stoppage is explained and should not trigger an alert.
    closed_lost_opp    = closed_lost_rows[0] if closed_lost_rows else None
    closed_lost_date   = closed_lost_opp["CloseDate"][:10] if closed_lost_opp else None
    closed_lost_name   = closed_lost_opp.get("Name", "") if closed_lost_opp else None
    closed_lost_explains = False
    if closed_lost_date:
        anchor = date.fromisoformat(last_call) if last_call else today
        delta  = abs((date.fromisoformat(closed_lost_date) - anchor).days)
        closed_lost_explains = delta <= 45

    if usage_present and not act_present:
        if closed_lost_explains:
            cls, pri = "Closed Lost", "grey"
        else:
            cls, pri = "Inbound Only", "red"
    elif (usage_dormant and prior_usage) or (activity_stale and usage_present):
        if closed_lost_explains:
            cls, pri = "Closed Lost", "grey"
        else:
            cls, pri = "At Risk", "red"
    elif not usage_present and act_present:
        cls, pri = "Outbound Only", "orange"
    elif usage_present and act_present and pen < 5:
        cls, pri = "Early Penetration", "orange"
    elif usage_multi and multi_sales:
        cls, pri = "Multi-Threaded Growth", "yellow"
    elif pen >= 9:
        cls, pri = "Strong Penetration", "yellow"
    elif not usage_present and not act_present:
        cls, pri = "White Space", "white"
    else:
        cls, pri = "Developing", "blue"

    return {
        "id":           acc["Id"],
        "name":         acc["Name"],
        "owner":        (acc.get("Owner") or {}).get("Name", "Unknown"),
        "owner_email":  (acc.get("Owner") or {}).get("Email", ""),
        "score":        acc.get("Account_Score__c") or 0,
        "open_opps":    int(acc.get("Count_of_Open_Opportunities__c") or 0),
        "is_customer":  (acc.get("Total_Revenue_Closed_Won__c") or 0) > 0,
        "total_7d":     int(total_7d),
        "total_30d":    int(total_30d),
        "weekly_avg":   round(weekly_avg, 1),
        "active_30":    active_30,
        "emails":       emails,
        "calls":        calls_,
        "mtgs":         mtgs,
        "contacts":     contacts,
        "has_mtg":      has_mtg,
        "has_reply":    has_reply,
        "usage_present": usage_present,
        "act_present":  act_present,
        "usage_multi":  usage_multi,
        "multi_sales":  multi_sales,
        "last_act":     last_act,
        "last_call":    last_call,
        "us":           us,
        "as_":          as_,
        "cov":          cov,
        "pen":          pen,
        "cls":              cls,
        "pri":              pri,
        "closed_lost_date": closed_lost_date,
        "closed_lost_name": closed_lost_name,
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",         default=None,
                        help="Report date string, e.g. 'April 22, 2026' (default: today)")
    parser.add_argument("--results-file", default="/tmp/pen_results.json")
    parser.add_argument("--dry-run",      action="store_true",
                        help="Pass --dry-run to penetration_publish.py (no Google Doc or Slack post)")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    date_str = args.date or date.today().strftime("%B %-d, %Y")

    print("Connecting to Salesforce...")
    sf = connect_sf()

    print("Fetching Tier 1 accounts...")
    accounts = fetch_accounts(sf)
    print(f"  {len(accounts)} accounts found")

    results = []
    for i, acc in enumerate(accounts, 1):
        name = acc.get("Name", "?")
        try:
            r = enrich_account(sf, acc)
            results.append(r)
            print(f"  [{i}/{len(accounts)}] ✓ {name}")
        except Exception as e:
            print(f"  [{i}/{len(accounts)}] ✗ {name}: {e}", file=sys.stderr)

    # Sort by account score descending (matches original query order)
    results.sort(key=lambda r: -(r.get("score") or 0))

    with open(args.results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written → {args.results_file}  ({len(results)} accounts)")

    print(f"\nPublishing report for '{date_str}'...")
    cmd = [sys.executable, PUBLISH_SCRIPT,
           "--results-file", args.results_file,
           "--date", date_str]
    if args.dry_run:
        cmd.append("--dry-run")
    proc = subprocess.run(cmd, cwd=SCRIPT_DIR)
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
