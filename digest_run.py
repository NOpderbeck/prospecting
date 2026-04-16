"""
digest_run.py — Weekly Team Meeting Digest.

Pulls open opportunities for 4 target AEs, enriches with SF Tasks and
Next_Step_Historical__c, then asks Claude to write a narrative paragraph per AE
summarising week-over-week changes. Posts to #team-weekly-digest.

Runs Monday 7 AM PT as Cloud Run job `digest-report`.

Usage (local):
    python3 digest_run.py [--date "April 21, 2026"]

Environment variables:
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN
    SLACK_BOT_TOKEN
    ANTHROPIC_API_KEY
"""

import argparse
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from dotenv import load_dotenv

ENV_PATH      = os.path.join(os.path.dirname(__file__), ".env")
SLACK_CHANNEL = "#team-weekly-digest"

# The 4 AEs this digest covers (matched against SF User.Name)
TARGET_AE_NAMES = [
    "David Wacker",
    "Ryan Reed",
    "Ryan Allred",
    "Andrew Miller-McKeever",
]

# Next-step history pattern: "Owner Name (Month DD, YYYY): text"
NS_PATTERN = re.compile(
    r'^(.+?)\s*\((\w+ \d{1,2}, \d{4})\):\s*(.+)$',
    re.MULTILINE,
)


# ── Salesforce ─────────────────────────────────────────────────────────────

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


def fetch_target_aes(sf) -> list[dict]:
    """Look up the 4 target AEs by exact name."""
    results = []
    for name in TARGET_AE_NAMES:
        rows = soql(sf, f"""
            SELECT Id, Name, Email
            FROM User
            WHERE IsActive = true
            AND Name = '{name}'
            LIMIT 1
        """)
        if rows:
            results.append(rows[0])
        else:
            print(f"  ⚠️  User not found: {name}", file=sys.stderr)
    return results


def _quarter_window() -> tuple[str, str]:
    """Return (first_day, last_day) of the current calendar quarter as YYYY-MM-DD strings."""
    today = date.today()
    q_start_month = ((today.month - 1) // 3) * 3 + 1          # 1, 4, 7, or 10
    q_end_month   = q_start_month + 2
    import calendar
    last_day = calendar.monthrange(today.year, q_end_month)[1]
    start = date(today.year, q_start_month, 1).strftime("%Y-%m-%d")
    end   = date(today.year, q_end_month, last_day).strftime("%Y-%m-%d")
    return start, end


def fetch_open_opps(sf, ae_ids: list[str]) -> list[dict]:
    """Fetch open opps closing this quarter (today → end of quarter), excluding overdue."""
    ids_str   = "', '".join(ae_ids)
    today_str = date.today().strftime("%Y-%m-%d")
    _, q_end  = _quarter_window()
    return soql(sf, f"""
        SELECT Id, Name, AccountId, Account.Name, Account.Account_Tier__c,
               StageName, Amount, CloseDate,
               NextStep, Next_Step_Historical__c,
               OwnerId, Owner.Name
        FROM Opportunity
        WHERE IsClosed = false
        AND OwnerId IN ('{ids_str}')
        AND CloseDate >= {today_str}
        AND CloseDate <= {q_end}
        ORDER BY CloseDate ASC NULLS LAST
    """)


def fetch_recent_tasks(sf, account_ids: list[str]) -> list[dict]:
    """
    Fetch SF Tasks for given account IDs in the last 90 days.
    90-day window lets us compute days_silent without a SOQL aggregate query.
    """
    if not account_ids:
        return []
    since = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")
    ids_str = "', '".join(account_ids)
    return soql(sf, f"""
        SELECT Id, WhatId, AccountId, OwnerId, Type, Subject,
               ActivityDate, Status, Description, Who.Name
        FROM Task
        WHERE AccountId IN ('{ids_str}')
        AND ActivityDate >= {since}
        ORDER BY ActivityDate DESC
    """)


# ── Next Step Parsing ───────────────────────────────────────────────────────

def parse_next_step_history(text: str) -> list[dict]:
    if not text:
        return []
    results = []
    for owner, date_str, content in NS_PATTERN.findall(text):
        try:
            dt = datetime.strptime(date_str.strip(), "%B %d, %Y").date()
            results.append({"owner": owner.strip(), "date": dt, "text": content.strip()})
        except ValueError:
            pass
    return sorted(results, key=lambda x: x["date"], reverse=True)


# ── Deal enrichment ──────────────────────────────────────────────────────────

def enrich_deals(opp_records: list[dict], tasks_by_account: dict) -> list[dict]:
    """Attach task splits and last-activity data to each opp."""
    today = date.today()
    deals = []

    for opp in opp_records:
        account_id = opp.get("AccountId") or ""
        account    = opp.get("Account") or {}
        tasks      = tasks_by_account.get(account_id, [])

        # Split tasks into this-week (0–7d) and last-week (7–14d)
        tasks_this_week: list[dict] = []
        tasks_last_week: list[dict] = []
        for t in tasks:
            ad = t.get("ActivityDate")
            if not ad:
                continue
            age = (today - date.fromisoformat(ad[:10])).days
            if age <= 7:
                tasks_this_week.append(t)
            else:
                tasks_last_week.append(t)

        # Days since most recent task in 90-day window
        task_dates = [
            date.fromisoformat(t["ActivityDate"][:10])
            for t in tasks if t.get("ActivityDate")
        ]
        last_task_date = max(task_dates) if task_dates else None
        days_silent = (today - last_task_date).days if last_task_date else None

        # Next step history
        ns_history = parse_next_step_history(opp.get("Next_Step_Historical__c") or "")

        # Close date
        close_date_str = opp.get("CloseDate")
        overdue = False
        days_to_close = None
        if close_date_str:
            try:
                cd = date.fromisoformat(close_date_str[:10])
                days_to_close = (cd - today).days
                overdue = days_to_close < 0
            except ValueError:
                pass

        deals.append({
            "id":              opp.get("Id"),
            "name":            opp.get("Name") or "?",
            "account_id":      account_id,
            "account_name":    account.get("Name") or "?",
            "tier":            account.get("Account_Tier__c") or "",
            "owner_name":      (opp.get("Owner") or {}).get("Name") or "?",
            "stage":           opp.get("StageName") or "?",
            "amount":          opp.get("Amount"),
            "close_date":      close_date_str,
            "days_to_close":   days_to_close,
            "overdue":         overdue,
            "next_step":       opp.get("NextStep") or "",
            "ns_history":      ns_history,
            "tasks_this_week": tasks_this_week,
            "tasks_last_week": tasks_last_week,
            "days_silent":     days_silent,
        })

    return deals


# ── Helpers ──────────────────────────────────────────────────────────────────

def short_name(full_name: str) -> str:
    """'David Wacker' → 'David W.'  'Andrew Miller-McKeever' → 'Andrew M.'"""
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[-1][0]}."
    return full_name


def _fmt_amount(amount) -> str:
    if not amount:
        return "$0"
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.1f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.0f}K"
    return f"${int(amount):,}"


def _deal_context(deal: dict) -> str:
    """Render one deal as a compact text block for the Claude prompt."""
    lines = [
        f"Deal: {deal['name']} ({deal['account_name']})",
        f"  Stage: {deal['stage']} | Amount: {_fmt_amount(deal.get('amount'))} | "
        f"Close: {deal['close_date'] or '?'}"
        + (" ⚠️ OVERDUE" if deal["overdue"] else ""),
    ]

    if deal["days_silent"] is None:
        lines.append("  Activity: never touched")
    elif deal["days_silent"] > 30:
        lines.append(f"  Activity: silent {deal['days_silent']}d")
    else:
        lines.append(f"  Activity: last touch {deal['days_silent']}d ago")

    tw = deal["tasks_this_week"]
    lw = deal["tasks_last_week"]
    if tw:
        summaries = "; ".join(
            f"{t.get('Type','?')}: {(t.get('Subject') or '')[:50]}" for t in tw[:3]
        )
        lines.append(f"  This week ({len(tw)} task(s)): {summaries}")
    if lw:
        summaries = "; ".join(
            f"{t.get('Type','?')}: {(t.get('Subject') or '')[:40]}" for t in lw[:2]
        )
        lines.append(f"  Last week ({len(lw)} task(s)): {summaries}")

    ns = deal.get("next_step", "")
    if ns:
        lines.append(f"  Next step: {ns[:120]}")

    ns_h = deal.get("ns_history") or []
    if ns_h:
        e = ns_h[0]
        lines.append(f"  Next step updated {e['date']}: {e['text'][:120]}")

    return "\n".join(lines)


def generate_deal_summaries(ae_name: str, deals: list[dict], client) -> dict[str, str]:
    """
    Ask Claude for a 1–2 sentence update per deal.
    Returns {account_name: sentence} parsed from JSON response.
    """
    import json as _json

    today_str = date.today().strftime("%B %-d, %Y")
    deal_blocks = "\n\n".join(_deal_context(d) for d in deals)
    account_names = [d["account_name"] for d in deals]

    prompt = f"""You are writing a weekly deal digest for a sales manager reviewing {ae_name}'s pipeline before a team call. Today is {today_str}.

Deal details:
{deal_blocks}

Write a 1–2 sentence update for each deal. Each sentence should convey what changed this week vs last week and the single most important next action or risk. Be specific — reference actual activity, dates, or people where available.

Return ONLY a JSON object where each key is the exact account name and each value is the update sentence. Account names to use as keys: {account_names}

Rules: no markdown, no bold, no extra keys. Plain sentences only."""

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:])
        if raw.endswith("```"):
            raw = "\n".join(raw.split("\n")[:-1])
        return _json.loads(raw.strip())
    except Exception as e:
        print(f"  Claude error for {ae_name}: {e}", file=sys.stderr)
        return {d["account_name"]: "(summary unavailable)" for d in deals}


# ── Slack output ─────────────────────────────────────────────────────────────

def build_message(ae_rows: list[dict], date_str: str) -> str:
    """ae_rows = [{name, deals, summaries: {account_name: sentence}}, ...]"""
    lines: list[str] = []

    total_deals = sum(len(r["deals"]) for r in ae_rows)
    total_amt   = sum(
        sum(d.get("amount") or 0 for d in r["deals"])
        for r in ae_rows
    )

    lines += [
        f"*📋 Team Digest — {date_str}*",
        f"_{total_deals} deals closing this quarter · {_fmt_amount(total_amt)} pipeline_",
        "",
    ]

    for row in ae_rows:
        display = short_name(row["name"])
        deals   = row["deals"]
        sums    = row["summaries"]  # {account_name: sentence}

        lines.append(f"*{display}*")

        for d in deals:
            acct   = d["account_name"]
            sentence = sums.get(acct, "")
            flags: list[str] = []
            if d["overdue"]:
                flags.append(f"⚠️ {abs(d['days_to_close'] or 0)}d overdue")
            elif d.get("days_to_close") is not None and d["days_to_close"] <= 14:
                flags.append(f"due in {d['days_to_close']}d")
            if (d["days_silent"] or 0) > 21:
                flags.append(f"silent {d['days_silent']}d")

            flag_str = f" _[{', '.join(flags)}]_" if flags else ""
            lines.append(f"• *{acct}*{flag_str} — {sentence}")

        lines.append("")

    return "\n".join(lines)


def post_to_slack(bot_token: str, text: str):
    import requests
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json"},
        json={"channel": SLACK_CHANNEL, "text": text, "mrkdwn": True, "unfurl_links": False},
        timeout=15,
    )
    result = resp.json()
    if result.get("ok"):
        print(f"✅ Posted to {SLACK_CHANNEL}")
    else:
        print(f"⚠️  Slack post failed: {result.get('error')}", file=sys.stderr)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None,
                        help="Report date string (default: today)")
    parser.add_argument("--no-post", action="store_true",
                        help="Print to stdout only, skip Slack post")
    args = parser.parse_args()

    load_dotenv(ENV_PATH, override=True)

    today     = date.today()
    date_str  = args.date or today.strftime("%B %-d, %Y")
    bot_token = os.getenv("SLACK_BOT_TOKEN", "")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")

    print(f"Date: {date_str}")

    # ── Salesforce ────────────────────────────────────────────────────────────
    print("Connecting to Salesforce...")
    sf = connect_sf()

    print("Resolving target AEs...")
    ae_records = fetch_target_aes(sf)
    if not ae_records:
        print("No AEs resolved — aborting.")
        return
    ae_ids = [r["Id"] for r in ae_records]
    print(f"  {', '.join(r['Name'] for r in ae_records)}")

    print("Fetching open opportunities...")
    opp_records = fetch_open_opps(sf, ae_ids)
    print(f"  {len(opp_records)} open opp(s)")

    # Unique account IDs for task queries
    account_ids = list({r["AccountId"] for r in opp_records if r.get("AccountId")})

    print(f"Fetching tasks (90d) for {len(account_ids)} accounts...")
    all_tasks = fetch_recent_tasks(sf, account_ids)
    print(f"  {len(all_tasks)} task(s)")

    tasks_by_account: dict[str, list] = defaultdict(list)
    for t in all_tasks:
        if t.get("AccountId"):
            tasks_by_account[t["AccountId"]].append(t)

    # ── Enrich and group by AE ────────────────────────────────────────────────
    all_deals = enrich_deals(opp_records, tasks_by_account)
    deals_by_owner: dict[str, list] = defaultdict(list)
    for d in all_deals:
        deals_by_owner[d["owner_name"]].append(d)

    ae_rows = []
    for ae in ae_records:
        name  = ae["Name"]
        deals = deals_by_owner.get(name, [])
        print(f"  {name}: {len(deals)} deal(s)")
        ae_rows.append({"name": name, "deals": deals, "summaries": {}})

    # ── Claude per-deal summaries (parallel) ──────────────────────────────────
    if not anthropic_key:
        print("⚠️  No ANTHROPIC_API_KEY — summaries will be blank", file=sys.stderr)
        for row in ae_rows:
            row["summaries"] = {d["account_name"]: "(unavailable)" for d in row["deals"]}
    else:
        import anthropic
        client = anthropic.Anthropic(api_key=anthropic_key)
        print(f"Generating deal summaries for {len(ae_rows)} AE(s)...")

        def _gen(row):
            row["summaries"] = generate_deal_summaries(row["name"], row["deals"], client)
            return row["name"]

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(_gen, row): row["name"] for row in ae_rows}
            for fut in as_completed(futures):
                try:
                    print(f"  ✓ {fut.result()}")
                except Exception as e:
                    print(f"  ✗ {futures[fut]}: {e}", file=sys.stderr)

    # ── Build + post ──────────────────────────────────────────────────────────
    message = build_message(ae_rows, date_str)
    print("\n" + message)

    if not args.no_post:
        if bot_token:
            post_to_slack(bot_token, message)
        else:
            print("⚠️  No SLACK_BOT_TOKEN — skipping Slack post", file=sys.stderr)


if __name__ == "__main__":
    main()
