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


def fetch_open_opps(sf, ae_ids: list[str]) -> list[dict]:
    ids_str = "', '".join(ae_ids)
    cutoff  = (date.today() + timedelta(days=60)).strftime("%Y-%m-%d")
    return soql(sf, f"""
        SELECT Id, Name, AccountId, Account.Name, Account.Account_Tier__c,
               StageName, Amount, CloseDate,
               NextStep, Next_Step_Historical__c,
               OwnerId, Owner.Name
        FROM Opportunity
        WHERE IsClosed = false
        AND OwnerId IN ('{ids_str}')
        AND CloseDate <= {cutoff}
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


# ── Claude narrative ─────────────────────────────────────────────────────────

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


def generate_ae_narrative(ae_name: str, deals: list[dict], client) -> str:
    """
    Ask Claude to write a 4–5 sentence manager narrative for this AE's pipeline,
    focused on week-over-week changes and what to discuss.
    """
    today_str = date.today().strftime("%B %-d, %Y")

    total_amt = sum(d.get("amount") or 0 for d in deals)
    overdue   = [d for d in deals if d["overdue"]]
    silent    = [d for d in deals if (d["days_silent"] or 0) > 21]
    active_tw = [d for d in deals if d["tasks_this_week"]]

    deal_blocks = "\n\n".join(_deal_context(d) for d in deals)

    prompt = f"""You are writing a weekly deal digest for a sales manager reviewing {ae_name}'s pipeline before a team call. Today is {today_str}.

{ae_name} has {len(deals)} open deal(s) totalling {_fmt_amount(total_amt)}.
- {len(active_tw)} deal(s) had activity this week
- {len(silent)} deal(s) have been silent for 21+ days
- {len(overdue)} deal(s) are past their close date

Deal details:
{deal_blocks}

Write a 4–5 sentence narrative for the manager. Cover:
1. What moved or changed this week vs last week (name the specific deal)
2. The biggest risk or stale deal needing attention
3. Any deal with strong momentum worth accelerating
4. One specific question to ask {ae_name} on the call

Rules: plain prose only — no markdown headers, no bullet points, no bold text, no section labels. Just sentences. Be direct and name specific deals. If a deal name contains pipe-separated suffixes like 'Renewal | $X | Plan', use only the company name portion."""

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        print(f"  Claude error for {ae_name}: {e}", file=sys.stderr)
        return f"(narrative unavailable — {e})"


# ── Slack output ─────────────────────────────────────────────────────────────

def build_message(ae_narratives: list[dict], date_str: str) -> str:
    """ae_narratives = [{name, deals, narrative}, ...]"""
    today = date.today()
    lines: list[str] = []

    total_deals = sum(len(r["deals"]) for r in ae_narratives)
    total_amt   = sum(
        sum(d.get("amount") or 0 for d in r["deals"])
        for r in ae_narratives
    )

    lines += [
        f"*📋 Team Digest — {date_str}*",
        f"_{total_deals} open deals · {_fmt_amount(total_amt)} pipeline · "
        f"{', '.join(r['name'].split()[0] for r in ae_narratives)}_",
        "",
    ]

    for row in ae_narratives:
        name   = row["name"]
        deals  = row["deals"]
        n      = len(deals)
        amt    = sum(d.get("amount") or 0 for d in deals)
        overdue = sum(1 for d in deals if d["overdue"])
        silent  = sum(1 for d in deals if (d["days_silent"] or 0) > 21)

        meta_parts = [f"{n} deals", _fmt_amount(amt)]
        if overdue:
            meta_parts.append(f"{overdue} overdue")
        if silent:
            meta_parts.append(f"{silent} silent 21d+")

        lines += [
            f"*{name}* _({', '.join(meta_parts)})_",
            row["narrative"],
            "",
        ]

    # ── Overdue deals footer ──────────────────────────────────────────────────
    all_deals = [d for r in ae_narratives for d in r["deals"]]
    overdue_deals = sorted(
        [d for d in all_deals if d["overdue"]],
        key=lambda d: d.get("close_date") or "",
    )
    if overdue_deals:
        lines.append("*⚠️ Overdue Close Dates*")
        for d in overdue_deals:
            days_over = abs(d["days_to_close"] or 0)
            silent_str = f" · {d['days_silent']}d silent" if (d["days_silent"] or 0) > 7 else ""
            # Strip dollar amounts / plan suffixes from SF opp name for readability
            short_name = d["name"].split(" | ")[0].strip()
            lines.append(
                f"• *{short_name}* ({d['owner_name']}) · {days_over}d overdue"
                + silent_str
            )
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
        ae_rows.append({"name": name, "deals": deals, "narrative": ""})

    # ── Claude narratives (parallel) ──────────────────────────────────────────
    if not anthropic_key:
        print("⚠️  No ANTHROPIC_API_KEY — narratives will be blank", file=sys.stderr)
        for row in ae_rows:
            row["narrative"] = "(narrative unavailable — no API key)"
    else:
        import anthropic
        client = anthropic.Anthropic(api_key=anthropic_key)
        print(f"Generating {len(ae_rows)} narrative(s)...")

        def _gen(row):
            row["narrative"] = generate_ae_narrative(row["name"], row["deals"], client)
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
