"""
digest_run.py — Weekly Team Meeting Digest.

Pulls all open opportunities for the AE team, enriches each with SF Tasks,
Gong call content (brief, key points, trackers), and Next_Step_Historical__c,
computes momentum/risk scores, generates Claude synopses for flagged deals,
and posts a structured digest to #team-weekly-digest.

Runs Monday 7 AM PT as Cloud Run job `digest-report`.

Usage (local):
    python3 digest_run.py [--date "April 21, 2026"] [--days 30]

Environment variables:
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN  — Salesforce creds
    GONG_API_KEY, GONG_API_SECRET                — Gong REST credentials
    SLACK_BOT_TOKEN                              — Slack bot token
    ANTHROPIC_API_KEY                            — Claude API key
"""

import argparse
import base64
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv

ENV_PATH      = os.path.join(os.path.dirname(__file__), ".env")
SLACK_CHANNEL = "#team-weekly-digest"
GONG_BASE     = "https://us-64844.api.gong.io"

# AEs who don't have UserRole = 'Sales - AE' but should be included
ADDITIONAL_AE_IDS: set[str] = {
    "005fo000000d1nSAAQ",  # David Wacker (VP, AE)
    "005fo000000d3AvAAI",  # Haroon Anwar
}

EXCLUDE_NAME_FRAGMENTS: list[str] = ["bluplanet", "blue planet"]

# Gong calls shorter than this (seconds) are skipped
MIN_CALL_SECONDS = 120

# Number of top discussion priorities to surface at end
DISCUSSION_TOP_N = 5

# Status labels
STATUS_CRITICAL  = "Critical"
STATUS_AT_RISK   = "At Risk"
STATUS_ADVANCING = "Advancing"
STATUS_STABLE    = "Stable"
STATUS_SILENT    = "Silent"

STATUS_EMOJI = {
    STATUS_CRITICAL:  "🔴",
    STATUS_AT_RISK:   "🟠",
    STATUS_ADVANCING: "🟢",
    STATUS_STABLE:    "🟡",
    STATUS_SILENT:    "⚫",
}

# Next-step history entry pattern: "Owner Name (Month DD, YYYY): text"
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


def is_excluded(name: str) -> bool:
    low = name.lower()
    return any(frag in low for frag in EXCLUDE_NAME_FRAGMENTS)


def fetch_ae_roster(sf) -> list[dict]:
    """Return SF User records for all active AEs."""
    records = soql(sf, """
        SELECT Id, Name, Email
        FROM User
        WHERE IsActive = true
        AND UserRole.Name = 'Sales - AE'
        ORDER BY Name
    """)
    if ADDITIONAL_AE_IDS:
        ids_str = "', '".join(ADDITIONAL_AE_IDS)
        extras = soql(sf, f"""
            SELECT Id, Name, Email
            FROM User
            WHERE Id IN ('{ids_str}')
            AND IsActive = true
        """)
        existing = {r["Id"] for r in records}
        for r in extras:
            if r["Id"] not in existing:
                records.append(r)
    return [r for r in records if not is_excluded(r.get("Name", ""))]


def fetch_open_opps(sf, ae_ids: list[str]) -> list[dict]:
    """Fetch all open opportunities owned by the AE team."""
    ids_str = "', '".join(ae_ids)
    return soql(sf, f"""
        SELECT Id, Name, AccountId, Account.Name, Account.Account_Tier__c,
               StageName, Amount, CloseDate,
               NextStep, Next_Step_Historical__c,
               OwnerId, Owner.Name
        FROM Opportunity
        WHERE IsClosed = false
        AND OwnerId IN ('{ids_str}')
        ORDER BY CloseDate ASC NULLS LAST
    """)


def fetch_recent_tasks(sf, account_ids: list[str], days: int) -> list[dict]:
    """
    Fetch SF Tasks for all given account IDs in the last N days.
    Includes WhatId so we can join opp-level tasks.
    """
    if not account_ids:
        return []
    since = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    ids_str = "', '".join(account_ids)
    return soql(sf, f"""
        SELECT Id, WhatId, AccountId, OwnerId, Type, Subject,
               ActivityDate, Status, Description, WhoId, Who.Name
        FROM Task
        WHERE AccountId IN ('{ids_str}')
        AND ActivityDate >= {since}
        ORDER BY ActivityDate DESC
    """)


# ── Next Step History Parsing ───────────────────────────────────────────────

def parse_next_step_history(text: str) -> list[dict]:
    """Parse timestamped next-step log into list of {owner, date, text}, newest first."""
    if not text:
        return []
    results = []
    for owner, date_str, content in NS_PATTERN.findall(text):
        try:
            dt = datetime.strptime(date_str.strip(), "%B %d, %Y").date()
            results.append({
                "owner": owner.strip(),
                "date":  dt,
                "text":  content.strip(),
            })
        except ValueError:
            pass
    return sorted(results, key=lambda x: x["date"], reverse=True)


def days_since_date(d) -> int | None:
    """Return days between d and today. d may be a date, datetime, or ISO string."""
    if d is None:
        return None
    if isinstance(d, str):
        d = d[:10]
        d = date.fromisoformat(d)
    if isinstance(d, datetime):
        d = d.date()
    return (date.today() - d).days


# ── Gong ────────────────────────────────────────────────────────────────────

def _gong_headers() -> dict:
    key    = os.environ["GONG_API_KEY"]
    secret = os.environ["GONG_API_SECRET"]
    token  = base64.b64encode(f"{key}:{secret}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def fetch_gong_extensive(from_dt: datetime, to_dt: datetime) -> dict[str, list]:
    """
    Fetch all Gong calls in [from_dt, to_dt] via /v2/calls/extensive.
    Returns {sf_account_id: [call_dict, ...]} indexed by Salesforce AccountId
    found in each call's context objects.
    """
    import requests

    url      = f"{GONG_BASE}/v2/calls/extensive"
    from_str = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str   = to_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    account_calls: dict[str, list] = defaultdict(list)
    cursor = None
    total  = 0

    while True:
        body: dict = {
            "filter": {
                "fromDateTime": from_str,
                "toDateTime":   to_str,
            },
            "contentSelector": {
                "exposedFields": {
                    "parties": True,
                    "content": {
                        "structure":  False,
                        "topics":     False,
                        "trackers":   True,
                        "brief":      True,
                        "keyPoints":  True,
                        "callOutcome": False,
                    },
                    "context": True,
                }
            },
        }
        if cursor:
            body["cursor"] = cursor

        try:
            resp = requests.post(
                url,
                headers={**_gong_headers(), "Content-Type": "application/json"},
                json=body,
                timeout=60,
            )
            resp.raise_for_status()
        except Exception as e:
            print(f"  Gong extensive API error: {e}", file=sys.stderr)
            break

        data  = resp.json()
        calls = data.get("calls", [])
        total += len(calls)

        for call in calls:
            dur = (call.get("metaData") or {}).get("duration") or 0
            if dur < MIN_CALL_SECONDS:
                continue

            # Extract SF Account IDs from context
            sf_account_ids: set[str] = set()
            for ctx in (call.get("context") or []):
                if ctx.get("system") != "Salesforce":
                    continue
                for obj in (ctx.get("objects") or []):
                    if obj.get("objectType") == "Account" and obj.get("objectId"):
                        sf_account_ids.add(obj["objectId"])

            for acct_id in sf_account_ids:
                account_calls[acct_id].append(call)

        cursor = (data.get("records") or {}).get("cursor")
        if not cursor:
            break

    print(f"  Gong: {total} call(s) fetched, mapped to {len(account_calls)} SF account(s)")
    return dict(account_calls)


# ── Scoring ─────────────────────────────────────────────────────────────────

def compute_days_since_activity(tasks: list[dict], gong_calls: list) -> int | None:
    """Return minimum days-since-last-touch across tasks and Gong calls."""
    candidates: list[int] = []

    for t in tasks:
        d = days_since_date(t.get("ActivityDate"))
        if d is not None:
            candidates.append(d)

    for call in gong_calls:
        started = (call.get("metaData") or {}).get("started")
        if started:
            d = days_since_date(started)
            if d is not None:
                candidates.append(d)

    return min(candidates) if candidates else None


def gong_calls_within(gong_calls: list, days: int) -> list:
    """Filter Gong calls to those within the last N days."""
    cutoff = date.today() - timedelta(days=days)
    result = []
    for call in gong_calls:
        started = (call.get("metaData") or {}).get("started")
        if started and date.fromisoformat(started[:10]) >= cutoff:
            result.append(call)
    return result


def momentum_score(tasks: list[dict], gong_calls: list, ns_history: list[dict]) -> int:
    """
    Compute momentum score 0–5.
      +1  any task in last 30d
      +1  task in last 14d
      +1  next step updated in last 10d
      +1  Gong call in last 30d
      +1  Gong call in last 7d
    """
    score = 0

    task_dates = [
        date.fromisoformat(t["ActivityDate"][:10])
        for t in tasks
        if t.get("ActivityDate")
    ]
    if task_dates:
        most_recent_task = max(task_dates)
        score += 1  # any task in 30d
        if (date.today() - most_recent_task).days <= 14:
            score += 1

    if ns_history:
        days_ns = (date.today() - ns_history[0]["date"]).days
        if days_ns <= 10:
            score += 1

    if gong_calls_within(gong_calls, 30):
        score += 1
    if gong_calls_within(gong_calls, 7):
        score += 1

    return min(score, 5)


def risk_score(days_silent: int | None, ns_history: list[dict], tasks: list, gong_calls: list) -> int:
    """
    Compute risk score 0–4.
      4  no activity in 30d+ (or never)
      3  no activity in 21–29d
      2  no activity in 14–20d
      1  activity present but next step stale (>14d) and no gong call in 14d
      0  otherwise
    """
    if days_silent is None or days_silent > 30:
        return 4
    if days_silent > 21:
        return 3
    if days_silent > 14:
        return 2

    # Mild risk: have recent activity but next step is stale and no recent Gong call
    ns_stale = not ns_history or (date.today() - ns_history[0]["date"]).days > 14
    no_recent_gong = not gong_calls_within(gong_calls, 14)
    if ns_stale and no_recent_gong and days_silent > 7:
        return 1

    return 0


def deal_status(momentum: int, risk: int) -> str:
    """
    Assign status bucket (first matching rule).
    Advancing threshold is 3 (not 4) because Gong CRM context may be unavailable,
    limiting max momentum from SF tasks alone to 3.
    """
    if risk >= 3:
        return STATUS_CRITICAL
    if risk >= 2 or (risk == 1 and momentum <= 1):
        return STATUS_AT_RISK
    if momentum >= 3:
        return STATUS_ADVANCING
    if momentum >= 2 or risk == 0:
        return STATUS_STABLE
    return STATUS_SILENT


# ── Claude Synopsis ──────────────────────────────────────────────────────────

def generate_synopsis(deal: dict, client) -> str:
    """
    Generate a 4-sentence internal deal synopsis using Claude haiku.
    Covers: current status, last interaction, key risk/opportunity, discussion point.
    """
    parts: list[str] = [
        f"Deal: {deal['name']} at {deal['account_name']}",
        f"Stage: {deal['stage']} | Amount: ${(deal.get('amount') or 0):,.0f} | Close: {deal['close_date']}",
        f"Owner: {deal['owner_name']}",
        f"Status: {deal['status']} | Momentum {deal['momentum']}/5 | Risk {deal['risk']}/4",
    ]

    ns = deal.get("next_step") or ""
    if ns:
        parts.append(f"Next step (SF): {ns[:200]}")

    ns_history = deal.get("ns_history") or []
    if ns_history:
        e = ns_history[0]
        parts.append(f"Last next-step update ({e['date']}): {e['text'][:200]}")
    else:
        parts.append("Next-step history: none recorded")

    tasks = deal.get("tasks") or []
    recent_tasks = tasks[:5]
    if recent_tasks:
        task_lines = []
        for t in recent_tasks:
            desc = (t.get("Description") or "")[:120].strip().replace("\n", " ")
            line = f"  {t.get('ActivityDate','?')} {t.get('Type','?')}: {t.get('Subject','?')[:60]}"
            if desc:
                line += f" — {desc}"
            task_lines.append(line)
        parts.append("Recent SF activity:\n" + "\n".join(task_lines))
    else:
        parts.append("Recent SF activity: none in last 30 days")

    gong_calls = deal.get("gong_calls") or []
    recent_calls = gong_calls[:2]
    if recent_calls:
        gong_lines = []
        for call in recent_calls:
            md    = call.get("metaData") or {}
            brief = (call.get("content") or {}).get("brief") or ""
            kpts  = (call.get("content") or {}).get("keyPoints") or []
            started = (md.get("started") or "")[:10]
            summary = brief[:250] if brief else "; ".join(str(k) for k in kpts[:3])
            gong_lines.append(f"  {started}: {summary}")
        parts.append("Gong calls:\n" + "\n".join(gong_lines))

    context_text = "\n".join(parts)

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{
                "role": "user",
                "content": (
                    "Write a 4-sentence internal deal digest for a sales manager reviewing "
                    "this opportunity before a team meeting. Cover: "
                    "(1) current momentum and where things stand, "
                    "(2) most recent meaningful interaction, "
                    "(3) the key risk or opportunity right now, "
                    "(4) specific question or action to discuss with the rep.\n\n"
                    f"{context_text}\n\n"
                    "Write only the 4 sentences. No headers, no bullets."
                ),
            }],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        print(f"  Claude synopsis error for {deal['name']}: {e}", file=sys.stderr)
        return "(synopsis unavailable)"


# ── Slack Message Builder ────────────────────────────────────────────────────

def _fmt_amount(amount) -> str:
    if not amount:
        return "$0"
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.1f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.0f}K"
    return f"${amount:,.0f}"


def _fmt_close(close_date_str: str | None) -> str:
    if not close_date_str:
        return "?"
    try:
        d = date.fromisoformat(close_date_str[:10])
        days_to = (d - date.today()).days
        if days_to < 0:
            return f"{d.strftime('%b %-d')} ⚠️overdue"
        if days_to <= 30:
            return f"{d.strftime('%b %-d')} ({days_to}d)"
        return d.strftime("%b %-d")
    except ValueError:
        return close_date_str[:10]


def build_message(deals: list[dict], date_str: str) -> str:
    # Partition by status
    by_status: dict[str, list] = defaultdict(list)
    for d in deals:
        by_status[d["status"]].append(d)

    critical  = by_status[STATUS_CRITICAL]
    at_risk   = by_status[STATUS_AT_RISK]
    advancing = by_status[STATUS_ADVANCING]
    stable    = by_status[STATUS_STABLE]
    silent    = by_status[STATUS_SILENT]

    total_opps = len(deals)
    flagged    = len(critical) + len(at_risk)
    lines: list[str] = []

    # ── Header ───────────────────────────────────────────────────────────────
    lines += [
        f"*📋 Team Meeting Digest — {date_str}*",
        f"_{total_opps} open deals across AE team · {flagged} flagged for attention_",
        "",
    ]

    # ── Team Pulse ───────────────────────────────────────────────────────────
    lines += [
        "*Team Pulse*",
        f"• 🔴 Critical: *{len(critical)}*  🟠 At Risk: *{len(at_risk)}*  "
        f"🟢 Advancing: *{len(advancing)}*  🟡 Stable: *{len(stable)}*  ⚫ Silent: *{len(silent)}*",
    ]

    # Overdue close dates
    overdue = [d for d in deals if _is_overdue(d.get("close_date"))]
    if overdue:
        lines.append(f"• ⚠️  {len(overdue)} deal(s) past close date: "
                     + ", ".join(f"*{d['name']}*" for d in overdue[:5]))
    lines.append("")

    # ── Critical + At Risk (with synopsis) ───────────────────────────────────
    flagged_deals = critical + at_risk
    if flagged_deals:
        lines.append("*🚨 Needs Attention*")
        lines.append("─" * 36)
        for d in flagged_deals:
            emoji = STATUS_EMOJI[d["status"]]
            days_silent_str = f"{d['days_silent']}d silent" if d["days_silent"] is not None else "never touched"
            lines += [
                f"{emoji} *{d['name']}* · {d['stage']} · {_fmt_amount(d.get('amount'))} · Close {_fmt_close(d.get('close_date'))}",
                f"↳ Owner: {d['owner_name']} · Tier: {d.get('tier','?')} · "
                f"Momentum {d['momentum']}/5 · Risk {d['risk']}/4 · {days_silent_str}",
            ]
            if d.get("synopsis"):
                lines.append(f"_{d['synopsis']}_")
            lines.append("")

    # ── Advancing (with synopsis) ─────────────────────────────────────────────
    if advancing:
        lines.append("*🟢 Advancing — Expand / Accelerate*")
        lines.append("─" * 36)
        for d in advancing:
            recent_call = _latest_gong_call(d.get("gong_calls", []))
            call_note   = f"Gong call {recent_call}" if recent_call else "no recent Gong"
            lines += [
                f"*{d['name']}* · {d['stage']} · {_fmt_amount(d.get('amount'))} · Close {_fmt_close(d.get('close_date'))}",
                f"↳ Owner: {d['owner_name']} · Momentum {d['momentum']}/5 · {call_note}",
            ]
            if d.get("synopsis"):
                lines.append(f"_{d['synopsis']}_")
            lines.append("")

    # ── Stable list ───────────────────────────────────────────────────────────
    if stable:
        lines.append("*🟡 Stable*")
        stable_sorted = sorted(stable, key=lambda d: d.get("close_date") or "9999")
        for d in stable_sorted:
            lines.append(
                f"• *{d['name']}* ({d['owner_name']}) · {d['stage']} · "
                f"{_fmt_amount(d.get('amount'))} · Close {_fmt_close(d.get('close_date'))} · "
                f"M{d['momentum']}/R{d['risk']}"
            )
        lines.append("")

    # ── Silent table ──────────────────────────────────────────────────────────
    if silent:
        lines.append(f"*⚫ Silent — {len(silent)} deal(s) with no recent signal*")
        for d in silent:
            days_str = f"{d['days_silent']}d" if d["days_silent"] is not None else "never"
            lines.append(
                f"• *{d['name']}* ({d['owner_name']}) · {d['stage']} · "
                f"{_fmt_amount(d.get('amount'))} · {days_str} silent"
            )
        lines.append("")

    # ── Discussion Priorities ─────────────────────────────────────────────────
    # Rank: Critical first, then At Risk, then by risk desc, momentum asc
    priority_pool = critical + at_risk
    if len(priority_pool) < DISCUSSION_TOP_N:
        priority_pool += advancing
    priority_pool = priority_pool[:DISCUSSION_TOP_N]

    if priority_pool:
        lines.append(f"*💬 Discussion Priorities ({len(priority_pool)})*")
        for i, d in enumerate(priority_pool, 1):
            emoji = STATUS_EMOJI[d["status"]]
            lines.append(f"{i}. {emoji} *{d['name']}* ({d['owner_name']}) — {d['stage']}, "
                         f"Close {_fmt_close(d.get('close_date'))}")
        lines.append("")

    return "\n".join(lines)


def _is_overdue(close_date_str: str | None) -> bool:
    if not close_date_str:
        return False
    try:
        return date.fromisoformat(close_date_str[:10]) < date.today()
    except ValueError:
        return False


def _latest_gong_call(calls: list) -> str | None:
    """Return human-readable date of most recent Gong call, or None."""
    best = None
    for call in calls:
        started = (call.get("metaData") or {}).get("started")
        if started:
            d = date.fromisoformat(started[:10])
            if best is None or d > best:
                best = d
    if best:
        return best.strftime("%b %-d")
    return None


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
                        help="Report date string, e.g. 'April 21, 2026' (default: today)")
    parser.add_argument("--days", type=int, default=30,
                        help="Activity lookback window in days (default: 30)")
    parser.add_argument("--no-synopsis", action="store_true",
                        help="Skip Claude synopsis generation")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    today     = date.today()
    date_str  = args.date or today.strftime("%B %-d, %Y")
    days      = args.days
    bot_token = os.getenv("SLACK_BOT_TOKEN", "")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")

    print(f"Date: {date_str}  |  Lookback: {days}d")

    # ── Salesforce setup ──────────────────────────────────────────────────────
    print("Connecting to Salesforce...")
    sf = connect_sf()

    print("Fetching AE roster...")
    ae_records = fetch_ae_roster(sf)
    if not ae_records:
        print("No AEs found — aborting.")
        return
    ae_ids = [r["Id"] for r in ae_records]
    print(f"  {len(ae_records)} AE(s): {', '.join(r['Name'] for r in ae_records)}")

    print("Fetching open opportunities...")
    opp_records = fetch_open_opps(sf, ae_ids)
    if not opp_records:
        print("No open opportunities found.")
        return
    print(f"  {len(opp_records)} open opp(s)")

    # Collect unique account IDs for batch task fetch
    account_ids: list[str] = list({
        r["AccountId"] for r in opp_records if r.get("AccountId")
    })

    print(f"Fetching SF Tasks for {len(account_ids)} account(s) (last {days}d)...")
    all_tasks = fetch_recent_tasks(sf, account_ids, days)
    print(f"  {len(all_tasks)} task(s)")

    # Index tasks by AccountId
    tasks_by_account: dict[str, list] = defaultdict(list)
    for t in all_tasks:
        if t.get("AccountId"):
            tasks_by_account[t["AccountId"]].append(t)

    # ── Gong calls ────────────────────────────────────────────────────────────
    print(f"Fetching Gong calls (last {days}d)...")
    now     = datetime.now(tz=timezone.utc)
    from_dt = now - timedelta(days=days)
    gong_by_account = fetch_gong_extensive(from_dt, now)

    # ── Enrich each opportunity ───────────────────────────────────────────────
    print("Computing scores...")
    deals: list[dict] = []

    for opp in opp_records:
        opp_id     = opp["Id"]
        account_id = opp.get("AccountId") or ""
        account    = opp.get("Account") or {}

        tasks      = tasks_by_account.get(account_id, [])
        gong_calls = gong_by_account.get(account_id, [])

        ns_historical = opp.get("Next_Step_Historical__c") or ""
        ns_history    = parse_next_step_history(ns_historical)

        days_silent = compute_days_since_activity(tasks, gong_calls)
        m = momentum_score(tasks, gong_calls, ns_history)
        r = risk_score(days_silent, ns_history, tasks, gong_calls)
        s = deal_status(m, r)

        deals.append({
            "id":           opp_id,
            "name":         opp.get("Name") or "?",
            "account_id":   account_id,
            "account_name": account.get("Name") or "?",
            "tier":         account.get("Account_Tier__c") or "?",
            "owner_name":   (opp.get("Owner") or {}).get("Name") or "?",
            "stage":        opp.get("StageName") or "?",
            "amount":       opp.get("Amount"),
            "close_date":   opp.get("CloseDate"),
            "next_step":    opp.get("NextStep"),
            "ns_history":   ns_history,
            "tasks":        tasks,
            "gong_calls":   gong_calls,
            "days_silent":  days_silent,
            "momentum":     m,
            "risk":         r,
            "status":       s,
            "synopsis":     None,
        })

    # Status summary
    by_status: dict[str, int] = defaultdict(int)
    for d in deals:
        by_status[d["status"]] += 1
    print("  Status breakdown: " + "  ".join(
        f"{STATUS_EMOJI[k]} {k}: {v}" for k, v in sorted(by_status.items())
    ))

    # ── Claude synopses for flagged + advancing deals ─────────────────────────
    # Cap Critical at top 20 soonest close dates to control API cost.
    # Include all At Risk and Advancing (typically smaller buckets).
    critical_for_synopsis = sorted(
        [d for d in deals if d["status"] == STATUS_CRITICAL],
        key=lambda d: d.get("close_date") or "9999",
    )[:20]
    needs_synopsis = critical_for_synopsis + [
        d for d in deals
        if d["status"] in (STATUS_AT_RISK, STATUS_ADVANCING)
    ]

    if needs_synopsis and not args.no_synopsis and anthropic_key:
        import anthropic
        client = anthropic.Anthropic(api_key=anthropic_key)
        print(f"Generating {len(needs_synopsis)} Claude synopsis(es)...")

        def _gen(deal):
            deal["synopsis"] = generate_synopsis(deal, client)
            return deal["name"]

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(_gen, d): d["name"] for d in needs_synopsis}
            for fut in as_completed(futures):
                try:
                    name = fut.result()
                    print(f"  ✓ {name}")
                except Exception as e:
                    print(f"  ✗ {futures[fut]}: {e}", file=sys.stderr)
    elif args.no_synopsis:
        print("Synopsis generation skipped (--no-synopsis)")
    else:
        print("⚠️  No ANTHROPIC_API_KEY — skipping synopses", file=sys.stderr)

    # ── Build + post ─────────────────────────────────────────────────────────
    message = build_message(deals, date_str)

    print("\n--- Slack preview (first 800 chars) ---")
    print(message[:800])
    print("---")

    if bot_token:
        post_to_slack(bot_token, message)
    else:
        print("⚠️  No SLACK_BOT_TOKEN — skipping Slack post", file=sys.stderr)


if __name__ == "__main__":
    main()
