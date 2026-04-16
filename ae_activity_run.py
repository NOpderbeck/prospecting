"""
ae_activity_run.py — Weekly AE Activity Dashboard.

Pulls Salesforce Tasks/Events + Gong conversations for each AE,
computes activity/engagement/coverage metrics, and posts a structured
Slack report to #sales-leadership.

AE roster: all active users with UserRole.Name = 'Sales - AE',
plus David Wacker and Haroon Anwar (VP/non-standard role).
BluPlanet placeholder accounts are excluded.

Usage (local):
    python3 ae_activity_run.py [--date "April 21, 2026"] [--days 7]

Environment variables:
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN  — Salesforce creds
    GONG_API_KEY, GONG_API_SECRET                — Gong REST credentials
    SLACK_BOT_TOKEN                              — Slack bot token
"""

import argparse
import base64
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv

ENV_PATH      = os.path.join(os.path.dirname(__file__), ".env")
SLACK_CHANNEL = "#sales-leadership"
GONG_BASE     = "https://us-64844.api.gong.io"

# AEs who don't have UserRole = 'Sales - AE' but should be included
ADDITIONAL_AE_IDS: set[str] = {
    "005fo000000d1nSAAQ",  # David Wacker (VP, AE)
    "005fo000000d3AvAAI",  # Haroon Anwar
}

# SF user name fragments to exclude (case-insensitive); catches BluPlanet placeholder
EXCLUDE_NAME_FRAGMENTS: list[str] = ["bluplanet", "blue planet"]

# Gong calls shorter than this are not counted as conversations
MIN_CONV_SECONDS = 120

TIER1_VALUES  = {"1. TARGET ACCOUNT", "Tier 1"}
TIER2A_VALUES = {"2.A", "2.A PROSPECT", "Tier 2"}


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
        SELECT Id, Name, Email, UserRole.Name, CreatedDate
        FROM User
        WHERE IsActive = true
        AND UserRole.Name = 'Sales - AE'
        ORDER BY Name
    """)

    # Add additional AEs by explicit SF ID
    if ADDITIONAL_AE_IDS:
        ids_str = "', '".join(ADDITIONAL_AE_IDS)
        extras = soql(sf, f"""
            SELECT Id, Name, Email, UserRole.Name, CreatedDate
            FROM User
            WHERE Id IN ('{ids_str}')
            AND IsActive = true
        """)
        existing = {r["Id"] for r in records}
        for r in extras:
            if r["Id"] not in existing:
                records.append(r)
                existing.add(r["Id"])

    # Remove placeholder / excluded names
    return [r for r in records if not is_excluded(r.get("Name", ""))]


def fetch_ae_activity(sf, ae_id: str, days: int) -> dict:
    """Pull SF Tasks and Events owned by this AE in the last N days."""
    from datetime import date, timedelta
    since_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    since_dt   = f"{since_date}T00:00:00Z"

    tasks = soql(sf, f"""
        SELECT Id, Type, Subject, ActivityDate, Status, AccountId
        FROM Task
        WHERE OwnerId = '{ae_id}'
        AND ActivityDate >= {since_date}
        ORDER BY ActivityDate DESC
    """)
    # SF Events are excluded: Einstein Activity Capture creates unreliable event volumes.
    # Engagement signal comes from Gong conversations instead.
    return {"tasks": tasks, "events": []}


def batch_fetch_account_tiers(sf, account_ids: set) -> dict[str, str]:
    """Return {account_id: Account_Tier__c} for the given set."""
    if not account_ids:
        return {}
    # SOQL IN clause limit is 10,000 items; fine for our volume
    ids_str = "', '".join(account_ids)
    rows = soql(sf, f"""
        SELECT Id, Account_Tier__c
        FROM Account
        WHERE Id IN ('{ids_str}')
    """)
    return {r["Id"]: (r.get("Account_Tier__c") or "") for r in rows}


# ── Gong ────────────────────────────────────────────────────────────────────

def _gong_headers() -> dict:
    key    = os.environ["GONG_API_KEY"]
    secret = os.environ["GONG_API_SECRET"]
    token  = base64.b64encode(f"{key}:{secret}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def fetch_gong_user_map() -> dict[str, str]:
    """Return {email_lower: gong_user_id} for all Gong users."""
    import requests
    url    = f"{GONG_BASE}/v2/users"
    result: dict[str, str] = {}
    cursor = None

    while True:
        params = {"cursor": cursor} if cursor else {}
        resp   = requests.get(url, headers=_gong_headers(), params=params, timeout=15)
        resp.raise_for_status()
        data   = resp.json()
        for u in data.get("users", []):
            email = (u.get("emailAddress") or "").lower()
            if email:
                result[email] = u["id"]
        cursor = data.get("records", {}).get("cursor")
        if not cursor:
            break

    return result


def fetch_gong_call_counts(from_dt: datetime, to_dt: datetime) -> dict[str, int]:
    """
    Return {gong_user_id: conversation_count} for all calls in [from_dt, to_dt]
    with duration >= MIN_CONV_SECONDS, attributed by primaryUserId.
    """
    import requests
    url      = f"{GONG_BASE}/v2/calls"
    counts: dict[str, int] = defaultdict(int)
    cursor   = None
    from_str = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str   = to_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        params: dict = {"fromDateTime": from_str, "toDateTime": to_str}
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(url, headers=_gong_headers(), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for call in data.get("calls", []):
            dur = call.get("duration") or 0
            if dur >= MIN_CONV_SECONDS:
                uid = call.get("primaryUserId")
                if uid:
                    counts[uid] += 1

        cursor = data.get("records", {}).get("cursor")
        if not cursor:
            break

    return dict(counts)


# ── Metrics computation ─────────────────────────────────────────────────────

def _classify_task(task: dict) -> dict:
    t    = task.get("Type") or ""
    subj = task.get("Subject") or ""
    return {
        "email":    t == "Email",
        "call":     t == "Call",
        "gong_out": "[Gong Out]" in subj or "[Gong In]" in subj,
        "apollo":   "[Apollo" in subj,
    }


def compute_metrics(sf_activity: dict, tier_map: dict, conversations: int) -> dict:
    tasks = sf_activity["tasks"]

    emails = calls = gong_out = apollo = 0
    t1_ids: set  = set()
    t2a_ids: set = set()
    all_ids: set = set()

    for task in tasks:
        c = _classify_task(task)
        if c["email"]:    emails   += 1
        if c["call"]:     calls    += 1
        if c["gong_out"]: gong_out += 1
        if c["apollo"]:   apollo   += 1

        acc = task.get("AccountId")
        if acc:
            all_ids.add(acc)
            tier = tier_map.get(acc, "")
            if tier in TIER1_VALUES:  t1_ids.add(acc)
            if tier in TIER2A_VALUES: t2a_ids.add(acc)

    # Outreach = any intentional touch: SF email/call + Gong-synced + Apollo
    outreach = emails + calls + gong_out + apollo

    return {
        "emails":          emails,
        "calls":           calls,
        "gong_out":        gong_out,
        "apollo":          apollo,
        "outreach":        outreach,
        "conversations":   conversations,
        "accounts":        len(all_ids),
        "t1_accounts":     len(t1_ids),
        "t2a_accounts":    len(t2a_ids),
        "target_accounts": len(t1_ids | t2a_ids),
    }


# ── Benchmarks ──────────────────────────────────────────────────────────────

def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    sv = sorted(values)
    k  = (len(sv) - 1) * pct / 100
    lo = int(k)
    hi = min(lo + 1, len(sv) - 1)
    return sv[lo] + (sv[hi] - sv[lo]) * (k - lo)


def compute_benchmarks(all_metrics: list[dict]) -> dict[str, dict]:
    keys = ["outreach", "conversations", "t1_accounts", "target_accounts"]
    out  = {}
    for key in keys:
        vals = [m[key] for m in all_metrics]
        out[key] = {
            "p25": round(_percentile(vals, 25), 1),
            "p50": round(_percentile(vals, 50), 1),
            "p75": round(_percentile(vals, 75), 1),
        }
    return out


def classify_quadrant(metrics: dict, benchmarks: dict) -> str:
    activity   = metrics["outreach"]
    engagement = metrics["conversations"]
    med_act    = benchmarks["outreach"]["p50"]
    med_eng    = benchmarks["conversations"]["p50"]

    if activity >= med_act and engagement >= med_eng:
        return "🟢 Full Coverage"
    if activity >= med_act:
        return "🟡 Active, Low Engagement"
    if engagement >= med_eng:
        return "🟠 Low Activity, Engaged"
    return "🔴 Low Activity & Engagement"


def tenure_cohort(created_date_str: str | None) -> str:
    if not created_date_str:
        return "Wk 6+"
    d    = date.fromisoformat(created_date_str[:10])
    days = (date.today() - d).days
    if days <= 14:  return "Wk 1-2"
    if days <= 42:  return "Wk 3-6"
    return "Wk 6+"


# ── Slack message builder ───────────────────────────────────────────────────

def build_message(ae_rows: list[dict], benchmarks: dict, date_str: str, days: int) -> str:
    n = len(ae_rows)
    lines: list[str] = []

    total_outreach = sum(r["m"]["outreach"] for r in ae_rows)
    total_convs    = sum(r["m"]["conversations"] for r in ae_rows)
    total_t1       = sum(r["m"]["t1_accounts"] for r in ae_rows)
    total_t2a      = sum(r["m"]["t2a_accounts"] for r in ae_rows)

    lines += [
        f"📊 *AE Activity Dashboard — {date_str}* _(last {days} days)_",
        "",
        "*Team Summary*",
        f"• {n} AEs · {total_outreach} outreach touches · {total_convs} Gong conversations",
        f"• T1 accounts touched: *{total_t1}* · T2A accounts touched: *{total_t2a}*",
        "",
    ]

    # ── Benchmark Bands ──────────────────────────────────────────────────────
    bm = benchmarks
    lines += [
        "*Benchmark Bands (P25 / P50 / P75)*",
        (f"• Outreach: {int(bm['outreach']['p25'])} / *{int(bm['outreach']['p50'])}* / {int(bm['outreach']['p75'])}   "
         f"Conversations: {int(bm['conversations']['p25'])} / *{int(bm['conversations']['p50'])}* / {int(bm['conversations']['p75'])}"),
        (f"• T1 Accts: {int(bm['t1_accounts']['p25'])} / *{int(bm['t1_accounts']['p50'])}* / {int(bm['t1_accounts']['p75'])}   "
         f"Target Accts: {int(bm['target_accounts']['p25'])} / *{int(bm['target_accounts']['p50'])}* / {int(bm['target_accounts']['p75'])}"),
        "",
    ]

    # ── AE Scorecard ─────────────────────────────────────────────────────────
    sorted_rows = sorted(ae_rows, key=lambda r: r["m"]["outreach"], reverse=True)

    lines.append("*AE Scorecard*")
    lines.append("```")
    lines.append(
        f"{'AE':<22} {'Outreach':>8} {'Convs':>5} "
        f"{'T1':>3} {'T2A':>4} {'Cohort':<7}  Quadrant"
    )
    lines.append("─" * 82)
    for row in sorted_rows:
        m = row["m"]
        lines.append(
            f"{row['name']:<22} {m['outreach']:>8} {m['conversations']:>5} "
            f"{m['t1_accounts']:>3} {m['t2a_accounts']:>4} {row['cohort']:<7}  {row['quadrant']}"
        )
    lines.append("```")
    lines.append("")

    # ── Onboarding Watch ─────────────────────────────────────────────────────
    onboarding = [r for r in sorted_rows if r["cohort"] in ("Wk 1-2", "Wk 3-6")]
    if onboarding:
        lines.append("*Onboarding Watch*")
        for row in onboarding:
            m = row["m"]
            lines.append(
                f"• *{row['name']}* ({row['cohort']}) — "
                f"{m['outreach']} outreach · {m['conversations']} convs · "
                f"{m['t1_accounts']} T1 accts · {row['quadrant']}"
            )
        lines.append("")

    # ── Outliers ─────────────────────────────────────────────────────────────
    p75_out  = benchmarks["outreach"]["p75"]
    p25_out  = benchmarks["outreach"]["p25"]
    p75_conv = benchmarks["conversations"]["p75"]

    senior = [r for r in sorted_rows if r["cohort"] == "Wk 6+"]
    over  = [r for r in senior if r["m"]["outreach"] >= p75_out and r["m"]["conversations"] >= p75_conv]
    under = [r for r in senior if r["m"]["outreach"] <= p25_out]

    if over or under:
        lines.append("*Outliers*")
        for row in over:
            m = row["m"]
            lines.append(
                f"⬆️  *{row['name']}* — {m['outreach']} outreach · {m['conversations']} convs "
                f"(above P75 on both)"
            )
        for row in under:
            m = row["m"]
            lines.append(
                f"⬇️  *{row['name']}* — {m['outreach']} outreach · {m['conversations']} convs "
                f"(at or below P25 outreach)"
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
                        help="Report date string, e.g. 'April 21, 2026' (default: today)")
    parser.add_argument("--days", type=int, default=7,
                        help="Lookback window in days (default: 7)")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    today    = date.today()
    date_str = args.date or today.strftime("%B %-d, %Y")
    days     = args.days
    bot_token = os.getenv("SLACK_BOT_TOKEN", "")

    print(f"Date: {date_str}  |  Lookback: {days} day(s)")

    # ── Salesforce: roster ──────────────────────────────────────────────────
    print("Connecting to Salesforce...")
    sf = connect_sf()

    print("Fetching AE roster...")
    ae_records = fetch_ae_roster(sf)
    if not ae_records:
        print("No AEs found — aborting.")
        return
    for r in ae_records:
        role = (r.get("UserRole") or {}).get("Name", "N/A")
        print(f"  {r['Name']} ({r['Email']}) [{role}]")
    print(f"  → {len(ae_records)} AE(s)")

    # ── Gong: user map + call counts ────────────────────────────────────────
    print("Fetching Gong user map...")
    gong_email_map = fetch_gong_user_map()
    print(f"  {len(gong_email_map)} Gong user(s) indexed")

    sf_to_gong: dict[str, str] = {}
    for ae in ae_records:
        email   = (ae.get("Email") or "").lower()
        gong_id = gong_email_map.get(email)
        if gong_id:
            sf_to_gong[ae["Id"]] = gong_id
        else:
            print(f"  ⚠️  No Gong match for {ae['Name']} ({email})", file=sys.stderr)

    print(f"Fetching Gong conversations (last {days} days)...")
    now     = datetime.now(tz=timezone.utc)
    from_dt = now - timedelta(days=days)
    gong_counts = fetch_gong_call_counts(from_dt, now)
    total_convs = sum(gong_counts.values())
    print(f"  {total_convs} qualifying conversation(s) across all users")

    # ── Salesforce: activity per AE ─────────────────────────────────────────
    print(f"Fetching SF Tasks + Events per AE...")
    all_account_ids: set[str] = set()
    ae_activity: dict[str, dict] = {}

    for ae in ae_records:
        print(f"  {ae['Name']}...")
        activity = fetch_ae_activity(sf, ae["Id"], days)
        ae_activity[ae["Id"]] = activity
        for t in activity["tasks"]:
            if t.get("AccountId"):
                all_account_ids.add(t["AccountId"])
        for e in activity["events"]:
            if e.get("AccountId"):
                all_account_ids.add(e["AccountId"])

    # ── Batch-fetch account tiers ───────────────────────────────────────────
    print(f"Fetching tiers for {len(all_account_ids)} account(s)...")
    tier_map = batch_fetch_account_tiers(sf, all_account_ids)

    # ── Compute metrics ─────────────────────────────────────────────────────
    ae_rows: list[dict] = []
    for ae in ae_records:
        ae_id    = ae["Id"]
        gong_id  = sf_to_gong.get(ae_id)
        convs    = gong_counts.get(gong_id, 0) if gong_id else 0
        metrics  = compute_metrics(ae_activity[ae_id], tier_map, convs)
        ae_rows.append({
            "id":     ae_id,
            "name":   ae["Name"],
            "email":  ae.get("Email", ""),
            "m":      metrics,
            "cohort": tenure_cohort(ae.get("CreatedDate")),
            "quadrant": "",  # computed after benchmarks
        })

    benchmarks = compute_benchmarks([r["m"] for r in ae_rows])
    for row in ae_rows:
        row["quadrant"] = classify_quadrant(row["m"], benchmarks)

    # ── Console preview ─────────────────────────────────────────────────────
    print("\nMetrics preview:")
    for row in sorted(ae_rows, key=lambda r: r["m"]["outreach"], reverse=True):
        m = row["m"]
        print(
            f"  {row['name']:<25}  outreach={m['outreach']:>3}  "
            f"convs={m['conversations']:>2}  "
            f"t1={m['t1_accounts']:>2}  t2a={m['t2a_accounts']:>2}  "
            f"[{row['cohort']}]  {row['quadrant']}"
        )

    # ── Build + post ─────────────────────────────────────────────────────────
    message = build_message(ae_rows, benchmarks, date_str, days)

    print("\n--- Slack preview (first 600 chars) ---")
    print(message[:600])
    print("---")

    if bot_token:
        post_to_slack(bot_token, message)
    else:
        print("⚠️  No SLACK_BOT_TOKEN — skipping Slack post", file=sys.stderr)


if __name__ == "__main__":
    main()
