"""
digest_run.py — Weekly Team Meeting Digest.

Pulls open opportunities for 4 target AEs, enriches with Granola meeting notes
and Slack channel activity, then asks Claude to write a narrative per-deal summary.
Posts to #team-weekly-digest.

Runs Monday 7 AM PT as Cloud Run job `digest-report`.

Usage (local):
    python3 digest_run.py [--date "April 21, 2026"] [--no-post]

Environment variables:
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN
    SLACK_BOT_TOKEN       — for posting to #team-weekly-digest
    SLACK_USER_TOKEN      — xoxp- token for search.messages (search:read scope)
    GRANOLA_API_KEY       — from Granola → Settings → API
    ANTHROPIC_API_KEY
"""

import argparse
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import requests

ENV_PATH      = os.path.join(os.path.dirname(__file__), ".env")
SLACK_CHANNEL = "C0ATEN05Q2W"  # #team-weekly-digest — ID is rename-safe

# The 4 AEs this digest covers (matched against SF User.Name)
TARGET_AE_NAMES = [
    "David Wacker",
    "Ryan Reed",
    "Ryan Allred",
    "Andrew Miller-McKeever",
]

# AE name → you.com email (for matching Granola attendees)
AE_EMAILS = {
    "David Wacker":          "david.wacker@you.com",
    "Ryan Reed":             "ryan.reed@you.com",
    "Ryan Allred":           "ryan.allred@you.com",
    "Andrew Miller-McKeever": "andrew.miller-mckeever@you.com",
}

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
    q_start_month = ((today.month - 1) // 3) * 3 + 1
    q_end_month   = q_start_month + 2
    import calendar
    last_day = calendar.monthrange(today.year, q_end_month)[1]
    start = date(today.year, q_start_month, 1).strftime("%Y-%m-%d")
    end   = date(today.year, q_end_month, last_day).strftime("%Y-%m-%d")
    return start, end


def fetch_open_opps(sf, ae_ids: list[str]) -> list[dict]:
    """Fetch open opps closing this quarter (today → end of quarter)."""
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


# ── Granola ─────────────────────────────────────────────────────────────────

def fetch_granola_note_detail(api_key: str, note_id: str) -> dict | None:
    """Fetch full note including summary_markdown."""
    try:
        resp = requests.get(
            f"https://public-api.granola.ai/v1/notes/{note_id}",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"  Granola detail error ({note_id}): {e}", file=sys.stderr)
    return None


def fetch_granola_notes(api_key: str, days: int = 14) -> list[dict]:
    """
    Fetch recent Granola notes (list only — titles + metadata).
    Returns list of note stubs: {id, title, created_at, attendees}.
    """
    since = (date.today() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    try:
        resp = requests.get(
            "https://public-api.granola.ai/v1/notes",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"created_after": since, "page_size": 30},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"  Granola list error: {resp.status_code}", file=sys.stderr)
            return []
        data = resp.json()
        notes = data.get("notes", [])
        print(f"  Granola: {len(notes)} note(s) in last {days}d")
        return notes
    except Exception as e:
        print(f"  Granola fetch error: {e}", file=sys.stderr)
        return []


def _account_keywords(account_name: str) -> list[str]:
    """
    Extract meaningful search tokens from an account name.
    'Recorded Future' → ['Recorded Future', 'Recorded']
    Drops generic words like Inc, LLC, Corp.
    """
    stopwords = {"inc", "llc", "corp", "corporation", "ltd", "co", "the", "and", "&"}
    words = [w for w in account_name.split() if w.lower() not in stopwords]
    keywords = [account_name]  # full name first
    if words:
        keywords.append(words[0])   # first meaningful word as fallback
    return keywords


def match_granola_notes(all_notes: list[dict], account_name: str, ae_email: str) -> list[dict]:
    """
    Return notes that are likely about this account.
    Matches on: account keyword in title OR AE email in attendees.
    Returns only stubs (no detail fetched yet).
    """
    keywords = _account_keywords(account_name)
    matched = []
    for note in all_notes:
        title = (note.get("title") or "").lower()
        attendee_emails = [
            (a.get("email") or "").lower()
            for a in (note.get("attendees") or [])
        ]
        title_match = any(kw.lower() in title for kw in keywords)
        ae_match    = ae_email.lower() in attendee_emails
        if title_match or ae_match:
            matched.append(note)
    return matched


def enrich_granola_notes(api_key: str, note_stubs: list[dict]) -> list[dict]:
    """Fetch full detail (summary_markdown) for each matched stub."""
    enriched = []
    for stub in note_stubs:
        detail = fetch_granola_note_detail(api_key, stub["id"])
        if detail:
            enriched.append(detail)
        else:
            enriched.append(stub)  # fall back to stub if detail fails
    return enriched


# ── Slack ────────────────────────────────────────────────────────────────────

def fetch_slack_messages(user_token: str, account_name: str, days: int = 14) -> list[str]:
    """
    Search Slack for recent messages mentioning the account name.
    Returns up to 5 most recent message texts.
    """
    if not user_token:
        return []

    keywords = _account_keywords(account_name)
    query    = f'"{keywords[0]}"'   # quoted exact match on primary keyword
    after    = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")

    try:
        resp = requests.get(
            "https://slack.com/api/search.messages",
            headers={"Authorization": f"Bearer {user_token}"},
            params={"query": f"{query} after:{after}", "count": 10, "sort": "timestamp"},
            timeout=15,
        )
        data = resp.json()
        if not data.get("ok"):
            print(f"  Slack search error for '{account_name}': {data.get('error')}", file=sys.stderr)
            return []

        matches = (data.get("messages") or {}).get("matches") or []
        texts   = []
        for m in matches[:5]:
            text = (m.get("text") or "").strip()
            if text:
                # strip Slack formatting noise, cap length
                text = re.sub(r'<[^>]+>', '', text)[:300]
                texts.append(text)
        return texts

    except Exception as e:
        print(f"  Slack search exception: {e}", file=sys.stderr)
        return []


# ── Deal enrichment ──────────────────────────────────────────────────────────

def enrich_deals(
    opp_records: list[dict],
    granola_notes: list[dict],      # all fetched note stubs
    granola_api_key: str,
    slack_user_token: str,
) -> list[dict]:
    """Attach Granola meeting notes and Slack activity to each opp."""
    today = date.today()
    deals = []

    for opp in opp_records:
        account_id   = opp.get("AccountId") or ""
        account      = opp.get("Account") or {}
        account_name = account.get("Name") or "?"
        owner_name   = (opp.get("Owner") or {}).get("Name") or "?"
        ae_email     = AE_EMAILS.get(owner_name, "")

        # Match Granola notes to this deal
        matched_stubs    = match_granola_notes(granola_notes, account_name, ae_email)
        matched_detailed = enrich_granola_notes(granola_api_key, matched_stubs) if matched_stubs else []

        # Compute last meeting date from matched notes
        note_dates = []
        for n in matched_detailed:
            created = n.get("created_at") or ""
            if created:
                try:
                    note_dates.append(date.fromisoformat(created[:10]))
                except ValueError:
                    pass
        last_meeting_date  = max(note_dates) if note_dates else None
        days_since_meeting = (today - last_meeting_date).days if last_meeting_date else None

        # Slack messages for this account
        slack_messages = fetch_slack_messages(slack_user_token, account_name)

        # Next step history
        ns_history = parse_next_step_history(opp.get("Next_Step_Historical__c") or "")

        # Close date
        close_date_str = opp.get("CloseDate")
        overdue        = False
        days_to_close  = None
        if close_date_str:
            try:
                cd = date.fromisoformat(close_date_str[:10])
                days_to_close = (cd - today).days
                overdue = days_to_close < 0
            except ValueError:
                pass

        deals.append({
            "id":                 opp.get("Id"),
            "name":               opp.get("Name") or "?",
            "account_id":         account_id,
            "account_name":       account_name,
            "tier":               account.get("Account_Tier__c") or "",
            "owner_name":         owner_name,
            "stage":              opp.get("StageName") or "?",
            "amount":             opp.get("Amount"),
            "close_date":         close_date_str,
            "days_to_close":      days_to_close,
            "overdue":            overdue,
            "next_step":          opp.get("NextStep") or "",
            "ns_history":         ns_history,
            "granola_notes":      matched_detailed,
            "days_since_meeting": days_since_meeting,
            "slack_messages":     slack_messages,
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

    # Next step
    ns = deal.get("next_step", "")
    if ns:
        lines.append(f"  Next step: {ns[:150]}")

    ns_h = deal.get("ns_history") or []
    if ns_h:
        e = ns_h[0]
        lines.append(f"  Next step updated {e['date']}: {e['text'][:150]}")

    # Granola meeting notes
    granola = deal.get("granola_notes") or []
    if granola:
        lines.append(f"  Meeting notes ({len(granola)} meeting(s)):")
        for note in granola[:3]:
            title   = note.get("title") or "(untitled)"
            created = (note.get("created_at") or "")[:10]
            summary = (note.get("summary_markdown") or "").strip()
            lines.append(f"    [{created}] {title}")
            if summary:
                # First 600 chars of the markdown summary
                lines.append(f"    {summary[:600]}")
    else:
        lines.append("  Meeting notes: none in last 14 days")

    # Slack activity
    slack = deal.get("slack_messages") or []
    if slack:
        lines.append(f"  Slack activity ({len(slack)} message(s)):")
        for msg in slack[:3]:
            lines.append(f"    · {msg[:200]}")
    else:
        lines.append("  Slack activity: none in last 14 days")

    return "\n".join(lines)


def generate_deal_summaries(ae_name: str, deals: list[dict], client) -> dict[str, str]:
    """
    Ask Claude for a 1–2 sentence update per deal grounded in meeting notes and Slack.
    Returns {account_name: sentence}.
    """
    import json as _json

    today_str    = date.today().strftime("%B %-d, %Y")
    deal_blocks  = "\n\n".join(_deal_context(d) for d in deals)
    account_names = [d["account_name"] for d in deals]

    prompt = f"""You are writing a weekly deal digest for a sales manager reviewing {ae_name}'s pipeline before a team call. Today is {today_str}.

Deal details (sourced from Granola meeting notes and Slack):
{deal_blocks}

Write a 1–2 sentence update for each deal. Ground your summary in actual meeting content and Slack messages where available — reference specific topics discussed, decisions made, or concerns raised. If no meeting notes or Slack activity exists, note that the deal is dark and flag the risk.

Return ONLY a JSON object where each key is the exact account name and each value is the update sentence. Account names: {account_names}

Rules: no markdown, no bold, no extra keys. Plain sentences only."""

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()

        start = raw.find("{")
        end   = raw.rfind("}") + 1
        if start != -1 and end > start:
            raw = raw[start:end]

        result = _json.loads(raw)

        for d in deals:
            if d["account_name"] not in result:
                result[d["account_name"]] = "(no summary)"
        return result

    except Exception as e:
        print(f"  Claude error for {ae_name}: {e}", file=sys.stderr)
        results = {}
        for d in deals:
            try:
                m = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=150,
                    messages=[{"role": "user", "content":
                        f"Write one sentence summarising this deal for a sales manager digest.\n\n"
                        f"{_deal_context(d)}\n\nOne sentence only, no markdown."}],
                )
                results[d["account_name"]] = m.content[0].text.strip()
            except Exception as e2:
                results[d["account_name"]] = f"(error: {e2})"
        return results


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
        sums    = row["summaries"]

        lines.append(f"*{display}*")

        for d in deals:
            acct     = d["account_name"]
            sentence = sums.get(acct, "")
            flags: list[str] = []
            if d["overdue"]:
                flags.append(f"⚠️ {abs(d['days_to_close'] or 0)}d overdue")
            elif d.get("days_to_close") is not None and d["days_to_close"] <= 14:
                flags.append(f"due in {d['days_to_close']}d")
            dsm = d.get("days_since_meeting")
            if dsm is not None and dsm > 14:
                flags.append(f"no meeting {dsm}d")
            elif not d.get("granola_notes") and not d.get("slack_messages"):
                flags.append("dark")

            flag_str = f" _[{', '.join(flags)}]_" if flags else ""
            lines.append(f"• *{acct}*{flag_str} — {sentence}")

            # Activity metadata line
            dsm = d.get("days_since_meeting")
            if dsm is None:
                meeting_str = "no meetings found"
            elif dsm == 0:
                meeting_str = "meeting today"
            elif dsm == 1:
                meeting_str = "meeting yesterday"
            else:
                meeting_str = f"last meeting {dsm}d ago"

            cd = d.get("close_date") or ""
            try:
                cd_fmt = date.fromisoformat(cd[:10]).strftime("%-m/%-d/%Y") if cd else "—"
            except ValueError:
                cd_fmt = cd

            lines.append(f"  _Last Meeting: {meeting_str} · Close Date: {cd_fmt}_")

        lines.append("")

    return "\n".join(lines)


def post_to_slack(bot_token: str, text: str):
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
    parser.add_argument("company", nargs="?", default=None,
                        help="Filter to a single account (case-insensitive substring). Implies --no-post.")
    parser.add_argument("--date", default=None,
                        help="Report date string (default: today)")
    parser.add_argument("--no-post", action="store_true",
                        help="Print to stdout only, skip Slack post")
    args = parser.parse_args()

    if args.company:
        args.no_post = True

    from dotenv import load_dotenv
    load_dotenv(ENV_PATH, override=True)

    today         = date.today()
    date_str      = args.date or today.strftime("%B %-d, %Y")
    bot_token     = os.getenv("SLACK_BOT_TOKEN", "")
    user_token    = os.getenv("SLACK_USER_TOKEN", "")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
    granola_key   = os.getenv("GRANOLA_API_KEY", "")

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

    # ── Granola ───────────────────────────────────────────────────────────────
    granola_notes: list[dict] = []
    if granola_key:
        print("Fetching Granola notes (last 14d)...")
        granola_notes = fetch_granola_notes(granola_key, days=14)
    else:
        print("⚠️  No GRANOLA_API_KEY — meeting notes unavailable", file=sys.stderr)

    # ── Filter by company if specified ────────────────────────────────────────
    if args.company:
        needle = args.company.lower()
        opp_records = [
            o for o in opp_records
            if needle in ((o.get("Account") or {}).get("Name") or "").lower()
        ]
        if not opp_records:
            print(f"No open deals found matching '{args.company}'.")
            return
        print(f"Filtered to {len(opp_records)} deal(s) matching '{args.company}'")

    # ── Enrich deals (Granola + Slack per deal) ───────────────────────────────
    print(f"Enriching {len(opp_records)} deal(s) with Granola notes and Slack...")
    all_deals = enrich_deals(opp_records, granola_notes, granola_key, user_token)

    deals_by_owner: dict[str, list] = defaultdict(list)
    for d in all_deals:
        deals_by_owner[d["owner_name"]].append(d)

    ae_rows = []
    for ae in ae_records:
        name  = ae["Name"]
        deals = deals_by_owner.get(name, [])
        n_meetings = sum(len(d.get("granola_notes") or []) for d in deals)
        n_slack    = sum(len(d.get("slack_messages") or []) for d in deals)
        print(f"  {name}: {len(deals)} deal(s), {n_meetings} meeting note(s), {n_slack} Slack message(s)")
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
