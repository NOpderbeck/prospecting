"""
ai_funding_scout.py — AI Funding Scout

Runs 4× per day via Cloud Scheduler → Cloud Run Job.

For each run:
  1. Search You.com News API + RSS for AI startup funding announcements (Seed → Series C)
  2. Claude extracts structured company info from each article
  3. Dedup against GCS seen-log (skip if same domain announced within 7 days)
  4. Check Salesforce for existing account (by domain or name)
  5. Net-new: create SF Account (Owner = Nick), DM Nick in Slack
     Existing: DM Nick in Slack (no SF create)

Usage (local):
    python3 ai_funding_scout.py [--dry-run] [--lookback-hours 6]

Environment:
    ANTHROPIC_API_KEY, YOUCOM_API_KEY, SF_USERNAME, SF_PASSWORD,
    SF_SECURITY_TOKEN, SLACK_BOT_TOKEN, GCS_BUCKET (optional, for seen-log)
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ── Constants ──────────────────────────────────────────────────────────────────

NICK_SF_ID       = "005Vq000008PlLhIAK"
SLACK_CHANNEL    = "C0BGAV0JCUW"  # #funding-announcements
SF_BASE          = "https://ydc.my.salesforce.com"
GCS_SEEN_BLOB    = "ai_scout_seen.json"
SEEN_TTL_DAYS    = 7          # don't re-alert on same domain within N days

CTD_BASE         = "https://api.ctd.ai/user/atc-paths-api/public/v1"
CTD_API_KEY      = os.getenv("CTD_API_KEY", "")
CTD_CLIENT_ID    = os.getenv("CTD_CLIENT_ID", "nick.opderbeck@you.com")

VALID_STAGES   = {"seed", "series a", "series b", "series c", "pre-seed"}

YOUCOM_URL     = "https://api.you.com/v1/search"
YOU_NEWS_QUERIES = [
    '"AI startup" "seed round" announced stealth',
    '"AI company" "Series A" OR "Series B" OR "Series C" launched funding',
    '"out of stealth" AI raised million',
    'artificial intelligence startup funding announced today',
]

RSS_FEEDS = [
    "https://techcrunch.com/tag/funding/feed/",
    "https://venturebeat.com/category/ai/feed/",
]

CLAUDE_MODEL = "claude-sonnet-4-6"

# ── GCS seen-log ───────────────────────────────────────────────────────────────

def _gcs_bucket():
    bucket_name = os.getenv("GCS_BUCKET", "you-sales-toolkit-forecast-data")
    if not bucket_name:
        return None
    try:
        from google.cloud import storage
        return storage.Client().bucket(bucket_name)
    except Exception as e:
        print(f"  ⚠️  GCS init failed: {e}", file=sys.stderr)
        return None


def load_seen_log() -> dict:
    """Returns {domain: iso_timestamp_str} of recently alerted companies."""
    bucket = _gcs_bucket()
    if bucket:
        try:
            blob = bucket.blob(GCS_SEEN_BLOB)
            if blob.exists():
                return json.loads(blob.download_as_text())
        except Exception as e:
            msg = str(e)
            if "403" in msg or "Permission" in msg or "does not have storage" in msg:
                pass  # local ADC lacks GCS access — fall through to local file silently
            else:
                print(f"  ⚠️  Could not load seen log from GCS: {e}", file=sys.stderr)
    # Local fallback
    local = os.path.join(os.path.dirname(__file__), GCS_SEEN_BLOB)
    if os.path.exists(local):
        with open(local) as f:
            return json.load(f)
    return {}


def save_seen_log(seen: dict):
    # Prune entries older than TTL
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SEEN_TTL_DAYS)).isoformat()
    seen = {k: v for k, v in seen.items() if v >= cutoff}

    bucket = _gcs_bucket()
    payload = json.dumps(seen, indent=2)
    if bucket:
        try:
            bucket.blob(GCS_SEEN_BLOB).upload_from_string(payload, content_type="application/json")
            print(f"  ✅ Seen log saved to GCS ({len(seen)} entries)")
            return
        except Exception as e:
            print(f"  ⚠️  GCS save failed, writing locally: {e}", file=sys.stderr)
    local = os.path.join(os.path.dirname(__file__), GCS_SEEN_BLOB)
    with open(local, "w") as f:
        f.write(payload)


def already_seen(domain: str, seen: dict) -> bool:
    if not domain:
        return False
    key = domain.lower().strip()
    if key not in seen:
        return False
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SEEN_TTL_DAYS)).isoformat()
    return seen[key] >= cutoff


# ── News gathering ─────────────────────────────────────────────────────────────

def _cutoff_dt(lookback_hours: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=lookback_hours)


def _parse_dt(s: str) -> Optional[datetime]:
    """Parse ISO or RFC-2822 date strings into a UTC-aware datetime."""
    if not s:
        return None
    from email.utils import parsedate_to_datetime
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        return parsedate_to_datetime(s).astimezone(timezone.utc)
    except Exception:
        return None


def fetch_youcom_news(query: str, lookback_hours: int = 6) -> list[dict]:
    """Search You.com News API for recent articles matching query."""
    key = os.getenv("YOUCOM_API_KEY", "")
    if not key:
        return []
    cutoff = _cutoff_dt(lookback_hours)
    try:
        resp = requests.get(
            YOUCOM_URL,
            headers={"X-API-Key": key},
            params={"query": query, "search_type": "news", "count": 15, "recency": "day"},
            timeout=15,
        )
        resp.raise_for_status()
        hits = resp.json().get("news", {}).get("results", [])
        if not hits:
            hits = resp.json().get("results", {}).get("news", [])
        results = []
        for h in hits:
            if not h.get("title"):
                continue
            pub_dt = _parse_dt(h.get("page_age") or h.get("published_date") or "")
            if pub_dt and pub_dt < cutoff:
                print(f"    ⏭️  Skipping old article ({pub_dt.date()}): {h['title'][:60]}")
                continue
            results.append({"title": h["title"], "url": h.get("url", ""), "snippet": h.get("description", ""), "pub_date": h.get("page_age", "")})
        return results
    except Exception as e:
        print(f"  ⚠️  You.com news query failed ({query[:40]}…): {e}", file=sys.stderr)
        return []


def fetch_rss(url: str, lookback_hours: int = 6) -> list[dict]:
    """Fetch and parse an RSS feed, returning only items within the lookback window."""
    cutoff = _cutoff_dt(lookback_hours)
    try:
        import xml.etree.ElementTree as ET
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        for item in root.iter("item"):
            title   = (item.findtext("title") or "").strip()
            link    = (item.findtext("link") or "").strip()
            desc    = (item.findtext("description") or "").strip()
            pub_raw = (item.findtext("pubDate") or item.findtext("dc:date") or "").strip()

            # Hard date gate — skip anything outside the lookback window
            pub_dt = _parse_dt(pub_raw)
            if pub_dt and pub_dt < cutoff:
                continue  # article is too old
            if not pub_dt:
                # No date at all — include but flag for Claude to assess
                pass

            combined = (title + " " + desc).lower()
            if any(kw in combined for kw in ["fund", "raise", "million", "seed", "series a", "series b", "series c", "stealth", "launch"]):
                if any(kw in combined for kw in ["ai", "artificial intelligence", "ml", "llm", "machine learning"]):
                    items.append({"title": title, "url": link, "snippet": desc[:400], "pub_date": pub_raw})
        return items[:15]
    except Exception as e:
        print(f"  ⚠️  RSS fetch failed ({url}): {e}", file=sys.stderr)
        return []


def gather_articles(lookback_hours: int) -> list[dict]:
    """Collect raw articles from all sources, dedup by URL."""
    articles = []
    seen_urls: set = set()

    print("  Searching You.com News API…")
    for q in YOU_NEWS_QUERIES:
        for art in fetch_youcom_news(q, lookback_hours):
            if art["url"] not in seen_urls:
                seen_urls.add(art["url"])
                articles.append(art)

    print(f"  Fetching {len(RSS_FEEDS)} RSS feeds…")
    for feed_url in RSS_FEEDS:
        for art in fetch_rss(feed_url, lookback_hours):
            if art["url"] not in seen_urls:
                seen_urls.add(art["url"])
                articles.append(art)

    print(f"  → {len(articles)} unique articles gathered")
    return articles


# ── Claude extraction ──────────────────────────────────────────────────────────

EXTRACT_SYSTEM = """You are an expert at extracting AI startup funding information from news articles.
Given a batch of article titles and snippets, identify announcements where an AI company is THE SUBJECT
raising money (Seed through Series C).

Strict qualification rules — ALL must be true:
1. The NAMED COMPANY itself closed the funding round (not a portfolio company, acquirer, or tool mentioned in passing)
2. The company's PRIMARY BUSINESS is building AI products, models, agents, or AI-powered software
3. The round is Pre-Seed, Seed, Series A, B, or C — ignore Series D+, debt, grants, acquisitions
4. The announcement is recent (not a recap of a round from >6 months ago)

Disqualify if:
- The company is primarily a consulting firm, system integrator, or IT services provider that uses AI
- AI is incidental (e.g. "SaaS company adds AI feature") — must be core to the product
- The article merely mentions the company as a customer, tool, or investor, not the fundee
- The domain clearly belongs to a non-AI company (railways, real estate, logistics, etc.)
- You are not confident the company is an AI startup

For each qualifying announcement return:
{
  "company": "Exact Company Name",
  "domain": "primarydomain.com",     // lowercase, no https://, no www
  "stage": "Seed",                   // one of: Pre-Seed, Seed, Series A, Series B, Series C
  "amount_m": 40,                    // funding in millions USD, null if unknown
  "investors": "DST Global, Lux Capital",
  "description": "One sentence: what the company builds and who uses it",
  "you_use_case": "One sentence: the most compelling way this company could use You.com's Search API — be specific to their product (e.g. real-time web grounding for their AI agent, news retrieval for their monitoring tool, research augmentation for their copilot). If unclear, write null.",
  "source_url": "https://..."
}

If no qualifying announcements are found, return [].
Return ONLY the JSON array, no other text."""


def extract_companies(articles: list[dict], lookback_hours: int = 6) -> list[dict]:
    """Use Claude to extract structured company info from raw articles."""
    if not articles:
        return []

    client = __import__("anthropic").Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    # Batch articles into chunks of 20 to stay within context limits
    results = []
    for i in range(0, len(articles), 20):
        chunk = articles[i:i+20]
        batch_text = "\n\n".join(
            f"[{j+1}] {a['title']}\nPublished: {a.get('pub_date','unknown')}\nURL: {a['url']}\n{a['snippet'][:300]}"
            for j, a in enumerate(chunk)
        )
        cutoff_str = _cutoff_dt(lookback_hours).strftime("%Y-%m-%d")
        try:
            msg = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=2048,
                system=EXTRACT_SYSTEM,
                messages=[{"role": "user", "content": f"Today is {datetime.now(timezone.utc).strftime('%Y-%m-%d')}. Only include announcements published on or after {cutoff_str}.\n\nExtract AI funding announcements from these articles:\n\n{batch_text}"}],
            )
            raw = msg.content[0].text.strip()
            # Strip markdown code fences if present
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw).strip()
            # Skip empty responses or prose ("No qualifying announcements…")
            if not raw or not raw.startswith("["):
                continue
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                results.extend(parsed)
        except Exception as e:
            print(f"  ⚠️  Claude extraction failed for batch {i//20 + 1}: {e}", file=sys.stderr)

    # Normalize stage, filter invalid
    valid = []
    for c in results:
        stage = (c.get("stage") or "").strip()
        if stage.lower() not in VALID_STAGES:
            continue
        domain = (c.get("domain") or "").lower().strip().lstrip("www.")
        c["domain"] = domain
        valid.append(c)

    print(f"  → {len(valid)} qualifying AI funding announcements extracted")
    return valid


# ── Salesforce ─────────────────────────────────────────────────────────────────

def sf_connect():
    from simple_salesforce import Salesforce
    return Salesforce(
        username=os.environ["SF_USERNAME"],
        password=os.environ["SF_PASSWORD"],
        security_token=os.environ["SF_SECURITY_TOKEN"],
    )


def sf_find_account(sf, company: str, domain: str) -> Optional[dict]:
    """Look up an account by exact domain match, then conservative name match."""

    # 1. Exact domain match — most reliable
    if domain:
        bare = domain.lstrip("www.")
        for pattern in [f"https://{bare}", f"http://{bare}", f"https://www.{bare}", bare]:
            res = sf.query_all(f"""
                SELECT Id, Name, Website, OwnerId, Owner.Name
                FROM Account
                WHERE Website = '{pattern}'
                LIMIT 1
            """)
            if res["totalSize"] > 0:
                return res["records"][0]
        # Loose domain match as fallback — but verify the result domain actually matches
        res = sf.query_all(f"""
            SELECT Id, Name, Website, OwnerId, Owner.Name
            FROM Account
            WHERE Website LIKE '%{bare}%'
            LIMIT 5
        """)
        for rec in res["records"]:
            site = (rec.get("Website") or "").lower().replace("https://","").replace("http://","").replace("www.","").rstrip("/")
            if site == bare or site.endswith("." + bare):
                return rec

    # 2. Name match — require the full company name (minus legal suffixes) to appear
    #    Use a conservative minimum: only match if the core name is >= 5 chars to avoid
    #    short-word collisions like "Railway" matching "Transco Railway Products"
    name_clean = re.sub(r'\b(ai|inc|corp|llc|ltd|co\.?|the|labs?|technologies?)\b', '', company, flags=re.I).strip()
    if len(name_clean) >= 5:
        name_esc = name_clean.replace("'", "\\'")
        res = sf.query_all(f"""
            SELECT Id, Name, Website, OwnerId, Owner.Name
            FROM Account
            WHERE Name LIKE '%{name_esc}%'
            LIMIT 5
        """)
        for rec in res["records"]:
            # Verify the SF account name actually contains the company's core name
            sf_name_lower = rec["Name"].lower()
            if name_clean.lower() in sf_name_lower:
                return rec

    return None


def sf_create_account(sf, company: dict) -> str:
    """Create a new Salesforce Account. Returns the new Account ID."""
    website = company["domain"]
    if website and not website.startswith("http"):
        website = "https://" + website

    payload = {
        "Name":        company["company"],
        "Website":     website or None,
        "Description": company.get("description") or "",
        "Type":        "Prospect",
        "OwnerId":     NICK_SF_ID,
    }
    # Remove None values — SF rejects explicit nulls for some fields
    payload = {k: v for k, v in payload.items() if v is not None}

    result = sf.Account.create(payload)
    return result["id"]


# ── CTD warm path enrichment ───────────────────────────────────────────────

def ctd_warm_paths(domain: str, max_show: int = 3) -> str:
    """Query CTD for strong intro paths into a domain. Always returns a Slack-ready blurb."""
    if not domain:
        return "🔗 *CTD:* No domain to search"
    headers = {"ctd-api-key": CTD_API_KEY, "ctd-client-id": CTD_CLIENT_ID}
    try:
        # Quick company check — bail on 404
        company_resp = requests.get(
            f"{CTD_BASE}/company",
            headers=headers,
            params={"company_domain": domain},
            timeout=20,
        )
        if company_resp.status_code == 404:
            return "🔗 *CTD:* Company not found in network"
        company_score = (company_resp.json() or {}).get("ctd_score_label", "")

        # Fetch paths — strong + medium to start, then filter to strong only
        paths_resp = requests.get(
            f"{CTD_BASE}/paths",
            headers=headers,
            params=[
                ("company_domain", domain),
                ("path_relationship_strength", "strong"),
                ("path_relationship_strength", "medium"),
                ("degree", "first"),
                ("degree", "second"),
                ("page_size", "40"),
            ],
            timeout=20,
        )
        if not paths_resp.ok:
            return f"🔗 *CTD:* Lookup failed ({paths_resp.status_code})"

        all_paths = paths_resp.json() if isinstance(paths_resp.json(), list) else (paths_resp.json() or {}).get("paths", []) or []
        strong = [p for p in all_paths if (p.get("path_relationship_strength_label") or "").lower() == "strong"]

        score_label = f" · _{company_score}_" if company_score else ""

        if not strong:
            return f"🔗 *CTD:* No strong warm paths found{score_label}"

        lines = []
        for path in strong[:max_show]:
            nodes     = path.get("nodes") or []
            degree    = path.get("degree", "")
            target    = next((n for n in reversed(nodes) if n.get("is_target_person")), nodes[-1] if nodes else {})
            connector = nodes[1] if len(nodes) >= 3 else None

            t_name  = target.get("name", "Unknown")
            t_title = target.get("title", "")
            t_str   = t_name + (f", {t_title}" if t_title else "")

            if degree == "first" or connector is None:
                lines.append(f"• {t_str} _(direct connection)_")
            else:
                c_name  = connector.get("name", "Unknown")
                c_title = connector.get("title", "")
                c_str   = c_name + (f", {c_title}" if c_title else "")
                lines.append(f"• {t_str} → via _{c_str}_")

        total = len(strong)
        header = f"🔗 *{total} warm path{'s' if total != 1 else ''} via CTD*{score_label}"
        return header + "\n" + "\n".join(lines)

    except Exception as e:
        print(f"  ⚠️  CTD lookup failed ({domain}): {e}", file=sys.stderr)
        return "🔗 *CTD:* Lookup timed out"


# ── Slack ──────────────────────────────────────────────────────────────────────

def slack_post(text: str, dry_run: bool = False):
    """Post a message to the funding announcements Slack channel."""
    token = os.getenv("SLACK_BOT_TOKEN") or os.getenv("SLACK_USER_TOKEN", "")
    if not token:
        print(f"  ⚠️  No Slack token — skipping post", file=sys.stderr)
        return

    if dry_run:
        print(f"\n── DRY RUN: Slack → {SLACK_CHANNEL} ──────────────")
        print(text)
        print("──────────────────────────────────────────────────\n")
        return

    post_resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "channel": SLACK_CHANNEL,
            "text": text,
            "mrkdwn": True,
            "unfurl_links": False,
        },
        timeout=10,
    ).json()

    if post_resp.get("ok"):
        print(f"  ✅ Posted to {SLACK_CHANNEL}")
    else:
        print(f"  ⚠️  Slack post failed: {post_resp.get('error')}", file=sys.stderr)


# ── End-of-day summary ───────────────────────────────────────────────────────────

def _pacific_tz():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/Los_Angeles")
    except Exception:
        return timezone.utc  # fall back to UTC if tzdata is unavailable


def count_found_today(seen: dict) -> list[str]:
    """Domains first flagged today (Pacific day), derived from seen-log timestamps."""
    tz = _pacific_tz()
    start = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    found = []
    for domain, ts in seen.items():
        dt = _parse_dt(ts)
        if dt and dt.astimezone(tz) >= start:
            found.append(domain)
    return sorted(found)


def post_daily_summary(seen: dict, dry_run: bool = False):
    """Post an end-of-day count to Slack (announcements are counted from the seen-log)."""
    found = count_found_today(seen)
    n = len(found)
    today = datetime.now(_pacific_tz()).strftime("%b %-d, %Y")
    if n == 0:
        body = "No funding announcements found today."
    else:
        noun = "announcement" if n == 1 else "announcements"
        body = f"{n} funding {noun} found today: " + ", ".join(found) + "."
    slack_post(f"📊 *AI Funding Scout — {today}*\n{body}", dry_run=dry_run)


def build_message_new(company: dict, sf_id: str, ctd_blurb: str = "") -> str:
    """Slack message for a net-new account created in SF."""
    sf_link   = f"{SF_BASE}/lightning/r/Account/{sf_id}/view"
    amount    = f"${company['amount_m']}M " if company.get("amount_m") else ""
    investors = f" · _{company['investors']}_" if company.get("investors") else ""
    use_case  = company.get("you_use_case")
    use_line  = f"\n💡 *You.com angle:* {use_case}" if use_case else ""
    ctd_line  = f"\n{ctd_blurb}" if ctd_blurb else ""
    return (
        f"<!here> 🚀 *New AI prospect added to Salesforce*\n"
        f"*{company['company']}* · {amount}{company.get('stage','Seed')}{investors}\n"
        f"`{company['domain']}`\n"
        f"_{company.get('description', '')}_"
        f"{use_line}"
        f"{ctd_line}\n"
        f"<{sf_link}|View in Salesforce>  ·  <{company.get('source_url','')}|Source>"
    )


def build_message_existing(company: dict, acct: dict, ctd_blurb: str = "") -> str:
    """Slack message for an announcement where we already have the account in SF."""
    sf_id     = acct["Id"]
    sf_link   = f"{SF_BASE}/lightning/r/Account/{sf_id}/view"
    owner     = (acct.get("Owner") or {}).get("Name", "unknown")
    amount    = f"${company['amount_m']}M " if company.get("amount_m") else ""
    investors = f" · _{company['investors']}_" if company.get("investors") else ""
    use_case  = company.get("you_use_case")
    use_line  = f"\n💡 *You.com angle:* {use_case}" if use_case else ""
    ctd_line  = f"\n{ctd_blurb}" if ctd_blurb else ""
    return (
        f"<!here> 📢 *Funding announcement — already in Salesforce*\n"
        f"*{company['company']}* · {amount}{company.get('stage','')}{investors}\n"
        f"`{company['domain']}` · Owner: {owner}\n"
        f"_{company.get('description', '')}_"
        f"{use_line}"
        f"{ctd_line}\n"
        f"<{sf_link}|View in Salesforce>  ·  <{company.get('source_url','')}|Source>"
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Don't write to SF or Slack")
    parser.add_argument("--lookback-hours", type=int, default=6, help="Hours of news to scan")
    parser.add_argument("--daily-summary", action="store_true",
                        help="Post an end-of-day count to Slack and exit (no scan)")
    args = parser.parse_args()

    # End-of-day summary mode: just count today's flagged domains and post the recap.
    # Triggerable via --daily-summary (local) or SCOUT_MODE=summary (Cloud Scheduler override).
    if args.daily_summary or os.getenv("SCOUT_MODE", "").lower() == "summary":
        print(f"AI Funding Scout — daily summary — {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC")
        seen = load_seen_log()
        post_daily_summary(seen, dry_run=args.dry_run)
        return

    print(f"\n{'='*60}")
    print(f"AI Funding Scout — {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"Lookback: {args.lookback_hours}h | Dry run: {args.dry_run}")
    print(f"{'='*60}\n")

    seen = load_seen_log()
    print(f"Seen log: {len(seen)} domains tracked\n")

    # 1. Gather articles
    articles = gather_articles(args.lookback_hours)
    if not articles:
        print("No articles found. Exiting.")
        return

    # 2. Extract companies via Claude
    companies = extract_companies(articles, args.lookback_hours)
    if not companies:
        print("No qualifying funding announcements found.")
        return

    # 3. Connect to Salesforce
    print("\nConnecting to Salesforce…")
    sf = sf_connect()
    print("  ✅ Connected\n")

    # 4. Process each company
    new_count = existing_count = skipped_count = 0

    for co in companies:
        domain  = co.get("domain", "")
        company = co.get("company", "unknown")

        # Dedup — skip if alerted recently
        if already_seen(domain, seen):
            print(f"  ⏭️  SKIP (seen recently): {company} ({domain})")
            skipped_count += 1
            continue

        print(f"  🔍 Checking SF: {company} ({domain})")
        acct = sf_find_account(sf, company, domain)

        # CTD warm path enrichment
        print(f"  🔗 Checking CTD warm paths: {domain}")
        ctd_blurb = ctd_warm_paths(domain)
        if ctd_blurb:
            print(f"     → Found warm paths")
        else:
            print(f"     → No strong paths found")

        if acct:
            print(f"     → EXISTS in SF: {acct['Name']} (owner: {(acct.get('Owner') or {}).get('Name','')})")
            msg = build_message_existing(co, acct, ctd_blurb)
            slack_post(msg, dry_run=args.dry_run)
            existing_count += 1
        else:
            print(f"     → NET NEW — creating SF account…")
            sf_id = None
            if not args.dry_run:
                try:
                    sf_id = sf_create_account(sf, co)
                    print(f"     → Created: {SF_BASE}/lightning/r/Account/{sf_id}/view")
                except Exception as e:
                    print(f"     ⚠️  SF create failed: {e}", file=sys.stderr)
                    sf_id = "ERROR"
            else:
                sf_id = "DRY_RUN_ID"
                print(f"     → [DRY RUN] Would create SF account")
            msg = build_message_new(co, sf_id, ctd_blurb)
            slack_post(msg, dry_run=args.dry_run)
            new_count += 1

        # Mark as seen regardless of SF outcome
        if not args.dry_run:
            seen[domain] = datetime.now(timezone.utc).isoformat()

    # 5. Persist seen log
    if not args.dry_run:
        save_seen_log(seen)

    print(f"\n{'='*60}")
    print(f"Done: {new_count} new accounts created, {existing_count} existing flagged, {skipped_count} skipped")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
