#!/usr/bin/env python3
"""
ai_digest_run.py — Daily AI Trends Digest

Collects signals from X (TwitterAPI.io), LinkedIn (RapidAPI Fresh LinkedIn Scraper),
and You.com news. Scores and normalizes them, synthesizes insights + competitive
intelligence via Claude, and posts a structured digest to #daily-digest.

Usage:
    python ai_digest_run.py                        # today, post to Slack
    python ai_digest_run.py --dry-run              # skip Slack, print to stdout
    python ai_digest_run.py --date 2026-04-15      # backfill specific date

Environment (required):
    ANTHROPIC_API_KEY          Claude API
    SLACK_BOT_TOKEN            Slack bot token (xoxb-...)
    TWITTER_API_KEY            TwitterAPI.io access token
    LINKEDIN_RAPIDAPI_KEY      RapidAPI key for Fresh LinkedIn Scraper

Environment (optional):
    YOUCOM_API_KEY             You.com search (enables news collection)
    SLACK_CHANNEL_AI_DIGEST    Override default channel (default: #daily-digest)
"""

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import anthropic
import requests
from dotenv import load_dotenv

# ── Constants ─────────────────────────────────────────────────────────────────

ENV_PATH     = Path(__file__).parent / ".env"
REPORTS_DIR  = Path(__file__).parent / "reports" / "ai_digest"
SLACK_CHANNEL_DEFAULT = "#daily-digest"

TWITTER_BASE  = "https://api.twitterapi.io/twitter/tweet/advanced_search"
LINKEDIN_BASE = "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/search/posts"
YOUCOM_BASE   = "https://api.you.com/v1/search"

TOP_N        = 40   # signals passed to Claude
MAX_SOURCES  = 8    # URLs shown in digest
DIVERSITY_CAP = 3   # max signals per raw query (prevents one viral thread dominating)

# ── Competitors ───────────────────────────────────────────────────────────────

# Map of display name → list of regex patterns (case-insensitive, applied to full text)
COMPETITORS: dict[str, list[str]] = {
    "Tavily":       [r"tavily"],
    "Exa":          [r"exa\.ai", r"\bexa\b"],
    "Parallel Web": [r"parallel\s+web", r"parallel\.ai"],
    "Nimble":       [r"nimble\s+ai", r"nimble\.com", r"\bnimble\b"],
    "LinkUp":       [r"linkup\s+ai", r"linkup\.ai", r"\blinkup\b"],
}

# Context type detection — first match wins (ordered by specificity)
CONTEXT_PATTERNS: list[tuple[str, list[str]]] = [
    ("comparison",        [r"\bvs\.?\b", r"compared to", r"better than", r"outperforms", r"beats\b"]),
    ("benchmark",         [r"\bbenchmark\b", r"\blatency\b", r"faster than", r"\baccuracy\b", r"speed test", r"tokens[/ ]s"]),
    ("product_launch",    [r"\blaunch(?:ed|ing)?\b", r"\brelease\b", r"\bannouncing\b", r"new feature", r"\bv\d+\.\d+\b", r"just shipped"]),
    ("partnership",       [r"\bpartner\b", r"\bintegration\b", r"built with", r"powered by", r"built on top of"]),
    ("funding",           [r"\braised\b", r"\bfunding\b", r"\bseries [a-z]\b", r"\bvaluation\b", r"\binvestment\b", r"\bseed round\b"]),
    ("developer_feedback",[r"\busing\b", r"switched to", r"\brecommend\b", r"\btried\b", r"works (?:great|well)", r"love(?:s)? it"]),
]

# ── Query sets ────────────────────────────────────────────────────────────────

TWITTER_QUERIES: list[str] = [
    # General AI/search trends
    '"AI search"',
    '"agent search"',
    '"search API" agent',
    '"AI retrieval"',
    '"web agents"',
    '"agentic search"',
    # Competitor-specific
    "Tavily",
    '"Exa AI" OR exa.ai',
    '"Parallel Web"',
    '"Nimble AI"',
    '"LinkUp AI"',
    # Cross-signals
    '"search API" benchmark',
]

LINKEDIN_KEYWORDS: list[str] = [
    "AI search API",
    "agent search",
    "agentic search",
    "Tavily",
    "Exa AI",
    "Parallel Web",
    "Nimble AI",
    "LinkUp AI",
    "search API agents",
    "AI retrieval benchmark",
]

YOUCOM_QUERIES: list[str] = [
    "AI search API news 2026",
    "Tavily OR Exa AI OR Parallel Web AI 2026",
    "agentic search infrastructure 2026",
    "web search API developer tools 2026",
]

# ── Config ────────────────────────────────────────────────────────────────────

def load_config() -> dict[str, str | None]:
    load_dotenv(ENV_PATH, override=True)
    config: dict[str, str | None] = {
        "anthropic_key":  os.getenv("ANTHROPIC_API_KEY"),
        "slack_token":    os.getenv("SLACK_BOT_TOKEN"),
        "twitter_key":    os.getenv("TWITTER_API_KEY"),
        "linkedin_key":   os.getenv("LINKEDIN_RAPIDAPI_KEY"),
        "youcom_key":     os.getenv("YOUCOM_API_KEY"),
        "slack_channel":  os.getenv("SLACK_CHANNEL_AI_DIGEST", SLACK_CHANNEL_DEFAULT),
    }
    required = ["anthropic_key", "slack_token", "twitter_key", "linkedin_key"]
    missing = [k for k in required if not config[k]]
    if missing:
        print(f"❌ Missing required env vars: {missing}", file=sys.stderr)
        sys.exit(1)
    return config

# ── Detection helpers ─────────────────────────────────────────────────────────

def detect_competitors(text: str) -> list[str]:
    t = text.lower()
    return [
        name
        for name, patterns in COMPETITORS.items()
        if any(re.search(p, t) for p in patterns)
    ]


def detect_context_type(text: str) -> str:
    t = text.lower()
    for ctx, patterns in CONTEXT_PATTERNS:
        if any(re.search(p, t) for p in patterns):
            return ctx
    return "general"

# ── Collectors ────────────────────────────────────────────────────────────────

def collect_twitter(api_key: str, since_ts: int, until_ts: int) -> list[dict]:
    """Fetch recent tweets across all query terms (last 24h window)."""
    headers = {"X-API-Key": api_key}
    seen_ids: set[str] = set()
    items: list[dict] = []

    for query_base in TWITTER_QUERIES:
        query = f"{query_base} since_time:{since_ts} until_time:{until_ts}"
        params: dict[str, str] = {"query": query, "queryType": "Latest", "cursor": ""}
        try:
            resp = requests.get(TWITTER_BASE, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  ⚠️  Twitter ({query_base!r}): {e}", file=sys.stderr)
            continue

        for tweet in data.get("tweets", []):
            tid = str(tweet.get("id", ""))
            if not tid or tid in seen_ids:
                continue
            seen_ids.add(tid)

            author = tweet.get("author", {})
            engagement = (
                tweet.get("retweetCount", 0)
                + tweet.get("likeCount", 0)
                + tweet.get("replyCount", 0)
                + tweet.get("quoteCount", 0)
            )
            items.append({
                "id":           tid,
                "source":       "twitter",
                "url":          tweet.get("url", f"https://twitter.com/i/web/status/{tid}"),
                "text":         tweet.get("text", ""),
                "author":       author.get("name", ""),
                "author_context": f"@{author.get('userName', '')}",
                "created_at":   tweet.get("createdAt", ""),
                "engagement":   engagement,
                "raw_query":    query_base,
            })

        time.sleep(0.3)

    print(f"  Twitter:  {len(items)} unique tweets")
    return items


def collect_linkedin(api_key: str) -> list[dict]:
    """Fetch LinkedIn posts from the last 24h across all keyword terms."""
    headers = {
        "x-rapidapi-key":  api_key,
        "x-rapidapi-host": "fresh-linkedin-scraper-api.p.rapidapi.com",
    }
    seen_ids: set[str] = set()
    items: list[dict] = []

    for keyword in LINKEDIN_KEYWORDS:
        params: dict[str, Any] = {
            "keyword":     keyword,
            "date_posted": "past_24h",
            "sort_by":     "relevance",
            "limit":       50,
            "page":        1,
        }
        try:
            resp = requests.get(LINKEDIN_BASE, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  ⚠️  LinkedIn ({keyword!r}): {e}", file=sys.stderr)
            continue

        for post in data.get("data", []):
            pid = str(post.get("id", ""))
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)

            author   = post.get("author", {})
            activity = post.get("activity", {})
            engagement = (
                activity.get("likes", 0)
                + activity.get("comments", 0)
                + activity.get("shares", 0)
            )
            # author.description is their LinkedIn headline — useful for
            # inferring their role/company context even when post title is sparse
            author_ctx = author.get("description", "")
            title = post.get("title", "")

            items.append({
                "id":           pid,
                "source":       "linkedin",
                "url":          post.get("url", ""),
                "text":         title,
                # Concatenate author headline for competitor/context detection
                # (not shown in digest output, used only for scoring)
                "_detect_text": f"{title} {author_ctx}".strip(),
                "author":       author.get("name", ""),
                "author_context": author_ctx,
                "created_at":   str(post.get("created_at", "")),
                "engagement":   engagement,
                "raw_query":    keyword,
            })

        time.sleep(0.3)

    print(f"  LinkedIn: {len(items)} unique posts")
    return items


def collect_news(api_key: str | None) -> list[dict]:
    """Fetch news snippets via You.com search."""
    if not api_key:
        print("  News:     skipped (YOUCOM_API_KEY not set)")
        return []

    headers = {"X-API-Key": api_key}
    seen_urls: set[str] = set()
    items: list[dict] = []

    for query in YOUCOM_QUERIES:
        params: dict[str, Any] = {"query": query, "num_web_results": 10}
        try:
            resp = requests.get(YOUCOM_BASE, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  ⚠️  News ({query!r}): {e}", file=sys.stderr)
            continue

        for result in data.get("web", {}).get("results", []):
            url = result.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            title   = result.get("title", "")
            snippet = result.get("description", "")
            text    = f"{title}. {snippet}".strip(". ")

            items.append({
                "id":           url,
                "source":       "news",
                "url":          url,
                "text":         text,
                "author":       result.get("source", ""),
                "author_context": "",
                "created_at":   "",
                "engagement":   0,
                "raw_query":    query,
            })

        time.sleep(0.2)

    print(f"  News:     {len(items)} unique articles")
    return items

# ── Normalize + score ─────────────────────────────────────────────────────────

def normalize(item: dict) -> dict:
    """Convert a raw collected item into the standard signal schema with score."""
    detect_text = item.get("_detect_text") or item["text"]
    competitors  = detect_competitors(detect_text)
    context_type = detect_context_type(detect_text)

    engagement  = item["engagement"]
    score       = math.log1p(engagement)

    # Competitor relevance boosts
    n = len(competitors)
    if n >= 2:
        score *= 1.3
    elif n == 1:
        score *= 1.2

    # Context quality boosts
    if context_type in ("comparison", "benchmark"):
        score *= 1.4
    elif context_type == "product_launch":
        score *= 1.1

    # News items get a small floor (no engagement signal available)
    if item["source"] == "news" and score == 0.0:
        score = 0.5

    return {
        "id":                  item["id"],
        "source":              item["source"],
        "url":                 item["url"],
        "text":                item["text"],
        "author":              item.get("author", ""),
        "author_context":      item.get("author_context", ""),
        "created_at":          item.get("created_at", ""),
        "engagement":          engagement,
        "mentioned_companies": competitors,
        "competitor_flag":     bool(competitors),
        "competitor_names":    competitors,
        "context_type":        context_type,
        "score":               score,
        "raw_query":           item.get("raw_query", ""),
    }


def select_top_signals(signals: list[dict], n: int = TOP_N) -> list[dict]:
    """Sort by score, apply per-query diversity cap, return top n."""
    ranked = sorted(signals, key=lambda s: s["score"], reverse=True)
    query_counts: dict[str, int] = {}
    selected: list[dict] = []
    for s in ranked:
        q = s["raw_query"]
        if query_counts.get(q, 0) < DIVERSITY_CAP:
            selected.append(s)
            query_counts[q] = query_counts.get(q, 0) + 1
        if len(selected) >= n:
            break
    return selected

# ── Synthesis ─────────────────────────────────────────────────────────────────

SYNTHESIS_SYSTEM = """\
You are an AI market analyst covering the AI search, retrieval, and agent infrastructure space.
Your audience is a sales leader at You.com — a company that sells a Search API used by AI agents
and enterprise applications.

Tracked competitors: Tavily, Exa, Parallel Web, Nimble, LinkUp.

## Rules
- Synthesize patterns across sources — do NOT list news items or recap individual posts
- Only mention competitors when they appear in multiple signals OR are part of a meaningful comparison
- When competitors appear together, explain HOW they are compared and what that implies about the market
- Identify implicit positioning shifts carefully — never overclaim or speculate beyond the signals
- Write as someone who deeply understands search infrastructure and AI agent architecture
- Suggested posts must feel like an operator's perspective: opinionated, specific, not generic AI commentary

## Output format
Return a single valid JSON object with exactly these keys:

{
  "summary_paragraphs": ["paragraph1", "paragraph2", "paragraph3"],
  "competitive_mentions": [
    {"competitor": "Name", "context": "one sentence on how they appeared today"}
  ],
  "trend_insight": "one sentence summarizing the dominant signal",
  "market_direction": "one sentence on where the space is heading",
  "linkedin_post": "150–250 word post",
  "x_post": "under 280 characters",
  "top_source_urls": ["url1", "url2", "..."]
}

## Paragraph guide (summary_paragraphs)
Write 3 paragraphs (4 only if a clear strategic takeaway exists for You.com):
  1. Major trend observed across sources today
  2. Competitive dynamic — only if competitors appeared meaningfully; omit if not
  3. Market implication: what does this suggest about where the space is heading?
  4. (optional) Strategic takeaway specific to You.com's positioning

## competitive_mentions
Only include competitors that genuinely appeared in meaningful signals.
Omit those with no real signal today — do not force mentions.

## Suggested posts
- linkedin_post: Opinionated, ties trend + competitive context, positions author as a practitioner
  not a commentator. Use concrete details from the signals.
- x_post: Sharp, insight-driven, ≤280 chars. One clear point.

## top_source_urls
Pick up to 8 of the most credible/relevant URLs from the provided signals.
"""


def synthesize(signals: list[dict], client: anthropic.Anthropic, run_date: date) -> dict:
    """Send top signals to Claude and return structured digest dict."""
    signal_data = [
        {
            "source":       s["source"],
            "text":         s["text"][:400],
            "author":       s["author"],
            "author_context": s["author_context"][:120] if s["author_context"] else "",
            "engagement":   s["engagement"],
            "competitors":  s["competitor_names"],
            "context_type": s["context_type"],
            "url":          s["url"],
        }
        for s in signals
    ]

    user_msg = (
        f"Date: {run_date.isoformat()}\n\n"
        f"Here are the top {len(signal_data)} signals collected today from X, LinkedIn, "
        f"and news sources:\n\n"
        f"{json.dumps(signal_data, indent=2)}\n\n"
        "Produce the daily digest JSON as specified."
    )

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=SYNTHESIS_SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    )

    raw = response.content[0].text.strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        return json.loads(match.group())
    raise ValueError(f"Could not parse synthesis JSON:\n{raw[:500]}")

# ── Slack ─────────────────────────────────────────────────────────────────────

def post_slack(token: str, channel: str, text: str, thread_ts: str | None = None) -> str | None:
    payload: dict[str, Any] = {
        "channel":      channel,
        "text":         text,
        "mrkdwn":       True,
        "unfurl_links": False,
    }
    if thread_ts:
        payload["thread_ts"] = thread_ts

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=15,
    )
    result = resp.json()
    if result.get("ok"):
        return result.get("ts")
    print(f"  ⚠️  Slack error: {result.get('error')}", file=sys.stderr)
    return None


def format_digest(digest: dict, run_date: date) -> str:
    date_str = run_date.strftime("%B %-d, %Y")
    lines: list[str] = [
        f"*Daily AI Trends Digest — {date_str}*",
        "━" * 42,
        "",
    ]

    for para in digest.get("summary_paragraphs", []):
        lines.append(para)
        lines.append("")

    comp_mentions = [
        m for m in digest.get("competitive_mentions", [])
        if m.get("competitor") and m.get("context")
    ]
    if comp_mentions:
        lines.append("*Competitive Mentions*")
        for m in comp_mentions:
            lines.append(f"• *{m['competitor']}* → {m['context']}")
        lines.append("")

    sources = digest.get("top_source_urls", [])[:MAX_SOURCES]
    if sources:
        lines.append("*Sources*")
        for url in sources:
            lines.append(f"• {url}")

    return "\n".join(lines)


def format_posts(digest: dict) -> str:
    lines: list[str] = []
    lp = (digest.get("linkedin_post") or "").strip()
    xp = (digest.get("x_post") or "").strip()
    if lp:
        lines += ["*Suggested LinkedIn Post*", "", lp, ""]
    if xp:
        lines += ["───", "*Suggested X Post*", "", xp]
    return "\n".join(lines).strip()

# ── Snapshot ──────────────────────────────────────────────────────────────────

def save_snapshot(run_date: date, signals: list[dict], digest: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / f"{run_date.isoformat()}.json"

    competitor_summary = {
        name: {
            "mentions": sum(1 for s in signals if name in s["competitor_names"]),
            "contexts": list({s["context_type"] for s in signals if name in s["competitor_names"]}),
        }
        for name in COMPETITORS
    }

    snapshot = {
        "date":             run_date.isoformat(),
        "signal_count":     len(signals),
        "competitors":      competitor_summary,
        "trend_insight":    digest.get("trend_insight", ""),
        "market_direction": digest.get("market_direction", ""),
        "digest_text":      format_digest(digest, run_date),
        "sources":          digest.get("top_source_urls", []),
    }
    path.write_text(json.dumps(snapshot, indent=2))
    print(f"  Snapshot: {path}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Daily AI Trends Digest")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip Slack post, print output to stdout")
    parser.add_argument("--date", metavar="YYYY-MM-DD",
                        help="Run for a specific date (default: today)")
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date) if args.date else date.today()
    config   = load_config()

    print(f"🗞️  AI Trends Digest — {run_date.isoformat()}")

    # 24h window ending at midnight UTC on run_date
    day_start = datetime(run_date.year, run_date.month, run_date.day, tzinfo=timezone.utc)
    until_ts  = int(day_start.timestamp())
    since_ts  = int((day_start - timedelta(hours=24)).timestamp())

    # ── Collect ──────────────────────────────────────────────────────────────
    print("\n📡 Collecting signals...")
    raw: list[dict] = []
    raw.extend(collect_twitter(config["twitter_key"], since_ts, until_ts))  # type: ignore[arg-type]
    raw.extend(collect_linkedin(config["linkedin_key"]))                     # type: ignore[arg-type]
    raw.extend(collect_news(config["youcom_key"]))
    print(f"  Total raw: {len(raw)}")

    if not raw:
        print("⚠️  No signals collected — aborting.", file=sys.stderr)
        sys.exit(1)

    # ── Normalize + rank ──────────────────────────────────────────────────────
    print("\n⚙️  Scoring and ranking...")
    signals     = [normalize(item) for item in raw]
    top_signals = select_top_signals(signals)
    print(f"  Selected: {len(top_signals)} of {len(signals)} signals")

    comp_counts = {}
    for s in top_signals:
        for c in s["competitor_names"]:
            comp_counts[c] = comp_counts.get(c, 0) + 1
    if comp_counts:
        print(f"  Competitors: {comp_counts}")

    # ── Synthesize ────────────────────────────────────────────────────────────
    print("\n🤖 Synthesizing with Claude...")
    client = anthropic.Anthropic(api_key=config["anthropic_key"])  # type: ignore[arg-type]
    digest = synthesize(top_signals, client, run_date)

    digest_text = format_digest(digest, run_date)
    posts_text  = format_posts(digest)

    # ── Output ────────────────────────────────────────────────────────────────
    if args.dry_run:
        print("\n" + "=" * 60)
        print(digest_text)
        if posts_text:
            print("\n[THREAD REPLY]")
            print(posts_text)
        print("=" * 60)
    else:
        print(f"\n📬 Posting to {config['slack_channel']}...")
        ts = post_slack(config["slack_token"], config["slack_channel"], digest_text)  # type: ignore[arg-type]
        if ts and posts_text:
            post_slack(config["slack_token"], config["slack_channel"], posts_text, thread_ts=ts)  # type: ignore[arg-type]
            print("  ✅ Thread reply posted")
        elif ts:
            print("  ✅ Digest posted")

    # ── Snapshot ──────────────────────────────────────────────────────────────
    save_snapshot(run_date, top_signals, digest)
    print("\n✅ Done.")


if __name__ == "__main__":
    main()
