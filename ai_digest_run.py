#!/usr/bin/env python3
"""
ai_digest_run.py — Daily AI Trends Digest

Collects signals from X (TwitterAPI.io), Reddit (SteadyAPI), and You.com news. Scores and normalizes them, synthesizes
insights + competitive intelligence via Claude, and posts a structured digest
to #daily-digest.

Usage:
    python ai_digest_run.py                        # today, post to Slack
    python ai_digest_run.py --dry-run              # skip Slack, print to stdout
    python ai_digest_run.py --date 2026-04-15      # backfill specific date

Environment (required):
    ANTHROPIC_API_KEY          Claude API
    SLACK_BOT_TOKEN            Slack bot token (xoxb-...)
    TWITTER_API_KEY            TwitterAPI.io access token

Environment (optional):
    STEADYAPI_KEY              SteadyAPI (enables Reddit collection)
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
SLACK_CHANNEL_DEFAULT = "daily-digest"

TWITTER_BASE  = "https://api.twitterapi.io/twitter/tweet/advanced_search"

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
    # Broad AI / agent trends (primary — 9 queries)
    '"AI agents"',
    '"agentic AI"',
    '"AI retrieval"',
    '"search API"',
    '"real-time search" AI',
    '"grounded AI" OR "grounding"',
    '"RAG" production',
    '"AI infrastructure"',
    '"web search" agents',
    # Competitor monitoring (secondary — 2 queries, intelligence only)
    'Tavily OR "Exa AI"',
    '"Parallel Web" OR "LinkUp AI" OR "Nimble AI"',
]

# Influential accounts to monitor — batched into groups of 5 to reduce API calls
# Each group becomes one from:a OR from:b OR ... query
TWITTER_ACCOUNTS: list[list[str]] = [
    # AI leaders / researchers
    ["sama", "demishassabis", "karpathy", "ylecun", "drfeifei"],
    ["AndrewYNg", "ID_AA_Carmack", "jasonwei", "lilianweng", "rasbt"],
    # Builders / practitioners / creators
    ["OfficialLoganK", "mattshumer", "alliekmiller", "rowancheung", "antgrasso"],
    ["swyx", "levelsio", "erikbryn", "bernardmarr", "noellerussell"],
    # Competitors + adjacent companies
    ["perplexity_ai", "ExaAILabs", "tavilyai", "parallelweb", "nimble_ai"],
    # AI orgs
    ["youdotcom", "OpenAI", "AnthropicAI", "GoogleDeepMind", "MetaAI"],
    # AI ecosystem
    ["MistralAI_", "cohere", "huggingface", "LangChainAI", "llama_index", "togethercompute"],
]

# Reddit: keyword searches (cross-subreddit, last 24h)
REDDIT_SEARCHES: list[str] = [
    "AI search API",
    "agentic search",
    "AI retrieval",
    "search API agents",
    "Tavily",
    "Exa AI",
]

# Reddit: high-signal subreddits to pull top daily posts from
REDDIT_SUBREDDITS: list[str] = [
    "LocalLLaMA",
    "artificial",
    "LangChain",
    "MachineLearning",
    "ChatGPT",
]

STEADYAPI_BASE = "https://api.steadyapi.com"

# ── Config ────────────────────────────────────────────────────────────────────

def load_config() -> dict[str, str | None]:
    load_dotenv(ENV_PATH, override=True)
    config: dict[str, str | None] = {
        "anthropic_key":  os.getenv("ANTHROPIC_API_KEY"),
        "slack_token":    os.getenv("SLACK_BOT_TOKEN"),
        "twitter_key":    os.getenv("TWITTER_API_KEY"),
        "steadyapi_key":  os.getenv("STEADYAPI_KEY"),
        "slack_channel":  os.getenv("SLACK_CHANNEL_AI_DIGEST", SLACK_CHANNEL_DEFAULT),
    }
    required = ["anthropic_key", "slack_token", "twitter_key"]
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
    """Fetch recent tweets across all query terms and named accounts (last 24h window)."""
    headers = {"X-API-Key": api_key}
    seen_ids: set[str] = set()
    items: list[dict] = []

    def _fetch_query(query_str: str, raw_query: str) -> None:
        params: dict[str, str] = {"query": query_str, "queryType": "Latest", "cursor": ""}
        try:
            resp = requests.get(TWITTER_BASE, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  ⚠️  Twitter ({raw_query!r}): {e}", file=sys.stderr)
            return

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
                "raw_query":    raw_query,
            })

        time.sleep(0.3)

    # Pass 1: keyword / topic queries
    for query_base in TWITTER_QUERIES:
        query = f"{query_base} since_time:{since_ts} until_time:{until_ts}"
        _fetch_query(query, query_base)

    # Pass 2: named influential accounts (batched — one API call per group)
    for account_group in TWITTER_ACCOUNTS:
        from_clause = " OR ".join(f"from:{a}" for a in account_group)
        query = f"({from_clause}) since_time:{since_ts} until_time:{until_ts}"
        # Label with first account in the group for diversity-cap bucketing
        raw_query = f"accounts:{account_group[0]}"
        _fetch_query(query, raw_query)

    print(f"  Twitter:  {len(items)} unique tweets")
    return items


def collect_reddit(api_key: str | None) -> list[dict]:
    """Collect Reddit posts via SteadyAPI — keyword searches + targeted subreddits."""
    if not api_key:
        print("  Reddit:   skipped (STEADYAPI_KEY not set)")
        return []

    headers = {"Authorization": f"Bearer {api_key}"}
    seen_ids: set[str] = set()
    items: list[dict] = []

    def _parse_posts(posts: list[dict], raw_query: str) -> None:
        for post in posts:
            pid = post.get("name", "")  # e.g. "t3_abc123"
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)

            title    = post.get("title", "")
            selftext = post.get("selftext", "") or ""
            # Combine title + first 600 chars of body for richer detection
            full_text = f"{title}. {selftext[:600]}".strip(". ")

            score    = post.get("score", 0) or 0
            comments = post.get("num_comments", 0) or 0
            engagement = score + comments

            permalink = post.get("permalink", "")
            url = f"https://reddit.com{permalink}" if permalink else ""

            items.append({
                "id":             pid,
                "source":         "reddit",
                "url":            url,
                "text":           full_text,
                "author":         post.get("author", ""),
                "author_context": f"r/{post.get('subreddit', '')}",
                "created_at":     str(post.get("created", "")),
                "engagement":     engagement,
                "raw_query":      raw_query,
            })

    # 1. Keyword searches (cross-subreddit, last 24h, top posts)
    for search_term in REDDIT_SEARCHES:
        params: dict[str, Any] = {
            "search":     search_term,
            "timeFilter": "day",
            "sortType":   "top",
            "limit":      25,
        }
        try:
            resp = requests.get(
                f"{STEADYAPI_BASE}/v1/reddit/search",
                headers=headers, params=params, timeout=15,
            )
            resp.raise_for_status()
            _parse_posts(resp.json().get("body", []), search_term)
        except Exception as e:
            print(f"  ⚠️  Reddit search ({search_term!r}): {e}", file=sys.stderr)
        time.sleep(0.3)

    # 2. Top daily posts from high-signal subreddits
    for subreddit in REDDIT_SUBREDDITS:
        params = {
            "subreddit":  subreddit,
            "timeFilter": "day",
            "sortType":   "top",
            "limit":      25,
        }
        try:
            resp = requests.get(
                f"{STEADYAPI_BASE}/v1/reddit/posts",
                headers=headers, params=params, timeout=15,
            )
            resp.raise_for_status()
            _parse_posts(resp.json().get("body", []), f"r/{subreddit}")
        except Exception as e:
            print(f"  ⚠️  Reddit r/{subreddit}: {e}", file=sys.stderr)
        time.sleep(0.3)

    print(f"  Reddit:   {len(items)} unique posts")
    return items

# ── Normalize + score ─────────────────────────────────────────────────────────

def normalize(item: dict) -> dict:
    """Convert a raw collected item into the standard signal schema with score."""
    detect_text = item.get("_detect_text") or item["text"]
    competitors  = detect_competitors(detect_text)
    context_type = detect_context_type(detect_text)

    engagement  = item["engagement"]
    score       = math.log1p(engagement)

    # Context quality boosts — reward signals that carry substantive insight
    if context_type in ("comparison", "benchmark"):
        score *= 1.2
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
You are a thought leadership writer for a senior sales leader at You.com.
You.com sells a Search API that AI agents and enterprise applications use to access
real-time, grounded web information. The API enables agents to search, retrieve,
and reason over current information — solving hallucination, staleness, and
knowledge-gap problems.

Your job is to produce a daily digest that:
1. Identifies meaningful trends in how AI is being built and deployed
2. Connects those trends to the problems You.com's Search API solves
3. Positions the reader as a knowledgeable, forward-thinking voice in the space

## Core rules

**Trends first, competitors last.**
The digest is primarily about what is happening in AI — agent architectures,
enterprise adoption patterns, developer workflows, infrastructure shifts.
Competitor mentions are strictly secondary intelligence, included only when
a competitor appears in multiple signals or signals a meaningful market shift.

**Never frame search APIs as commodities.**
Do not write anything that argues search retrieval is interchangeable, race-to-zero,
or undifferentiated. You.com's API has distinct value: real-time grounding, citation
quality, accuracy in agent reasoning loops, and enterprise reliability. The post
suggestions must reinforce this value, not undermine it.

**Posts must be constructive, not reactive.**
Suggested LinkedIn and X posts should be about a trend the reader finds genuinely
interesting and can speak to from an operator's perspective. They should NOT be
about competitors, NOT frame a problem without a solution, and NOT imply the
reader's market is broken or commoditized.

**Synthesis over summary.**
Do not list news. Identify patterns across signals and explain what they mean
for how AI systems are being built — and how search APIs are increasingly central
to that infrastructure.

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
Write 3 paragraphs:
  1. The dominant trend in AI building/deployment today — what practitioners are
     talking about, what problems they are encountering, what they are trying to build
  2. Why real-time search and retrieval is central to solving these problems —
     grounding agents in current information, reducing hallucinations, enabling
     accurate multi-step reasoning. Ground this in specific signals from today.
  3. Where this is heading — what this means for how enterprise AI will be built,
     and why search infrastructure that is accurate, citable, and reliable will
     be foundational (not optional) in these systems

## competitive_mentions
Only include if a competitor appeared in multiple independent signals today
or made a move that signals a genuine market shift worth tracking.
If no competitor had meaningful signal, return an empty array.

## Suggested posts
Both posts should follow this structure:
  - Open with an observation about a real trend from today's signals
  - Explain why it matters to people building AI systems
  - Show how real-time, grounded search is part of the answer
  - Close with a point of view — not a question, not a call to action

**linkedin_post**: 150–250 words. Specific, practitioner-voiced, forward-looking.
  Formatting rules (strictly enforced):
  - Line 1 must be a short, punchy hook that stands alone above the fold.
    It should be a bold observation or a surprising reframe — one or two sentences max.
    Example pattern: "Most [X] aren't [expected cause]. They're [real cause]. [emoji]"
  - Add 3–5 emojis placed naturally at the end of key sentences or paragraphs.
    Do not cluster emojis together. Use them to punctuate a point, not decorate.
  - Never use em dashes (— or --). Use a colon, comma, or new sentence instead.
  - Do NOT mention competitors.
  - Do NOT use the word "commodity" or imply the API market is undifferentiated.
  - Do NOT end with a question.
  - NEVER cite individual posts, users, or anecdotes from the signals (e.g. "one builder
    documented...", "a practitioner wrote..."). The post will be published without sources
    on LinkedIn and anonymous anecdotes read as unverified gossip. Instead, generalize
    signals into broader trend statements (e.g. "Developers are increasingly reporting...",
    "Teams building production agents are finding...", "The pattern emerging across the
    community is..."). The insight should feel like an informed operator's observation,
    not a summary of something you read.

**x_post**: ≤280 chars. One sharp, specific insight about the trend.
  No em dashes. Should make the reader think, not restate the obvious.

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
        f"Here are the top {len(signal_data)} signals collected today from X (Twitter) "
        f"and Reddit:\n\n"
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
    raw.extend(collect_reddit(config["steadyapi_key"]))
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
