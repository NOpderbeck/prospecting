#!/usr/bin/env python3
"""
linkedin_content_analysis.py — LinkedIn Content Intelligence

Searches LinkedIn for high-performing posts across a configurable set of queries.
Ranks by engagement, analyzes content patterns with Claude, publishes a structured
Google Doc report, and posts a summary to Slack.

Usage (local):
    python linkedin_content_analysis.py --dry-run              # 5 queries, top 5, no publish
    python linkedin_content_analysis.py --max-queries 10 --top 10
    python linkedin_content_analysis.py                        # full run — all queries, top 30

Reprocess existing report (skip collection):
    python linkedin_content_analysis.py --reprocess reports/li_analysis/youcom_li_analysis_2026-05-05_d7.json

Environment (required):
    ANTHROPIC_API_KEY           Claude API
    LINKEDIN_RAPIDAPI_KEY       RapidAPI key subscribed to fresh-linkedin-scraper-api

Environment (optional):
    GOOGLE_APPLICATION_CREDENTIALS  Service account key path (Cloud Run)
    GOOGLE_CREDENTIALS_FILE         OAuth credentials JSON (local dev)
    SLACK_BOT_TOKEN                 Slack bot token (xoxb-...)
    LI_ANALYSIS_DAYS                Override lookback window (default 7)
    LI_ANALYSIS_TOP                 Override top-N posts (default 30)
    LI_SLACK_CHANNEL                Slack channel ID (default: #linkedin-research)
"""

import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import anthropic
import requests
from dotenv import load_dotenv

# ── Constants ─────────────────────────────────────────────────────────────────

ENV_PATH    = Path(__file__).parent / ".env"
REPORTS_DIR = Path(__file__).parent / "reports" / "li_analysis"

LINKEDIN_HOST      = "fresh-linkedin-scraper-api.p.rapidapi.com"
DRIVE_FOLDER_ID    = "1AiUMWo32ObJ4pU9bXwuAjQrtbJg-_Lse"
GOOGLE_TOKEN_PATH  = Path(__file__).parent / ".credentials" / "google_token_li_analysis.json"
GOOGLE_SCOPES      = ["https://www.googleapis.com/auth/drive"]

DEFAULT_DAYS     = int(os.getenv("LI_ANALYSIS_DAYS", "7"))
DEFAULT_TOP_N    = int(os.getenv("LI_ANALYSIS_TOP",  "30"))
DEFAULT_CHANNEL  = os.getenv("LI_SLACK_CHANNEL", "C0B115FAS69")  # override with LI_SLACK_CHANNEL
DRY_RUN_QUERIES  = 5
MAX_PAGES        = 5      # 10 results/page → up to 50 posts per query
PAGE_DELAY       = 1.5   # seconds between requests

# ── Queries ───────────────────────────────────────────────────────────────────

DOMAIN_QUERIES: list[str] = [
    # AI search & retrieval
    "AI search API",
    "web search API developers",
    "real-time AI search",
    "LLM web grounding",
    "grounding language models",
    "RAG production deployment",
    "retrieval augmented generation",
    "agentic AI search",
    # Agent tooling
    "AI agents tools",
    "tools for AI agents",
    "function calling LLM",
    "tool use AI agents",
    "AI agent architecture",
    "building AI agents",
    # Enterprise AI
    "enterprise AI search",
    "enterprise AI assistant",
    "AI-powered enterprise",
    "AI copilot enterprise",
    # Competitive / ecosystem
    "You.com API",
    "Perplexity API",
    "Exa search",
    "Tavily search",
    "AI search comparison",
    "best search API LLM",
    # Data & freshness
    "real-time data AI",
    "fresh data LLM",
    "live data AI agents",
    "news AI agent",
]

STOPWORDS = {
    "a","an","the","and","or","but","in","on","at","to","for","of","with","by",
    "from","is","it","its","as","be","was","are","were","has","have","had","not",
    "this","that","they","their","them","we","you","your","i","my","he","she",
    "his","her","our","will","can","do","does","did","so","if","all","more",
    "also","just","up","out","about","into","than","when","what","how","who",
    "which","been","get","got","one","new","now","use","used","using","like",
    "no","any","would","could","should","there","some","make","made","very",
    "even","need","want","time","way","via","re","s","t","don","here","just",
    # URL noise
    "https","http","www","com","org","net","io","co","html","utm",
    # LinkedIn-specific noise
    "linkedin","post","share","comment","follow","connection","connect","network",
    "like","likes","repost","reposts","reaction","reactions",
    # Generic filler
    "great","good","bad","big","old","new","top","best","first","last","next",
    "let","say","see","know","think","going","said","says","well","still",
    "really","actually","literally","basically","already","never","always",
    "every","each","both","few","many","much","most","only","own","same",
    "other","another","such","too","very","yes","oh","hey","hi",
    "guy","guys","people","person","team","everyone","anyone","someone",
    "thing","things","stuff","lot","tons","bunch",
    "today","yesterday","week","month","year","day","ago","soon","ever",
    "past","future","back","coming","after","before","during","while",
}

# ── Config ────────────────────────────────────────────────────────────────────

def load_config(require_linkedin: bool = True) -> dict:
    load_dotenv(ENV_PATH, override=True)
    cfg = {
        "anthropic_key":  os.getenv("ANTHROPIC_API_KEY", ""),
        "linkedin_key":   os.getenv("LINKEDIN_RAPIDAPI_KEY", ""),
        "slack_token":    os.getenv("SLACK_BOT_TOKEN", ""),
        "creds_file":     os.getenv("GOOGLE_CREDENTIALS_FILE", ""),
    }
    required = ["anthropic_key"] + (["linkedin_key"] if require_linkedin else [])
    missing  = [k for k in required if not cfg[k]]
    if missing:
        print(f"❌ Missing required env vars: {missing}", file=sys.stderr)
        sys.exit(1)
    return cfg

# ── Collection ────────────────────────────────────────────────────────────────

def _engagement(post: dict) -> int:
    a = post.get("activity", {}) or {}
    return (
        (a.get("num_likes",    0) or 0)
        + (a.get("num_comments", 0) or 0) * 2
        + (a.get("num_shares",   0) or 0) * 3
    )


def _reaction_breakdown(post: dict) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in (post.get("activity", {}) or {}).get("reaction_counts", []):
        counts[r.get("type", "UNKNOWN")] = r.get("count", 0)
    return counts


def collect_posts(api_key: str, since_dt: datetime, max_queries: int = 0) -> list[dict]:
    headers = {
        "x-rapidapi-host": LINKEDIN_HOST,
        "x-rapidapi-key":  api_key,
        "Content-Type":    "application/json",
    }
    seen_ids: set[str] = set()
    posts: list[dict] = []

    def _fetch(keyword: str, label: str) -> None:
        for page in range(1, MAX_PAGES + 1):
            data = None
            delay = 15
            for attempt in range(3):
                try:
                    resp = requests.get(
                        f"https://{LINKEDIN_HOST}/api/v1/search/posts",
                        params={"keyword": keyword, "sort_by": "date_posted", "page": page},
                        headers=headers,
                        timeout=15,
                    )
                    if resp.status_code == 429:
                        print(f"  ⏳ [{label}] p{page}: rate limited — waiting {delay}s (attempt {attempt+1}/3)", file=sys.stderr)
                        time.sleep(delay)
                        delay *= 2
                        continue
                    if resp.status_code != 200:
                        print(f"  ⚠️  [{label}] p{page}: HTTP {resp.status_code}", file=sys.stderr)
                        break
                    data = resp.json()
                    break
                except Exception as e:
                    print(f"  ⚠️  [{label}] p{page}: {e}", file=sys.stderr)
                    break
            if data is None:
                break

            items = data.get("data", [])
            if not items:
                break

            any_recent = False
            for item in items:
                pid = str(item.get("id", ""))
                if not pid or pid in seen_ids:
                    continue

                # Parse and filter by date
                raw_ts = item.get("created_at", "")
                try:
                    created = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    created = None

                if created and created >= since_dt:
                    any_recent = True
                elif created and created < since_dt:
                    # Sorted by date desc — once we pass the window, stop paging
                    continue

                seen_ids.add(pid)
                author = item.get("author", {}) or {}
                reactions = _reaction_breakdown(item)
                posts.append({
                    "id":         pid,
                    "text":       item.get("title", ""),
                    "author":     author.get("name", ""),
                    "author_title": author.get("description", ""),
                    "author_url": author.get("url", ""),
                    "created_at": raw_ts,
                    "likes":      (item.get("activity", {}) or {}).get("num_likes",    0) or 0,
                    "comments":   (item.get("activity", {}) or {}).get("num_comments", 0) or 0,
                    "shares":     (item.get("activity", {}) or {}).get("num_shares",   0) or 0,
                    "reactions":  reactions,
                    "engagement": _engagement(item),
                    "url":        item.get("url", ""),
                    "query":      label,
                })

            if not data.get("has_more") or not any_recent:
                break
            time.sleep(PAGE_DELAY)
        time.sleep(PAGE_DELAY)

    queries = DOMAIN_QUERIES[:max_queries] if max_queries else DOMAIN_QUERIES
    days    = (datetime.now(timezone.utc) - since_dt).days
    print(f"\n📡 Collecting posts ({days}d window, {len(queries)}/{len(DOMAIN_QUERIES)} queries)...")
    for q in queries:
        _fetch(q, q)
        print(f"  [{len(posts):4d}] {q}")
    return posts


def rank_posts(posts: list[dict], top_n: int) -> list[dict]:
    seen: set[str] = set()
    unique = [p for p in posts if not (p["id"] in seen or seen.add(p["id"]))]  # type: ignore
    ranked = sorted(unique, key=lambda p: p["engagement"], reverse=True)
    print(f"\n📊 {len(unique)} unique posts — analyzing top {min(top_n, len(ranked))}")
    return ranked[:top_n]

# ── Keyword density ───────────────────────────────────────────────────────────

def keyword_density(posts: list[dict], top_n: int = 25) -> list[tuple[str, int]]:
    counts: Counter = Counter()
    for p in posts:
        text   = re.sub(r"https?://\S+", " ", p["text"]).lower()
        tokens = re.findall(r"\b[a-z][a-z0-9\-]{2,}\b", text)
        clean  = [t for t in tokens if t not in STOPWORDS]
        for tok in clean:
            counts[tok] += 1
        for a, b in zip(clean, clean[1:]):
            counts[f"{a} {b}"] += 1
    filtered = {k: v for k, v in counts.items() if v >= 2}
    return sorted(filtered.items(), key=lambda x: x[1], reverse=True)[:top_n]

# ── Claude analysis ───────────────────────────────────────────────────────────

ANALYSIS_SYSTEM = """\
You are an analysis agent reverse-engineering why certain LinkedIn posts perform well.

Analyze the given posts (AI agents, RAG, real-time retrieval, enterprise AI, and search tooling space) and return a single valid JSON object. No markdown, no preamble — raw JSON only.

Schema:
{
  "executive_summary": "INTELLIGENCE BRIEF — 4 sentences, declarative and confident. Do NOT reference post numbers or specific authors. Structure: (1) the dominant content pattern or theme driving top performance this week; (2) the sharpest dividing line between high- and low-performing posts — what separates them; (3) the single most actionable takeaway for someone creating LinkedIn content in this space right now; (4) the most surprising or counterintuitive signal in the data. Write as conclusions, not observations. No hedging language.",
  "slack_teaser": "2–3 sentence Slack preview of the report. Do NOT reference post numbers or author names. Lead with the single sharpest insight from the dataset, follow with the most actionable takeaway, and close with a hook that makes them want to open the full report. Plain prose — no bullet points, no markdown.",
  "aggregate": {
    "angles": [
      {"label": "angle name", "detail": "why it performs on LinkedIn, 1–2 sentences", "posts": [1, 3]}
    ],
    "hooks": [
      {"name": "hook pattern name", "pattern": "fill-in-the-blank template", "example": "example from data (cite post #)", "why": "1 sentence"}
    ],
    "tactics": ["specific LinkedIn tactic observed across multiple posts — format, structure, opener style"],
    "success_factors": ["specific, actionable signal — reference post numbers"],
    "anti_patterns": ["what's absent or suppresses performance — reference post numbers"]
  },
  "playbook": {
    "rules": [{"title": "Rule title", "body": "specific, actionable LinkedIn rule — no generic advice"}],
    "templates": [{"name": "Template name", "pattern": "fill-in-the-blank", "example": "example for AI search/agent content"}],
    "structures": [{"name": "Structure name", "hook": "hook skeleton", "body": "body skeleton", "close": "close skeleton", "derived_from": "Post N, N"}]
  },
  "posts": [
    {"n": 1, "theme": "primary theme", "hook": "hook type", "format": "Single-block/Multi-line/Numbered-list/Story/Insight-drop", "triggers": "comma-separated key triggers", "verdict": "1–2 sentences why it performed — grounded in metrics"}
  ]
}

LinkedIn-specific guidance:
- LinkedIn rewards personal credibility anchors ("I've spent 10 years...", "We just shipped...") — flag these as a hook pattern when present.
- Numbered lists and line-break formatting are strong LinkedIn signals — note when they appear.
- Reaction diversity matters: EMPATHY + INTEREST reactions signal thought-leadership content; LIKE-only signals informational/utility content.
- Comments-to-likes ratio > 0.15 indicates a post sparked genuine discussion — flag these.
- Professional titles in author_title that match the content domain amplify credibility — note when author authority reinforces the post.

Constraints:
- executive_summary + slack_teaser: NO post numbers, NO author names. Confident declarative statements only.
- aggregate/playbook/posts: Every claim must cite a post number from the input.
- No generic advice ("be engaging", "post quality content").
- Anti-patterns must be grounded in posts that underperformed relative to their engagement.
- Keep each "verdict" under 40 words.
- Keep each success_factor and anti_pattern under 25 words.
- Keep each rule "body" under 40 words.
- Return ONLY the JSON object — no surrounding text.
"""


def analyze(posts: list[dict], client: anthropic.Anthropic, days: int) -> dict:
    post_data = []
    for i, p in enumerate(posts, 1):
        reactions = p.get("reactions", {})
        post_data.append({
            "n":           i,
            "text":        p["text"][:250] + ("…" if len(p["text"]) > 250 else ""),
            "author":      p["author"],
            "author_title": p.get("author_title", ""),
            "likes":       p["likes"],
            "comments":    p["comments"],
            "shares":      p["shares"],
            "reactions":   reactions,
            "created_at":  p["created_at"][:10] if p.get("created_at") else "",
        })

    user_msg = (
        f"Window: past {days} days. Posts: {len(posts)} (highest engagement first).\n\n"
        f"{json.dumps(post_data, indent=2)}\n\n"
        "Return the JSON analysis object. Keep all string values concise per the constraints."
    )

    print("\n🤖 Analyzing with Claude...")
    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        system=ANALYSIS_SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = resp.content[0].text.strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        return json.loads(match.group())
    raise ValueError(f"Could not parse analysis JSON:\n{raw[:400]}")

# ── Google auth ───────────────────────────────────────────────────────────────

def get_google_creds(creds_file: str):
    sa_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if sa_key and os.path.exists(sa_key):
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_file(sa_key, scopes=GOOGLE_SCOPES)

    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request as GoogleRequest

    creds = None
    if GOOGLE_TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(GOOGLE_TOKEN_PATH), GOOGLE_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())
        elif creds_file and os.path.exists(creds_file):
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, GOOGLE_SCOPES)
            creds = flow.run_local_server(port=0)
        else:
            creds = None
        if creds:
            try:
                GOOGLE_TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
                GOOGLE_TOKEN_PATH.write_text(creds.to_json())
            except OSError:
                Path("/tmp/google_token_li_analysis.json").write_text(creds.to_json())

    if creds and creds.valid:
        return creds

    try:
        import google.auth
        creds, _ = google.auth.default(scopes=GOOGLE_SCOPES)
        return creds
    except Exception:
        pass

    print("❌ No Google credentials found", file=sys.stderr)
    sys.exit(1)

# ── Docs helpers ──────────────────────────────────────────────────────────────

def batch_update(docs_svc, doc_id: str, requests_: list):
    from googleapiclient.errors import HttpError
    delay = 15
    for attempt in range(6):
        try:
            docs_svc.documents().batchUpdate(
                documentId=doc_id, body={"requests": requests_}
            ).execute()
            return
        except HttpError as e:
            if e.resp.status == 429 and attempt < 5:
                print(f"  Rate limited — retrying in {delay}s...", file=sys.stderr)
                time.sleep(delay)
                delay *= 2
            else:
                raise


def doc_end(docs_svc, doc_id: str) -> int:
    d = docs_svc.documents().get(documentId=doc_id).execute()
    return d["body"]["content"][-1]["endIndex"] - 1


def append_segments(docs_svc, doc_id: str, segments: list):
    if not segments:
        return
    insert_pos = doc_end(docs_svc, doc_id)
    full_text  = "\n".join(t for t, _, _ in segments) + "\n"
    batch_update(docs_svc, doc_id,
        [{"insertText": {"location": {"index": insert_pos}, "text": full_text}}])

    style_reqs = []
    pos = insert_pos
    for text, style, bold in segments:
        end = pos + len(text) + 1
        if style != "NORMAL_TEXT":
            style_reqs.append({"updateParagraphStyle": {
                "range": {"startIndex": pos, "endIndex": end},
                "paragraphStyle": {"namedStyleType": style},
                "fields": "namedStyleType",
            }})
        if bold and text:
            style_reqs.append({"updateTextStyle": {
                "range": {"startIndex": pos, "endIndex": end - 1},
                "textStyle": {"bold": True},
                "fields": "bold",
            }})
        pos = end
    for i in range(0, len(style_reqs), 200):
        batch_update(docs_svc, doc_id, style_reqs[i:i+200])


def append_bullets(docs_svc, doc_id: str, items: list):
    if not items:
        return
    lines = []
    for label, body in items:
        lines.append(f"• {label}  {body}" if label else f"• {body}")
    full_text  = "\n".join(lines) + "\n"
    insert_pos = doc_end(docs_svc, doc_id)
    batch_update(docs_svc, doc_id,
        [{"insertText": {"location": {"index": insert_pos}, "text": full_text}}])

    bold_reqs = []
    pos = insert_pos
    for label, body in items:
        line_len = len(f"• {label}  {body}" if label else f"• {body}") + 1
        if label:
            bold_start = pos + 2
            bold_end   = pos + 2 + len(label)
            bold_reqs.append({"updateTextStyle": {
                "range": {"startIndex": bold_start, "endIndex": bold_end},
                "textStyle": {"bold": True}, "fields": "bold",
            }})
        pos += line_len
    if bold_reqs:
        batch_update(docs_svc, doc_id, bold_reqs)


def append_table(docs_svc, doc_id: str, headers: list, rows: list, urls: list = None):
    if not rows:
        return
    insert_pos = doc_end(docs_svc, doc_id)
    n_cols     = len(headers)
    batch_update(docs_svc, doc_id, [{"insertTable": {
        "rows": len(rows) + 1, "columns": n_cols,
        "location": {"index": insert_pos},
    }}])

    d          = docs_svc.documents().get(documentId=doc_id).execute()
    table_elem = next(
        (e["table"] for e in reversed(d["body"]["content"]) if "table" in e), None
    )
    if not table_elem:
        return

    cell_data = []
    for ci, h in enumerate(headers):
        cell = table_elem["tableRows"][0]["tableCells"][ci]
        cell_data.append((cell["content"][0]["startIndex"], h, True, None))

    for ri, row in enumerate(rows, start=1):
        url = urls[ri - 1] if urls else None
        for ci, val in enumerate(row):
            cell = table_elem["tableRows"][ri]["tableCells"][ci]
            cell_url = url if ci == 0 else None
            cell_data.append((cell["content"][0]["startIndex"], str(val), False, cell_url))

    cell_data.sort(key=lambda x: -x[0])
    cell_reqs = []
    for idx, text, bold, url in cell_data:
        cell_reqs.append({"insertText": {"location": {"index": idx}, "text": text}})
        if bold and text:
            cell_reqs.append({"updateTextStyle": {
                "range": {"startIndex": idx, "endIndex": idx + len(text)},
                "textStyle": {"bold": True}, "fields": "bold",
            }})
        if url and text:
            cell_reqs.append({"updateTextStyle": {
                "range": {"startIndex": idx, "endIndex": idx + len(text)},
                "textStyle": {"link": {"url": url}}, "fields": "link",
            }})
    for i in range(0, len(cell_reqs), 200):
        batch_update(docs_svc, doc_id, cell_reqs[i:i+200])


def build_toc(docs_svc, doc_id: str, sections: list[tuple[str, str]]):
    d       = docs_svc.documents().get(documentId=doc_id).execute()
    content = d["body"]["content"]

    heading_map = {}
    for elem in content:
        if "paragraph" not in elem:
            continue
        para  = elem["paragraph"]
        style = para.get("paragraphStyle", {})
        if style.get("namedStyleType") != "HEADING_2":
            continue
        text = "".join(r.get("textRun", {}).get("content", "") for r in para.get("elements", []))
        hid  = style.get("headingId")
        if hid:
            heading_map[text] = hid

    resolved = [
        (label, next((hid for txt, hid in heading_map.items() if kw in txt), None))
        for label, kw in sections
    ]

    para_count, insert_pos = 0, None
    for elem in content:
        if "paragraph" in elem:
            para_count += 1
            if para_count == 2:
                insert_pos = elem["endIndex"]
                break
    if insert_pos is None:
        return

    header = "Table of Contents"
    lines  = ["\n", header + "\n"] + [label + "\n" for label, _ in resolved]
    batch_update(docs_svc, doc_id,
        [{"insertText": {"location": {"index": insert_pos}, "text": "".join(lines)}}])

    style_reqs = []
    pos = insert_pos + 1
    style_reqs.append({"updateTextStyle": {
        "range": {"startIndex": pos, "endIndex": pos + len(header)},
        "textStyle": {"bold": True}, "fields": "bold",
    }})
    pos += len(header) + 1
    for label, hid in resolved:
        if hid:
            url = f"https://docs.google.com/document/d/{doc_id}/edit#heading={hid}"
            style_reqs.append({"updateTextStyle": {
                "range": {"startIndex": pos, "endIndex": pos + len(label)},
                "textStyle": {"link": {"url": url}}, "fields": "link",
            }})
        pos += len(label) + 1
    if style_reqs:
        batch_update(docs_svc, doc_id, style_reqs)


def _walk_content(content: list, runs: list) -> None:
    for elem in content:
        if "paragraph" in elem:
            for el in elem["paragraph"].get("elements", []):
                if "textRun" in el:
                    runs.append((el["startIndex"], el["endIndex"], el["textRun"]["content"]))
        elif "table" in elem:
            for row in elem["table"].get("tableRows", []):
                for cell in row.get("tableCells", []):
                    _walk_content(cell.get("content", []), runs)
        elif "tableOfContents" in elem:
            _walk_content(elem["tableOfContents"].get("content", []), runs)


def collect_post_heading_ids(docs_svc, doc_id: str) -> dict[int, str]:
    d       = docs_svc.documents().get(documentId=doc_id).execute()
    result  = {}
    pattern = re.compile(r"^Post #(\d+)\b")
    for elem in d["body"]["content"]:
        if "paragraph" not in elem:
            continue
        para  = elem["paragraph"]
        style = para.get("paragraphStyle", {})
        if style.get("namedStyleType") != "HEADING_3":
            continue
        text = "".join(
            r.get("textRun", {}).get("content", "") for r in para.get("elements", [])
        ).strip()
        hid = style.get("headingId")
        m   = pattern.match(text)
        if m and hid:
            result[int(m.group(1))] = hid
    return result


def apply_post_links(docs_svc, doc_id: str, post_heading_ids: dict[int, str]) -> int:
    if not post_heading_ids:
        return 0
    d    = docs_svc.documents().get(documentId=doc_id).execute()
    runs: list = []
    _walk_content(d["body"]["content"], runs)

    pattern   = re.compile(r"#(\d+)\b")
    link_reqs = []
    for start, end, text in runs:
        for m in pattern.finditer(text):
            n = int(m.group(1))
            if n not in post_heading_ids:
                continue
            hid       = post_heading_ids[n]
            url       = f"https://docs.google.com/document/d/{doc_id}/edit#heading={hid}"
            abs_start = start + m.start()
            abs_end   = start + m.end()
            link_reqs.append({"updateTextStyle": {
                "range": {"startIndex": abs_start, "endIndex": abs_end},
                "textStyle": {"link": {"url": url}},
                "fields": "link",
            }})
    for i in range(0, len(link_reqs), 200):
        batch_update(docs_svc, doc_id, link_reqs[i:i+200])
    return len(link_reqs)


def append_source_post(docs_svc, doc_id: str, n: int, p: dict):
    H3 = "HEADING_3"
    heading  = f"Post #{n} — {p['author']}"
    if p.get("author_title"):
        heading += f"  ·  {p['author_title'][:60]}"
    metrics  = f"👍 {p['likes']:,}  💬 {p['comments']:,}  🔁 {p['shares']:,}"
    reactions = p.get("reactions", {})
    if reactions:
        rxn_str = "  ".join(f"{k.title()}: {v}" for k, v in sorted(reactions.items(), key=lambda x: -x[1]))
        metrics += f"  ·  {rxn_str}"
    metrics += "  ·  View →"
    preview  = p["text"][:300] + ("…" if len(p["text"]) > 300 else "")
    full     = f"{heading}\n{metrics}\n{preview}\n\n"

    insert_pos = doc_end(docs_svc, doc_id)
    batch_update(docs_svc, doc_id,
        [{"insertText": {"location": {"index": insert_pos}, "text": full}}])

    reqs = []
    reqs.append({"updateParagraphStyle": {
        "range": {"startIndex": insert_pos, "endIndex": insert_pos + len(heading) + 1},
        "paragraphStyle": {"namedStyleType": H3},
        "fields": "namedStyleType",
    }})
    view_text  = "View →"
    met_start  = insert_pos + len(heading) + 1
    view_start = met_start + len(metrics) - len(view_text)
    view_end   = met_start + len(metrics)
    if p.get("url"):
        reqs.append({"updateTextStyle": {
            "range": {"startIndex": view_start, "endIndex": view_end},
            "textStyle": {"link": {"url": p["url"]}},
            "fields": "link",
        }})
    batch_update(docs_svc, doc_id, reqs)

# ── Google Doc builder ────────────────────────────────────────────────────────

def build_google_doc(docs_svc, doc_id: str, posts: list[dict], analysis: dict,
                     kd: list[tuple[str, int]], date_str: str, days: int,
                     total_collected: int = 0, queries_run: int = 0):
    H1 = "HEADING_1"; H2 = "HEADING_2"; H3 = "HEADING_3"; N = "NORMAL_TEXT"
    def S(*args): return list(args)

    agg      = analysis.get("aggregate", {})
    playbook = analysis.get("playbook",  {})
    post_analyses = {p["n"]: p for p in analysis.get("posts", [])}

    print("  Writing title & summary...", file=sys.stderr)
    append_segments(docs_svc, doc_id, [
        S(f"LinkedIn Content Intelligence — {date_str}", H1, False),
        S(
            f"Analysis based upon top {len(posts)} performing posts, "
            f"sourced from {total_collected:,} posts across {queries_run} search queries  ·  {days}-day window",
            N, False,
        ),
    ])

    exec_summary = analysis.get("executive_summary", "")
    append_segments(docs_svc, doc_id, [
        S("", N, False),
        S("📊  Executive Summary", H2, False),
    ])
    if exec_summary:
        insert_pos = doc_end(docs_svc, doc_id)
        batch_update(docs_svc, doc_id,
            [{"insertText": {"location": {"index": insert_pos}, "text": exec_summary + "\n"}}])
        batch_update(docs_svc, doc_id, [{"updateTextStyle": {
            "range": {"startIndex": insert_pos, "endIndex": insert_pos + len(exec_summary)},
            "textStyle": {"italic": True},
            "fields": "italic",
        }}])
    append_segments(docs_svc, doc_id, [
        S("", N, False),
        S("Keyword Density — most frequent terms across analyzed posts", N, True),
    ])
    kd_rows = [[term, str(count)] for term, count in kd[:20]]
    append_table(docs_svc, doc_id, ["Term", "Count"], kd_rows)
    append_segments(docs_svc, doc_id, [S("", N, False)])

    print("  Writing aggregate findings...", file=sys.stderr)
    append_segments(docs_svc, doc_id, [S("🎯  Aggregate Findings", H2, False)])

    append_segments(docs_svc, doc_id, [S("Common Angles & Themes", H3, False)])
    angle_items = []
    for a in agg.get("angles", []):
        posts_ref = ", ".join(f"#{n}" for n in a.get("posts", []))
        label     = f"{a['label']}  [{posts_ref}]" if posts_ref else a["label"]
        angle_items.append((label, a.get("detail", "")))
    append_bullets(docs_svc, doc_id, angle_items)
    append_segments(docs_svc, doc_id, [S("", N, False)])

    append_segments(docs_svc, doc_id, [S("Hook Patterns That Dominate", H3, False)])
    hook_rows = [
        [h.get("name",""), h.get("pattern",""), h.get("why","")]
        for h in agg.get("hooks", [])
    ]
    if hook_rows:
        append_table(docs_svc, doc_id, ["Hook Name", "Pattern", "Why It Works"], hook_rows)
    append_segments(docs_svc, doc_id, [S("", N, False)])

    append_segments(docs_svc, doc_id, [S("Winning Tactics & Formats", H3, False)])
    append_bullets(docs_svc, doc_id, [(None, t) for t in agg.get("tactics", [])])
    append_segments(docs_svc, doc_id, [S("", N, False)])

    append_segments(docs_svc, doc_id, [S("High-Confidence Success Factors", H3, False)])
    append_bullets(docs_svc, doc_id, [(None, f) for f in agg.get("success_factors", [])])
    append_segments(docs_svc, doc_id, [S("", N, False)])

    append_segments(docs_svc, doc_id, [S("Anti-Patterns to Avoid", H3, False)])
    append_bullets(docs_svc, doc_id, [(None, ap) for ap in agg.get("anti_patterns", [])])
    append_segments(docs_svc, doc_id, [S("", N, False)])

    print("  Writing playbook...", file=sys.stderr)
    append_segments(docs_svc, doc_id, [
        S("📋  Actionable Playbook", H2, False),
        S("Rules for Writing High-Performing LinkedIn Posts in the AI Search & Retrieval Space", N, True),
    ])
    for r in playbook.get("rules", []):
        append_segments(docs_svc, doc_id, [
            S(r.get("title", ""), N, True),
            S(r.get("body",  ""), N, False),
        ])
    append_segments(docs_svc, doc_id, [S("", N, False)])

    append_segments(docs_svc, doc_id, [S("Hook Templates", N, True)])
    tmpl_rows = [
        [t.get("name",""), t.get("pattern",""), t.get("example","")]
        for t in playbook.get("templates", [])
    ]
    if tmpl_rows:
        append_table(docs_svc, doc_id, ["Name", "Fill-in Pattern", "Example"], tmpl_rows)
    append_segments(docs_svc, doc_id, [S("", N, False)])

    append_segments(docs_svc, doc_id, [S("Post Structures That Work", N, True)])
    for st in playbook.get("structures", []):
        body = (
            f"Hook: {st.get('hook','')}\n"
            f"Body: {st.get('body','')}\n"
            f"Close: {st.get('close','')}\n"
            f"Derived from: {st.get('derived_from','')}"
        )
        append_segments(docs_svc, doc_id, [
            S(st.get("name", ""), N, True),
            S(body, N, False),
            S("", N, False),
        ])

    print("  Writing per-post analysis...", file=sys.stderr)
    append_segments(docs_svc, doc_id, [S("🔍  Per-Post Analysis", H2, False)])
    pa_rows = []
    for i, p in enumerate(posts, 1):
        pa = post_analyses.get(i, {})
        pa_rows.append([
            f"#{i} {p['author']}",
            pa.get("theme",    ""),
            pa.get("hook",     ""),
            pa.get("format",   ""),
            pa.get("triggers", ""),
            pa.get("verdict",  ""),
        ])
    append_table(
        docs_svc, doc_id,
        ["Post", "Theme", "Hook Type", "Format", "Key Triggers", "Why It Performed"],
        pa_rows,
        urls=[p["url"] for p in posts],
    )
    append_segments(docs_svc, doc_id, [S("", N, False)])

    print("  Writing source posts...", file=sys.stderr)
    append_segments(docs_svc, doc_id, [
        S("📑  Source Posts — Ranked by Engagement", H2, False),
        S("Posts collected from LinkedIn via Fresh LinkedIn Scraper API (RapidAPI), "
          "ranked by weighted engagement score. "
          "Each post heading is a named anchor — #N references throughout this report link here.",
          N, False),
        S("", N, False),
    ])
    for i, p in enumerate(posts, 1):
        append_source_post(docs_svc, doc_id, i, p)

    print("  Writing methodology...", file=sys.stderr)
    append_segments(docs_svc, doc_id, [
        S("📐  Methodology — Reference", H2, False),
        S("Engagement Score  =  likes  +  (comments × 2)  +  (shares × 3)", N, True),
        S("", N, False),
        S("Comments weighted 2× and shares 3× as stronger amplification/discussion signals. "
          "No view data available from API.", N, False),
        S("", N, False),
        S(f"Queries run: {queries_run}  ·  Platform: LinkedIn via Fresh LinkedIn Scraper API  ·  "
          f"Sort: date_posted (then re-ranked by engagement score)", N, False),
        S("", N, False),
        S("Search Queries Used", N, True),
    ])
    q_rows = [[str(i+1), q] for i, q in enumerate(DOMAIN_QUERIES[:queries_run])]
    append_table(docs_svc, doc_id, ["#", "Query"], q_rows)
    append_segments(docs_svc, doc_id, [S("", N, False)])

    print("  Inserting TOC...", file=sys.stderr)
    build_toc(docs_svc, doc_id, [
        ("Executive Summary",    "Executive Summary"),
        ("Aggregate Findings",   "Aggregate Findings"),
        ("Actionable Playbook",  "Actionable Playbook"),
        ("Per-Post Analysis",    "Per-Post Analysis"),
        ("Source Posts",         "Source Posts"),
        ("Methodology",          "Methodology"),
    ])

    print("  Applying post anchor links...", file=sys.stderr)
    post_heading_ids = collect_post_heading_ids(docs_svc, doc_id)
    n_links = apply_post_links(docs_svc, doc_id, post_heading_ids)
    print(f"  → {n_links} links applied across {len(post_heading_ids)} post anchors",
          file=sys.stderr)


def get_or_create_doc(docs_svc, drive_svc, title: str) -> str:
    existing = drive_svc.files().list(
        q=(f"name='{title}' and '{DRIVE_FOLDER_ID}' in parents "
           f"and mimeType='application/vnd.google-apps.document' and trashed=false"),
        supportsAllDrives=True, includeItemsFromAllDrives=True, fields="files(id)",
    ).execute().get("files", [])

    if existing:
        doc_id = existing[0]["id"]
        print(f"  Overwriting existing doc ({doc_id})...", file=sys.stderr)
        d   = docs_svc.documents().get(documentId=doc_id).execute()
        end = d["body"]["content"][-1]["endIndex"]
        if end > 2:
            batch_update(docs_svc, doc_id,
                [{"deleteContentRange": {"range": {"startIndex": 1, "endIndex": end - 1}}}])
    else:
        doc = drive_svc.files().create(
            body={"name": title, "mimeType": "application/vnd.google-apps.document",
                  "parents": [DRIVE_FOLDER_ID]},
            supportsAllDrives=True, fields="id",
        ).execute()
        doc_id = doc["id"]
        drive_svc.permissions().create(
            fileId=doc_id,
            body={"type": "domain", "role": "reader", "domain": "you.com"},
            supportsAllDrives=True, fields="id",
        ).execute()

    return doc_id


def publish_google_doc(creds, title: str, posts: list[dict], analysis: dict,
                       kd: list[tuple[str, int]], date_str: str, days: int,
                       total_collected: int = 0, queries_run: int = 0) -> str:
    from googleapiclient.discovery import build
    docs_svc  = build("docs", "v1", credentials=creds)
    drive_svc = build("drive", "v3", credentials=creds)

    doc_id = get_or_create_doc(docs_svc, drive_svc, title)
    build_google_doc(docs_svc, doc_id, posts, analysis, kd, date_str, days,
                     total_collected=total_collected, queries_run=queries_run)
    return f"https://docs.google.com/document/d/{doc_id}/edit"

# ── Slack ─────────────────────────────────────────────────────────────────────

def build_slack_message(posts: list[dict], analysis: dict, kd: list[tuple[str, int]],
                        date_str: str, days: int, doc_url: str,
                        total_collected: int = 0, queries_run: int = 0) -> str:
    teaser = analysis.get("slack_teaser", "") or analysis.get("executive_summary", "")

    if total_collected and queries_run:
        subtitle = (f"Top {len(posts)} posts from {total_collected:,} collected · "
                    f"{queries_run} queries · {days}-day window")
    else:
        subtitle = f"{days}-day window · {len(posts)} posts analyzed"

    kd_line = "  ".join(f"`{t}` ({c})" for t, c in kd[:10])

    lines = [
        f"*💼 LinkedIn Content Intelligence — {date_str}*",
        f"_{subtitle}_",
        "",
        teaser,
        "",
        f"*📊 Top Keywords:*  {kd_line}",
        "",
        f"📄 *<{doc_url}|Full Report →>*",
    ]
    return "\n".join(lines)


def post_to_slack(token: str, channel: str, posts: list[dict], analysis: dict,
                  kd: list[tuple[str, int]], date_str: str, days: int, doc_url: str,
                  dry_run: bool = False, total_collected: int = 0, queries_run: int = 0):
    text = build_slack_message(posts, analysis, kd, date_str, days, doc_url,
                               total_collected=total_collected, queries_run=queries_run)
    if dry_run:
        print("\n── DRY RUN: Slack message (not posted) ──────────────────────────")
        print(text)
        print("─────────────────────────────────────────────────────────────────\n")
        return
    if not token:
        print("⚠️  No SLACK_BOT_TOKEN — skipping Slack post", file=sys.stderr)
        return
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"channel": channel, "text": text, "mrkdwn": True, "unfurl_links": False},
        timeout=10,
    )
    result = resp.json()
    if result.get("ok"):
        print(f"✅ Posted to {channel}")
    else:
        print(f"⚠️  Slack error: {result.get('error')}", file=sys.stderr)

# ── Reprocess ─────────────────────────────────────────────────────────────────

def load_posts_for_reprocess(path: Path) -> tuple[list[dict], dict]:
    if path.suffix == ".json":
        print(f"📂 Loading posts from JSON: {path.name}")
        data = json.loads(path.read_text())
        if isinstance(data, list):
            return data, {}
        return data.get("posts", []), data.get("meta", {})
    print("❌ Only .json sidecars are supported for --reprocess.", file=sys.stderr)
    sys.exit(1)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="LinkedIn Content Intelligence")
    parser.add_argument("--days",        type=int,  default=DEFAULT_DAYS,  help=f"Lookback window in days (default {DEFAULT_DAYS})")
    parser.add_argument("--top",         type=int,  default=DEFAULT_TOP_N, help=f"Top N posts to analyze (default {DEFAULT_TOP_N})")
    parser.add_argument("--max-queries", type=int,  default=0,             help="Limit queries (0=all; use 3–5 for fast tests)")
    parser.add_argument("--dry-run",     action="store_true",              help="5 queries, top 5, print only — no Drive/Slack publish")
    parser.add_argument("--reprocess",   type=str,  default="",            help="Path to existing .json sidecar — skips collection")
    parser.add_argument("--no-publish",  action="store_true",              help="Run full collection + analysis but skip Drive/Slack")
    parser.add_argument("--channel",     type=str,  default=DEFAULT_CHANNEL, help="Slack channel ID")
    args = parser.parse_args()

    reprocessing = bool(args.reprocess)
    config = load_config(require_linkedin=not reprocessing)
    client = anthropic.Anthropic(api_key=config["anthropic_key"])

    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%B %-d, %Y")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    total_collected = 0
    queries_run     = 0

    if reprocessing:
        path        = Path(args.reprocess).expanduser()
        posts, meta = load_posts_for_reprocess(path)
        m           = re.search(r"_d(\d+)", path.stem)
        days            = meta.get("days") or (int(m.group(1)) if m else args.days)
        total_collected = meta.get("total_collected", 0)
        queries_run     = meta.get("queries_run", 0)
    else:
        days        = args.days
        since_dt    = now - timedelta(days=days)
        max_queries = args.max_queries or (DRY_RUN_QUERIES if args.dry_run else 0)
        top_n       = 5 if args.dry_run and not args.max_queries else args.top

        raw   = collect_posts(config["linkedin_key"], since_dt, max_queries=max_queries)
        posts = rank_posts(raw, top_n)

        if not posts:
            print("❌ No posts found. Try expanding --days or --max-queries.", file=sys.stderr)
            sys.exit(1)

        total_collected = len(raw)
        queries_run     = min(max_queries, len(DOMAIN_QUERIES)) if max_queries else len(DOMAIN_QUERIES)

        stem    = f"youcom_li_analysis_{now.strftime('%Y-%m-%d')}_d{days}"
        sidecar = REPORTS_DIR / f"{stem}.json"
        sidecar.write_text(json.dumps({
            "meta": {"total_collected": total_collected, "queries_run": queries_run, "days": days},
            "posts": posts,
        }, indent=2))
        print(f"💾 Sidecar saved → {sidecar}  (use --reprocess to re-analyze)")

    kd       = keyword_density(posts)
    analysis = analyze(posts, client, days)

    skip_publish = args.dry_run or args.no_publish

    if skip_publish:
        post_to_slack(
            config["slack_token"], args.channel, posts, analysis, kd,
            date_str, days, doc_url="<not published>", dry_run=True,
            total_collected=total_collected, queries_run=queries_run,
        )
        print("ℹ️  Run without --dry-run to publish to Drive + Slack.")
        return

    print("\n📝 Authenticating with Google...")
    creds = get_google_creds(config["creds_file"])
    title = f"LinkedIn Content Intelligence — {date_str}"
    if reprocessing:
        title += " (reprocessed)"
    print(f"📝 Building Google Doc: '{title}'...")
    doc_url = publish_google_doc(creds, title, posts, analysis, kd, date_str, days,
                                 total_collected=total_collected, queries_run=queries_run)
    print(f"✅ Doc published → {doc_url}")

    post_to_slack(config["slack_token"], args.channel, posts, analysis, kd,
                  date_str, days, doc_url,
                  total_collected=total_collected, queries_run=queries_run)


if __name__ == "__main__":
    main()
