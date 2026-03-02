"""
score.py — You.com API Fit Scorer

Profiles and scores a company against You.com Search API / RAG buying signals.
Optionally ingests an existing research report from research.py as additional context.

Usage:
    python score.py "Salesforce"
    python score.py "Salesforce" --research-file reports/salesforce_2026-02-28.md
    python score.py "Salesforce" --output-dir my_reports --verbose
"""

import os
import re
import sys
import time
import glob
import argparse
from datetime import date

import requests
import anthropic
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Scoring Dimensions
# Each has a key, label, max score, and description of what 0/1/2 means.
# ---------------------------------------------------------------------------

DIMENSIONS = [
    {
        "key": "llm_chat_citations",
        "label": "External-facing LLM / chat with citations",
        "max": 2,
        "rubric": (
            "2 = Clear evidence of a customer-facing chat or LLM product that cites sources, "
            "displays grounded answers, or markets research/intelligence/monitoring capabilities. "
            "1 = Product exists but citation/grounding is unclear or secondary. "
            "0 = No external-facing LLM/chat found, or product is purely internal."
        ),
    },
    {
        "key": "agent_workflows",
        "label": "Agent workflows requiring browsing / research",
        "max": 2,
        "rubric": (
            "2 = Builds or promotes AI agents that research, compare, browse the web, or act on "
            "behalf of users (e.g. coding assistants, DevOps copilots, deep-research agents). "
            "1 = Some agentic capabilities but browsing/retrieval is not a stated core use case. "
            "0 = No agent workflows, or agents operate on internal data only."
        ),
    },
    {
        "key": "timeliness_critical",
        "label": "Timeliness-critical domain",
        "max": 2,
        "rubric": (
            "2 = Operates in a domain where data freshness is essential (news, financial markets, "
            "travel, commerce pricing, cybersecurity, regulatory updates) AND markets real-time, "
            "live, or continuously updated content. "
            "1 = Some freshness requirements but not a primary differentiator. "
            "0 = Domain is largely static or freshness is not relevant."
        ),
    },
    {
        "key": "existing_search_integration",
        "label": "Existing third-party search integration",
        "max": 2,
        "rubric": (
            "2 = Confirmed use of a third-party search or SERP API (e.g. Bing, Google, SerpAPI, "
            "Brave, Exa, Tavily, or similar), or references RAG / retrieval pipelines in public "
            "engineering content, architecture diagrams, or job postings. "
            "1 = Indirect signals (e.g. general mention of web grounding without specifying vendor). "
            "0 = No evidence of external search tooling."
        ),
    },
    {
        "key": "hiring_retrieval",
        "label": "Hiring for retrieval / search roles",
        "max": 2,
        "rubric": (
            "2 = Active or recent job postings for retrieval engineers, search relevance engineers, "
            "RAG evaluation specialists, ranking/ML engineers, or vector database experts. "
            "1 = General ML/AI hiring but no specific retrieval-focused roles identified. "
            "0 = No relevant hiring signals found."
        ),
    },
]

SCORE_TIERS = [
    (8, 10, "HIGH",   "🟢", "Strong fit — prioritize outreach"),
    (5,  7, "MEDIUM", "🟡", "Moderate fit — qualify further"),
    (0,  4, "LOW",    "🔴", "Weak fit — deprioritize"),
]


def get_tier(total: int):
    for lo, hi, label, icon, note in SCORE_TIERS:
        if lo <= total <= hi:
            return label, icon, note
    return "LOW", "🔴", "Weak fit — deprioritize"


# ---------------------------------------------------------------------------
# CLI & Config
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Score a company for You.com Search API / RAG fit.",
        epilog='Example: python score.py "Salesforce" --url https://www.salesforce.com --verbose',
    )
    parser.add_argument("company", help="Company name to score")
    parser.add_argument(
        "--research-file",
        default=None,
        help="Path to an existing research report (.md) to use as additional context",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Company website URL to fetch and use as additional scoring context",
    )
    parser.add_argument(
        "--output-dir",
        default="reports",
        help="Directory to save score reports (default: reports/)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed API response info",
    )
    return parser.parse_args()


def load_config():
    load_dotenv(override=True)
    config = {
        "youcom_api_key": os.getenv("YOUCOM_API_KEY"),
        "anthropic_api_key": os.getenv("ANTHROPIC_API_KEY"),
    }
    missing = [k for k, v in config.items() if not v or v.startswith("your_")]
    if missing:
        print(f"ERROR: Missing or placeholder values for: {', '.join(missing)}")
        print("Edit .env and fill in your real API keys.")
        sys.exit(1)
    return config


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def slugify(company: str) -> str:
    slug = company.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


def find_research_report(company: str, output_dir: str) -> str | None:
    """Auto-detect the most recent research report for a company."""
    slug = slugify(company)
    pattern = os.path.join(output_dir, f"{slug}_*.md")
    # Exclude score files
    matches = [f for f in glob.glob(pattern) if "_fit_score" not in f]
    if not matches:
        return None
    # Return most recently modified
    return max(matches, key=os.path.getmtime)


def load_research_report(filepath: str) -> str:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        print(f"  WARNING: Research file not found: {filepath}")
        return ""
    except Exception as e:
        print(f"  WARNING: Could not read research file: {e}")
        return ""


def fetch_url_content(url: str, verbose: bool = False) -> str:
    """
    Fetches a URL and returns cleaned plain text, capped at 10,000 characters.
    Uses requests (already a dependency) — no additional libraries required.
    Returns "" on any failure, printing a WARNING to stdout.
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        html = response.text

        if verbose:
            print(f"    [verbose] Fetched {url} — HTTP {response.status_code}, {len(html):,} bytes")

        # Remove <script> and <style> blocks entirely
        text = re.sub(r"<(script|style)[^>]*>.*?</(script|style)>", " ", html, flags=re.IGNORECASE | re.DOTALL)
        # Strip all remaining HTML tags
        text = re.sub(r"<[^>]+>", " ", text)
        # Decode common HTML entities
        text = (
            text.replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&nbsp;", " ")
                .replace("&#39;", "'")
                .replace("&quot;", '"')
        )
        # Collapse whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        return text[:10_000]

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else "unknown"
        print(f"  WARNING: Could not fetch URL (HTTP {status}): {url}")
        return ""
    except requests.exceptions.ConnectionError:
        print(f"  WARNING: Could not connect to URL: {url}")
        return ""
    except requests.exceptions.Timeout:
        print(f"  WARNING: URL fetch timed out: {url}")
        return ""
    except Exception as e:
        print(f"  WARNING: URL fetch failed: {e}")
        return ""


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def build_scoring_queries(company: str) -> list:
    return [
        {
            "name": "product_signals",
            "label": "Product Signals (LLM / chat / research / intelligence)",
            "query": (
                f'"{company}" AI chat LLM citations sources grounded answers '
                f"research intelligence monitoring agent"
            ),
        },
        {
            "name": "agent_rag_signals",
            "label": "Agent & RAG Signals (browsing / retrieval workflows)",
            "query": (
                f'"{company}" AI agent research browsing web search retrieval '
                f"RAG grounding tool-use deep research"
            ),
        },
        {
            "name": "timeliness_signals",
            "label": "Timeliness Signals (real-time / freshness)",
            "query": (
                f'"{company}" real-time live news financial market data freshness '
                f"streaming continuously updated alerts"
            ),
        },
        {
            "name": "technical_signals",
            "label": "Technical Signals (search APIs / retrieval architecture)",
            "query": (
                f'"{company}" search API SERP Bing Brave Exa Tavily retrieval pipeline '
                f"engineering blog architecture vector database RAG"
            ),
        },
        {
            "name": "hiring_signals",
            "label": "Hiring Signals (retrieval / search / RAG roles)",
            "query": (
                f'"{company}" jobs hiring retrieval engineer search relevance '
                f"ranking ML RAG evaluation vector database"
            ),
        },
        {
            "name": "pain_expansion_signals",
            "label": "Pain & Expansion Signals (switching triggers / growth)",
            "query": (
                f'"{company}" deep research agent search quality latency rate limit '
                f"coverage new vertical citation accuracy"
            ),
        },
    ]


def search_youcom(query: str, api_key: str, verbose: bool = False) -> dict:
    url = "https://api.you.com/v1/search"
    headers = {"X-API-Key": api_key, "Accept": "application/json"}
    params = {"query": query, "num_web_results": 5}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if verbose:
            hits = data.get("results", {}).get("web", [])
            print(f"    [verbose] HTTP {response.status_code} — {len(hits)} results")
        return data

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else "unknown"
        if status == 401:
            print("  ERROR: You.com API key invalid or unauthorized.")
        elif status == 429:
            print("  ERROR: You.com rate limit exceeded. Try again shortly.")
        else:
            print(f"  ERROR: You.com API HTTP {status}: {e}")
        return {"results": {"web": []}, "error": str(e)}

    except requests.exceptions.ConnectionError:
        print("  ERROR: Cannot connect to You.com API.")
        return {"results": {"web": []}, "error": "connection_error"}

    except requests.exceptions.Timeout:
        print("  ERROR: You.com API request timed out.")
        return {"results": {"web": []}, "error": "timeout"}


def run_all_searches(company: str, queries: list, api_key: str, verbose: bool) -> dict:
    results = {}
    for i, q in enumerate(queries):
        print(f"  ({i+1}/{len(queries)}) {q['label']}...")
        data = search_youcom(q["query"], api_key, verbose)
        results[q["name"]] = {"label": q["label"], "query": q["query"], "data": data}
        if i < len(queries) - 1:
            time.sleep(0.5)
    return results


def extract_snippets(search_results: dict) -> dict:
    extracted = {}
    all_urls = []

    for name, result in search_results.items():
        web_results = result["data"].get("results", {}).get("web", [])
        snippets = []
        urls = []

        for hit in web_results:
            title = hit.get("title", "")
            raw_snippets = hit.get("snippets", [])
            if isinstance(raw_snippets, str):
                raw_snippets = [raw_snippets]
            description = hit.get("description", "")
            url = hit.get("url", "")

            snippet_text = " ".join(raw_snippets) if raw_snippets else description
            if snippet_text:
                header = f"### {title}" if title else "### (untitled)"
                snippets.append(f"{header}\n{snippet_text.strip()}")
            if url:
                urls.append(url)
                all_urls.append(url)

        extracted[name] = {"label": result["label"], "snippets": snippets, "urls": urls}

    extracted["_all_urls"] = list(dict.fromkeys(all_urls))
    return extracted


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def build_scoring_prompt(
    company: str,
    extracted: dict,
    research_content: str,
    url_content: str = "",
) -> str:
    # Build the search evidence block
    evidence_block = ""
    for name, data in extracted.items():
        if name.startswith("_"):
            continue
        evidence_block += f"\n{'=' * 60}\n"
        evidence_block += f"SEARCH CATEGORY: {data['label']}\n"
        evidence_block += f"{'=' * 60}\n"
        if data["snippets"]:
            evidence_block += "\n\n".join(data["snippets"])
        else:
            evidence_block += "[No results returned for this category]"
        evidence_block += "\n"

    # Build the company website block (primary first-party source)
    url_block = ""
    if url_content:
        url_block = f"""
{'=' * 60}
COMPANY WEBSITE CONTENT (direct fetch)
{'=' * 60}
{url_content[:10000]}
"""

    # Build the research report block
    research_block = ""
    if research_content:
        research_block = f"""
{'=' * 60}
EXISTING RESEARCH REPORT (additional context)
{'=' * 60}
{research_content[:12000]}
"""

    # Build the dimensions block for the prompt
    dimensions_block = ""
    for i, d in enumerate(DIMENSIONS, 1):
        dimensions_block += f"\n### Dimension {i}: {d['label']} (0–{d['max']} points)\n"
        dimensions_block += f"Scoring rubric: {d['rubric']}\n"

    prompt = f"""You are a senior sales engineer at You.com evaluating whether "{company}" is a strong potential buyer of You.com's Search API and RAG solutions.

Below is research evidence gathered from targeted web searches, optionally preceded by the company's own website content and a broader research report.

{url_block}
{research_block}

{evidence_block}

---

## Your Task

Score "{company}" across the following 5 dimensions. For each dimension, you must:
1. Assign a score of 0, 1, or 2
2. Cite specific evidence from the search results or research report that justifies the score
3. Note any anti-signals (internal-only tools, low-frequency AI, strict data isolation) that lower the score

{dimensions_block}

---

## Output Format

Write the score report using EXACTLY this markdown structure. Be specific — quote product names, job titles, blog post titles, or direct statements from the evidence. Do not speculate beyond what the evidence supports.

## Score Summary

| # | Dimension | Score | Max |
|---|-----------|-------|-----|
| 1 | External-facing LLM / chat with citations | [0-2] | 2 |
| 2 | Agent workflows requiring browsing / research | [0-2] | 2 |
| 3 | Timeliness-critical domain | [0-2] | 2 |
| 4 | Existing third-party search integration | [0-2] | 2 |
| 5 | Hiring for retrieval / search roles | [0-2] | 2 |
| | **Total** | **[sum]** | **10** |

---

## Dimension Scores & Evidence

### 1. External-facing LLM / chat with citations — [X]/2

**Evidence:**
[Cite specific products, features, marketing copy, or press releases found in the evidence]

**Anti-signals (if any):**
[Note any signals that reduce confidence — e.g. product is enterprise-internal only]

---

### 2. Agent workflows requiring browsing / research — [X]/2

**Evidence:**
[Cite specific agents, use cases, blog posts, or product pages]

**Anti-signals (if any):**
[...]

---

### 3. Timeliness-critical domain — [X]/2

**Evidence:**
[Describe the domain and cite specific messaging about freshness, real-time data, or live updates]

**Anti-signals (if any):**
[...]

---

### 4. Existing third-party search integration — [X]/2

**Evidence:**
[Cite specific vendor names, engineering blog posts, architecture descriptions, or job posting language]

**Anti-signals (if any):**
[...]

---

### 5. Hiring for retrieval / search roles — [X]/2

**Evidence:**
[Cite specific job titles, teams, or postings found in the evidence]

**Anti-signals (if any):**
[...]

---

## Key Buying Signals

List the 3–5 strongest specific signals that make this account attractive (or unattractive) for You.com. Be concrete — name products, quote statements, reference job postings.

## Pain & Expansion Triggers

Describe any signals that suggest the account is actively searching for a better search/retrieval solution, expanding into new verticals, or building new agent capabilities that would require increased search volume.

## Anti-Signals Summary

List any signals that reduce fit priority (internal-only tools, no external knowledge requirements, strict data isolation, low-frequency AI usage).

## Sales Rep Recommendation

Write 2–3 sentences summarizing who at this company to approach, what angle to lead with (Search API, RAG, freshness/grounding, agent workflows), and any specific products or announcements to reference in outreach.

---

Do NOT write an overall score header — that will be added programmatically.
"""
    return prompt


def score_with_claude(
    company: str,
    extracted: dict,
    research_content: str,
    api_key: str,
    verbose: bool,
    url_content: str = "",
) -> tuple[str, int]:
    """
    Returns (report_body: str, total_score: int).
    Parses the score table from Claude's output to extract the total.
    """
    print("  Calling Claude to evaluate and score...")
    client = anthropic.Anthropic(api_key=api_key)
    prompt = build_scoring_prompt(company, extracted, research_content, url_content)

    if verbose:
        print(f"  [verbose] Scoring prompt: {len(prompt):,} characters")

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=(
                "You are a senior sales engineer at You.com. Your job is to evaluate potential "
                "enterprise accounts for fit with You.com's Search API and RAG solutions. "
                "You are rigorous, evidence-based, and concise. You never fabricate signals — "
                "if evidence is absent, you say so and score accordingly."
            ),
            messages=[{"role": "user", "content": prompt}],
        )
        body = message.content[0].text

        # Parse total score from the markdown table Claude writes
        total = _parse_total_score(body)
        return body, total

    except anthropic.AuthenticationError:
        print("ERROR: Anthropic API key is invalid.")
        sys.exit(1)
    except anthropic.RateLimitError:
        print("ERROR: Anthropic API rate limit exceeded.")
        sys.exit(1)
    except anthropic.APIError as e:
        print(f"ERROR: Anthropic API error: {e}")
        sys.exit(1)


def _parse_total_score(body: str) -> int:
    """
    Extract the total score from Claude's markdown table.
    Looks for a bold total row like: | | **Total** | **8** | **10** |
    Falls back to summing individual scores if the total row isn't found.
    """
    # Try to find the bold total line: **Total** | **N** | **10**
    total_match = re.search(
        r"\*\*Total\*\*\s*\|\s*\*\*(\d+)\*\*\s*\|\s*\*\*10\*\*",
        body,
    )
    if total_match:
        return min(int(total_match.group(1)), 10)

    # Fallback: sum individual dimension scores from the table
    # Matches lines like: | 1 | Some label | 2 | 2 |
    scores = re.findall(r"^\|\s*\d+\s*\|[^|]+\|\s*(\d+)\s*\|\s*2\s*\|", body, re.MULTILINE)
    if scores:
        return min(sum(int(s) for s in scores), 10)

    return -1  # Could not parse


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_score_report(
    company: str,
    report_body: str,
    total_score: int,
    all_urls: list,
    output_dir: str,
) -> str:
    os.makedirs(output_dir, exist_ok=True)
    slug = slugify(company)
    today = date.today().strftime("%Y-%m-%d")
    filename = f"{slug}_{today}_fit_score.md"
    filepath = os.path.join(output_dir, filename)

    tier_label, tier_icon, tier_note = get_tier(total_score)
    score_display = f"{total_score}/10" if total_score >= 0 else "N/A"

    if all_urls:
        sources = "\n".join(f"- {url}" for url in all_urls)
    else:
        sources = "_No source URLs captured._"

    header = (
        f"# {company} — You.com API Fit Score\n\n"
        f"_Scored: {today}_\n\n"
        f"---\n\n"
        f"## Overall Fit: {tier_icon} {score_display} — {tier_label}\n\n"
        f"_{tier_note}_\n\n"
        f"---\n\n"
    )

    content = header + report_body.strip() + f"\n\n## Sources\n\n{sources}\n"

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    config = load_config()

    company = args.company.strip()
    print(f"\nScoring: {company}")
    print("=" * 50)

    # --- Load research report (if available) ---
    research_content = ""
    research_path = args.research_file

    if research_path:
        print(f"\nUsing research file: {research_path}")
        research_content = load_research_report(research_path)
    else:
        auto_path = find_research_report(company, args.output_dir)
        if auto_path:
            print(f"\nAuto-detected research report: {auto_path}")
            research_content = load_research_report(auto_path)
        else:
            print("\nNo existing research report found — scoring from web searches only.")

    if research_content:
        print(f"  Research context: {len(research_content):,} characters loaded")

    # --- Fetch URL content (if provided) ---
    url_content = ""
    if args.url:
        print(f"\nFetching URL: {args.url}")
        url_content = fetch_url_content(args.url, args.verbose)
        if url_content:
            print(f"  URL context: {len(url_content):,} characters loaded")

    # --- Run scoring searches ---
    queries = build_scoring_queries(company)
    print(f"\n[1/3] Running {len(queries)} scoring searches...")
    search_results = run_all_searches(company, queries, config["youcom_api_key"], args.verbose)

    print("\n[2/3] Processing results...")
    extracted = extract_snippets(search_results)
    total_snippets = sum(len(v["snippets"]) for k, v in extracted.items() if not k.startswith("_"))
    total_urls = len(extracted["_all_urls"])
    print(f"  Collected {total_snippets} snippets from {total_urls} sources")

    # --- Score with Claude ---
    print("\n[3/3] Scoring with Claude...")
    report_body, total_score = score_with_claude(
        company,
        extracted,
        research_content,
        config["anthropic_api_key"],
        args.verbose,
        url_content,
    )

    tier_label, tier_icon, _ = get_tier(total_score)
    score_display = f"{total_score}/10" if total_score >= 0 else "N/A (parse error)"
    print(f"  Score: {score_display} — {tier_icon} {tier_label}")

    filepath = write_score_report(
        company,
        report_body,
        total_score,
        extracted["_all_urls"],
        args.output_dir,
    )

    print(f"\nDone! Score report saved to: {filepath}")
    print("=" * 50)


if __name__ == "__main__":
    main()
