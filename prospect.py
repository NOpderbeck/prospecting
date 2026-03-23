#!/usr/bin/env python3
"""
prospect.py — You.com Prospect Research

Researches a company, identifies the most relevant executives, looks up their
LinkedIn profiles via the Fresh LinkedIn Scraper API, and generates personalized
LinkedIn intro messages grounded in a You.com Search API POV.

Usage:
    python prospect.py <company> [--domain domain.com] [--count N] [--verbose]
"""

import argparse
import json
import os
import re
import sys
from datetime import date
from pathlib import Path

import anthropic
import requests
from dotenv import load_dotenv

from context import slugify

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

BASE_DIR        = Path(__file__).parent
REPORTS_DIR     = BASE_DIR / "reports"
TODAY           = date.today().isoformat()

ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
YOUCOM_API_KEY  = os.getenv("YOUCOM_API_KEY", "")
RAPIDAPI_KEY    = "4cbc79c401msh9947248b864282ep1435c4jsnb8837d27b623"
LINKEDIN_HOST   = "fresh-linkedin-scraper-api.p.rapidapi.com"


# ---------------------------------------------------------------------------
# Web search
# ---------------------------------------------------------------------------

def youcom_search(query: str, num_results: int = 4) -> str:
    """Run a You.com Search API query and return formatted snippets."""
    if not YOUCOM_API_KEY:
        return ""
    try:
        resp = requests.get(
            "https://api.you.com/v1/search",
            params={"query": query, "num_web_results": num_results},
            headers={"X-API-Key": YOUCOM_API_KEY, "Accept": "application/json"},
            timeout=15,
        )
        if resp.status_code != 200:
            return ""
        hits = resp.json().get("results", {}).get("web", [])
        parts = []
        for h in hits:
            snippet = " ".join(h.get("snippets", []))
            parts.append(f"**{h.get('title','')}** ({h.get('url','')})\n{snippet}")
        return "\n\n".join(parts)
    except Exception:
        return ""


def research_company(company: str, domain: str | None, verbose: bool = False) -> str:
    """Run targeted web searches and return compiled research text."""
    queries = []
    if domain:
        queries += [
            f"site:{domain}",
            f'"{domain}" AI OR LLM OR agents OR "web search" OR "search API"',
        ]
    queries += [
        f'"{company}" AI OR LLM OR search OR agents OR "machine learning"',
        f'"{company}" product site:techcrunch.com OR site:venturebeat.com',
        f'"{company}" funding OR "series" OR acquisition OR "raised"',
    ]

    parts = []
    for q in queries[:4]:
        if verbose:
            print(f"    → {q}")
        result = youcom_search(q)
        if result:
            parts.append(f"### {q}\n{result}")

    return "\n\n".join(parts) if parts else "(no web results — YOUCOM_API_KEY not set)"


def find_named_executives(company: str, domain: str | None, verbose: bool = False) -> str:
    """
    Run searches specifically designed to surface named individuals at the company.
    Returns raw search text for Claude to extract confirmed names from.
    These searches target org charts, leadership pages, and press coverage
    where real names with real titles are mentioned explicitly.
    """
    # Lead with the most name-rich sources (org charts, press) before domain pages
    queries = [
        f'"{company}" CEO OR CTO OR "VP of" OR "Head of" OR "Chief" site:linkedin.com OR site:theorg.com OR site:crunchbase.com',
        f'"{company}" executives site:craft.co OR site:rocketreach.co OR site:theorg.com',
        f'"{company}" leadership team executives',
        f'"{company}" founders OR "co-founder" OR "executive team"',
    ]
    if domain:
        queries.append(f"site:{domain} team OR leadership OR about OR founders")

    parts = []
    for q in queries[:5]:
        if verbose:
            print(f"    → {q}")
        result = youcom_search(q, num_results=5)
        if result:
            parts.append(f"### {q}\n{result}")

    return "\n\n".join(parts) if parts else ""


# ---------------------------------------------------------------------------
# LinkedIn
# ---------------------------------------------------------------------------

def linkedin_lookup(first: str, last: str, company: str) -> str | None:
    """
    Look up a person via Fresh LinkedIn Scraper API.

    Two-stage search:
      1. With company filter — fast and precise when the API knows the company name.
      2. Without company filter — broader; verify by checking if the company name
         appears in the returned title (e.g. "CTO at Instacart").

    Returns the confirmed LinkedIn URL, or None if not found.
    """
    headers = {
        "x-rapidapi-host": LINKEDIN_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
        "Content-Type": "application/json",
    }

    def _fetch(params: dict) -> list[dict]:
        try:
            resp = requests.get(
                f"https://{LINKEDIN_HOST}/api/v1/search/people",
                params={**params, "page": 1},
                headers=headers,
                timeout=10,
            )
            return resp.json().get("data", []) if resp.status_code == 200 else []
        except Exception:
            return []

    def _last_name_matches(result: dict) -> bool:
        return last.lower() in result.get("full_name", "").lower()

    def _company_in_title(result: dict) -> bool:
        """Check if the company name appears in the title, e.g. 'CTO at Instacart'."""
        return company.lower() in result.get("title", "").lower()

    # Stage 1: strict — company filter applied
    for r in _fetch({"first_name": first, "last_name": last, "company": company}):
        if _last_name_matches(r):
            return r.get("url") or None

    # Stage 2: broad — no company filter, verify via title
    for r in _fetch({"first_name": first, "last_name": last}):
        if _last_name_matches(r) and _company_in_title(r):
            return r.get("url") or None

    return None


# ---------------------------------------------------------------------------
# Claude helpers
# ---------------------------------------------------------------------------

def _strip_fences(text: str) -> str:
    """Remove markdown code fences from a Claude response."""
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    return match.group(1).strip() if match else text.strip()


def identify_executives(
    company: str,
    domain: str | None,
    research: str,
    exec_search: str,
    count: int,
    client: anthropic.Anthropic,
) -> list[dict]:
    """
    Extract confirmed, named executives from web search results.

    Only names that are explicitly mentioned in the search text are returned —
    Claude must not invent or guess names. Fewer but verified contacts are
    better than a full list of hallucinated ones.
    """
    domain_line = f"Domain: {domain}" if domain else ""
    prompt = f"""You are a B2B sales researcher extracting confirmed executive contacts for a You.com Search API pitch.

Company: {company}
{domain_line}

COMPANY RESEARCH (context only):
{research[:3000]}

EXECUTIVE SEARCH RESULTS (primary source — extract names from here):
{exec_search[:12000]}

---

TASK: Extract up to {count} real, named executives from the search results above.

CRITICAL RULES:
1. ONLY include people whose full name (first AND last name) appears explicitly in the search text above.
2. Do NOT invent, guess, or fill in names from general knowledge. If you only see a title with no name, skip it.
3. Do NOT include names from unrelated companies that appear in the search results.
4. Fewer confirmed contacts is better than a full list with made-up names.

Priority order (include higher-tier roles first):
Tier 1: CTO, VP/SVP Engineering, Head of AI Infrastructure, Head of Applied AI, Chief AI Officer, VP Search, Head of Platform, Head of Web Data
Tier 2: CEO/Co-founder, CPO/VP Product, VP Data Engineering, Head of ML, Director AI Products, Head of Agent Engineering
Tier 3: CDO, VP Infrastructure, Director Software Engineering, Head of Partnerships, VP Sales/CRO, COO/President

Return ONLY a JSON array. Each object must have exactly:
  "first_name", "last_name", "title", "tier" (1/2/3)

If you cannot confirm any names, return []. Do not pad the list."""

    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    try:
        return json.loads(_strip_fences(msg.content[0].text)) or []
    except Exception:
        return []


def generate_prospect_package(
    company: str,
    domain: str | None,
    research: str,
    execs: list[dict],
    client: anthropic.Anthropic,
) -> str:
    """
    Generate the full formatted output: company summary, POV, and per-exec intros.

    Claude produces only text content (descriptions, POV angles, intro messages)
    as structured JSON. Python then assembles the final markdown and inserts the
    LinkedIn URLs — so confirmed URLs are never left up to Claude to reproduce.
    """
    # Build a clean exec list for Claude: names + titles only, no URLs
    execs_for_claude = [
        {"full_name": f"{e.get('first_name','')} {e.get('last_name','')}".strip(),
         "title": e.get("title", ""),
         "tier": e.get("tier", 2)}
        for e in execs
    ]

    prompt = f"""You are a senior enterprise sales strategist at You.com generating a prospect research package.

COMPANY: {company}
{"DOMAIN: " + domain if domain else ""}

RESEARCH:
{research[:5000]}

EXECUTIVES (ranked by relevance):
{json.dumps(execs_for_claude, indent=2)}

---

Return ONLY a JSON object with this exact structure (no markdown, no fences, raw JSON only):

{{
  "company_description": "1-2 sentences — what they do and their AI/tech focus, grounded in the research",
  "you_com_pov": "1-2 sentences — why You.com's Web Search API is a natural fit for THIS company specifically, based on their need for live web data at scale — not generic buzzwords",
  "contacts": [
    {{
      "full_name": "exact name from the executives list",
      "title": "exact title from the executives list",
      "pov_angle": "10 words max — angle used for this role",
      "intro": "the LinkedIn intro message"
    }}
  ]
}}

LINKEDIN INTRO RULES — every intro must pass ALL of these:
1. Under 300 characters. Count before writing.
2. No em dashes (— or –). Use periods or commas instead.
3. Do NOT open by describing their job. Never "Building X at Y" or "Leading X strategy".
4. Lead with what You.com does and why it fits this company.
5. Avoid: impressive, amazing, love, congrats, exciting, compelling, "I wanted to reach out", "I came across", "I'd love to connect".
6. Structure: [You.com value + why it fits their situation] + [light ask].
7. 2-3 sentences max. Vary the opening — no two intros start the same way.

YOU.COM VALUE ANGLES BY ROLE SIGNAL:
- AI agents / agentic workflows → purpose-built for high-volume agents, sub-500ms, real-time web data
- External-facing AI product → real-time web search keeps responses current at any query volume
- Enterprise / platform scale → 99.9% SLA, SOC2, zero data retention, predictable pricing
- Financial / legal / compliance → verifiable web sources, auditability, no data retention
- Post-Bing migration → leading Bing replacement, API-compatible migration path"""

    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    try:
        data = json.loads(_strip_fences(msg.content[0].text))
    except Exception:
        # Fallback: return the raw text if JSON parse fails
        return msg.content[0].text.strip()

    # Build a lookup: full_name → confirmed LinkedIn URL
    url_by_name: dict[str, str | None] = {}
    for e in execs:
        full = f"{e.get('first_name','')} {e.get('last_name','')}".strip()
        url_by_name[full.lower()] = e.get("linkedin_url")

    # Assemble markdown — Python owns the LinkedIn URLs
    lines = [
        f"## {company}",
        "",
        f"**Company:** {data.get('company_description', '')}",
        "",
        f"**You.com POV:** {data.get('you_com_pov', '')}",
        "",
        "---",
    ]

    for contact in data.get("contacts", []):
        full_name  = contact.get("full_name", "")
        title      = contact.get("title", "")
        pov_angle  = contact.get("pov_angle", "")
        intro      = contact.get("intro", "")
        linkedin   = url_by_name.get(full_name.lower())

        # Build the name+link header — Python inserts the URL, not Claude
        if linkedin:
            name_part = f"**[{full_name}]({linkedin})**"
        else:
            search_url = (
                f"https://www.linkedin.com/search/results/people/"
                f"?keywords={full_name.replace(' ', '+')}+{company.replace(' ', '+')}"
            )
            name_part = f"**[{full_name}]({search_url})** *(LinkedIn search)*"

        lines += [
            "",
            f"{name_part} · {title} · *{pov_angle}*",
            "",
            "```",
            intro,
            "```",
            "",
            "---",
        ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="You.com Prospect Research")
    parser.add_argument("company", help="Company name, e.g. 'Flashpoint'")
    parser.add_argument("--domain", help="Company domain, e.g. 'flashpoint.io'")
    parser.add_argument("--count", type=int, default=10, help="Number of contacts (default: 10)")
    parser.add_argument("--output-dir", default=str(REPORTS_DIR))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not ANTHROPIC_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set.")
        sys.exit(1)

    client  = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    company = args.company.strip()
    domain  = args.domain.strip() if args.domain else None
    count   = args.count
    slug    = slugify(company)

    print(f"\n🎯  Prospect Research: {company}" + (f" ({domain})" if domain else ""))
    print("─" * 60)

    # ── Step 1: Research ───────────────────────────────────────────────────
    print("\n📡  Step 1/4  Researching company...")
    research = research_company(company, domain, verbose=args.verbose)
    chars = len(research)
    print(f"  {'✓' if chars > 100 else '⚠'}  {chars:,} chars of research gathered.")

    # ── Step 2: Find named executives via targeted search ──────────────────
    print(f"\n🔎  Step 2/4  Searching for confirmed executive names...")
    exec_search = find_named_executives(company, domain, verbose=args.verbose)
    exec_chars = len(exec_search)
    print(f"  {'✓' if exec_chars > 100 else '⚠'}  {exec_chars:,} chars of exec search results.")

    execs = identify_executives(company, domain, research, exec_search, count, client)
    if not execs:
        print("  ⚠  No confirmed executives found in search results.")
        print("      Try adding a domain (--domain klue.com) or check the company name.")
        sys.exit(1)
    print(f"  ✓  {len(execs)} confirmed executives found:")
    for e in execs:
        print(f"      [{e.get('tier','?')}] {e.get('first_name')} {e.get('last_name')} — {e.get('title')}")

    # ── Step 3: LinkedIn API ───────────────────────────────────────────────
    print(f"\n🔗  Step 3/4  Looking up LinkedIn profiles...")
    confirmed = 0
    for e in execs:
        first = e.get("first_name", "")
        last  = e.get("last_name", "")
        url   = linkedin_lookup(first, last, company)
        e["linkedin_url"] = url
        if url:
            confirmed += 1
            print(f"  ✓  {first} {last}")
        else:
            print(f"  ✗  {first} {last} — not found")
    print(f"\n  LinkedIn: {confirmed}/{len(execs)} profiles confirmed.")

    # ── Step 4: Generate output ────────────────────────────────────────────
    print(f"\n✍️   Step 4/4  Generating POV and LinkedIn intros...")
    output_md = generate_prospect_package(company, domain, research, execs, client)

    # ── Save ───────────────────────────────────────────────────────────────
    output_dir = Path(args.output_dir) / slug
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{TODAY}_prospect.md"
    filepath.write_text(output_md, encoding="utf-8")

    print(f"\n✅  Report saved → {filepath}")
    print("\n" + "═" * 60)
    print(output_md)
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
