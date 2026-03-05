"""
research.py — Company Research CLI Tool

Usage:
    python research.py "Salesforce"
    python research.py "Salesforce" --output-dir my_reports --verbose
"""

import os
import re
import sys
import time
import argparse
from datetime import date

import requests
import anthropic
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# CLI & Config
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Research a company and generate a structured markdown report.",
        epilog='Example: python research.py "Salesforce" --verbose',
    )
    parser.add_argument("company", help="Company name to research")
    parser.add_argument(
        "--output-dir",
        default="reports",
        help="Directory to save reports (default: reports/)",
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


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def build_search_queries(company: str) -> list:
    return [
        {
            "name": "company_overview",
            "label": "Company Overview",
            "query": f'"{company}" company overview business 2025',
        },
        {
            "name": "leadership",
            "label": "Leadership Team",
            "query": f'"{company}" CEO CTO leadership executive team',
        },
        {
            "name": "ai_initiatives",
            "label": "Generative AI Initiatives",
            "query": f'"{company}" generative AI LLM artificial intelligence initiative 2024 2025',
        },
        {
            "name": "strategy",
            "label": "Strategic Priorities",
            "query": f'"{company}" strategic priorities technology investment roadmap',
        },
        {
            "name": "news",
            "label": "Recent News",
            "query": f'"{company}" news announcement press release 2024 2025',
        },
    ]


def search_youcom(query: str, api_key: str, verbose: bool = False) -> dict:
    url = "https://api.you.com/v1/search"
    headers = {
        "X-API-Key": api_key,
        "Accept": "application/json",
    }
    params = {
        "query": query,
        "num_web_results": 5,
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if verbose:
            hits = data.get("results", {}).get("web", [])
            print(f"    [verbose] HTTP {response.status_code} — {len(hits)} results returned")

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
        print("  ERROR: Cannot connect to You.com API. Check internet connection.")
        return {"results": {"web": []}, "error": "connection_error"}

    except requests.exceptions.Timeout:
        print("  ERROR: You.com API request timed out.")
        return {"results": {"web": []}, "error": "timeout"}


def run_all_searches(company: str, queries: list, api_key: str, verbose: bool) -> dict:
    results = {}
    for i, q in enumerate(queries):
        print(f"  ({i+1}/{len(queries)}) {q['label']}...")
        data = search_youcom(q["query"], api_key, verbose)
        results[q["name"]] = {
            "label": q["label"],
            "query": q["query"],
            "data": data,
        }
        if i < len(queries) - 1:
            time.sleep(0.5)
    return results


# ---------------------------------------------------------------------------
# Extract & Synthesize
# ---------------------------------------------------------------------------

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

        extracted[name] = {
            "label": result["label"],
            "snippets": snippets,
            "urls": urls,
        }

    # Deduplicate URLs while preserving order
    extracted["_all_urls"] = list(dict.fromkeys(all_urls))
    return extracted


def build_synthesis_prompt(company: str, extracted: dict) -> str:
    sections_text = ""

    for name, data in extracted.items():
        if name.startswith("_"):
            continue
        label = data["label"]
        snippets = data["snippets"]

        sections_text += f"\n{'=' * 60}\n"
        sections_text += f"SEARCH CATEGORY: {label}\n"
        sections_text += f"{'=' * 60}\n"

        if snippets:
            sections_text += "\n\n".join(snippets)
        else:
            sections_text += "[No results returned for this search category]"
        sections_text += "\n"

    prompt = f"""Below are web search results for "{company}", organized by research category. Using ONLY this information, write a structured company research report.

{sections_text}

---

Now write the research report using this EXACT markdown structure. Write in clear, professional prose. If a section cannot be supported by the search results above, write "Insufficient data found in search results." for that section.

## Executive Summary

Write 3-5 sentences covering what {company} does, its market position, and one key current development.

## Company Overview

Cover: industry and market segment, company size (employees/revenue if available), core products and services, recent financial performance or growth indicators.

## Leadership Team

List key executives (CEO, CTO, CFO, and others mentioned in search results). For each, include their name, title, and any notable strategic focus areas or recent statements found in the search results. Use a bullet list.

## Strategic Priorities

Describe {company}'s current strategic focus areas, major investments, and stated direction based on what appears in the search results. Focus on 2024-2025 priorities.

## Generative AI Initiatives

This section is critical. Describe specifically:
- Named AI products or features {company} has launched or announced
- AI partnerships (with OpenAI, Google, Anthropic, Microsoft, AWS, etc.)
- Public statements from executives about AI strategy
- Reported investment amounts in AI (if mentioned)
- Any AI-related acquisitions or significant hires
- Specific use cases or customer deployments mentioned

If limited AI information was found, state that clearly and include whatever is available.

## Recent News & Announcements

List 4-6 notable developments from the past 12 months. Format as bullet points with approximate dates where available from the search results.

---

Do NOT write a Sources section — it will be appended automatically.
"""
    return prompt


def synthesize_with_claude(company: str, extracted: dict, api_key: str, verbose: bool) -> str:
    print("  Calling Claude to synthesize findings...")
    client = anthropic.Anthropic(api_key=api_key)
    prompt = build_synthesis_prompt(company, extracted)

    if verbose:
        print(f"  [verbose] Synthesis prompt: {len(prompt):,} characters")

    try:
        full_text = []
        buf = ""
        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=(
                "You are a senior business analyst preparing concise, accurate company "
                "research reports for a B2B sales team. You write in clear, professional "
                "prose. You only report facts supported by the provided search results. "
                "When information is unavailable or unclear from the sources, you state "
                "that explicitly rather than speculating."
            ),
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            for chunk in stream.text_stream:
                full_text.append(chunk)
                buf += chunk
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    print(line, flush=True)
        if buf:
            print(buf, flush=True)
        return "".join(full_text)

    except anthropic.AuthenticationError:
        print("ERROR: Anthropic API key is invalid.")
        sys.exit(1)
    except anthropic.RateLimitError:
        print("ERROR: Anthropic API rate limit exceeded.")
        sys.exit(1)
    except anthropic.APIError as e:
        print(f"ERROR: Anthropic API error: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_report(company: str, report_body: str, all_urls: list, output_dir: str) -> str:
    slug = slugify(company)
    today = date.today().strftime("%Y-%m-%d")
    company_dir = os.path.join(output_dir, slug)
    os.makedirs(company_dir, exist_ok=True)
    filename = f"{today}_research.md"
    filepath = os.path.join(company_dir, filename)

    if all_urls:
        sources = "\n".join(f"- {url}" for url in all_urls)
    else:
        sources = "_No source URLs were captured from search results._"

    content = (
        f"# {company} — Company Research Report\n\n"
        f"_Generated: {today}_\n\n"
        f"---\n\n"
        f"{report_body.strip()}\n\n"
        f"## Sources\n\n"
        f"{sources}\n"
    )

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
    print(f"\nResearching: {company}")
    print("=" * 50)

    queries = build_search_queries(company)

    print(f"\n[1/3] Running {len(queries)} web searches...")
    search_results = run_all_searches(company, queries, config["youcom_api_key"], args.verbose)

    print("\n[2/3] Processing results...")
    extracted = extract_snippets(search_results)
    total_snippets = sum(len(v["snippets"]) for k, v in extracted.items() if not k.startswith("_"))
    total_urls = len(extracted["_all_urls"])
    print(f"  Collected {total_snippets} snippets from {total_urls} sources")

    print("\n[3/3] Generating report...")
    report_body = synthesize_with_claude(company, extracted, config["anthropic_api_key"], args.verbose)

    filepath = write_report(company, report_body, extracted["_all_urls"], args.output_dir)

    print(f"\nDone! Report saved to: {filepath}")
    print("=" * 50)


if __name__ == "__main__":
    main()
