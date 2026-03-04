"""
ask.py — Query existing account reports using natural language.

Reads the latest (or specified) report for a company from the reports/
directory and answers a question about it using Claude. No connectors,
no external API calls — just the local filesystem + Claude.

Usage:
    python ask.py "Deloitte"                                 # print latest report
    python ask.py "Deloitte" "What are the action items?"    # ask a question
    python ask.py "Deloitte" --type lookup                   # latest lookup only
    python ask.py "Deloitte" --list                          # list all reports on file
    python ask.py "Deloitte" --all "What changed over time?" # query all reports combined

Report types: lookup, context, research, score
"""

import os
import sys
import argparse
from pathlib import Path

import anthropic
from dotenv import load_dotenv

from context import slugify


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Query existing account reports with natural language.",
        epilog='Example: python ask.py "Deloitte" "What is the status of the NDA?"',
    )
    parser.add_argument("company", help="Company name to look up")
    parser.add_argument(
        "question",
        nargs="?",
        default="",
        help='Question to ask about the report (optional — omit to print the report)',
    )
    parser.add_argument(
        "--type",
        dest="report_type",
        default="",
        metavar="TYPE",
        help="Filter to a specific report type: lookup, context, research, score",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available reports for this company and exit",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Load all reports for this company (not just the latest) before answering",
    )
    parser.add_argument(
        "--output-dir",
        default="reports",
        metavar="DIR",
        help="Base reports directory (default: reports/)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Report discovery
# ---------------------------------------------------------------------------

def find_reports(slug: str, output_dir: str, report_type: str = "") -> list[Path]:
    """
    Return a date-sorted list of report paths for a given company slug.
    Optionally filter by report type (lookup, context, research, score).
    """
    company_dir = Path(output_dir) / slug
    if not company_dir.exists():
        return []

    files = sorted(company_dir.glob("*.md"))  # YYYY-MM-DD_type.md → lexicographic = date order

    if report_type:
        files = [f for f in files if f.stem.endswith(f"_{report_type}")]

    return files


def load_reports(paths: list[Path]) -> str:
    """Read and concatenate report files, labelling each with its filename."""
    parts = []
    for p in paths:
        try:
            text = p.read_text(encoding="utf-8").strip()
            parts.append(f"=== {p.name} ===\n{text}")
        except OSError:
            parts.append(f"=== {p.name} === [Could not read file]")
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Claude Q&A
# ---------------------------------------------------------------------------

def ask_claude(report_text: str, question: str, api_key: str) -> str:
    client = anthropic.Anthropic(api_key=api_key)
    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=(
                "You are a senior sales analyst. You are given one or more account reports "
                "for a B2B sales team. Answer the user's question based only on the information "
                "in the reports. Be concise and specific — cite names, dates, and amounts where "
                "they appear. If the answer is not in the reports, say so directly."
            ),
            messages=[
                {
                    "role": "user",
                    "content": f"{report_text}\n\n---\n\nQuestion: {question}",
                }
            ],
        )
        return message.content[0].text
    except anthropic.AuthenticationError:
        print("ERROR: Anthropic API key is invalid.")
        sys.exit(1)
    except anthropic.APIError as e:
        print(f"ERROR: Anthropic API error: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    load_dotenv(override=True)
    args = parse_args()

    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key or api_key.startswith("your_"):
        print("ERROR: Missing or placeholder ANTHROPIC_API_KEY in .env")
        sys.exit(1)

    slug = slugify(args.company.strip())
    reports = find_reports(slug, args.output_dir, args.report_type)

    # --list: just show what's available
    if args.list:
        if not reports:
            label = f" ({args.report_type})" if args.report_type else ""
            print(f"No reports found for '{args.company}'{label} in {args.output_dir}/{slug}/")
            return
        print(f"Reports for {args.company} ({len(reports)} file{'s' if len(reports) != 1 else ''}):")
        for r in reports:
            print(f"  {r.name}")
        return

    if not reports:
        label = f" of type '{args.report_type}'" if args.report_type else ""
        print(
            f"No reports found for '{args.company}'{label}.\n"
            f"Run one of the following first:\n"
            f"  python lookup.py \"{args.company}\"\n"
            f"  python context.py \"{args.company}\"\n"
            f"  python research.py \"{args.company}\"\n"
            f"  python score.py \"{args.company}\""
        )
        sys.exit(1)

    # Decide which reports to load
    if args.all:
        paths_to_load = reports           # all reports, oldest → newest
    else:
        paths_to_load = [reports[-1]]     # latest only

    report_text = load_reports(paths_to_load)

    # No question: just print the report
    if not args.question:
        print(report_text)
        return

    # Ask Claude
    type_label = f" [{args.report_type}]" if args.report_type else ""
    source_label = "all reports" if args.all else reports[-1].name
    print(f"\nAsking about {args.company}{type_label} ({source_label})...\n")
    answer = ask_claude(report_text, args.question, api_key)
    print(answer)


if __name__ == "__main__":
    main()
