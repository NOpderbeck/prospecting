"""
bulk_score.py — Bulk Fit Scorer from Google Sheets

Reads a Google Sheet of prospects, scores each one for You.com API fit,
looks up the Salesforce owner, and writes results back to the sheet.

Sheet format (columns A–E, row 1 = header):
  A: Account Name (input)
  B: Domain       (input, optional)
  C: SF Owner     (output)
  D: Fit Score    (output — row is skipped if already set)
  E: Rationale    (output)

Usage:
    python bulk_score.py --sheet-url "https://docs.google.com/spreadsheets/d/..."
"""

import os
import re
import sys
import time
import argparse
from pathlib import Path

from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# CLI & Config
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Score prospects in bulk from a Google Sheet."
    )
    parser.add_argument("--sheet-url", required=True, help="URL of the Google Sheet")
    parser.add_argument(
        "--output-dir", default="reports", help="Directory for local score reports (default: reports/)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max number of unscored rows to process (default: all)",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    return parser.parse_args()


def load_config():
    load_dotenv(override=True)
    config = {
        "youcom_api_key":          os.getenv("YOUCOM_API_KEY"),
        "anthropic_api_key":       os.getenv("ANTHROPIC_API_KEY"),
        "sf_username":             os.getenv("SF_USERNAME"),
        "sf_password":             os.getenv("SF_PASSWORD"),
        "sf_security_token":       os.getenv("SF_SECURITY_TOKEN"),
        "sf_domain":               os.getenv("SF_DOMAIN", "login"),
        "google_credentials_file": os.getenv(
            "GOOGLE_CREDENTIALS_FILE", ".credentials/google_credentials.json"
        ),
    }
    missing = [k for k in ("youcom_api_key", "anthropic_api_key") if not config.get(k)]
    if missing:
        print(f"ERROR: Missing required API keys: {', '.join(missing)}")
        sys.exit(1)
    return config


# ---------------------------------------------------------------------------
# Google Sheets OAuth
# ---------------------------------------------------------------------------

GOOGLE_TOKEN_PATH = os.path.join(".credentials", "google_token_bulk.json")
GOOGLE_SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]


def get_google_creds(config: dict, verbose: bool):
    """Return valid Google credentials with Sheets read/write scope."""
    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request as GoogleRequest
    except ImportError:
        print(
            "ERROR: Google libraries not installed. Run:\n"
            "  pip install google-api-python-client google-auth-oauthlib google-auth-httplib2"
        )
        sys.exit(1)

    creds = None
    if os.path.exists(GOOGLE_TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_PATH, GOOGLE_SCOPES)
        except Exception:
            creds = None  # Corrupted or scope-mismatched — re-auth

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(GoogleRequest())
                if verbose:
                    print("    [verbose] Google token refreshed")
            except Exception:
                creds = None

        if not creds:
            creds_file = config["google_credentials_file"]
            if not os.path.exists(creds_file):
                print(
                    f"ERROR: Google credentials file not found at '{creds_file}'.\n"
                    "Download credentials.json from Google Cloud Console → "
                    "APIs & Services → Credentials → OAuth 2.0 Client ID (Desktop app)."
                )
                sys.exit(1)
            print("  Google OAuth: opening browser for authorization (one-time only)...")
            try:
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, GOOGLE_SCOPES)
                creds = flow.run_local_server(port=0)
            except Exception as e:
                print(f"ERROR: Google OAuth failed: {e}")
                sys.exit(1)

        os.makedirs(".credentials", exist_ok=True)
        try:
            with open(GOOGLE_TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
            if verbose:
                print(f"    [verbose] Google token saved to {GOOGLE_TOKEN_PATH}")
        except Exception:
            pass

    return creds


def get_sheets_service(config: dict, verbose: bool):
    """Return an authorized Google Sheets API service object."""
    try:
        from googleapiclient.discovery import build
    except ImportError:
        print("ERROR: google-api-python-client not installed.")
        sys.exit(1)
    creds = get_google_creds(config, verbose)
    return build("sheets", "v4", credentials=creds)


# ---------------------------------------------------------------------------
# Sheet parsing
# ---------------------------------------------------------------------------

def parse_sheet_id(sheet_url: str) -> str:
    match = re.search(r"/spreadsheets/d/([^/?#]+)", sheet_url)
    if not match:
        print(f"ERROR: Could not parse spreadsheet ID from URL: {sheet_url}")
        sys.exit(1)
    return match.group(1)


# ---------------------------------------------------------------------------
# Rationale extraction
# ---------------------------------------------------------------------------

def _extract_rationale(report_body: str, total_score: int, tier_label: str) -> str:
    """
    Extract a short rationale string (~1-2 sentences) from the score report body.
    Tries Sales Rep Recommendation first; falls back to first Key Buying Signal.
    """
    # Try Sales Rep Recommendation section
    rec_match = re.search(
        r"##\s*Sales Rep Recommendation\s*\n+(.*?)(?:\n##|\Z)",
        report_body,
        re.DOTALL,
    )
    if rec_match:
        text = rec_match.group(1).strip()
        if len(text) > 220:
            cutoff = text.rfind(". ", 0, 220)
            text = text[: cutoff + 1] if cutoff > 50 else text[:220]
        return text

    # Fallback: first non-empty line from Key Buying Signals
    sig_match = re.search(
        r"##\s*Key Buying Signals\s*\n+(.*?)(?:\n##|\Z)",
        report_body,
        re.DOTALL,
    )
    if sig_match:
        lines = [l.strip().lstrip("- •*").strip() for l in sig_match.group(1).splitlines() if l.strip()]
        if lines:
            return lines[0][:220]

    return f"{tier_label} ({total_score}/12)"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args   = parse_args()
    config = load_config()

    # Lazy imports from sibling scripts (avoids circular imports and startup overhead)
    from score import (
        build_scoring_queries,
        run_all_searches,
        extract_snippets,
        score_with_claude,
        write_score_report,
        get_tier,
        _parse_total_score,
    )
    from context import pull_salesforce, slugify

    sheet_id = parse_sheet_id(args.sheet_url)
    service  = get_sheets_service(config, args.verbose)
    sheets   = service.spreadsheets()

    # Read all rows (A:E)
    result = sheets.values().get(spreadsheetId=sheet_id, range="Sheet1!A:E").execute()
    rows   = result.get("values", [])

    if not rows or len(rows) < 2:
        print("Sheet is empty or has only a header row. Nothing to process.")
        return

    data_rows  = rows[1:]  # skip header
    total_rows = len(data_rows)
    processed  = 0
    skipped    = 0
    reused     = 0
    errors     = 0

    # Track slugs processed in this run to catch in-sheet duplicates
    seen_slugs: set[str] = set()

    limit_str = f" (limit: {args.limit})" if args.limit else ""
    print(f"\nBulk Fit Score — {total_rows} prospect rows found{limit_str}")
    print("=" * 60)

    sf_config = {
        "sf_username":       config["sf_username"],
        "sf_password":       config["sf_password"],
        "sf_security_token": config["sf_security_token"],
        "sf_domain":         config["sf_domain"],
        "db_path":           "",
    }

    output_dir = Path(args.output_dir)

    for i, row in enumerate(data_rows, start=2):  # sheet row index (1-indexed, row 1 = header)
        company          = row[0].strip() if row else ""
        domain           = row[1].strip() if len(row) > 1 else ""
        existing_score   = row[3].strip() if len(row) > 3 else ""

        row_num = i - 1  # display index (1 = first data row)

        # ── Guard 1: sheet cell already has a score ───────────────────────
        if existing_score and existing_score != "ERROR":
            slug = slugify(company) if company else ""
            seen_slugs.add(slug)
            skipped += 1
            print(f"  [{row_num}/{total_rows}] {company or '(blank)'} — skipped (score in sheet: {existing_score})", flush=True)
            continue

        if not company:
            skipped += 1
            print(f"  [{row_num}/{total_rows}] (blank row) — skipped", flush=True)
            continue

        slug = slugify(company)

        # ── Guard 2: same company already processed earlier in this sheet ─
        if slug in seen_slugs:
            skipped += 1
            print(f"  [{row_num}/{total_rows}] {company} — skipped (duplicate in sheet, already processed above)", flush=True)
            continue

        # ── Guard 3: existing local score report ──────────────────────────
        score_files = sorted((output_dir / slug).glob("*_score.md"), reverse=True) \
                      if (output_dir / slug).exists() else []
        if score_files:
            existing_file = score_files[0]
            try:
                existing_text  = existing_file.read_text(encoding="utf-8")
                existing_total = _parse_total_score(existing_text)
                if existing_total >= 0:
                    tier_label, tier_icon, _ = get_tier(existing_total)
                    score_str = f"{tier_icon} {existing_total}/10 ({tier_label})"
                    rationale = _extract_rationale(existing_text, existing_total, tier_label)
                    sheets.values().update(
                        spreadsheetId=sheet_id,
                        range=f"Sheet1!D{i}:E{i}",
                        valueInputOption="RAW",
                        body={"values": [[score_str, rationale]]},
                    ).execute()
                    seen_slugs.add(slug)
                    reused += 1
                    print(
                        f"  [{row_num}/{total_rows}] {company} — reused existing report "
                        f"({existing_file.name}, score: {score_str})",
                        flush=True,
                    )
                    continue
            except Exception as reuse_err:
                print(f"  [{row_num}/{total_rows}] {company} — could not reuse existing report ({reuse_err}), re-scoring", flush=True)

        # Enforce limit (counts only rows that will actually be processed)
        if args.limit and processed >= args.limit:
            print(f"  Limit of {args.limit} reached — stopping.", flush=True)
            break

        seen_slugs.add(slug)
        print(f"\n[{row_num}/{total_rows}] Scoring: {company}", flush=True)
        print("-" * 40, flush=True)

        sf_owner       = ""
        sf_account_url = ""
        report_path    = ""

        try:
            # 1. Run You.com searches
            queries        = build_scoring_queries(company)
            search_results = run_all_searches(company, queries, config["youcom_api_key"], args.verbose)
            extracted      = extract_snippets(search_results)

            # 2. Score with Claude (streams output line-by-line via print)
            report_body, total_score = score_with_claude(
                company,
                extracted,
                "",  # no pre-existing research report
                config["anthropic_api_key"],
                args.verbose,
            )

            # 3. Save local report
            report_path = write_score_report(
                company,
                report_body,
                total_score,
                extracted["_all_urls"],
                args.output_dir,
            )

            # 4. Look up Salesforce account — search by domain first, fall back to name
            try:
                sf_result      = pull_salesforce(company, sf_config, verbose=False, domain=domain)
                account        = sf_result.get("account") or {}
                sf_owner       = (account.get("Owner") or {}).get("Name", "")
                sf_account_url = sf_result.get("sf_account_url", "")
            except Exception as sf_err:
                print(f"  WARNING: SF lookup failed: {sf_err}", flush=True)

            # 5. Build short rationale
            tier_label, tier_icon, _ = get_tier(total_score)
            score_str = f"{tier_icon} {total_score}/12 ({tier_label})"
            rationale = _extract_rationale(report_body, total_score, tier_label)

            # 6. Write results back to sheet
            #    Col C: SF Owner  |  Col D: Fit Score  |  Col E: Rationale
            sheets.values().update(
                spreadsheetId=sheet_id,
                range=f"Sheet1!C{i}:E{i}",
                valueInputOption="RAW",
                body={"values": [[sf_owner, score_str, rationale]]},
            ).execute()

            #    Col A: Account Name — link to the SF account record when found
            if sf_account_url:
                escaped_url  = sf_account_url.replace('"', '%22')
                escaped_name = company.replace('"', '""')
                sheets.values().update(
                    spreadsheetId=sheet_id,
                    range=f"Sheet1!A{i}",
                    valueInputOption="USER_ENTERED",
                    body={"values": [[f'=HYPERLINK("{escaped_url}","{escaped_name}")']]},
                ).execute()

            print(f"  ✓ Score: {score_str}", flush=True)
            print(f"  ✓ SF Owner: {sf_owner or '(not found)'}", flush=True)
            if sf_account_url:
                print(f"  ✓ SF Account: {sf_account_url}", flush=True)
            if report_path:
                print(f"  ✓ Report saved: {report_path}", flush=True)

            processed += 1

        except SystemExit:
            # Fatal errors (bad API key, etc.) — propagate immediately
            raise
        except Exception as exc:
            errors += 1
            print(f"  ERROR scoring '{company}': {exc}", flush=True)
            # Write ERROR to col D so it's visible; row will be re-processed on next run
            try:
                sheets.values().update(
                    spreadsheetId=sheet_id,
                    range=f"Sheet1!D{i}",
                    valueInputOption="RAW",
                    body={"values": [["ERROR"]]},
                ).execute()
            except Exception:
                pass

        # Brief pause between rows to stay within API rate limits
        if row_num < total_rows:
            time.sleep(1)

    print(f"\n{'=' * 60}")
    print(f"Done.  Processed: {processed}  |  Reused: {reused}  |  Skipped: {skipped}  |  Errors: {errors}")


if __name__ == "__main__":
    main()
