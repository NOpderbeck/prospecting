"""
penetration_publish.py — Publish penetration report to Google Drive + Slack.

Usage:
    python3 penetration_publish.py \
        --results-file /tmp/pen_results.json \
        --summary "Slack summary text" \
        --date "April 15, 2026"
"""

import argparse
import json
import os
import subprocess
import sys
import time
import requests as req_lib
from collections import defaultdict
from dotenv import load_dotenv

ENV_PATH          = os.path.join(os.path.dirname(__file__), ".env")
GOOGLE_TOKEN_PATH = os.path.join(os.path.dirname(__file__), ".credentials", "google_token_penetration.json")
DRIVE_FOLDER_ID   = "1gujAtCzSVHZQtNbvH33Js0k-Oqjtr7V8"
SLACK_CHANNEL     = "C0AT4Q506Q2"   # #sales-target-alerts — ID is rename-safe
GOOGLE_SCOPES     = ["https://www.googleapis.com/auth/drive"]

TABLE_HEADERS    = ["Account", "Classification", "Score", "Owner", "API (30d)", "Last API Call", "Last Touch"]
AT_RISK_DAYS     = 60   # accounts whose last API call is older than this drop out of At Risk


# ── Google auth ────────────────────────────────────────────────────────────────

def get_google_creds(creds_file: str):
    # ── Service account key file (explicit path via env var) ──────────────────
    sa_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if sa_key and os.path.exists(sa_key):
        from google.oauth2 import service_account
        print("  Using service account credentials", file=sys.stderr)
        return service_account.Credentials.from_service_account_file(
            sa_key, scopes=GOOGLE_SCOPES
        )

    # ── OAuth user token (local dev — preferred when token file exists) ─────────
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request as GoogleRequest

    creds = None
    if os.path.exists(GOOGLE_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_PATH, GOOGLE_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())
        else:
            if not creds_file or not os.path.exists(creds_file):
                print(f"ERROR: credentials file not found: {creds_file}", file=sys.stderr)
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, GOOGLE_SCOPES)
            creds = flow.run_local_server(port=0)
        os.makedirs(os.path.dirname(GOOGLE_TOKEN_PATH), exist_ok=True)
        with open(GOOGLE_TOKEN_PATH, "w") as f:
            f.write(creds.to_json())
    if creds:
        return creds

    # ── Application Default Credentials (Cloud Run attached SA, gcloud ADC) ──
    try:
        import google.auth
        creds, _ = google.auth.default(scopes=GOOGLE_SCOPES)
        print("  Using Application Default Credentials", file=sys.stderr)
        return creds
    except Exception:
        pass

    print("ERROR: No Google credentials found", file=sys.stderr)
    sys.exit(1)


# ── Docs helpers ───────────────────────────────────────────────────────────────

def batch_update(docs_svc, doc_id: str, requests: list):
    """Execute a batchUpdate with exponential backoff on 429 rate-limit errors."""
    from googleapiclient.errors import HttpError
    delay = 15
    for attempt in range(6):
        try:
            docs_svc.documents().batchUpdate(
                documentId=doc_id, body={"requests": requests}
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
    """Return the index just before the final newline — safe insert point."""
    d = docs_svc.documents().get(documentId=doc_id).execute()
    return d["body"]["content"][-1]["endIndex"] - 1


def append_segments(docs_svc, doc_id: str, segments: list):
    """
    Append a list of (text, named_style, bold) tuples to the end of the doc.
    Applies paragraph styles and bold in a second batch.
    """
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
            style_reqs.append({
                "updateParagraphStyle": {
                    "range": {"startIndex": pos, "endIndex": end},
                    "paragraphStyle": {"namedStyleType": style},
                    "fields": "namedStyleType",
                }
            })
        if bold and text:
            style_reqs.append({
                "updateTextStyle": {
                    "range": {"startIndex": pos, "endIndex": end - 1},
                    "textStyle": {"bold": True},
                    "fields": "bold",
                }
            })
        pos = end

    for i in range(0, len(style_reqs), 200):
        batch_update(docs_svc, doc_id, style_reqs[i:i+200])


SF_BASE = "https://ydc.my.salesforce.com/"


def append_table(docs_svc, doc_id: str, headers: list, rows: list, row_ids: list = None):
    """
    Append a table to the end of the doc. Inserts cells in reverse index order
    so that each insertion doesn't shift subsequent cell positions.

    row_ids: optional list of Salesforce Account IDs (one per row). When provided,
             column 0 (account name) is rendered as a hyperlink to the SF record.
    """
    if not rows:
        return

    insert_pos = doc_end(docs_svc, doc_id)
    n_cols     = len(headers)

    batch_update(docs_svc, doc_id, [{"insertTable": {
        "rows": len(rows) + 1,
        "columns": n_cols,
        "location": {"index": insert_pos},
    }}])

    # Re-fetch to get the table structure with real cell indices
    d          = docs_svc.documents().get(documentId=doc_id).execute()
    table_elem = next(
        (e["table"] for e in reversed(d["body"]["content"]) if "table" in e), None
    )
    if not table_elem:
        return

    # Collect (cell_start_index, text, bold, url) for all cells
    cell_data = []
    for ci, h in enumerate(headers):
        cell = table_elem["tableRows"][0]["tableCells"][ci]
        cell_data.append((cell["content"][0]["startIndex"], h, True, None))

    for ri, row in enumerate(rows, start=1):
        sf_id = row_ids[ri - 1] if row_ids else None
        for ci, val in enumerate(row):
            cell = table_elem["tableRows"][ri]["tableCells"][ci]
            url  = (SF_BASE + sf_id) if (ci == 0 and sf_id) else None
            cell_data.append((cell["content"][0]["startIndex"], str(val), False, url))

    # Sort descending by index — insert from the end so earlier indices stay stable
    cell_data.sort(key=lambda x: -x[0])

    cell_reqs = []
    for idx, text, bold, url in cell_data:
        cell_reqs.append({"insertText": {"location": {"index": idx}, "text": text}})
        if bold and text:
            cell_reqs.append({
                "updateTextStyle": {
                    "range": {"startIndex": idx, "endIndex": idx + len(text)},
                    "textStyle": {"bold": True},
                    "fields": "bold",
                }
            })
        if url and text:
            cell_reqs.append({
                "updateTextStyle": {
                    "range": {"startIndex": idx, "endIndex": idx + len(text)},
                    "textStyle": {"link": {"url": url}},
                    "fields": "link",
                }
            })

    for i in range(0, len(cell_reqs), 200):
        batch_update(docs_svc, doc_id, cell_reqs[i:i+200])


# ── Classification refinement ─────────────────────────────────────────────────

def reclassify(results: list):
    """
    Refine At Risk: only accounts whose last API call was within AT_RISK_DAYS
    are genuinely at risk. Older accounts fall through to their natural
    classification based on current usage + activity signals.
    """
    from datetime import date
    today = date.today()

    for r in results:
        if r["cls"] != "At Risk":
            continue
        last_call = r.get("last_call")
        days_since = (today - date.fromisoformat(last_call)).days if last_call else 9999
        if days_since <= AT_RISK_DAYS:
            continue  # stays At Risk

        # Reclassify based on current signals
        if r["usage_present"] and not r["act_present"]:
            r["cls"] = "Inbound Only";  r["pri"] = "red"
        elif not r["usage_present"] and r["act_present"]:
            r["cls"] = "Outbound Only"; r["pri"] = "orange"
        else:
            r["cls"] = "White Space";   r["pri"] = "white"


# ── Salesforce enrichment ──────────────────────────────────────────────────────

def _soql(query: str) -> list:
    """Run a SOQL query via the sf CLI and return the records list."""
    r = subprocess.run(
        ["sf", "data", "query", "--target-org", "ydc", "--query", query, "--json"],
        capture_output=True, text=True,
    )
    try:
        data = json.loads(r.stdout)
        if data.get("status") != 0:
            print(f"  SOQL error: {data.get('message','')[:120]}", file=sys.stderr)
            return []
        return data.get("result", {}).get("records", [])
    except Exception as e:
        print(f"  SOQL parse error: {e} — stdout[:200]: {r.stdout[:200]}", file=sys.stderr)
        return []


def fetch_contact_names(results: list) -> dict:
    """
    Return {account_id: [sorted unique contact names]} for every account
    that has activity in the last 30 days. Queries Task + Event in batches.
    """
    ids = [r["id"] for r in results if r.get("contacts", 0) > 0]
    if not ids:
        return {}

    acc_names: dict[str, set] = {}

    for i in range(0, len(ids), 200):
        batch   = ids[i : i + 200]
        id_list = "','".join(batch)

        for obj, date_field in [("Task", "ActivityDate"), ("Event", "ActivityDateTime")]:
            records = _soql(
                f"SELECT AccountId, Who.Name FROM {obj} "
                f"WHERE AccountId IN ('{id_list}') "
                f"AND {date_field} >= LAST_N_DAYS:30 "
                f"AND WhoId != null"
                + (" AND Status != 'Not Started'" if obj == "Task" else "")
            )
            for rec in records:
                who  = rec.get("Who") or {}
                name = who.get("Name")
                if name:
                    acc_names.setdefault(rec["AccountId"], set()).add(name)

    return {k: sorted(v) for k, v in acc_names.items()}


# ── Report content ─────────────────────────────────────────────────────────────


def account_row(r: dict) -> list:
    """Convert a result dict into a table row matching TABLE_HEADERS."""
    api = f"{r['total_30d']:,} calls · {r['active_30']} user(s)"
    return [
        r['name'],
        r['cls'],
        f"{r['pen']}/15",
        r['owner'],
        api,
        r['last_call'] or "—",
        r['last_act']  or "—",
    ]


def build_google_doc(docs_svc, doc_id: str, results: list, date_str: str):
    """Write the full formatted report into an existing (empty) Google Doc."""
    from datetime import date as date_cls

    customers = [r for r in results if r['is_customer']]
    prospects = [r for r in results if not r['is_customer']]
    total     = len(results)
    n_pros    = len(prospects)

    # Same logic as Slack: prospects-only for activity and usage
    pen_pct   = round(len(customers) / total   * 100) if total  else 0
    act_n     = sum(1 for r in prospects if r['act_present'])
    act_pct   = round(act_n   / n_pros * 100) if n_pros else 0
    usage_n   = sum(1 for r in prospects if r['usage_present'])
    usage_pct = round(usage_n / n_pros * 100) if n_pros else 0
    avg_score = round(sum(r['pen'] for r in prospects) / n_pros, 1) if n_pros else 0

    reclassify(results)

    red    = sorted([r for r in prospects if r['pri'] == 'red'],    key=lambda x: -x['pen'])
    orange = sorted([r for r in prospects if r['pri'] == 'orange'], key=lambda x: -x['pen'])
    yellow = sorted([r for r in prospects if r['pri'] == 'yellow'], key=lambda x: -x['pen'])
    white  = [r for r in prospects if r['cls'] == 'White Space']
    blue   = sorted([r for r in prospects if r['pri'] == 'blue'],   key=lambda x: -x['pen'])

    inbound_only  = [r for r in prospects if r['cls'] == 'Inbound Only']
    inbound_names = ', '.join(f"{r['name']} ({r['owner']})" for r in inbound_only)

    def S(*args): return list(args)   # segment shorthand
    H1 = 'HEADING_1'; H2 = 'HEADING_2'; N = 'NORMAL_TEXT'

    # ── Title + Summary ────────────────────────────────────────────────────────
    append_segments(docs_svc, doc_id, [
        S(f"Penetration Intelligence — {date_str}", H1, False),
        S(f"Org-wide  ·  {total} Tier 1 accounts scanned  ·  {len(customers)} customers", N, False),
        S("", N, False),
        S("Coverage Summary", H2, False),
        S(f"Target Account Penetration    {pen_pct}%  ({len(customers)} of {total} converted)", N, False),
        S(f"Sales Activity (Last 30d)     {act_pct}%  ({act_n} of {n_pros} prospects)", N, False),
        S(f"API Usage (Last 30d)          {usage_pct}%  ({usage_n} of {n_pros} prospects)", N, False),
        S(f"Avg penetration score         {avg_score} / 15", N, False),
        S("", N, False),
        S("🚨  Alerts", H2, False),
    ])
    alert_lines = []
    if inbound_names:
        alert_lines.append(S(f"Uncovered demand ({len(inbound_only)})  —  {inbound_names}", N, True))
        alert_lines.append(S("API usage is live with zero sales coverage. Act immediately.", N, False))
    alert_lines.append(S(f"{len(white)} accounts with no API footprint and no sales activity (White Space)", N, False))
    alert_lines.append(S("", N, False))
    append_segments(docs_svc, doc_id, alert_lines)

    # ── 🔴 Act Now ─────────────────────────────────────────────────────────────
    print(f"  Writing Act Now table ({len(red)} rows)...", file=sys.stderr)
    append_segments(docs_svc, doc_id, [
        S(f"🔴  Act Now — {len(red)} Account(s)", H2, False),
        S(
            "Accounts requiring immediate commercial action. "
            "At Risk accounts had active API usage within the last 60 days that has since gone silent — "
            "a direct churn signal. Inbound Only accounts have live product adoption with zero sales coverage.",
            N, False,
        ),
        S("", N, False),
    ])
    append_table(docs_svc, doc_id, TABLE_HEADERS, [account_row(r) for r in red],    row_ids=[r['id'] for r in red])
    append_segments(docs_svc, doc_id, [S("", N, False)])

    # ── 🟡 Expand / Convert ────────────────────────────────────────────────────
    expand = sorted(yellow + blue, key=lambda x: -x['pen'])
    print(f"  Writing Expand table ({len(expand)} rows)...", file=sys.stderr)
    append_segments(docs_svc, doc_id, [
        S(f"🟡  Expand / Convert — {len(expand)} Account(s)", H2, False),
        S(
            "Accounts with API adoption and active sales coverage. "
            "The foundation is in place — focus on expanding use cases, mapping the buying committee, "
            "and converting momentum into a commercial deal.",
            N, False,
        ),
        S("", N, False),
    ])
    append_table(docs_svc, doc_id, TABLE_HEADERS, [account_row(r) for r in expand], row_ids=[r['id'] for r in expand])
    append_segments(docs_svc, doc_id, [S("", N, False)])

    # ── 🟠 Continue Outreach ───────────────────────────────────────────────────
    print(f"  Writing Continue Outreach table ({len(orange)} rows)...", file=sys.stderr)
    append_segments(docs_svc, doc_id, [
        S(f"🟠  Continue Outreach — {len(orange)} Account(s)", H2, False),
        S(
            "Accounts with active sales coverage but no API adoption. "
            "The team is investing time here without yet breaking through.",
            N, False,
        ),
        S("", N, False),
    ])
    append_table(docs_svc, doc_id, TABLE_HEADERS, [account_row(r) for r in orange], row_ids=[r['id'] for r in orange])
    append_segments(docs_svc, doc_id, [S("", N, False)])

    # ── ⚪ White Space ─────────────────────────────────────────────────────────
    by_owner = defaultdict(list)
    for r in white:
        by_owner[r['owner']].append(r['name'])
    ws_rows = [
        [owner, str(len(names)), ",  ".join(names)]
        for owner, names in sorted(by_owner.items(), key=lambda kv: -len(kv[1]))
    ]
    append_segments(docs_svc, doc_id, [
        S(f"⚪  White Space — {len(white)} Account(s)", H2, False),
        S("No API footprint and no sales activity. Prospect immediately.", N, False),
        S("", N, False),
    ])
    append_table(docs_svc, doc_id, ["Owner", "#", "Accounts"], ws_rows)
    append_segments(docs_svc, doc_id, [S("", N, False)])

    # ── 🔘 Closed Lost — suppressed At Risk ───────────────────────────────────
    closed_lost = sorted([r for r in prospects if r['cls'] == 'Closed Lost'], key=lambda x: -x['pen'])
    if closed_lost:
        cl_rows = []
        for r in closed_lost:
            cl_name = r.get("closed_lost_name") or "—"
            cl_date = r.get("closed_lost_date") or "—"
            cl_rows.append([r['name'], r['owner'], cl_name, cl_date,
                             f"{r.get('total_30d',0):,} calls"])
        append_segments(docs_svc, doc_id, [
            S(f"🔘  Closed Lost — {len(closed_lost)} Account(s)", H2, False),
            S(
                "These accounts showed dormant API usage but have a recent Closed Lost opportunity "
                "that explains the drop-off. Suppressed from At Risk alerts.",
                N, False,
            ),
            S("", N, False),
        ])
        append_table(docs_svc, doc_id,
                     ["Account", "Owner", "Lost Opp", "Close Date", "API (30d)"],
                     cl_rows)
        append_segments(docs_svc, doc_id, [S("", N, False)])

    # ── 💰 Customers ───────────────────────────────────────────────────────────
    print(f"  Writing Customers table ({len(customers)} rows)...", file=sys.stderr)
    cust_headers = ["Account", "Owner", "API (30d)", "Active Users"]
    cust_rows = []
    for r in sorted(customers, key=lambda x: -x['pen']):
        cust_rows.append([
            r['name'],
            r['owner'],
            f"{r['total_30d']:,} calls",
            str(r['active_30']),
        ])
    append_segments(docs_svc, doc_id, [
        S(f"💰  Customers — {len(customers)} Accounts", H2, False),
        S("", N, False),
    ])
    append_table(docs_svc, doc_id, cust_headers, cust_rows, row_ids=[r['id'] for r in sorted(customers, key=lambda x: -x['pen'])])

    # ── 📐 Penetration Score legend ────────────────────────────────────────────
    print("  Writing legend...", file=sys.stderr)
    append_segments(docs_svc, doc_id, [
        S("", N, False),
        S("📐  Penetration Score — Reference", H2, False),
        S(
            "The Penetration Score (0 – 15) measures the depth of commercial engagement "
            "with each Tier 1 account. It combines product adoption and sales coverage "
            "into a single signal.",
            N, False,
        ),
        S("", N, False),
        S("Score  =  (Usage Score  +  Activity Score)  ×  Coverage Multiplier", N, True),
        S("", N, False),
        S("Usage Score  (0 – 5)", N, True),
    ])
    append_table(docs_svc, doc_id, ["Score", "Condition"], [
        ["0", "No API usage ever"],
        ["1", "Minimal — < 500 calls/30d, single user"],
        ["2", "Light — ≥ 500 calls/30d, single user"],
        ["3", "Active — ≥ 1,000 calls/30d or consistent weekly cadence"],
        ["4", "Strong — ≥ 10,000 calls/30d or growing week-over-week"],
        ["5", "Multi-user + growing — ≥ 2 active users AND ≥ 1,000 calls/30d"],
    ])
    append_segments(docs_svc, doc_id, [S("", N, False), S("Activity Score  (0 – 5)", N, True)])
    append_table(docs_svc, doc_id, ["Score", "Condition"], [
        ["0", "No tasks or meetings in last 30 days"],
        ["1", "Emails only, 1 contact"],
        ["2", "Emails + calls, 1 contact"],
        ["3", "Consistent outreach — ≥ 3 activities, last touch ≤ 14 days"],
        ["4", "Meeting held or inbound reply received"],
        ["5", "Multi-contact meeting — meeting + ≥ 2 contacts engaged"],
    ])
    append_segments(docs_svc, doc_id, [S("", N, False), S("Coverage Quality Multiplier", N, True)])
    append_table(docs_svc, doc_id, ["Multiplier", "Condition"], [
        ["1.0×", "Outbound only — no replies, no meetings"],
        ["1.2×", "Two-way engagement — reply or meeting held"],
        ["1.5×", "Full coverage — meeting + ≥ 2 contacts engaged"],
    ])
    append_segments(docs_svc, doc_id, [S("", N, False)])

    # Link every "Score" header cell in the doc to the legend heading
    _link_score_headers(docs_svc, doc_id)

    # Insert quick-navigation block near the top
    _build_toc(docs_svc, doc_id)


def _build_toc(docs_svc, doc_id: str):
    """Insert a Table of Contents block after the report subtitle."""
    TOC_SECTIONS = [
        ("Act Now",           "Act Now"),
        ("Expand / Convert",  "Expand / Convert"),
        ("Continue Outreach", "Continue Outreach"),
        ("White Space",       "White Space"),
        ("Customers",         "Customers"),
        ("Reference",         "Reference"),
    ]
    TOC_HEADER = "Table of Contents"

    d = docs_svc.documents().get(documentId=doc_id).execute()
    content = d["body"]["content"]

    # Build {keyword: headingId} from all H2s
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

    sections = [
        (label, next((hid for txt, hid in heading_map.items() if kw in txt), None))
        for label, kw in TOC_SECTIONS
    ]

    # Find insert position: right after the second paragraph (subtitle)
    para_count, insert_pos = 0, None
    for elem in content:
        if "paragraph" in elem:
            para_count += 1
            if para_count == 2:
                insert_pos = elem["endIndex"]
                break
    if insert_pos is None:
        return

    # Build full insert text: blank line + header + one entry per line + blank line
    lines = ["\n", TOC_HEADER + "\n"] + [label + "\n" for label, _ in sections]
    insert_text = "".join(lines)
    batch_update(docs_svc, doc_id, [
        {"insertText": {"location": {"index": insert_pos}, "text": insert_text}}
    ])

    # Track positions for styling
    style_reqs = []
    pos = insert_pos + 1  # skip the leading blank line

    # Bold the "Table of Contents" header
    style_reqs.append({"updateTextStyle": {
        "range": {"startIndex": pos, "endIndex": pos + len(TOC_HEADER)},
        "textStyle": {"bold": True},
        "fields": "bold",
    }})
    pos += len(TOC_HEADER) + 1  # +1 for \n

    # Hyperlink each section entry
    for label, hid in sections:
        if hid:
            url = f"https://docs.google.com/document/d/{doc_id}/edit#heading={hid}"
            style_reqs.append({"updateTextStyle": {
                "range": {"startIndex": pos, "endIndex": pos + len(label)},
                "textStyle": {"link": {"url": url}},
                "fields": "link",
            }})
        pos += len(label) + 1  # +1 for \n

    if style_reqs:
        batch_update(docs_svc, doc_id, style_reqs)


def _find_heading_id(docs_svc, doc_id: str, text_contains: str) -> str | None:
    """Return the headingId of the first H2 whose text contains text_contains."""
    d = docs_svc.documents().get(documentId=doc_id).execute()
    for elem in d["body"]["content"]:
        if "paragraph" not in elem:
            continue
        para  = elem["paragraph"]
        style = para.get("paragraphStyle", {})
        if style.get("namedStyleType") != "HEADING_2":
            continue
        full_text = "".join(
            r.get("textRun", {}).get("content", "")
            for r in para.get("elements", [])
        )
        if text_contains in full_text:
            return style.get("headingId")
    return None


def _link_score_headers(docs_svc, doc_id: str):
    """Hyperlink every 'Score' header cell in every table to the legend heading."""
    heading_id = _find_heading_id(docs_svc, doc_id, "Reference")
    if not heading_id:
        return
    score_url = f"https://docs.google.com/document/d/{doc_id}/edit#heading={heading_id}"

    d = docs_svc.documents().get(documentId=doc_id).execute()
    link_reqs = []
    for elem in d["body"]["content"]:
        if "table" not in elem:
            continue
        header_row = elem["table"].get("tableRows", [None])[0]
        if not header_row:
            continue
        for cell in header_row.get("tableCells", []):
            for content_elem in cell.get("content", []):
                for run in content_elem.get("paragraph", {}).get("elements", []):
                    if run.get("textRun", {}).get("content", "").strip() == "Score":
                        start = run["startIndex"]
                        end   = run["endIndex"] - 1
                        link_reqs.append({"updateTextStyle": {
                            "range": {"startIndex": start, "endIndex": end},
                            "textStyle": {"link": {"url": score_url}},
                            "fields": "link",
                        }})
    if link_reqs:
        batch_update(docs_svc, doc_id, link_reqs)


# ── Drive: find or create doc ──────────────────────────────────────────────────

def get_or_create_doc(docs_svc, drive_svc, title: str) -> str:
    existing = drive_svc.files().list(
        q=f"name='{title}' and '{DRIVE_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        fields="files(id)",
    ).execute().get("files", [])

    if existing:
        doc_id = existing[0]["id"]
        print(f"  Overwriting existing doc...", file=sys.stderr)
        d = docs_svc.documents().get(documentId=doc_id).execute()
        end = d["body"]["content"][-1]["endIndex"]
        if end > 2:
            batch_update(docs_svc, doc_id,
                [{"deleteContentRange": {"range": {"startIndex": 1, "endIndex": end - 1}}}])
    else:
        # Create the doc directly inside the shared folder via Drive API.
        # This avoids the "create in root then move" pattern which fails for
        # service accounts that don't have personal Drive root access.
        doc = drive_svc.files().create(
            body={
                "name":     title,
                "mimeType": "application/vnd.google-apps.document",
                "parents":  [DRIVE_FOLDER_ID],
            },
            supportsAllDrives=True,
            fields="id",
        ).execute()
        doc_id = doc["id"]
        drive_svc.permissions().create(
            fileId=doc_id,
            body={"type": "domain", "role": "reader", "domain": "you.com"},
            supportsAllDrives=True, fields="id",
        ).execute()

    return doc_id


def create_google_doc(creds, title: str, results: list, date_str: str) -> str:
    from googleapiclient.discovery import build
    docs_svc  = build("docs", "v1", credentials=creds)
    drive_svc = build("drive", "v3", credentials=creds)

    doc_id = get_or_create_doc(docs_svc, drive_svc, title)
    build_google_doc(docs_svc, doc_id, results, date_str)
    return f"https://docs.google.com/document/d/{doc_id}/edit"


# ── Slack ──────────────────────────────────────────────────────────────────────

_slack_id_cache: dict[str, str | None] = {}


def resolve_slack_id(token: str, email: str) -> str | None:
    """Look up a Slack user ID by email. Returns the user ID string or None."""
    if not token or not email:
        return None
    if email in _slack_id_cache:
        return _slack_id_cache[email]
    resp = req_lib.get(
        "https://slack.com/api/users.lookupByEmail",
        headers={"Authorization": f"Bearer {token}"},
        params={"email": email},
        timeout=10,
    )
    r = resp.json()
    uid = r["user"]["id"] if r.get("ok") else None
    _slack_id_cache[email] = uid
    return uid


def owner_mention(token: str, owner_email: str, owner_name: str) -> str:
    """Return <@USERID> if resolvable, else plain owner name."""
    uid = resolve_slack_id(token, owner_email)
    return f"<@{uid}>" if uid else owner_name


def _bar(pct: float, width: int = 20) -> str:
    filled = round(pct / 100 * width)
    return "█" * filled + "░" * (width - filled)


def build_slack_message(results: list, date_str: str, doc_url: str, token: str = "") -> str:
    customers = [r for r in results if r["is_customer"]]
    prospects = [r for r in results if not r["is_customer"]]
    total     = len(results)
    n_pros    = len(prospects)

    owners = {r["owner"] for r in results}
    owner_label = next(iter(owners)) if len(owners) == 1 else "Org-wide"

    pen_pct   = len(customers) / total   * 100 if total  else 0
    act_n     = sum(1 for r in prospects if r["act_present"])
    act_pct   = act_n   / n_pros * 100 if n_pros else 0
    usage_n   = sum(1 for r in prospects if r["usage_present"])
    usage_pct = usage_n / n_pros * 100 if n_pros else 0

    inbound = [r for r in prospects if r["cls"] == "Inbound Only"]
    white   = [r for r in prospects if r["cls"] == "White Space"]
    inbound_links = ", ".join(
        f"<{SF_BASE}{r['id']}|{r['name']}> ({owner_mention(token, r.get('owner_email',''), r['owner'])})"
        for r in inbound
    )

    lines = [
        f"*📊 Penetration Intelligence — {date_str}*",
        f"_{owner_label} · {total} Tier 1 accounts_",
        "",
        "*Target Account Penetration*",
        f"`{_bar(pen_pct)}`  {pen_pct:.0f}%  _({len(customers)} of {total} converted)_",
        "",
        "*Sales Activity (Last 30d)*",
        f"`{_bar(act_pct)}`  {act_pct:.0f}%  _({act_n} of {n_pros} prospects)_",
        "",
        "*API Usage (Last 30d)*",
        f"`{_bar(usage_pct)}`  {usage_pct:.0f}%  _({usage_n} of {n_pros} prospects)_",
    ]

    alerts = []
    if inbound:
        alerts.append(f"• {len(inbound)} with usage + zero sales activity → uncovered demand: _{inbound_links}_")

    lines += ["", "", "🚨 *Alerts*"]
    if alerts:
        lines += alerts
    else:
        lines.append("• No alerts this period")

    lines += ["", "", f"📄 *<{doc_url}|Full Report>*"]
    return "\n".join(lines)


def post_to_slack(token: str, results: list, date_str: str, doc_url: str, dry_run: bool = False):
    text = build_slack_message(results, date_str, doc_url, token=token)
    if dry_run:
        print("\n── DRY RUN: Slack message (not posted) ──────────────────────────")
        print(text)
        print("─────────────────────────────────────────────────────────────────\n")
        return
    resp = req_lib.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"channel": SLACK_CHANNEL, "text": text, "mrkdwn": True, "unfurl_links": False},
        timeout=10,
    )
    result = resp.json()
    if result.get("ok"):
        print(f"✅ Posted to {SLACK_CHANNEL}")
    else:
        print(f"⚠️ Slack post failed: {result.get('error')}", file=sys.stderr)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-file", required=True)
    parser.add_argument("--date",         required=True)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print Slack message to stdout; skip Google Doc creation and Slack post")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)
    creds_file  = os.getenv("GOOGLE_CREDENTIALS_FILE", "")
    slack_token = os.getenv("SLACK_BOT_TOKEN", "")

    if not os.path.exists(args.results_file):
        print(f"ERROR: results file not found: {args.results_file}", file=sys.stderr)
        sys.exit(1)

    results = json.load(open(args.results_file))
    # Normalize field names produced by digest_run.py vs what this script expects
    CLS_MAP = {
        "Inbound Only":        ("Inbound Only",        "red"),
        "At Risk":             ("At Risk",             "red"),
        "Outbound Only":       ("Outbound Only",       "orange"),
        "Early Penetration":   ("Early Penetration",   "orange"),
        "Multi-Threaded Growth": ("Multi-Threaded Growth", "yellow"),
        "Strong Penetration":  ("Strong Penetration",  "yellow"),
        "White Space":         ("White Space",         "white"),
        "Closed Lost":         ("Closed Lost",         "grey"),
        "Developing":          ("Developing",          "blue"),
    }
    for r in results:
        if "cls" not in r:
            raw_cls = r.get("classification", "Developing")
            cls, pri = CLS_MAP.get(raw_cls, (raw_cls, "blue"))
            r["cls"] = cls
            r["pri"] = pri
        # Alias pen_score → pen
        if "pen" not in r:
            r["pen"] = r.get("pen_score", 0)
        # Alias active_30d → active_30
        if "active_30" not in r:
            r["active_30"] = r.get("active_30d", 0)
        # last_call: not stored as ISO date in digest output; derive from days_usage
        if "last_call" not in r:
            days = r.get("days_usage")
            if days is not None:
                from datetime import date, timedelta
                r["last_call"] = (date.today() - timedelta(days=int(days))).isoformat()
            else:
                r["last_call"] = None
    reclassify(results)

    if args.dry_run:
        print("── DRY RUN — no Google Doc or Slack post ──────────────────────", file=sys.stderr)
        post_to_slack(slack_token, results, args.date, doc_url="<doc-url-not-created>", dry_run=True)
        return

    print("Authenticating with Google...", file=sys.stderr)
    creds = get_google_creds(creds_file)

    title = f"Penetration Intelligence — {args.date}"
    print(f"Building doc: '{title}'...", file=sys.stderr)
    doc_url = create_google_doc(creds, title, results, args.date)
    print(f"Created: {doc_url}")

    if slack_token:
        post_to_slack(slack_token, results, args.date, doc_url)
    else:
        print("⚠️ No SLACK_BOT_TOKEN — skipping Slack post", file=sys.stderr)


if __name__ == "__main__":
    main()
