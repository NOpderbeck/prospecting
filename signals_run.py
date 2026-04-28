"""
signals_run.py — Daily signal alerts for Tier 1 target accounts.

Detects three signals:
  1. New users added to a target account (Product_User__c.CreatedDate within lookback)
  2. New API traffic from those users (First_API_Call_Date__c within lookback + calls > 0)
  3. Usage burst — account-level spike that clears both a minimum absolute delta
     (MIN_BURST_DELTA) and a minimum ratio vs weekly average (MIN_BURST_RATIO),
     preventing false positives from low-volume noise (10→20 calls is not a burst).

Posts a single Slack message to #sales-target-alerts tagging account owners.
Silent if no signals found.

Usage (local):
    python3 signals_run.py [--date "April 16, 2026"] [--lookback 1]

Environment variables:
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN  — Salesforce creds
    SLACK_BOT_TOKEN                               — Slack bot token
    TEST_OWNER_EMAIL                              — When set, all owner tags
                                                    resolve to this email
                                                    (use during testing)
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, timedelta
from dotenv import load_dotenv

ENV_PATH      = os.path.join(os.path.dirname(__file__), ".env")
SLACK_CHANNEL = "C0AT4Q506Q2"  # #sales-target-alerts — ID is rename-safe
SF_BASE       = "https://ydc.my.salesforce.com/"

# ── Burst detection thresholds ─────────────────────────────────────────────────
# Both conditions must be true to fire. The absolute floor prevents false
# positives at low volumes (10→20 is noise); the ratio ensures the spike is
# proportionally meaningful at higher volumes (5000→5800 is growth, not a burst).
MIN_BURST_DELTA     = 500   # minimum absolute increase over weekly average (calls)
MIN_BURST_RATIO     = 1.75  # this week must be ≥ 1.75× the weekly average
MIN_UNTIERED_CALLS  = 500   # minimum 30d calls for an untiered account to surface
MIN_DORMANT_ALLTIME = 1_000 # minimum all-time calls for a dormant account to surface
BURST_COOLDOWN_DAYS = 4     # suppress repeat burst alerts for the same account within N days

# State file — tracks when each account last fired a burst alert
# Stored next to the script so it persists across Cloud Run executions via the same volume,
# or falls back to /tmp if the script directory isn't writable (e.g. read-only container).
_BURST_STATE_FILE = os.path.join(os.path.dirname(__file__), ".burst_state.json")

# Tiers already covered by the standard Tier 1 / Tier 2.A scans
MONITORED_TIERS = {"1. TARGET ACCOUNT", "Tier 1", "2.A", "Tier 2"}

# ── Dormant owner filter ───────────────────────────────────────────────────────
# When set, --dormant results are limited to accounts owned by these names.
# Exact case-insensitive match against Owner.Name. Empty set = show all owners.
DORMANT_OWNERS: set[str] = {
    "Nick Opderbeck",
    "David Wacker",
    "Integration",
    "Ryan Allred",
    "Ryan Reed",
    "Andrew Miller-McKeever",
}

# Regions to exclude from --dormant results. Exact case-insensitive match
# against Region__c (or BillingCountry fallback). Empty set = show all regions.
DORMANT_EXCLUDE_REGIONS: set[str] = {
    "EMEA",
    "DACH",
}

# ── Alert blocklist ────────────────────────────────────────────────────────────
# Accounts listed here are silently excluded from all signal alerts (new users
# and usage bursts). Match is case-insensitive substring of Account Name.
ALERT_BLOCKLIST = [
    "BytePlus",
]

# ── Untiered blocklist ─────────────────────────────────────────────────────────
# Accounts listed here are excluded from --untiered scans only. Use for accounts
# with high API volume that are known noise (e.g. internal tools, resellers,
# universities with bulk free-tier usage). Case-insensitive substring match.
UNTIERED_BLOCKLIST = [
    "Alumni Ventures",
    "University of Gloucestershire",
    "PrivateRelay",
    "Web",
    "Domain.com",
]


def is_blocked(account_name: str) -> bool:
    name_lower = account_name.lower()
    return any(entry.lower() in name_lower for entry in ALERT_BLOCKLIST)


def is_untiered_blocked(account_name: str) -> bool:
    return account_name.lower() in {entry.lower() for entry in UNTIERED_BLOCKLIST}


# ── Salesforce ─────────────────────────────────────────────────────────────────

def connect_sf():
    from simple_salesforce import Salesforce
    return Salesforce(
        username=os.environ["SF_USERNAME"],
        password=os.environ["SF_PASSWORD"],
        security_token=os.environ["SF_SECURITY_TOKEN"],
    )


def soql(sf, query: str) -> list:
    try:
        return sf.query_all(query.strip()).get("records", [])
    except Exception as e:
        print(f"  SOQL error: {e}", file=sys.stderr)
        return []


def soql_in_chunks(sf, query_template: str, ids: list, chunk_size: int = 200) -> list:
    """
    Run a SOQL query that uses an IN clause against a potentially large list of
    IDs. Splits into chunks to avoid HTTP 414 URI Too Long errors (simple_salesforce
    uses GET requests, so the query ends up in the URL).

    `query_template` must contain exactly one `{id_list}` placeholder, e.g.:
        "SELECT Id FROM Account WHERE Id IN ('{id_list}')"
    """
    results = []
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        id_list = "', '".join(chunk)
        results.extend(soql(sf, query_template.format(id_list=id_list)))
    return results


TIER1_FILTER = "('1. TARGET ACCOUNT', 'Tier 1')"
TIER2_FILTER = "('2.A', 'Tier 2')"
ALL_TIERS_FILTER = "('1. TARGET ACCOUNT', 'Tier 1', '2.A', 'Tier 2')"


def fetch_new_users(sf, lookback_cutoff: date, tier_filter: str = TIER1_FILTER) -> list:
    """
    Return all Product_User__c records on accounts of the given tier(s) created
    on or after the lookback_cutoff date (midnight UTC). Uses an explicit
    calendar-date boundary instead of LAST_N_DAYS so consecutive daily runs
    never overlap — a user created on Tuesday is reported Tuesday and never again.

    Both a lower AND upper bound are applied so the window is exactly one
    calendar day. Without the upper bound a user created on day N would
    re-appear on day N+1 (cutoff = N, user CreatedDate = N still matches).
    """
    since = lookback_cutoff.strftime("%Y-%m-%dT00:00:00Z")
    until = date.today().strftime("%Y-%m-%dT00:00:00Z")
    return soql(sf, f"""
        SELECT Email__c,
               CreatedDate,
               First_API_Call_Date__c,
               API_Calls_Last_7_Days__c,
               API_Calls_Last_30_Days__c,
               Account__c,
               Account__r.Name,
               Account__r.Id,
               Account__r.Owner.Name,
               Account__r.Owner.Email
        FROM Product_User__c
        WHERE Account__r.Account_Tier__c IN {tier_filter}
        AND CreatedDate >= {since}
        AND CreatedDate < {until}
        ORDER BY Account__r.Name, CreatedDate DESC
    """)


def fetch_all_usage(sf, tier_filter: str = TIER1_FILTER) -> list:
    """
    Return API_Calls_Last_7_Days__c and API_Calls_Last_30_Days__c for every
    Product_User__c on accounts of the given tier(s). Used to detect
    account-level bursts independent of whether the user is new.
    """
    return soql(sf, f"""
        SELECT Account__c,
               Account__r.Name,
               Account__r.Id,
               Account__r.Owner.Name,
               Account__r.Owner.Email,
               API_Calls_Last_7_Days__c,
               API_Calls_Last_30_Days__c
        FROM Product_User__c
        WHERE Account__r.Account_Tier__c IN {tier_filter}
        ORDER BY Account__r.Name
    """)


_GCS_BURST_BLOB = "burst_state.json"


def _gcs_bucket():
    """Return a GCS Bucket object if BURST_STATE_BUCKET is set, else None."""
    bucket_name = os.getenv("BURST_STATE_BUCKET", "")
    if not bucket_name:
        return None
    try:
        from google.cloud import storage
        return storage.Client().bucket(bucket_name)
    except Exception as e:
        print(f"  ⚠️  GCS init failed, falling back to local state: {e}", file=sys.stderr)
        return None


def _load_burst_state() -> dict:
    """Load burst cooldown state from GCS (if configured) or local disk."""
    bucket = _gcs_bucket()
    if bucket is not None:
        try:
            blob = bucket.blob(_GCS_BURST_BLOB)
            if blob.exists():
                return json.loads(blob.download_as_text())
            return {}
        except Exception as e:
            print(f"  ⚠️  GCS load failed, falling back to local state: {e}", file=sys.stderr)

    path = _BURST_STATE_FILE
    if not os.path.exists(path):
        path_tmp = "/tmp/.burst_state.json"
        if not os.path.exists(path_tmp):
            return {}
        path = path_tmp
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_burst_state(state: dict) -> None:
    """Persist burst cooldown state to GCS (if configured) or local disk."""
    bucket = _gcs_bucket()
    if bucket is not None:
        try:
            bucket.blob(_GCS_BURST_BLOB).upload_from_string(
                json.dumps(state, indent=2), content_type="application/json"
            )
            return
        except Exception as e:
            print(f"  ⚠️  GCS save failed, falling back to local state: {e}", file=sys.stderr)

    path = _BURST_STATE_FILE
    try:
        with open(path, "w") as f:
            json.dump(state, f, indent=2)
    except OSError:
        path = "/tmp/.burst_state.json"
        with open(path, "w") as f:
            json.dump(state, f, indent=2)


def detect_bursts(usage_records: list, dry_run: bool = False) -> dict:
    """
    Aggregate usage per account and return those that clear both burst gates:
      1. delta (total_7d − weekly_avg) >= MIN_BURST_DELTA
      2. total_7d >= weekly_avg × MIN_BURST_RATIO

    Returns:
    {
      account_id: {
        'name':        str,
        'sf_id':       str,
        'owner_name':  str,
        'owner_email': str,
        'total_7d':    int,
        'weekly_avg':  float,
        'delta':       int,
      }
    }
    """
    agg: dict = {}
    for r in usage_records:
        acc_id  = r.get("Account__c") or r.get("Account__r", {}).get("Id", "")
        acc_ref = r.get("Account__r") or {}
        if acc_id not in agg:
            agg[acc_id] = {
                "name":        acc_ref.get("Name", "Unknown"),
                "sf_id":       acc_ref.get("Id", acc_id),
                "owner_name":  (acc_ref.get("Owner") or {}).get("Name", ""),
                "owner_email": (acc_ref.get("Owner") or {}).get("Email", ""),
                "total_7d":    0,
                "total_30d":   0,
            }
        agg[acc_id]["total_7d"]  += int(r.get("API_Calls_Last_7_Days__c")  or 0)
        agg[acc_id]["total_30d"] += int(r.get("API_Calls_Last_30_Days__c") or 0)

    state    = _load_burst_state()
    today_s  = date.today().isoformat()
    bursts: dict = {}
    state_dirty  = False

    for acc_id, acc in agg.items():
        if is_blocked(acc["name"]):
            continue
        total_7d   = acc["total_7d"]
        weekly_avg = acc["total_30d"] / 4
        delta      = total_7d - weekly_avg
        if not (weekly_avg > 0
                and delta >= MIN_BURST_DELTA
                and total_7d >= weekly_avg * MIN_BURST_RATIO):
            continue

        # Cooldown check — suppress if this account already fired within N days
        last_fired = state.get(acc_id)
        if last_fired:
            days_since = (date.today() - date.fromisoformat(last_fired)).days
            if days_since < BURST_COOLDOWN_DAYS:
                print(f"  ⏸  Burst suppressed for {acc['name']} "
                      f"(last fired {days_since}d ago, cooldown={BURST_COOLDOWN_DAYS}d)",
                      file=sys.stderr)
                continue

        bursts[acc_id] = {
            "name":        acc["name"],
            "sf_id":       acc["sf_id"],
            "owner_name":  acc["owner_name"],
            "owner_email": acc["owner_email"],
            "total_7d":    total_7d,
            "weekly_avg":  round(weekly_avg, 1),
            "delta":       int(delta),
        }
        state[acc_id] = today_s
        state_dirty   = True

    if state_dirty and not dry_run:
        _save_burst_state(state)
    elif state_dirty and dry_run:
        print("  ℹ️  Dry-run: burst state NOT saved (cooldown unchanged)", file=sys.stderr)

    return bursts


def fetch_untiered_usage(sf) -> list:
    """
    Return all Product_User__c records with any 30-day activity, regardless of
    account tier. Filtering by > 0 calls keeps the result set manageable.
    Tier exclusion is handled in Python (detect_untiered) so we can also catch
    accounts with a null or unrecognised tier value.
    """
    return soql(sf, """
        SELECT Account__c,
               Account__r.Name,
               Account__r.Id,
               Account__r.Account_Tier__c,
               Account__r.Region__c,
               Account__r.BillingCountry,
               Account__r.Owner.Name,
               Account__r.Owner.Email,
               API_Calls_Last_7_Days__c,
               API_Calls_Last_30_Days__c,
               API_Calls_per_User_All_Time__c
        FROM Product_User__c
        WHERE API_Calls_Last_30_Days__c > 0
        ORDER BY Account__r.Name
    """)


def detect_untiered(usage_records: list, sf=None) -> list:
    """
    Aggregate usage by account, exclude accounts already covered by Tier 1 / 2.A
    scans (MONITORED_TIERS), apply MIN_UNTIERED_CALLS floor, exclude customers
    (Total_Revenue_Closed_Won__c > 0), and return a list sorted by total_30d desc.

    Pass `sf` (a live Salesforce connection) to enable customer exclusion. If sf
    is None, the customer filter is skipped (degraded mode — all accounts shown).

    Returns:
    [
      {
        'name':         str,
        'sf_id':        str,
        'tier':         str,   # raw tier value or '—' if null
        'owner_name':   str,
        'owner_email':  str,
        'total_7d':     int,
        'total_30d':    int,
        'total_alltime':int,
        'active_users': int,
        'weekly_avg':   float,
        'is_customer':  bool,
      }, ...
    ]
    """
    agg: dict = {}
    for r in usage_records:
        acc_ref = r.get("Account__r") or {}
        acc_id  = r.get("Account__c") or acc_ref.get("Id", "")
        tier    = acc_ref.get("Account_Tier__c") or ""

        if not acc_id:
            continue
        if tier in MONITORED_TIERS:
            continue

        if acc_id not in agg:
            region = (acc_ref.get("Region__c") or
                      acc_ref.get("BillingCountry") or "—")
            agg[acc_id] = {
                "name":         acc_ref.get("Name", "Unknown"),
                "sf_id":        acc_ref.get("Id", acc_id),
                "tier":         tier or "—",
                "region":       region,
                "owner_name":   (acc_ref.get("Owner") or {}).get("Name", ""),
                "owner_email":  (acc_ref.get("Owner") or {}).get("Email", ""),
                "total_7d":     0,
                "total_30d":    0,
                "total_alltime": 0,
                "active_users": 0,
            }
        agg[acc_id]["total_7d"]       += int(r.get("API_Calls_Last_7_Days__c")       or 0)
        agg[acc_id]["total_30d"]      += int(r.get("API_Calls_Last_30_Days__c")      or 0)
        agg[acc_id]["total_alltime"]  += int(r.get("API_Calls_per_User_All_Time__c") or 0)
        agg[acc_id]["active_users"]   += 1

    # Apply threshold + blocklist before the customer lookup to keep the
    # batch query small — no point fetching revenue data for low-volume accounts.
    candidates = {
        acc_id: acc
        for acc_id, acc in agg.items()
        if acc["total_30d"] >= MIN_UNTIERED_CALLS
        and not is_blocked(acc["name"])
        and not is_untiered_blocked(acc["name"])
    }

    # ── Customer exclusion ─────────────────────────────────────────────────────
    # Two passes, both run in one query each:
    #   Pass 1 — accounts with Total_Revenue_Closed_Won__c > 0 (paid customers)
    #   Pass 2 — accounts with any Closed Won opp in the last 12 months (catches
    #             $0 PAYG agreements that show zero revenue but are signed customers)
    customer_ids: set[str] = set()
    if sf and candidates:
        id_list = "', '".join(candidates.keys())

        # Pass 1: revenue-based
        rev_records = soql(sf, f"""
            SELECT Id, Total_Revenue_Closed_Won__c
            FROM Account
            WHERE Id IN ('{id_list}')
        """)
        revenue_customer_ids = {
            r["Id"]
            for r in rev_records
            if (r.get("Total_Revenue_Closed_Won__c") or 0) > 0
        }

        # Pass 2: recently closed won (last 12 months) — catches $0 PAYG deals
        opp_records = soql(sf, f"""
            SELECT AccountId
            FROM Opportunity
            WHERE AccountId IN ('{id_list}')
            AND StageName = 'Closed Won'
            AND CloseDate >= LAST_N_DAYS:365
        """)
        recent_closedwon_ids = {r["AccountId"] for r in opp_records}

        customer_ids = revenue_customer_ids | recent_closedwon_ids

        if customer_ids:
            customer_names = [candidates[cid]["name"] for cid in customer_ids if cid in candidates]
            print(f"  Excluding {len(customer_ids)} customer(s) from untiered results: "
                  f"{', '.join(customer_names)}", file=sys.stderr)

    results = [
        {**acc, "weekly_avg": round(acc["total_30d"] / 4, 1), "is_customer": acc_id in customer_ids}
        for acc_id, acc in candidates.items()
        if acc_id not in customer_ids
    ]
    results.sort(key=lambda a: -a["total_30d"])
    return results


def print_untiered_report(accounts: list, date_str: str):
    """Print a ranked console report of untiered accounts with significant usage."""
    print(f"\n── Untiered Usage Scan — {date_str} {'─' * 30}")
    print(f"   Accounts with ≥ {MIN_UNTIERED_CALLS:,} API calls/30d · excluding Tier 1, Tier 2.A, and customers\n")

    if not accounts:
        print("   No untiered accounts above threshold.")
        return

    # Column widths
    name_w   = min(max(len(a["name"])        for a in accounts), 40)
    region_w = min(max(len(a["region"])      for a in accounts), 16)
    owner_w  = min(max(len(a["owner_name"])  for a in accounts), 22)

    header = (f"  {'#':>3}  {'Account':<{name_w}}  {'Tier':<12}  "
              f"{'Region':<{region_w}}  "
              f"{'30d calls':>10}  {'7d calls':>9}  {'Wk avg':>8}  "
              f"{'Users':>5}  {'Owner':<{owner_w}}")
    print(header)
    print("  " + "─" * (len(header) - 2))

    for i, acc in enumerate(accounts, 1):
        name   = acc["name"][:name_w]
        region = acc["region"][:region_w]
        owner  = acc["owner_name"][:owner_w]
        print(
            f"  {i:>3}.  {name:<{name_w}}  {acc['tier']:<12}  "
            f"{region:<{region_w}}  "
            f"{acc['total_30d']:>10,}  {acc['total_7d']:>9,}  "
            f"{acc['weekly_avg']:>8,.0f}  {acc['active_users']:>5}  {owner:<{owner_w}}"
        )

    print(f"\n  {len(accounts)} account(s) shown · all-time total: "
          f"{sum(a['total_alltime'] for a in accounts):,} calls")
    print("─" * 70)


# ── Dormant untiered scan ──────────────────────────────────────────────────────

def fetch_dormant_usage(sf) -> list:
    """
    Return Product_User__c records with all-time usage but zero activity in the
    last 30 days. Includes First/Last call dates for recency calculations.
    """
    return soql(sf, """
        SELECT Account__c,
               Account__r.Name,
               Account__r.Id,
               Account__r.Account_Tier__c,
               Account__r.Region__c,
               Account__r.BillingCountry,
               Account__r.Owner.Name,
               Account__r.Owner.Email,
               API_Calls_Last_30_Days__c,
               API_Calls_per_User_All_Time__c,
               First_API_Call_Date__c,
               Last_API_Call_Date__c
        FROM Product_User__c
        WHERE API_Calls_per_User_All_Time__c > 0
        AND (API_Calls_Last_30_Days__c = 0 OR API_Calls_Last_30_Days__c = null)
        ORDER BY Account__r.Name
    """)


def detect_dormant(usage_records: list, sf=None) -> list:
    """
    Aggregate by account, exclude Tier 1/2.A (MONITORED_TIERS), customers,
    and the UNTIERED_BLOCKLIST. Apply MIN_DORMANT_ALLTIME floor. Sort by
    all-time calls descending.

    Returns list of dicts with keys: name, sf_id, tier, region, owner_name,
    owner_email, total_alltime, total_30d, last_call_date, days_dark,
    first_call_date, users.
    """
    from datetime import date as date_type
    today = date.today()

    agg: dict = {}
    for r in usage_records:
        acc_ref = r.get("Account__r") or {}
        acc_id  = r.get("Account__c") or acc_ref.get("Id", "")
        tier    = acc_ref.get("Account_Tier__c") or ""

        if not acc_id:
            continue
        if tier in MONITORED_TIERS:
            continue

        # Parse last/first call dates
        raw_last  = r.get("Last_API_Call_Date__c")
        raw_first = r.get("First_API_Call_Date__c")
        last_dt   = date.fromisoformat(raw_last[:10])  if raw_last  else None
        first_dt  = date.fromisoformat(raw_first[:10]) if raw_first else None

        if acc_id not in agg:
            region = (acc_ref.get("Region__c") or
                      acc_ref.get("BillingCountry") or "—")
            agg[acc_id] = {
                "name":          acc_ref.get("Name", "Unknown"),
                "sf_id":         acc_ref.get("Id", acc_id),
                "tier":          tier or "—",
                "region":        region,
                "owner_name":    (acc_ref.get("Owner") or {}).get("Name", ""),
                "owner_email":   (acc_ref.get("Owner") or {}).get("Email", ""),
                "total_alltime": 0,
                "total_30d":     0,
                "last_call_date":  last_dt,
                "first_call_date": first_dt,
                "users":         0,
            }

        agg[acc_id]["total_alltime"] += int(r.get("API_Calls_per_User_All_Time__c") or 0)
        agg[acc_id]["total_30d"]     += int(r.get("API_Calls_Last_30_Days__c")      or 0)
        agg[acc_id]["users"]         += 1

        # Keep the most recent last_call_date and earliest first_call_date
        if last_dt and (agg[acc_id]["last_call_date"] is None or last_dt > agg[acc_id]["last_call_date"]):
            agg[acc_id]["last_call_date"] = last_dt
        if first_dt and (agg[acc_id]["first_call_date"] is None or first_dt < agg[acc_id]["first_call_date"]):
            agg[acc_id]["first_call_date"] = first_dt

    # Threshold + blocklist before customer lookup
    candidates = {
        acc_id: acc
        for acc_id, acc in agg.items()
        if acc["total_alltime"] >= MIN_DORMANT_ALLTIME
        and not is_blocked(acc["name"])
        and not is_untiered_blocked(acc["name"])
    }

    # Account-level activity check — exclude any account that has at least one
    # user with 30d activity. The dormant query filters per-user, so an account
    # with a mix of active and inactive users would otherwise appear here.
    partially_active_ids: set[str] = set()
    if sf and candidates:
        candidate_ids = list(candidates.keys())
        active_records = soql_in_chunks(sf,
            "SELECT Account__c FROM Product_User__c "
            "WHERE Account__c IN ('{id_list}') AND API_Calls_Last_30_Days__c > 0",
            candidate_ids)
        partially_active_ids = {r["Account__c"] for r in active_records}
        if partially_active_ids:
            active_names = [candidates[cid]["name"] for cid in partially_active_ids if cid in candidates]
            print(f"  Excluding {len(partially_active_ids)} partially-active account(s): "
                  f"{', '.join(active_names)}", file=sys.stderr)

    candidates = {
        acc_id: acc for acc_id, acc in candidates.items()
        if acc_id not in partially_active_ids
    }

    # Customer exclusion — same two-pass logic as detect_untiered
    customer_ids: set[str] = set()
    if sf and candidates:
        candidate_ids = list(candidates.keys())

        rev_records = soql_in_chunks(sf,
            "SELECT Id, Total_Revenue_Closed_Won__c FROM Account WHERE Id IN ('{id_list}')",
            candidate_ids)
        revenue_customer_ids = {
            r["Id"] for r in rev_records
            if (r.get("Total_Revenue_Closed_Won__c") or 0) > 0
        }

        opp_records = soql_in_chunks(sf,
            "SELECT AccountId FROM Opportunity WHERE AccountId IN ('{id_list}') "
            "AND StageName = 'Closed Won' AND CloseDate >= LAST_N_DAYS:365",
            candidate_ids)
        recent_closedwon_ids = {r["AccountId"] for r in opp_records}

        customer_ids = revenue_customer_ids | recent_closedwon_ids
        if customer_ids:
            customer_names = [candidates[cid]["name"] for cid in customer_ids if cid in candidates]
            print(f"  Excluding {len(customer_ids)} customer(s): {', '.join(customer_names)}",
                  file=sys.stderr)

    results = []
    for acc_id, acc in candidates.items():
        if acc_id in customer_ids:
            continue
        days_dark = (today - acc["last_call_date"]).days if acc["last_call_date"] else None
        results.append({**acc, "days_dark": days_dark})

    # Owner filter — applied last so customer/activity exclusions still run in full
    if DORMANT_OWNERS:
        owner_lower = {o.lower() for o in DORMANT_OWNERS}
        results = [a for a in results if a["owner_name"].lower() in owner_lower]

    # Region exclusion — drop regions we don't want in the dormant view
    if DORMANT_EXCLUDE_REGIONS:
        excl_lower = {r.lower() for r in DORMANT_EXCLUDE_REGIONS}
        results = [a for a in results if a["region"].lower() not in excl_lower]

    results.sort(key=lambda a: (a["days_dark"] is None, a["days_dark"]))
    return results


def print_dormant_report(accounts: list, date_str: str):
    """Print a ranked console report of dormant untiered prospects."""
    print(f"\n── Dormant Untiered Scan — {date_str} {'─' * 26}")
    print(f"   Untiered prospects · zero usage last 30d · ≥ {MIN_DORMANT_ALLTIME:,} all-time calls\n")

    if not accounts:
        print("   No dormant untiered accounts above threshold.")
        return

    name_w   = min(max(len(a["name"])       for a in accounts), 40)
    region_w = min(max(len(a["region"])     for a in accounts), 16)
    owner_w  = min(max(len(a["owner_name"]) for a in accounts), 22)

    header = (f"  {'#':>3}  {'Account':<{name_w}}  {'Tier':<12}  "
              f"{'Region':<{region_w}}  {'All-time':>10}  "
              f"{'Last call':<12}  {'Days dark':>9}  {'Users':>5}  {'Owner':<{owner_w}}")
    print(header)
    print("  " + "─" * (len(header) - 2))

    for i, acc in enumerate(accounts, 1):
        name      = acc["name"][:name_w]
        region    = acc["region"][:region_w]
        owner     = acc["owner_name"][:owner_w]
        last_call = acc["last_call_date"].strftime("%Y-%m-%d") if acc["last_call_date"] else "never"
        days_dark = f"{acc['days_dark']:,}d" if acc["days_dark"] is not None else "—"
        print(
            f"  {i:>3}.  {name:<{name_w}}  {acc['tier']:<12}  "
            f"{region:<{region_w}}  {acc['total_alltime']:>10,}  "
            f"{last_call:<12}  {days_dark:>9}  {acc['users']:>5}  {owner:<{owner_w}}"
        )

    print(f"\n  {len(accounts)} account(s) shown")
    print("─" * 70)


# ── Slack helpers ──────────────────────────────────────────────────────────────

_slack_id_cache: dict[str, str] = {}


def resolve_slack_id(bot_token: str, email: str) -> str | None:
    """Look up a Slack user ID by email. Returns '<@USERID>' or None."""
    if email in _slack_id_cache:
        return _slack_id_cache[email]
    import requests
    resp = requests.get(
        "https://slack.com/api/users.lookupByEmail",
        headers={"Authorization": f"Bearer {bot_token}"},
        params={"email": email},
        timeout=10,
    )
    r = resp.json()
    if r.get("ok"):
        uid = r["user"]["id"]
        _slack_id_cache[email] = uid
        return uid
    print(f"  ⚠️  users.lookupByEmail failed for {email}: {r.get('error')}", file=sys.stderr)
    _slack_id_cache[email] = None
    return None


def owner_mention(bot_token: str, owner_email: str, owner_name: str,
                  test_email: str | None) -> str:
    """
    Return a Slack mention string for the account owner.
    If TEST_OWNER_EMAIL is set, resolves to that user instead.
    Falls back to plain owner name if lookup fails.
    """
    lookup_email = test_email or owner_email
    uid = resolve_slack_id(bot_token, lookup_email)
    if uid:
        return f"<@{uid}>"
    return owner_name


def fmt_name(email: str) -> str:
    """
    Convert an email address to a display name.
    bob.woolworth@acme.com → Bob Woolworth
    """
    local = email.split("@")[0]
    return " ".join(part.capitalize() for part in local.replace(".", " ").replace("_", " ").split())


# ── LinkedIn lookup ────────────────────────────────────────────────────────────

_linkedin_cache: dict[str, str | None] = {}

YOUCOM_SEARCH_URL = "https://ydc-index.io/v1/search"


def linkedin_url_for_email(email: str, display_name: str, company_name: str) -> str | None:
    """
    Search for a LinkedIn profile URL using a site-scoped You.com query:
      site:linkedin.com/in/ {display_name} {company_name}
    Returns the first linkedin.com/in/ URL found, or None.
    Cached per email to avoid duplicate API calls.

    Confidence gates (both must pass):
      1. Name completeness — email local part must contain '.' or '_', meaning
         we have at least a first and last name. Single-name emails like
         ethan@... or basia@... are skipped entirely.
      2. Slug verification — every token of display_name must appear as a
         substring in the LinkedIn URL slug (case-insensitive). This rejects
         results where the search returned an unrelated person.
    """
    if email in _linkedin_cache:
        return _linkedin_cache[email]

    # Gate 1: require a full name (first + last) derivable from the email
    local = email.split("@")[0]
    if "." not in local and "_" not in local:
        print(f"  ⏭️  LinkedIn skipped for {display_name}: single-name email, low confidence",
              file=sys.stderr)
        _linkedin_cache[email] = None
        return None

    youcom_key = os.getenv("YOUCOM_API_KEY", "")
    if not youcom_key:
        _linkedin_cache[email] = None
        return None

    import requests, re
    # Gate 2 uses only the last name token — given names are unstable (Robert/Bob,
    # William/Bill) but family names are stable. Requiring the surname in the slug
    # is enough to reject unrelated people without over-blocking nickname variants.
    name_tokens = [t.lower() for t in display_name.split() if t]
    last_name   = name_tokens[-1] if name_tokens else ""
    query = f"site:linkedin.com/in/ {display_name} {company_name}"
    try:
        resp = requests.get(
            YOUCOM_SEARCH_URL,
            headers={"Accept": "application/json", "X-API-KEY": youcom_key},
            params={"query": query, "count": 5, "language": "EN", "crawl_timeout": 10},
            timeout=15,
        )
        hits = resp.json().get("results", {}).get("web", [])
        for hit in hits:
            url = hit.get("url", "")
            m = re.match(r"https://www\.linkedin\.com/in/([^/]+)/?$", url)
            if not m:
                continue
            # Gate 2: verify the last name (surname) appears in the URL slug
            slug = m.group(1).lower()
            if last_name and last_name in slug:
                print(f"  🔗 LinkedIn found for {display_name}: {url}", file=sys.stderr)
                _linkedin_cache[email] = url
                return url
            else:
                print(f"  ⚠️  LinkedIn skipped unconfident match for {display_name}: {url}",
                      file=sys.stderr)
    except Exception as e:
        print(f"  ⚠️  LinkedIn lookup failed for {display_name}: {e}", file=sys.stderr)

    _linkedin_cache[email] = None
    return None


def fmt_name_linked(display_name: str, email: str, company_name: str) -> str:
    """
    Return display_name hyperlinked to their LinkedIn profile if found,
    otherwise plain display_name. Formatted for Slack mrkdwn.
    """
    url = linkedin_url_for_email(email, display_name, company_name)
    if url:
        return f"<{url}|{display_name}>"
    return display_name


# ── Signal grouping ────────────────────────────────────────────────────────────

def group_signals(records: list, lookback_cutoff: date) -> dict:
    """
    Group new-user records by account. For each user, classify as:
      - 'with_calls'  : new user + first API calls within lookback window
      - 'user_only'   : new user added, no new API calls yet

    Returns:
    {
      account_id: {
        'name':        str,
        'sf_id':       str,
        'owner_name':  str,
        'owner_email': str,
        'with_calls':  [(display_name, email, call_count), ...],
        'user_only':   [(display_name, email), ...],
      }
    }
    """
    accounts: dict = {}

    for r in records:
        acc_id    = r.get("Account__c") or r.get("Account__r", {}).get("Id", "")
        acc_ref   = r.get("Account__r") or {}
        acc_name  = acc_ref.get("Name", "Unknown")
        if is_blocked(acc_name):
            continue
        owner     = acc_ref.get("Owner") or {}
        email     = r.get("Email__c") or ""
        calls_7d  = int(r.get("API_Calls_Last_7_Days__c") or 0)

        # Determine if this user's first API call falls within the lookback window
        first_call_raw = r.get("First_API_Call_Date__c")
        first_call_recent = False
        if first_call_raw:
            first_call_date = date.fromisoformat(first_call_raw[:10])
            first_call_recent = first_call_date >= lookback_cutoff

        if acc_id not in accounts:
            accounts[acc_id] = {
                "name":        acc_name,
                "sf_id":       acc_ref.get("Id", acc_id),
                "owner_name":  owner.get("Name", ""),
                "owner_email": owner.get("Email", ""),
                "with_calls":  [],
                "user_only":   [],
            }

        display = fmt_name(email) if email else "Unknown User"
        if first_call_recent and calls_7d > 0:
            accounts[acc_id]["with_calls"].append((display, email, calls_7d))
        else:
            accounts[acc_id]["user_only"].append((display, email))

    return accounts


# ── Message building ───────────────────────────────────────────────────────────

def build_alert_lines(accounts: dict, bursts: dict, bot_token: str,
                      test_email: str | None, date_str: str) -> list[str]:
    """
    Build one alert line per signal event (not per account).
    An account can produce multiple lines if it fires more than one signal type.
    """
    lines = []

    # ── New user signals ───────────────────────────────────────────────────────
    for acc_id, acc in sorted(accounts.items(), key=lambda x: x[1]["name"]):
        name    = acc["name"]
        sf_url  = SF_BASE + acc["sf_id"]
        mention = owner_mention(bot_token, acc["owner_email"], acc["owner_name"], test_email)

        for display, email, calls in acc["with_calls"]:
            linked = fmt_name_linked(display, email, name)
            lines.append(
                f"• *<{sf_url}|{name}>* — {linked} added as a new user "
                f"and made *{calls:,} API calls* for the first time. {mention}"
            )

        if acc["user_only"]:
            users = acc["user_only"]
            if len(users) == 1:
                display, email = users[0]
                linked = fmt_name_linked(display, email, name)
                lines.append(
                    f"• *<{sf_url}|{name}>* — {linked} added as a new user. {mention}"
                )
            else:
                names_fmt = ", ".join(fmt_name_linked(d, e, name) for d, e in users)
                lines.append(
                    f"• *<{sf_url}|{name}>* — {len(users)} new users added: "
                    f"{names_fmt}. {mention}"
                )

    # ── Usage burst signals ────────────────────────────────────────────────────
    for acc_id, acc in sorted(bursts.items(), key=lambda x: x[1]["name"]):
        name    = acc["name"]
        sf_url  = SF_BASE + acc["sf_id"]
        mention = owner_mention(bot_token, acc["owner_email"], acc["owner_name"], test_email)
        lines.append(
            f"• *<{sf_url}|{name}>* — usage spike: *{acc['total_7d']:,} API calls* "
            f"this week vs *{acc['weekly_avg']:,.0f}/wk* avg "
            f"(+{acc['delta']:,}). {mention}"
        )

    return lines


def post_to_slack(bot_token: str, lines: list[str], date_str: str,
                  dry_run: bool = False, tier_label: str = "Tier 1"):
    import requests
    header = f"🔔 *Daily Signal Alerts — {date_str}*"
    text   = header + "\n\n" + "\n".join(lines)

    if dry_run:
        label = f" [{tier_label}]" if tier_label != "Tier 1" else ""
        print(f"\n── DRY RUN{label}: Slack message (not posted) ───────────────────")
        print(text)
        print("─────────────────────────────────────────────────────────────────\n")
        return

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json"},
        json={"channel": SLACK_CHANNEL, "text": text, "mrkdwn": True, "unfurl_links": False},
        timeout=10,
    )
    result = resp.json()
    if result.get("ok"):
        print(f"✅ Posted {len(lines)} alert(s) to {SLACK_CHANNEL}")
    else:
        print(f"⚠️  Slack post failed: {result.get('error')}", file=sys.stderr)


# ── Main ───────────────────────────────────────────────────────────────────────

SIMULATE_ACCOUNTS = [
    {
        "name":        "Acme Corp",
        "sf_id":       "001000000000001AAA",
        "owner_name":  "Nick Opderbeck",
        "owner_email": "nick.opderbeck@you.com",
        "with_calls":  [("Bob Woolworth", "bob.woolworth@acme.com", 22)],
        "user_only":   [],
    },
    {
        "name":        "Globex Inc",
        "sf_id":       "001000000000002AAA",
        "owner_name":  "Nick Opderbeck",
        "owner_email": "nick.opderbeck@you.com",
        "with_calls":  [],
        "user_only":   [("Sally Jones", "sally.jones@globex.com"), ("John Smith", "john.smith@globex.com"), ("Carol White", "carol.white@globex.com")],
    },
]

SIMULATE_BURSTS = {
    "001000000000003AAA": {
        "name":        "Initech LLC",
        "sf_id":       "001000000000003AAA",
        "owner_name":  "Nick Opderbeck",
        "owner_email": "nick.opderbeck@you.com",
        "total_7d":    1_500,
        "weekly_avg":  100.0,
        "delta":       1_400,
    },
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",     default=None,
                        help="Report date string, e.g. 'April 16, 2026' (default: today)")
    parser.add_argument("--lookback", type=int, default=None,
                        help="Days to look back (default: 3 on Monday, 1 otherwise)")
    parser.add_argument("--simulate", action="store_true",
                        help="Inject synthetic signals to verify Slack plumbing end-to-end")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print Slack message to stdout; do not post to channel")
    parser.add_argument("--tier2", action="store_true",
                        help="Scan Tier 2.A accounts instead of Tier 1. Always dry-run — never posts to Slack")
    parser.add_argument("--untiered", action="store_true",
                        help="Scan all accounts with significant usage that are NOT Tier 1 or Tier 2.A. "
                             "Always dry-run — never posts to Slack")
    parser.add_argument("--dormant", action="store_true",
                        help="Scan untiered prospects with zero usage in the last 30d but significant "
                             "all-time history. Always dry-run — never posts to Slack")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    today     = date.today()
    date_str  = args.date or today.strftime("%B %-d, %Y")

    lookback_days   = args.lookback if args.lookback is not None else 1
    lookback_cutoff = today - timedelta(days=lookback_days)

    # --dormant: untiered prospects with historical usage but gone dark in last 30d
    if args.dormant:
        print(f"Date: {date_str}  |  Mode: Dormant untiered scan  |  Floor: {MIN_DORMANT_ALLTIME:,} all-time calls")
        print("Connecting to Salesforce...")
        sf = connect_sf()
        print("Fetching dormant usage records (all tiers)...")
        usage_records = fetch_dormant_usage(sf)
        print(f"  {len(usage_records)} dormant user record(s) found")
        accounts = detect_dormant(usage_records, sf=sf)
        print(f"  {len(accounts)} dormant untiered prospect(s) above threshold (customers excluded)")
        print_dormant_report(accounts, date_str)
        return

    # --untiered: scan accounts outside Tier 1/2.A by usage volume, always dry-run
    if args.untiered:
        print(f"Date: {date_str}  |  Mode: Untiered usage scan  |  Floor: {MIN_UNTIERED_CALLS:,} calls/30d")
        print("Connecting to Salesforce...")
        sf = connect_sf()
        print("Fetching active usage records (all tiers)...")
        usage_records = fetch_untiered_usage(sf)
        print(f"  {len(usage_records)} active user record(s) found")
        accounts = detect_untiered(usage_records, sf=sf)
        print(f"  {len(accounts)} untiered prospect account(s) above threshold (customers excluded)")
        print_untiered_report(accounts, date_str)
        return

    # --tier2 is always dry-run — Tier 2 output is exploratory, never posted to Slack
    if args.tier2:
        args.dry_run = True
        tier_filter  = TIER2_FILTER
        tier_label   = "Tier 2.A"
    else:
        tier_filter  = TIER1_FILTER
        tier_label   = "Tier 1"

    print(f"Date: {date_str}  |  Tier: {tier_label}  |  Lookback: {lookback_days} day(s) (since {lookback_cutoff})")

    bot_token  = os.getenv("SLACK_BOT_TOKEN", "")
    test_email = os.getenv("TEST_OWNER_EMAIL", "")  # override owner tags for testing
    simulate   = args.simulate or os.getenv("SIMULATE_SIGNALS", "").lower() == "true"

    if test_email:
        print(f"⚠️  TEST MODE — all owner tags will resolve to: {test_email}")

    if simulate:
        print("🧪 SIMULATE MODE — using synthetic signals, skipping Salesforce")
        args.dry_run = True  # simulate never posts to the real channel
        accounts = {acc["sf_id"]: acc for acc in SIMULATE_ACCOUNTS}
        bursts   = SIMULATE_BURSTS
    else:
        print("Connecting to Salesforce...")
        sf = connect_sf()

        print(f"Fetching new Product_User__c records (since {lookback_cutoff})...")
        records = fetch_new_users(sf, lookback_cutoff, tier_filter)
        print(f"  {len(records)} new user record(s) found")

        print("Fetching usage data for burst detection...")
        usage_records = fetch_all_usage(sf, tier_filter)
        bursts = detect_bursts(usage_records, dry_run=args.dry_run)
        print(f"  {len(bursts)} burst account(s) detected")

        if not records and not bursts:
            print(f"No signals found for {tier_label} accounts.")
            return

        accounts = group_signals(records, lookback_cutoff) if records else {}
        print(f"  New users across {len(accounts)} account(s)")

    lines = build_alert_lines(accounts, bursts, bot_token, test_email or None, date_str)

    if not lines:
        print(f"No actionable signals for {tier_label} accounts.")
        return

    print(f"\nAlert preview ({tier_label}):")
    for line in lines:
        print(f"  {line}")

    if args.dry_run:
        post_to_slack(bot_token, lines, date_str, dry_run=True, tier_label=tier_label)
    elif bot_token:
        post_to_slack(bot_token, lines, date_str)
    else:
        print("⚠️  No SLACK_BOT_TOKEN — skipping Slack post", file=sys.stderr)


if __name__ == "__main__":
    main()
