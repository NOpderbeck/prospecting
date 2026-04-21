# signals_run.py — Capabilities Reference

**Last updated:** April 21, 2026

Daily signal detection for sales accounts. Detects new API users, first-time
activations, and usage bursts — posts a single Slack message to
`#sales-target-alerts`. Silent if nothing fires.

---

## Run Modes

| Flag | What it does | Posts to Slack? |
|------|-------------|-----------------|
| *(none)* | Standard daily run — Tier 1 accounts, new users + bursts | ✅ Yes |
| `--tier2` | Same signals for Tier 2.A accounts | ❌ Dry-run only |
| `--untiered` | Active usage scan for accounts outside Tier 1/2.A | ❌ Dry-run only |
| `--dormant` | Lapsed usage scan for untiered prospects gone dark | ❌ Dry-run only |
| `--dry-run` | Print Slack message to stdout, don't post | ❌ |
| `--simulate` | Inject synthetic signals to test Slack plumbing | ❌ |

---

## Signal Detection (Tier 1 + Tier 2)

### 1. New Users
- New `Product_User__c` records created since the lookback cutoff
- Uses explicit calendar-date SOQL boundary (not rolling 24h window) to prevent duplicate alerts

### 2. First API Call
- Users created recently whose `First_API_Call_Date__c` falls within the lookback window and have calls > 0

### 3. Usage Burst
Account-level spike requiring **both** conditions to fire (prevents low-volume false positives):
- **Absolute delta:** `7d calls − weekly avg ≥ 500`
- **Ratio:** `7d calls ≥ 1.75× weekly avg`

> Moving from 10 → 20 calls is noise. Moving from 100 → 1,500 is a burst.

---

## `--untiered` Scan

Surfaces accounts **outside Tier 1/2.A** with active usage this month.

**Floor:** ≥ 500 API calls/30d

**Exclusions:**
- Tier 1 and Tier 2.A accounts (already covered by main scan)
- Customers — two passes:
  - `Total_Revenue_Closed_Won__c > 0` (paid customers)
  - Any Closed Won opportunity in the last 12 months (catches $0 PAYG deals)
- `UNTIERED_BLOCKLIST` — exact case-insensitive name match

**Current blocklist:**
```python
UNTIERED_BLOCKLIST = [
    "Alumni Ventures",
    "University of Gloucestershire",
    "PrivateRelay",
    "Web",
    "Domain.com",
]
```

**Output:** Ranked table by 30d calls descending — account, tier, region, 30d calls, 7d calls, weekly avg, active users, owner

---

## `--dormant` Scan

Surfaces untiered prospects that **used to be active but have gone quiet**.

**Floor:** ≥ 1,000 all-time API calls

**Exclusions:**
- Same customer two-pass check as `--untiered`
- Partially-active accounts — any account with at least one user showing 30d activity is excluded (prevents multi-user accounts with a mix of active/dormant users from surfacing)
- `UNTIERED_BLOCKLIST`
- `DORMANT_OWNERS` — results limited to accounts owned by specific reps (empty set = all owners)
- `DORMANT_EXCLUDE_REGIONS` — suppresses specific regions from output

**Current owner filter:**
```python
DORMANT_OWNERS = {
    "Nick Opderbeck",
    "David Wacker",
    "Integration",
    "Ryan Allred",
    "Ryan Reed",
    "Andrew Miller-McKeever",
}
```

**Current region exclusions:**
```python
DORMANT_EXCLUDE_REGIONS = {
    "EMEA",
    "DACH",
}
```

**Output:** Ranked by days dark ascending (most recently lapsed first) — account, tier, region, all-time calls, last call date, days dark, users, owner

---

## Shared Filtering

| Constant | Applies to | Match type | Current entries |
|----------|-----------|------------|-----------------|
| `ALERT_BLOCKLIST` | Tier 1/2 signals + bursts | Substring | BytePlus |
| `UNTIERED_BLOCKLIST` | `--untiered` + `--dormant` | Exact | See above |
| `MONITORED_TIERS` | `--untiered` + `--dormant` | Exact set | Tier 1, 2.A |

---

## Additional Options

| Option | Default | Description |
|--------|---------|-------------|
| `--date "April 16, 2026"` | Today | Override report date string |
| `--lookback N` | 1 | Days to look back for new users |

---

## Thresholds (configurable constants)

| Constant | Value | Purpose |
|----------|-------|---------|
| `MIN_BURST_DELTA` | 500 | Minimum absolute call increase to fire burst |
| `MIN_BURST_RATIO` | 1.75 | Minimum ratio of 7d vs weekly avg to fire burst |
| `MIN_UNTIERED_CALLS` | 500 | 30d call floor for `--untiered` |
| `MIN_DORMANT_ALLTIME` | 1,000 | All-time call floor for `--dormant` |

---

## Slack Output Format

```
📊 Daily Signals — April 21, 2026  |  Tier 1

🆕 New Users / First Activations
• *Account Name* — user@example.com just activated...

⚡ Usage Bursts
• *Account Name* — usage spike: 1,500 calls this week vs 100/wk avg (+1,400). @owner
```

Owner mentions are resolved via `users.lookupByEmail` (Slack API) and cached per run.

---

## LinkedIn Matching

New user alerts attempt to find a LinkedIn profile for each user email. Two-gate confidence check:

1. **Gate 1 — Single-name email skip:** Emails without `.` or `_` in the local part (e.g. `basia@`, `ethan@`) are skipped — not enough signal to match confidently
2. **Gate 2 — Surname verification:** Profile slug must contain the user's last name to be accepted — prevents wrong-person matches while allowing nickname variants (robert/bob)

---

## Infrastructure

- **Runtime:** Cloud Run job (`signals-report`)
- **Schedule:** Daily at 8:00 AM PT via Cloud Scheduler
- **Credentials:** Secret Manager (SF creds, Slack bot token)
- **Project:** `you-sales-toolkit` (GCP), region `us-central1`
- **Slack channel:** `#sales-target-alerts` (ID: `C0AT4Q506Q2` — rename-safe)

### Deploy
```bash
gcloud builds submit --tag gcr.io/you-sales-toolkit/signals-report
gcloud run jobs update signals-report \
  --image gcr.io/you-sales-toolkit/signals-report \
  --region us-central1
```

### Run locally
```bash
# Standard dry-run
python3 signals_run.py --dry-run

# Untiered active scan
python3 signals_run.py --untiered

# Dormant prospects
python3 signals_run.py --dormant

# Tier 2.A scan
python3 signals_run.py --tier2

# Specific date + lookback
python3 signals_run.py --date "April 16, 2026" --lookback 3 --dry-run
```
