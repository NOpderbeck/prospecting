#!/usr/bin/env python3
"""
weekly_sales_summary.py — Parse the forecast dashboard snapshot(s) into a compact
weekly sales digest (the quantitative backbone for the weekly-sales-summary skill).

Reads:
  reports/forecast_data.json          (current snapshot — the dashboard's data source)
  reports/forecast_snapshot_prev.json (prior week — for WoW context; optional)

Emits a markdown digest to stdout covering:
  - Performance vs. target (per team + combined, attainment %)
  - Week-over-week deltas (closed won, pipeline, commit)
  - Closed-won deals in the window
  - Notable deal movement (stage advances / slips, sorted by $)
  - Pipeline growth (new opps + WoW pipeline change)
  - Consumption snapshot (API call volume + logos vs. target)

Field themes (#weekly-gong-insights) and per-deal Slack narrative are layered on
by the skill — this script only handles the numbers.

Usage:
  .venv/bin/python3 weekly_sales_summary.py [--days N] [--json PATH] [--top N]
"""
import argparse, json, os, sys
from datetime import date, datetime, timedelta

DIR = "/Users/nick/Prospecting"
CUR = os.path.join(DIR, "reports", "forecast_data.json")
PREV = os.path.join(DIR, "reports", "forecast_snapshot_prev.json")


def money(x):
    try:
        return f"${float(x):,.0f}"
    except (TypeError, ValueError):
        return "$0"


def pct(n, d):
    return f"{(100 * n / d):.0f}%" if d else "—"


def stage_rank(s):
    """Numeric rank for a stage string; Closed Won/Lost handled separately."""
    if not s:
        return None
    s = str(s)
    if s[0].isdigit():
        return int(s[0])
    return None


def load(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7, help="rolling lookback window (default 7)")
    ap.add_argument("--json", default=CUR, help="path to forecast_data.json")
    ap.add_argument("--top", type=int, default=12, help="max notable deal changes to list")
    ap.add_argument("--as-of", default=None, help="anchor the window end to this date (YYYY-MM-DD); default = today")
    args = ap.parse_args()

    d = load(args.json)
    if not d:
        print(f"ERROR: {args.json} not found. Run forecast_refresh.py first.", file=sys.stderr)
        sys.exit(1)

    # The window is ALWAYS the rolling past N days ending today (or --as-of), so the
    # summary looks back a consistent 7 days regardless of when the snapshot was built.
    window_end = date.fromisoformat(args.as_of) if args.as_of else date.today()
    window_start = window_end - timedelta(days=args.days)

    # The snapshot's own generation date — used only for staleness + partial-month logic.
    gen = d.get("generated_at", "")
    try:
        gen_date = datetime.fromisoformat(gen.replace("Z", "+00:00")).date()
    except Exception:
        gen_date = window_end
    # Days of the window the snapshot cannot see (it was built before window_end).
    coverage_gap = (window_end - gen_date).days

    fc = d.get("forecast", {})
    out = []
    out.append(f"# Weekly Sales Summary — {window_start.isoformat()} → {window_end.isoformat()} (past {args.days} days)")
    snap_line = f"_Snapshot: {gen_date.isoformat()}_"
    if coverage_gap > 0:
        snap_line += (f"  ·  ⚠️ **Snapshot is {coverage_gap}d behind the window** — "
                      f"closed-won & deal movement may be incomplete. Run `forecast_refresh.py` (--refresh) for the true past {args.days} days.")
    out.append(snap_line + "\n")

    # ── Performance vs. target (collective sales team only) ───────────────────
    # Report the sales team as a single unit — do NOT split by sub-team.
    t = fc.get("combined", {})
    q = t.get("quota", 0) or 0
    cw = t.get("closed_won", 0) or 0
    out.append("## Performance vs. Target (Sales Team)")
    out.append("| Quota | Closed Won | Attainment | Commit | Best Case | Open Pipe |")
    out.append("|--:|--:|--:|--:|--:|--:|")
    out.append(f"| {money(q)} | {money(cw)} | {pct(cw, q)} | "
               f"{money(t.get('commit', 0))} | {money(t.get('best_case', 0))} | {money(t.get('pipeline', 0))} |")

    # ── WoW deltas ────────────────────────────────────────────────────────────
    w = d.get("wow_delta", {})
    if w:
        def arrow(v):
            v = v or 0
            return f"▲ {money(abs(v))}" if v > 0 else (f"▼ {money(abs(v))}" if v < 0 else "flat")
        out.append(f"\n## Week-over-Week (vs. {w.get('prev_week_of', 'prior week')})")
        out.append(f"- Closed Won: **{arrow(w.get('closed_won_change'))}**")
        out.append(f"- Pipeline: **{arrow(w.get('pipeline_change'))}**")
        out.append(f"- Commit: **{arrow(w.get('commit_change'))}**")

    # ── Closed-won in window ──────────────────────────────────────────────────
    cq = fc.get("cq_deals", [])
    def in_window(ds):
        try:
            return window_start <= date.fromisoformat(ds) <= window_end
        except Exception:
            return False
    won = sorted([x for x in cq if x.get("is_won") and in_window(x.get("close_date", ""))],
                 key=lambda x: -(x.get("amount") or 0))
    out.append(f"\n## Closed-Won (last {args.days} days) — {len(won)} deal(s)")
    if won:
        for x in won:
            out.append(f"- **{money(x.get('amount'))}** — {x.get('account')} "
                       f"_(owner: {x.get('owner')}, closed {x.get('close_date')})_  ·  `{x.get('opp_id')}`")
    else:
        out.append("- _No deals closed-won in this window._")

    # ── Notable deal movement ────────────────────────────────────────────────
    dc = d.get("deal_changes", [])
    stage_moves = [x for x in dc if x.get("field") == "StageName"]
    date_moves = [x for x in dc if x.get("field") == "CloseDate"]

    def classify(start, end):
        old_r, new_r = stage_rank(start), stage_rank(end)
        end = str(end or "")
        if end.startswith("Closed Won"):
            return "🏆 WON"
        if end.startswith("Closed Lost"):
            return "❌ LOST"
        if old_r is not None and new_r is not None:
            if new_r > old_r:
                return "⬆️ advanced"
            if new_r < old_r:
                return "⬇️ regressed"
        return "↔︎ moved"

    # Collapse multiple hops on the same opp into one net move (earliest → latest).
    by_opp = {}
    for x in sorted(stage_moves, key=lambda r: r.get("changed_at", "")):
        oid = x.get("opp_id")
        if oid not in by_opp:
            by_opp[oid] = {"start": x.get("old_value"), **x}
        by_opp[oid]["end"] = x.get("current_stage") or x.get("new_value")
        by_opp[oid]["days_in_stage"] = x.get("days_in_stage", by_opp[oid].get("days_in_stage"))
    moves = sorted(by_opp.values(), key=lambda x: -(x.get("amount") or 0))
    out.append(f"\n## Notable Deal Movement — {len(moves)} opp(s) moved, {len(date_moves)} close-date change(s)")
    for x in moves[:args.top]:
        usage = ""
        if x.get("api_calls_month3"):
            usage = f" · usage ~{int(x['api_calls_month3']):,} calls/mo"
            if x.get("blended_cpm"):
                usage += f" @ {money(x['blended_cpm'])} CPM"
        out.append(f"- {classify(x.get('start'), x.get('end'))} · **{money(x.get('amount'))}** — {x.get('account')} "
                   f"({x.get('start')} → {x.get('end')}) _{x.get('owner')}_, "
                   f"{x.get('days_in_stage', '?')}d in stage{usage}  ·  `{x.get('opp_id')}`")
    # Close-date slips worth flagging (pushed out)
    slips = [x for x in date_moves if str(x.get("new_value", "")) > str(x.get("old_value", ""))]
    if slips:
        slips.sort(key=lambda x: -(x.get("amount") or 0))
        out.append(f"\n**Close-date pushes ({len(slips)}):**")
        for x in slips[:6]:
            out.append(f"- ⏳ **{money(x.get('amount'))}** — {x.get('account')} "
                       f"({x.get('old_value')} → {x.get('new_value')}) _{x.get('owner')}_  ·  `{x.get('opp_id')}`")

    # ── Pipeline growth ──────────────────────────────────────────────────────
    ph = d.get("pipeline_health", {})
    act = d.get("activity", {})
    out.append("\n## Pipeline Growth")
    created = {r: v.get("opps_created_cq", 0) for r, v in ph.get("by_rep", {}).items()}
    total_created = sum(created.values())
    # New opps this week = last column of opps_by_rep_by_week
    new_this_week = 0
    for r, series in act.get("opps_by_rep_by_week", {}).items():
        if series:
            new_this_week += series[-1]
    out.append(f"- New opps created this quarter: **{total_created}**")
    out.append(f"- New opps created this week: **{new_this_week}**")
    if w.get("pipeline_change") is not None:
        out.append(f"- Net open-pipeline change WoW: **{money(w.get('pipeline_change'))}**")

    # ── Consumption snapshot ─────────────────────────────────────────────────
    v = d.get("volume_data", {})
    if v:
        out.append("\n## Consumption Snapshot (API call volume)")
        monthly = v.get("monthly_excl_top2", {})
        cur_month = gen_date.strftime("%Y-%m")
        # Latest month that is (a) not in the future and (b) has real volume.
        past_nonzero = [m for m in sorted(monthly) if m < cur_month and (monthly.get(m) or 0) > 0]
        if past_nonzero:
            m = past_nonzero[-1]
            out.append(f"- Latest full month (excl. top 2): **{int(monthly[m]):,} calls** ({m})")
        else:
            out.append("- Latest month volume: _unavailable this refresh (volume sheet returned no data)_")
        if v.get("q2_excl_top2") and v.get("q1_excl_top2"):
            q1, q2 = v["q1_excl_top2"], v["q2_excl_top2"]
            out.append(f"- QoQ volume (excl. top 2): {int(q1):,} → {int(q2):,} calls (**{pct(q2 - q1, q1)} growth**)")
        tm = v.get("targets_m", {})
        if tm:
            out.append(f"- Monthly-call targets (M): Q2 {tm.get('q2')} · Q3 {tm.get('q3')} · Q4 {tm.get('q4')}")
        lt = v.get("logo_targets", {})
        if lt:
            parts = [f"{k.upper()} {x.get('total')} total / {x.get('net_new')} net-new" for k, x in lt.items()]
            out.append(f"- Logo targets: " + " · ".join(parts))

    # ── PayGo land motion ────────────────────────────────────────────────────
    pg = d.get("paygo", {})
    if pg:
        c = pg.get("combined", {})
        out.append("\n## PayGo Land Motion")
        out.append(f"- Logos landed: **{c.get('count', 0)} / {c.get('target', 0)} goal**")

    print("\n".join(out))


if __name__ == "__main__":
    main()
