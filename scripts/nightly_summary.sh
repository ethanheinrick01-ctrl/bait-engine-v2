#!/usr/bin/env bash
set -euo pipefail

cd /Users/ethanheinrick/.openclaw/workspace/bait-engine-v2
export PYTHONPATH=src

PY_BIN="/opt/homebrew/opt/python@3.14/bin/python3.14"
DATE_TAG="$(date +%F)"
NOW_HUMAN="$(date '+%Y-%m-%d %H:%M:%S %Z')"

BASE_DIR="$HOME/Desktop/BAITED_RETARDS"
DAILY_DIR="$BASE_DIR/Daily Logs"
DAY_DIR="$DAILY_DIR/$DATE_TAG"
mkdir -p "$BASE_DIR" "$DAILY_DIR" "$DAY_DIR"

CSV_OUT="$BASE_DIR/BAIT_LOG.csv"
SUMMARY_MD="$BASE_DIR/DAILY_SUMMARY.md"
DETAIL_MD="$DAY_DIR/NIGHTLY_REPORT_${DATE_TAG}.md"
DETAIL_HTML="$DAY_DIR/NIGHTLY_REPORT_${DATE_TAG}.html"
DETAIL_PDF="$DAY_DIR/NIGHTLY_REPORT_${DATE_TAG}.pdf"
CSV_SNAPSHOT="$DAY_DIR/BAIT_LOG_${DATE_TAG}.csv"
SUMMARY_SNAPSHOT="$DAY_DIR/DAILY_SUMMARY_${DATE_TAG}.md"

"$PY_BIN" -m bait_engine.cli.main report-markdown --since-hours 24 --out "$SUMMARY_MD"

SUMMARY_TEXT=$(python3 - <<'PY'
from pathlib import Path
p = Path.home()/"Desktop/BAITED_RETARDS/DAILY_SUMMARY.md"
text = p.read_text(encoding="utf-8") if p.exists() else "No summary generated."
lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
msg = "\n".join(lines[:25])
if len(msg) > 3500:
    msg = msg[:3500] + "\n..."
print(msg)
PY
)

NOW_HUMAN="$NOW_HUMAN" DETAIL_MD="$DETAIL_MD" CSV_OUT="$CSV_OUT" "$PY_BIN" - <<'PY'
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from pathlib import Path
import csv
import os
import re
import sqlite3

from bait_engine.storage import RunRepository

repo = RunRepository('.data/bait-engine.db')
outbox = repo.list_emit_outbox(limit=2000)
now = datetime.now(timezone.utc)
window_start = now - timedelta(hours=24)


def parse_dt(s: str | None):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except Exception:
        return None

# Build detailed row objects with run + envelope context
rows: list[dict] = []
for item in outbox:
    run_id = item.get('run_id')
    if not run_id:
        continue
    run = repo.get_run(int(run_id))
    analysis = run.get('analysis') or {}
    arche = analysis.get('archetype_blend') or {}

    env = item.get('envelope') or {}
    target = env.get('target') or {}
    metadata = env.get('metadata') or {}
    thread_ctx = metadata.get('thread_context') or {}

    platform = (item.get('platform') or run.get('platform') or 'unknown').strip()
    thread_id = target.get('thread_id') or target.get('reply_to_id') or ''
    permalink = target.get('permalink') or thread_ctx.get('url') or thread_ctx.get('permalink')
    if not permalink and platform.lower() == 'reddit' and thread_id.startswith('t3_'):
        permalink = f"https://reddit.com/comments/{thread_id[3:]}"

    author = (
        target.get('author_handle')
        or thread_ctx.get('root_author_handle')
        or 'unknown'
    )

    outcome = run.get('outcome') or {}
    outcome_label = outcome.get('result_label') or ''
    got_reply = bool(outcome.get('got_reply'))

    rows.append(
        {
            'emit_id': item.get('id'),
            'run_id': run_id,
            'created_at': item.get('created_at') or '',
            'created_dt': parse_dt(item.get('created_at')),
            'platform': platform,
            'username': author,
            'subject': thread_ctx.get('subject') or '(no title captured)',
            'link': permalink or '(no link captured)',
            'persona': run.get('persona') or '(unknown)',
            'objective': run.get('selected_objective') or '(unknown)',
            'tactic': run.get('selected_tactic') or '(unknown)',
            'archetype_blend': arche,
            'response': env.get('body') or '(no generated body recorded)',
            'status': item.get('status') or 'unknown',
            'outcome_label': outcome_label,
            'got_reply': got_reply,
        }
    )

# Include outcome-only historical runs that may not have emit_outbox rows
existing_run_ids = {int(r['run_id']) for r in rows if r.get('run_id') is not None}
conn = sqlite3.connect('.data/bait-engine.db')
conn.row_factory = sqlite3.Row
for o in conn.execute("SELECT run_id, got_reply, result_label, notes FROM outcomes"):
    run_id = int(o['run_id'])
    if run_id in existing_run_ids:
        continue
    run = repo.get_run(run_id)
    notes = o['notes'] or ''
    m = re.search(r"Target\s+([A-Za-z0-9_\-]+)", notes)
    username = m.group(1) if m else f"run_{run_id}_target"
    analysis = run.get('analysis') or {}
    arche = analysis.get('archetype_blend') or {}
    outcome_label = o['result_label'] or ''
    rows.append(
        {
            'emit_id': None,
            'run_id': run_id,
            'created_at': run.get('created_at') or '',
            'created_dt': parse_dt(run.get('created_at')),
            'platform': run.get('platform') or 'unknown',
            'username': username,
            'subject': (run.get('source_text') or '').split('\n', 1)[0][:140] or '(no title captured)',
            'link': '(no link captured)',
            'persona': run.get('persona') or '(unknown)',
            'objective': run.get('selected_objective') or '(unknown)',
            'tactic': run.get('selected_tactic') or '(unknown)',
            'archetype_blend': arche,
            'response': ((run.get('candidates') or [{}])[0].get('text') if run.get('candidates') else '(no generated body recorded)'),
            'status': 'historical',
            'outcome_label': outcome_label,
            'got_reply': bool(o['got_reply']),
        }
    )
conn.close()

# 1) Build BAIT_LOG.csv in the style requested (username-centric table)
by_user: dict[str, list[dict]] = {}
for r in rows:
    by_user.setdefault(r['username'], []).append(r)

csv_rows = []
for username, items in by_user.items():
    items_sorted = sorted(items, key=lambda x: x['created_at'])
    first = items_sorted[0]
    last = items_sorted[-1]
    blend = last.get('archetype_blend') or {}
    top = sorted(blend.items(), key=lambda kv: kv[1], reverse=True)
    primary = top[0][0] if top else 'unknown'
    blend_text = '; '.join([f"{k}: {v:.1%}" for k, v in top[:6]]) if top else 'unknown'

    if last.get('outcome_label') in {'no_bite', 'mod_removed_high_engagement'}:
        status = 'ARCHIVED'
    else:
        status = 'ACTIVE'

    csv_rows.append(
        {
            'username': username,
            'platform': last.get('platform'),
            'first_contact_date': first.get('created_at', '')[:10],
            'last_bait_date': last.get('created_at', '')[:10],
            'status': status,
            'primary_archetype': primary,
            'archetype_blend': blend_text,
            'latest_post_link': last.get('link'),
            'latest_persona': last.get('persona'),
            'latest_response': last.get('response'),
            'latest_outcome': last.get('outcome_label') or last.get('status') or 'unknown',
        }
    )

csv_rows.sort(key=lambda r: (r['status'] != 'ACTIVE', r['last_bait_date']), reverse=True)

csv_path = Path(os.environ['CSV_OUT'])
csv_path.parent.mkdir(parents=True, exist_ok=True)
with csv_path.open('w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            'username', 'platform', 'first_contact_date', 'last_bait_date', 'status',
            'primary_archetype', 'archetype_blend', 'latest_post_link',
            'latest_persona', 'latest_response', 'latest_outcome'
        ],
    )
    writer.writeheader()
    writer.writerows(csv_rows)

# 2) Build clean nightly PDF report (last 24h details by platform)
window_rows = [r for r in rows if r['created_dt'] and r['created_dt'] >= window_start]
window_rows.sort(key=lambda r: (r['platform'].lower(), r['created_at']))

out = []
out.append('# Nightly Bait Activity Report')
out.append('')
out.append(f"Generated: {os.environ.get('NOW_HUMAN', '')}")
out.append('Window: Last 24 hours')
out.append('')
out.append('Clean human view: platform sections, numbered entries, links, persona, archetype breakdown, response, and outcome.')
out.append('')

if not window_rows:
    out.append('## No posts were processed in the last 24 hours.')
else:
    platforms = {}
    for r in window_rows:
        platforms.setdefault(r['platform'], []).append(r)

    for platform, items in sorted(platforms.items(), key=lambda kv: kv[0].lower()):
        out.append(f"## Platform: {platform}")
        out.append('')
        for i, r in enumerate(items, 1):
            top = sorted((r.get('archetype_blend') or {}).items(), key=lambda kv: kv[1], reverse=True)
            arche_txt = ', '.join([f"{k} ({v:.0%})" for k, v in top[:3]]) if top else 'N/A'
            out.append(f"### {i}) {r['subject']}")
            out.append(f"- **Link:** {r['link']}")
            out.append(f"- **Username:** {r['username']}")
            out.append(f"- **Persona:** {r['persona']}")
            out.append(f"- **Objective/Tactic:** {r['objective']} / {r['tactic']}")
            out.append(f"- **Archetype Breakdown (Top):** {arche_txt}")
            out.append(f"- **Generated Response:** \"{r['response']}\"")
            out.append(f"- **Outcome So Far:** {r['outcome_label'] or r['status']}")
            out.append('')
        out.append('---')
        out.append('')

out.append('## Executive Summary (Telegram Mirror)')
out.append('')
out.append('See the Telegram message sent at 10 PM for concise tactical summary.')
out.append('')

Path(os.environ['DETAIL_MD']).write_text('\n'.join(out), encoding='utf-8')
PY

pandoc "$DETAIL_MD" -s -o "$DETAIL_HTML"
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless --disable-gpu --no-pdf-header-footer \
  --print-to-pdf="$DETAIL_PDF" "file://$DETAIL_HTML" >/dev/null 2>&1 || true

# Keep dated snapshots inside the per-day folder for organization.
cp "$CSV_OUT" "$CSV_SNAPSHOT"
cp "$SUMMARY_MD" "$SUMMARY_SNAPSHOT"

# Telegram push disabled by user request. Report artifacts are still generated locally.

echo "Nightly report written: $DETAIL_PDF"
