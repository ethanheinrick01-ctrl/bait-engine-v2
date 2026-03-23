#!/usr/bin/env python3
import sqlite3
import csv
import os
from datetime import datetime, timedelta

# Configuration
DB_PATH = os.path.expanduser("~/bait-engine-v2/.data/bait-engine.db")
LOG_CSV = os.path.expanduser("~/Desktop/BAITED_RETARDS/BAIT_LOG.csv")
REPORT_MD = os.path.expanduser("~/Desktop/BAITED_RETARDS/DAILY_REPORT.md")

def sync_verified_bites():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # We ONLY pull successful 'bite' outcomes from the last 24 hours
    # This prevents 'dispatched but not engaged' false positives
    query = """
    SELECT 
        o.created_at,
        r.platform,
        r.persona,
        r.selected_tactic,
        o.engagement_score,
        r.source_text
    FROM outcomes o
    JOIN runs r ON o.run_id = r.id
    WHERE o.verdict = 'bite'
    AND o.created_at >= datetime('now', '-24 hours')
    ORDER BY o.created_at DESC
    """
    
    try:
        cursor.execute(query)
        new_bites = cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(f"Database error: {e}")
        return
    finally:
        conn.close()

    if not new_bites:
        print("No new verified bites found in the last 24 hours.")
        return

    # Update CSV (Append only)
    os.makedirs(os.path.dirname(LOG_CSV), exist_ok=True)
    file_exists = os.path.isfile(LOG_CSV)
    
    with open(LOG_CSV, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Timestamp", "Platform", "Persona", "Tactic", "Score", "Context"])
        
        for bite in new_bites:
            writer.writerow(bite)

    # Generate Markdown Summary
    with open(REPORT_MD, mode='w', encoding='utf-8') as f:
        f.write(f"# Daily Verified Bait Report\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"### New Verified Bites (Last 24h): {len(new_bites)}\n\n")
        f.write("| Time | Persona | Tactic | Score | Context |\n")
        f.write("|------|---------|--------|-------|---------|\n")
        for b in new_bites:
            # Truncate context for readability
            ctx = (b[5][:75] + '..') if len(b[5]) > 75 else b[5]
            f.write(f"| {b[0]} | {b[2]} | {b[3]} | {b[4]} | {ctx} |\n")

    print(f"Success: {len(new_bites)} verified bites synced to {LOG_CSV}")

if __name__ == "__main__":
    sync_verified_bites()
