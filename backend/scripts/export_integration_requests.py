#!/usr/bin/env python3
"""
Export Integration Requests for Manual Exploration

Exports all integration_requests messages to CSV and readable text format
for creative opportunity discovery.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import sqlite3
import csv
from datetime import datetime
from collections import Counter

# Load environment
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")


def export_integration_requests():
    """Export all integration requests to CSV and text"""

    db_path = Path(__file__).parent.parent / "data" / "discredit.db"
    output_dir = Path(__file__).parent.parent / "reports"
    output_dir.mkdir(exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all integration requests with user info
    print("\n🔍 Fetching integration requests from database...")
    cursor.execute("""
        SELECT
            m.content,
            m.platform,
            m.timestamp,
            m.source,
            u.username,
            u.message_count
        FROM messages m
        JOIN message_taxonomy mt ON m.id = mt.message_id
        JOIN users u ON m.author_id = u.id
        WHERE mt.category = 'integration_requests'
        ORDER BY m.timestamp DESC
    """)

    rows = cursor.fetchall()
    print(f"✅ Found {len(rows)} integration request messages\n")

    # Export to CSV
    csv_path = output_dir / "integration_requests.csv"
    print(f"📊 Exporting to CSV: {csv_path}")
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['content', 'platform', 'date', 'source', 'username', 'user_msg_count'])

        for row in rows:
            content, platform, timestamp, source, username, msg_count = row
            date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')
            writer.writerow([content, platform, date, source, username, msg_count])

    print(f"✅ CSV exported: {csv_path}\n")

    # Export to readable text format
    txt_path = output_dir / "integration_requests_readable.txt"
    print(f"📝 Exporting to readable text: {txt_path}")

    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("INTEGRATION REQUESTS - MANUAL EXPLORATION\n")
        f.write("="*80 + "\n")
        f.write(f"\nTotal Messages: {len(rows)}\n")
        f.write(f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Platform breakdown
        platforms = Counter(row[1] for row in rows)
        f.write(f"\nPlatform Breakdown:\n")
        for platform, count in platforms.items():
            f.write(f"  • {platform}: {count} messages ({count*100/len(rows):.1f}%)\n")

        f.write("\n" + "="*80 + "\n")
        f.write("MESSAGES (Most Recent First)\n")
        f.write("="*80 + "\n\n")

        for i, row in enumerate(rows, 1):
            content, platform, timestamp, source, username, msg_count = row
            date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')

            f.write(f"[{i}/{len(rows)}] {platform.upper()} | {date} | {source}\n")
            f.write(f"User: {username} ({msg_count} total messages)\n")
            f.write(f"─" * 80 + "\n")
            f.write(f"{content}\n")
            f.write("\n" + "="*80 + "\n\n")

    print(f"✅ Text exported: {txt_path}\n")

    # Print sample to console
    print("\n" + "="*80)
    print("📋 SAMPLE INTEGRATION REQUESTS (First 20)")
    print("="*80 + "\n")

    for i, row in enumerate(rows[:20], 1):
        content, platform, timestamp, source, username, msg_count = row
        date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

        print(f"\n[{i}] {platform.upper()} | {date} | @{username}")
        print("─" * 80)
        print(content[:300] + ("..." if len(content) > 300 else ""))
        print()

    print("\n" + "="*80)
    print(f"💡 NEXT STEPS:")
    print(f"   1. Read through: {txt_path}")
    print(f"   2. Open in spreadsheet: {csv_path}")
    print(f"   3. Look for patterns, pain points, and creative opportunities")
    print(f"   4. Note what sparks ideas - not just exact tool names!")
    print("="*80 + "\n")

    conn.close()


if __name__ == "__main__":
    export_integration_requests()
