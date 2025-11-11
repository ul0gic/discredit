"""
View Taxonomy Classification Results

Displays statistics and breakdowns of taxonomy classifications.
"""

import sys
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.sqlite_db import DiscreditDB
from analysis.taxonomy import MARKET_TAXONOMY
from config import Config


def main():
    db = DiscreditDB(str(Config.SQLITE_DB_PATH))

    print("🎯 DISCREDIT TAXONOMY RESULTS")
    print("=" * 80)
    print()

    # Get taxonomy runs
    cursor = db.conn.cursor()
    cursor.execute("""
        SELECT id, model, taxonomy_version, n_messages, total_batches,
               processing_time_seconds, created_at
        FROM taxonomy_runs
        ORDER BY created_at DESC
    """)

    runs = cursor.fetchall()

    if not runs:
        print("❌ No taxonomy runs found. Run the classifier first:")
        print("   poetry run python -m analysis.taxonomy_classifier")
        return

    print(f"📊 Found {len(runs)} taxonomy run(s)\n")

    # Show latest run details
    latest = runs[0]
    run_id = latest[0]

    print(f"Latest Run (#{run_id}):")
    print(f"  Model:            {latest[1]}")
    print(f"  Taxonomy Version: {latest[2]}")
    print(f"  Messages:         {latest[3]:,}")
    print(f"  Batches:          {latest[4]}")
    print(f"  Processing Time:  {latest[5]:.1f}s")
    print()

    # Get category breakdown
    cursor.execute("""
        SELECT category, COUNT(*) as count
        FROM message_taxonomy
        WHERE taxonomy_run_id = ?
        GROUP BY category
        ORDER BY count DESC
    """, (run_id,))

    categories = cursor.fetchall()

    print("📈 Category Breakdown:")
    print("-" * 80)

    # Show all categories
    for category, count in categories:
        pct = (count / latest[3]) * 100
        print(f"  {category:30s} {count:6,} messages ({pct:5.1f}%)")

    # Get top messages per category
    print("\n" + "=" * 80)
    print("📝 Sample Messages (Top 5 Categories):")
    print("=" * 80)

    # Show top 5 categories
    for category, count in categories[:5]:
        cursor.execute("""
            SELECT m.content, m.platform, m.source
            FROM message_taxonomy mt
            JOIN messages m ON mt.message_id = m.id
            WHERE mt.category = ? AND mt.taxonomy_run_id = ?
            LIMIT 3
        """, (category, run_id))

        samples = cursor.fetchall()

        print(f"\n{category} ({count} messages):")
        for content, platform, source in samples:
            preview = content[:100].replace('\n', ' ')
            print(f"  [{platform}] {preview}...")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
