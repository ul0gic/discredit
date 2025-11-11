"""
Taxonomy Classification Module - Discredit Phase 3.5

This module classifies messages into market intelligence taxonomy categories using GPT-5.
It processes messages in batches and forces classification into predefined categories for
consistent market analysis.

Pipeline:
---------
1. Load messages from SQLite
2. Batch messages (default 100 per batch)
3. Send to GPT-5 with taxonomy prompt
4. Parse JSON responses with category assignments
5. Store classifications in SQLite
6. Track progress and create run metadata

Usage:
------
# Basic usage (all messages)
python -m analysis.taxonomy_classifier

# Custom batch size
python -m analysis.taxonomy_classifier --batch-size 50

# Process specific number of messages (for testing)
python -m analysis.taxonomy_classifier --limit 1000

# Dry run (show stats without processing)
python -m analysis.taxonomy_classifier --dry-run
"""

import sys
import json
import time
import argparse
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from openai import OpenAI, AsyncOpenAI

# Add backend to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.sqlite_db import DiscreditDB
from analysis.taxonomy import get_taxonomy_prompt, get_flat_categories, MARKET_TAXONOMY
from config import Config


class TaxonomyClassifier:
    """
    Handles taxonomy classification pipeline for Discredit messages.

    Batches messages and uses GPT-5 to classify them into predefined
    market intelligence categories.
    """

    TAXONOMY_VERSION = "v1.0"
    DEFAULT_MODEL = "gpt-5"

    def __init__(
        self,
        sqlite_path: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        batch_size: int = 100,
        model: str = DEFAULT_MODEL,
        max_concurrent_batches: int = 10
    ):
        """
        Initialize taxonomy classifier.

        Args:
            sqlite_path: Path to SQLite database
            openai_api_key: OpenAI API key
            batch_size: Number of messages per batch
            model: OpenAI model to use
            max_concurrent_batches: Max batches to process in parallel
        """
        self.sqlite_path = sqlite_path or str(Config.SQLITE_DB_PATH)
        self.openai_api_key = openai_api_key or Config.OPENAI_API_KEY
        self.batch_size = batch_size
        self.model = model
        self.max_concurrent_batches = max_concurrent_batches

        self.db = DiscreditDB(self.sqlite_path)
        self.client = OpenAI(api_key=self.openai_api_key)
        self.async_client = AsyncOpenAI(api_key=self.openai_api_key)

        # Stats tracking
        self.stats = {
            'total_messages': 0,
            'processed_messages': 0,
            'failed_messages': 0,
            'total_classifications': 0,
            'start_time': None,
            'end_time': None
        }

        # Thread-safe lock for stats updates
        self.stats_lock = asyncio.Lock()

    def get_unclassified_messages(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get messages that haven't been classified yet.

        Args:
            limit: Max number of messages to return

        Returns:
            List of message dictionaries
        """
        query = """
            SELECT m.id, m.content, m.platform, m.source, m.timestamp
            FROM messages m
            LEFT JOIN message_taxonomy mt ON m.id = mt.message_id
            WHERE mt.id IS NULL
            AND LENGTH(m.content) >= 10
            ORDER BY m.timestamp DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor = self.db.conn.cursor()
        cursor.execute(query)

        messages = []
        for row in cursor.fetchall():
            messages.append({
                'id': row[0],
                'content': row[1],
                'platform': row[2],
                'source': row[3],
                'timestamp': row[4]
            })

        return messages

    async def classify_batch_async(
        self,
        batch_num: int,
        total_batches: int,
        messages: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """
        Classify a batch of messages using GPT-5 (async).

        Args:
            batch_num: Current batch number
            total_batches: Total number of batches
            messages: List of message dicts with 'id' and 'content'

        Returns:
            Dictionary mapping message_id to single category
        """
        # Build messages payload
        messages_text = ""
        for msg in messages:
            # Escape and truncate content for token efficiency
            content = msg['content'][:500].replace('"', '\\"').replace('\n', ' ')
            messages_text += f'"{msg["id"]}": "{content}"\n'

        # Get taxonomy prompt
        system_prompt = get_taxonomy_prompt()
        user_prompt = f"""Classify these messages:

{{
{messages_text}
}}"""

        try:
            # Call GPT-5 using Responses API (async)
            response = await self.async_client.responses.create(
                model=self.model,
                input=f"{system_prompt}\n\n{user_prompt}",
                reasoning={"effort": "minimal"},  # Minimal reasoning for fast classification
                text={"verbosity": "low"}         # Concise JSON output
            )

            # Parse response
            result_text = response.output_text
            classifications = json.loads(result_text)

            return classifications

        except Exception as e:
            print(f"   ❌ Error classifying batch {batch_num}/{total_batches}: {e}")
            return {}

    def save_classifications(
        self,
        taxonomy_run_id: int,
        classifications: Dict[str, str]
    ):
        """
        Save classifications to database.

        Args:
            taxonomy_run_id: ID of the current taxonomy run
            classifications: Dict mapping message_id to single category
        """
        cursor = self.db.conn.cursor()
        timestamp = int(time.time())

        for message_id, category in classifications.items():
            cursor.execute("""
                INSERT INTO message_taxonomy
                (taxonomy_run_id, message_id, category, created_at)
                VALUES (?, ?, ?, ?)
            """, (taxonomy_run_id, message_id, category, timestamp))

        self.db.conn.commit()

    async def process_batch_with_progress(
        self,
        batch_num: int,
        total_batches: int,
        batch: List[Dict[str, Any]],
        taxonomy_run_id: int,
        total_messages: int,
        semaphore: asyncio.Semaphore
    ):
        """
        Process a single batch with concurrency control and progress tracking.
        """
        async with semaphore:
            batch_start = time.time()

            # Classify batch
            classifications = await self.classify_batch_async(batch_num, total_batches, batch)

            batch_duration = time.time() - batch_start

            if classifications:
                # Save to database (synchronous, but fast)
                self.save_classifications(taxonomy_run_id, classifications)

                # Update stats (thread-safe)
                async with self.stats_lock:
                    self.stats['processed_messages'] += len(classifications)
                    self.stats['total_classifications'] += len(classifications)

                    # Calculate rate
                    rate = len(classifications) / batch_duration if batch_duration > 0 else 0
                    progress_pct = (self.stats['processed_messages'] / total_messages) * 100

                    print(f"📦 Batch {batch_num}/{total_batches} complete: {len(classifications)} messages in {batch_duration:.1f}s ({rate:.1f} msg/sec)")
                    print(f"   📊 Total progress: {self.stats['processed_messages']}/{total_messages} messages ({progress_pct:.1f}%)")
            else:
                async with self.stats_lock:
                    self.stats['failed_messages'] += len(batch)

    async def run_async(
        self,
        messages: List[Dict[str, Any]],
        taxonomy_run_id: int
    ):
        """
        Run classification asynchronously with parallel batch processing.
        """
        total_batches = (len(messages) + self.batch_size - 1) // self.batch_size

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent_batches)

        print(f"🔬 Starting parallel taxonomy classification (Run #{taxonomy_run_id})...")
        print(f"   Max concurrent batches: {self.max_concurrent_batches}")
        print()

        # Create tasks for all batches
        tasks = []
        for i in range(0, len(messages), self.batch_size):
            batch_num = i // self.batch_size + 1
            batch = messages[i:i + self.batch_size]

            task = self.process_batch_with_progress(
                batch_num,
                total_batches,
                batch,
                taxonomy_run_id,
                len(messages),
                semaphore
            )
            tasks.append(task)

        # Process all batches in parallel (with concurrency limit)
        await asyncio.gather(*tasks)

    def run(
        self,
        limit: Optional[int] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Run taxonomy classification pipeline.

        Args:
            limit: Max messages to process (None for all)
            dry_run: If True, only show stats without processing

        Returns:
            Statistics dictionary
        """
        self.stats['start_time'] = time.time()

        print("🎯 DISCREDIT TAXONOMY CLASSIFICATION")
        print("=" * 80)
        print(f"SQLite DB:       {self.sqlite_path}")
        print(f"Model:           {self.model}")
        print(f"Batch size:      {self.batch_size}")
        print(f"Concurrency:     {self.max_concurrent_batches} parallel batches")
        print(f"Taxonomy:        {self.TAXONOMY_VERSION}")
        print("=" * 80)
        print()

        # Get unclassified messages
        print("📥 Loading unclassified messages...")
        messages = self.get_unclassified_messages(limit=limit)
        self.stats['total_messages'] = len(messages)
        print(f"   Found {len(messages):,} unclassified messages")
        print()

        if len(messages) == 0:
            print("✅ No messages to classify!")
            return self.stats

        if dry_run:
            print(f"🔍 DRY RUN: Would process {len(messages):,} messages in {len(messages) // self.batch_size + 1} batches")
            return self.stats

        # Create taxonomy run
        cursor = self.db.conn.cursor()
        cursor.execute("""
            INSERT INTO taxonomy_runs
            (model, taxonomy_version, n_messages, batch_size, total_batches, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            self.model,
            self.TAXONOMY_VERSION,
            len(messages),
            self.batch_size,
            len(messages) // self.batch_size + 1,
            int(time.time())
        ))
        taxonomy_run_id = cursor.lastrowid
        self.db.conn.commit()

        # Run async classification
        asyncio.run(self.run_async(messages, taxonomy_run_id))

        self.stats['end_time'] = time.time()

        # Update run metadata
        processing_time = self.stats['end_time'] - self.stats['start_time']
        cursor.execute("""
            UPDATE taxonomy_runs
            SET processing_time_seconds = ?
            WHERE id = ?
        """, (processing_time, taxonomy_run_id))
        self.db.conn.commit()

        # Print summary
        print()
        print("=" * 80)
        print("📊 TAXONOMY CLASSIFICATION SUMMARY")
        print("=" * 80)
        print(f"Total messages:            {self.stats['total_messages']:,}")
        print(f"Successfully classified:    {self.stats['processed_messages']:,}")
        print(f"Failed:                    {self.stats['failed_messages']:,}")
        print(f"Total classifications:     {self.stats['total_classifications']:,}")
        print(f"Duration:                  {processing_time:.1f}s ({processing_time/60:.1f} min)")
        print(f"Rate:                      {self.stats['processed_messages']/processing_time:.1f} messages/sec")
        print()
        print(f"✅ Taxonomy run #{taxonomy_run_id} complete!")
        print("=" * 80)

        return self.stats


def main():
    """CLI entry point for taxonomy classification."""
    parser = argparse.ArgumentParser(
        description="Classify Discredit messages into market intelligence taxonomy"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of messages per batch (default: 100)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of messages to process (for testing)"
    )
    parser.add_argument(
        "--model",
        type=str,
        default=TaxonomyClassifier.DEFAULT_MODEL,
        help=f"OpenAI model to use (default: {TaxonomyClassifier.DEFAULT_MODEL})"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show statistics without processing"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Max concurrent batches to process in parallel (default: 10)"
    )

    args = parser.parse_args()

    # Validate OpenAI credentials
    try:
        Config.validate_openai_credentials()
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)

    # Run classification
    classifier = TaxonomyClassifier(
        batch_size=args.batch_size,
        model=args.model,
        max_concurrent_batches=args.concurrency
    )

    classifier.run(
        limit=args.limit,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()
