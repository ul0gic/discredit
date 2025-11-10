"""
Embedding Generation Module - Discredit Phase 3

This module handles the generation of OpenAI embeddings for all collected messages,
storing them in ChromaDB for semantic search and creating reference links in SQLite.

Pipeline:
---------
1. Query messages from SQLite that haven't been embedded yet
2. Filter messages (min 20 chars, exclude bots, system messages)
3. Batch process with OpenAI text-embedding-3-small model
4. Store embeddings in ChromaDB with metadata
5. Create reference links in SQLite embeddings_reference table
6. Track progress and costs

Usage:
------
# Basic usage
python -m analysis.embedder

# With custom batch size
python -m analysis.embedder --batch-size 500

# Dry run to estimate costs only
python -m analysis.embedder --dry-run
"""

import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import argparse

# Add backend to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.sqlite_db import DiscreditDB
from storage.vector_store import VectorStore
from config import Config


class MessageEmbedder:
    """
    Handles embedding generation pipeline for Discredit messages.

    Filters, batches, embeds, and stores messages with progress tracking
    and cost estimation.
    """

    def __init__(
        self,
        sqlite_path: Optional[str] = None,
        chromadb_path: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        batch_size: int = 100,
        min_content_length: int = 20
    ):
        """
        Initialize embedder with database connections.

        Args:
            sqlite_path: Path to SQLite database (defaults to config)
            chromadb_path: Path to ChromaDB storage (defaults to config)
            openai_api_key: OpenAI API key (defaults to config)
            batch_size: Number of messages per embedding batch
            min_content_length: Minimum message length to embed
        """
        # Initialize database connections
        self.sqlite_path = sqlite_path or str(Config.SQLITE_DB_PATH)
        self.chromadb_path = chromadb_path or str(Config.CHROMADB_PATH)
        self.openai_api_key = openai_api_key or Config.OPENAI_API_KEY

        self.batch_size = batch_size
        self.min_content_length = min_content_length

        self.db = DiscreditDB(self.sqlite_path)
        self.vector_store = VectorStore(self.chromadb_path, self.openai_api_key)

        # Stats tracking
        self.stats = {
            'total_messages': 0,
            'filtered_messages': 0,
            'already_embedded': 0,
            'embedded_successfully': 0,
            'embedding_errors': 0,
            'total_cost_usd': 0.0,
            'start_time': None,
            'end_time': None
        }

    def filter_messages(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter messages for embedding quality.

        Filters out:
        - Messages shorter than min_content_length
        - Bot messages (based on metadata)
        - System/automated messages
        - Empty content

        Args:
            messages: List of message dictionaries from SQLite

        Returns:
            Filtered list of messages suitable for embedding
        """
        filtered = []

        for msg in messages:
            content = msg.get('content', '').strip()
            metadata = msg.get('metadata', {}) or {}

            # Filter: minimum length
            if len(content) < self.min_content_length:
                continue

            # Filter: empty content
            if not content:
                continue

            # Filter: Discord bots
            if msg['platform'] == 'discord':
                if metadata.get('author', {}).get('bot', False):
                    continue
                # Filter system messages (message_type != 0 is system message)
                if metadata.get('type', 0) != 0:
                    continue

            # Filter: Reddit bots and automoderator
            if msg['platform'] == 'reddit':
                author_name = metadata.get('author', '').lower()
                if 'bot' in author_name or author_name == 'automoderator':
                    continue
                # Filter removed/deleted content
                if content.lower() in ['[removed]', '[deleted]']:
                    continue

            filtered.append(msg)

        return filtered

    def get_messages_to_embed(self) -> List[Dict[str, Any]]:
        """
        Query SQLite for messages that need embedding.

        Returns:
            List of message dictionaries ready for embedding
        """
        print("📥 Querying messages from SQLite...")

        # Get messages without embeddings (min_length filter applied in query)
        messages = self.db.get_messages_without_embeddings(
            min_length=self.min_content_length
        )

        self.stats['total_messages'] = len(messages)
        print(f"   Found {len(messages):,} messages without embeddings")

        # Apply additional filters
        print("🔍 Filtering messages...")
        filtered = self.filter_messages(messages)

        self.stats['filtered_messages'] = len(filtered)
        filtered_out = len(messages) - len(filtered)
        print(f"   Filtered to {len(filtered):,} messages ({filtered_out:,} excluded)")

        return filtered

    def estimate_cost(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Estimate OpenAI API cost for embedding messages.

        Args:
            messages: List of messages to embed

        Returns:
            Dictionary with cost estimates
        """
        # Calculate average tokens per message (rough estimate: 1 token ~= 4 chars)
        total_chars = sum(len(msg['content']) for msg in messages)
        avg_chars = total_chars / len(messages) if messages else 0
        avg_tokens = int(avg_chars / 4)

        cost_info = self.vector_store.estimate_embedding_cost(
            text_count=len(messages),
            avg_tokens_per_text=avg_tokens
        )

        return cost_info

    def embed_messages(
        self,
        messages: List[Dict[str, Any]],
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Generate and store embeddings for messages.

        Args:
            messages: List of message dictionaries to embed
            dry_run: If True, only estimate costs without embedding

        Returns:
            Statistics dictionary
        """
        if not messages:
            print("⚠️  No messages to embed")
            return self.stats

        # Estimate costs
        print("\n💰 Cost Estimation:")
        cost_info = self.estimate_cost(messages)
        print(f"   Messages: {cost_info['text_count']:,}")
        print(f"   Estimated tokens: {cost_info['estimated_tokens']:,}")
        print(f"   Estimated cost: ${cost_info['estimated_cost_usd']:.4f} USD")

        if dry_run:
            print("\n🏃 Dry run mode - skipping actual embedding")
            return self.stats

        # Confirm before proceeding
        if cost_info['estimated_cost_usd'] > 1.0:
            response = input(f"\n⚠️  Estimated cost is ${cost_info['estimated_cost_usd']:.4f}. Continue? (y/n): ")
            if response.lower() != 'y':
                print("❌ Embedding cancelled by user")
                return self.stats

        # Initialize vector store
        print("\n🔧 Initializing ChromaDB...")
        self.vector_store.initialize()

        # Process messages in batches
        print(f"\n🚀 Starting embedding generation ({self.batch_size} per batch)...")
        self.stats['start_time'] = datetime.now()

        total_batches = (len(messages) + self.batch_size - 1) // self.batch_size

        for batch_idx in range(0, len(messages), self.batch_size):
            batch = messages[batch_idx:batch_idx + self.batch_size]
            batch_num = (batch_idx // self.batch_size) + 1

            print(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} messages)...")

            try:
                # Embed and store batch
                success_count, error_count = self.vector_store.add_messages_batch(
                    messages=batch,
                    batch_size=self.batch_size,
                    show_progress=True
                )

                # Create reference links in SQLite
                if success_count > 0:
                    print(f"   💾 Creating reference links in SQLite...")
                    for msg in batch:
                        # ChromaDB uses message_id as the document ID
                        self.db.insert_embedding_reference(
                            message_id=msg['id'],
                            chromadb_id=msg['id'],
                            embedding_model=self.vector_store.EMBEDDING_MODEL
                        )

                self.stats['embedded_successfully'] += success_count
                self.stats['embedding_errors'] += error_count

                print(f"   ✅ Batch complete: {success_count} embedded, {error_count} errors")

            except Exception as e:
                print(f"   ❌ Batch failed: {e}")
                self.stats['embedding_errors'] += len(batch)
                continue

        self.stats['end_time'] = datetime.now()

        return self.stats

    def print_summary(self):
        """Print final embedding statistics."""
        print("\n" + "="*60)
        print("📊 EMBEDDING SUMMARY")
        print("="*60)
        print(f"Total messages queried:     {self.stats['total_messages']:,}")
        print(f"Messages after filtering:   {self.stats['filtered_messages']:,}")
        print(f"Successfully embedded:      {self.stats['embedded_successfully']:,}")
        print(f"Errors:                     {self.stats['embedding_errors']:,}")

        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            print(f"Duration:                   {duration:.1f}s ({duration/60:.1f} min)")

            if self.stats['embedded_successfully'] > 0:
                rate = self.stats['embedded_successfully'] / duration
                print(f"Embedding rate:             {rate:.1f} messages/sec")

        # Check ChromaDB final count
        chroma_stats = self.vector_store.get_collection_stats()
        print(f"\nChromaDB total vectors:     {chroma_stats['count']:,}")

        # Check SQLite reference count
        db_stats = self.db.get_database_stats()
        print(f"SQLite reference links:     {db_stats['total_embeddings']:,}")

        print("="*60)

    def run(self, dry_run: bool = False):
        """
        Run the complete embedding pipeline.

        Args:
            dry_run: If True, only estimate costs without embedding
        """
        print("🎯 DISCREDIT EMBEDDING PIPELINE")
        print("="*60)
        print(f"SQLite DB:       {self.sqlite_path}")
        print(f"ChromaDB:        {self.chromadb_path}")
        print(f"Batch size:      {self.batch_size}")
        print(f"Min length:      {self.min_content_length} chars")
        print(f"Mode:            {'DRY RUN' if dry_run else 'PRODUCTION'}")
        print("="*60 + "\n")

        # Get messages to embed
        messages = self.get_messages_to_embed()

        if not messages:
            print("\n✅ All messages are already embedded!")
            return

        # Embed messages
        self.embed_messages(messages, dry_run=dry_run)

        # Print summary
        if not dry_run:
            self.print_summary()


def main():
    """Command-line interface for the embedder."""
    parser = argparse.ArgumentParser(
        description="Generate OpenAI embeddings for Discredit messages"
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of messages per embedding batch (default: 100)'
    )
    parser.add_argument(
        '--min-length',
        type=int,
        default=20,
        help='Minimum message length to embed (default: 20 chars)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Estimate costs only, do not generate embeddings'
    )
    parser.add_argument(
        '--sqlite-path',
        type=str,
        default=None,
        help='Path to SQLite database (optional, uses config default)'
    )
    parser.add_argument(
        '--chromadb-path',
        type=str,
        default=None,
        help='Path to ChromaDB storage (optional, uses config default)'
    )

    args = parser.parse_args()

    # Validate OpenAI credentials
    try:
        Config.validate_openai_credentials()
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)

    # Create and run embedder
    embedder = MessageEmbedder(
        sqlite_path=args.sqlite_path,
        chromadb_path=args.chromadb_path,
        batch_size=args.batch_size,
        min_content_length=args.min_length
    )

    try:
        embedder.run(dry_run=args.dry_run)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        embedder.print_summary()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Clean up connections
        embedder.db.close()


if __name__ == "__main__":
    main()
