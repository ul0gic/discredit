"""
SQLite Database Module - Discredit Source of Truth

This module provides the SQLite database schema and operations for the Discredit project.
SQLite serves as the central source of truth for all scraped data, entities, and embeddings.

Architecture Decisions:
--------------------
1. ID Strategy: Prefixed strings (e.g., "discord_123456", "reddit_abc123")
   - Avoids collisions between platforms
   - Human-readable for debugging
   - Simple to query and join

2. Platform Unification: Unified messages table with nullable fields + JSON metadata
   - Common fields (content, author, timestamp) are normalized columns
   - Platform-specific data (reactions, awards, etc.) stored in JSON metadata
   - Single 'source' column stores channel/subreddit name

3. Timestamps: Unix timestamps (INTEGER) for efficient sorting and filtering

4. No ORM: Raw SQL for full control and transparency

Schema Overview:
---------------
- messages: Unified Discord + Reddit messages with threading support
- users: Cross-platform user profiles with activity metrics
- extracted_entities: LLM-extracted pain points, integrations, features
- embeddings_reference: Links messages to ChromaDB vector IDs
"""

import sqlite3
import json
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple
from datetime import datetime


class DiscreditDB:
    """
    SQLite database wrapper for Discredit.

    Provides schema creation, CRUD operations, and query utilities
    for all scraped and processed data.
    """

    def __init__(self, db_path: str = "backend/data/discredit.db"):
        """
        Initialize database connection.

        Args:
            db_path: Path to SQLite database file (will be created if doesn't exist)
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row  # Access columns by name

    def initialize_schema(self):
        """
        Create all tables and indexes if they don't exist.
        Safe to run multiple times (idempotent).
        """
        cursor = self.conn.cursor()

        # Messages table - unified Discord and Reddit messages
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                platform TEXT NOT NULL CHECK(platform IN ('discord', 'reddit')),
                content TEXT NOT NULL,
                author_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                source TEXT,
                parent_id TEXT,
                metadata TEXT,
                scraped_at INTEGER NOT NULL,
                FOREIGN KEY (author_id) REFERENCES users(id),
                FOREIGN KEY (parent_id) REFERENCES messages(id)
            )
        """)

        # Users table - cross-platform user profiles
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                platform TEXT NOT NULL CHECK(platform IN ('discord', 'reddit')),
                username TEXT NOT NULL,
                display_name TEXT,
                message_count INTEGER DEFAULT 0,
                first_seen INTEGER,
                last_seen INTEGER,
                metadata TEXT
            )
        """)

        # Extracted entities table - LLM extraction results
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS extracted_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL,
                entity_type TEXT NOT NULL CHECK(
                    entity_type IN ('pain_point', 'integration', 'feature', 'workaround')
                ),
                entity_name TEXT NOT NULL,
                canonical_name TEXT,
                category TEXT,
                confidence REAL,
                context TEXT,
                extraction_metadata TEXT,
                FOREIGN KEY (message_id) REFERENCES messages(id)
            )
        """)

        # Embeddings reference table - links to ChromaDB
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS embeddings_reference (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL UNIQUE,
                chromadb_id TEXT NOT NULL,
                embedding_model TEXT,
                created_at INTEGER NOT NULL,
                FOREIGN KEY (message_id) REFERENCES messages(id)
            )
        """)

        # Create indexes for common query patterns
        self._create_indexes(cursor)

        self.conn.commit()

    def _create_indexes(self, cursor):
        """Create indexes for optimized queries."""

        # Messages indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_platform ON messages(platform)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_author ON messages(author_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_source ON messages(source)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_parent ON messages(parent_id)")

        # Users indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_platform ON users(platform)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")

        # Extracted entities indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entities_message ON extracted_entities(message_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entities_type ON extracted_entities(entity_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entities_name ON extracted_entities(entity_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entities_canonical ON extracted_entities(canonical_name)")

        # Embeddings index
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_message ON embeddings_reference(message_id)")

    # ==================== MESSAGES CRUD ====================

    def insert_message(
        self,
        id: str,
        platform: str,
        content: str,
        author_id: str,
        timestamp: int,
        source: Optional[str] = None,
        parent_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        scraped_at: Optional[int] = None
    ) -> bool:
        """
        Insert a message into the database.

        Args:
            id: Prefixed message ID (e.g., "discord_123456")
            platform: "discord" or "reddit"
            content: Message text content
            author_id: Prefixed user ID
            timestamp: Unix timestamp of message creation
            source: Channel name (Discord) or subreddit (Reddit)
            parent_id: ID of parent message (for threading)
            metadata: Platform-specific JSON metadata
            scraped_at: Unix timestamp of scraping (defaults to now)

        Returns:
            True if inserted, False if already exists (duplicate ID)
        """
        if scraped_at is None:
            scraped_at = int(datetime.now().timestamp())

        metadata_json = json.dumps(metadata) if metadata else None

        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO messages (id, platform, content, author_id, timestamp,
                                     source, parent_id, metadata, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (id, platform, content, author_id, timestamp, source,
                  parent_id, metadata_json, scraped_at))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Message already exists (duplicate ID)
            return False

    def get_message(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a message by ID.

        Args:
            message_id: Prefixed message ID

        Returns:
            Dictionary of message data or None if not found
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
        row = cursor.fetchone()

        if row is None:
            return None

        return self._row_to_dict(row, parse_metadata=True)

    def get_messages_by_platform(
        self,
        platform: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get all messages from a specific platform.

        Args:
            platform: "discord" or "reddit"
            limit: Maximum number of messages to return
            offset: Number of messages to skip

        Returns:
            List of message dictionaries
        """
        cursor = self.conn.cursor()

        query = "SELECT * FROM messages WHERE platform = ? ORDER BY timestamp DESC"
        params = [platform]

        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])

        cursor.execute(query, params)
        return [self._row_to_dict(row, parse_metadata=True) for row in cursor.fetchall()]

    def get_messages_by_timerange(
        self,
        start_timestamp: int,
        end_timestamp: int,
        platform: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get messages within a time range.

        Args:
            start_timestamp: Unix timestamp (inclusive)
            end_timestamp: Unix timestamp (inclusive)
            platform: Optional platform filter

        Returns:
            List of message dictionaries
        """
        cursor = self.conn.cursor()

        if platform:
            cursor.execute("""
                SELECT * FROM messages
                WHERE timestamp BETWEEN ? AND ? AND platform = ?
                ORDER BY timestamp ASC
            """, (start_timestamp, end_timestamp, platform))
        else:
            cursor.execute("""
                SELECT * FROM messages
                WHERE timestamp BETWEEN ? AND ?
                ORDER BY timestamp ASC
            """, (start_timestamp, end_timestamp))

        return [self._row_to_dict(row, parse_metadata=True) for row in cursor.fetchall()]

    def get_message_count(self, platform: Optional[str] = None) -> int:
        """
        Get total message count, optionally filtered by platform.

        Args:
            platform: Optional platform filter

        Returns:
            Message count
        """
        cursor = self.conn.cursor()

        if platform:
            cursor.execute("SELECT COUNT(*) FROM messages WHERE platform = ?", (platform,))
        else:
            cursor.execute("SELECT COUNT(*) FROM messages")

        return cursor.fetchone()[0]

    # ==================== USERS CRUD ====================

    def insert_user(
        self,
        id: str,
        platform: str,
        username: str,
        display_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Insert a user into the database.

        Args:
            id: Prefixed user ID (e.g., "discord_user_123")
            platform: "discord" or "reddit"
            username: User's username
            display_name: User's display name (if different from username)
            metadata: Platform-specific JSON metadata

        Returns:
            True if inserted, False if already exists
        """
        metadata_json = json.dumps(metadata) if metadata else None
        now = int(datetime.now().timestamp())

        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO users (id, platform, username, display_name,
                                  message_count, first_seen, last_seen, metadata)
                VALUES (?, ?, ?, ?, 0, ?, ?, ?)
            """, (id, platform, username, display_name, now, now, metadata_json))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a user by ID.

        Args:
            user_id: Prefixed user ID

        Returns:
            Dictionary of user data or None if not found
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        row = cursor.fetchone()

        if row is None:
            return None

        return self._row_to_dict(row, parse_metadata=True)

    def upsert_user(
        self,
        id: str,
        platform: str,
        username: str,
        display_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Insert user if new, update if exists.

        Args:
            id: Prefixed user ID
            platform: "discord" or "reddit"
            username: User's username
            display_name: User's display name
            metadata: Platform-specific JSON metadata
        """
        metadata_json = json.dumps(metadata) if metadata else None
        now = int(datetime.now().timestamp())

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO users (id, platform, username, display_name,
                              message_count, first_seen, last_seen, metadata)
            VALUES (?, ?, ?, ?, 0, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                username = excluded.username,
                display_name = excluded.display_name,
                last_seen = excluded.last_seen,
                metadata = excluded.metadata
        """, (id, platform, username, display_name, now, now, metadata_json))
        self.conn.commit()

    def increment_user_message_count(self, user_id: str, timestamp: int):
        """
        Increment user's message count and update activity timestamps.

        Args:
            user_id: Prefixed user ID
            timestamp: Message timestamp
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE users
            SET message_count = message_count + 1,
                first_seen = MIN(first_seen, ?),
                last_seen = MAX(last_seen, ?)
            WHERE id = ?
        """, (timestamp, timestamp, user_id))
        self.conn.commit()

    def get_top_users_by_activity(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get most active users by message count.

        Args:
            limit: Number of users to return

        Returns:
            List of user dictionaries ordered by message count
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM users
            ORDER BY message_count DESC
            LIMIT ?
        """, (limit,))
        return [self._row_to_dict(row, parse_metadata=True) for row in cursor.fetchall()]

    # ==================== EXTRACTED ENTITIES CRUD ====================

    def insert_entity(
        self,
        message_id: str,
        entity_type: str,
        entity_name: str,
        canonical_name: Optional[str] = None,
        category: Optional[str] = None,
        confidence: Optional[float] = None,
        context: Optional[str] = None,
        extraction_metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Insert an extracted entity.

        Args:
            message_id: Source message ID
            entity_type: One of: pain_point, integration, feature, workaround
            entity_name: Raw extracted entity name
            canonical_name: Normalized/deduplicated name (set during Phase 4.6)
            category: Entity category
            confidence: Extraction confidence score (0-1)
            context: Text snippet providing context
            extraction_metadata: JSON metadata (model, timestamp, batch_id)

        Returns:
            ID of inserted entity
        """
        metadata_json = json.dumps(extraction_metadata) if extraction_metadata else None

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO extracted_entities
            (message_id, entity_type, entity_name, canonical_name,
             category, confidence, context, extraction_metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (message_id, entity_type, entity_name, canonical_name,
              category, confidence, context, metadata_json))
        self.conn.commit()
        return cursor.lastrowid

    def get_entities_by_type(
        self,
        entity_type: str,
        min_confidence: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Get all entities of a specific type.

        Args:
            entity_type: Entity type filter
            min_confidence: Minimum confidence threshold

        Returns:
            List of entity dictionaries
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM extracted_entities
            WHERE entity_type = ? AND (confidence IS NULL OR confidence >= ?)
            ORDER BY confidence DESC
        """, (entity_type, min_confidence))
        return [self._row_to_dict(row, parse_metadata=True) for row in cursor.fetchall()]

    def get_entity_frequency(self) -> List[Tuple[str, str, int]]:
        """
        Get entity frequency counts grouped by type and name.

        Returns:
            List of (entity_type, entity_name, count) tuples ordered by frequency
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT entity_type,
                   COALESCE(canonical_name, entity_name) as name,
                   COUNT(*) as frequency
            FROM extracted_entities
            GROUP BY entity_type, name
            ORDER BY frequency DESC
        """)
        return cursor.fetchall()

    # ==================== EMBEDDINGS REFERENCE CRUD ====================

    def insert_embedding_reference(
        self,
        message_id: str,
        chromadb_id: str,
        embedding_model: str = "text-embedding-3-small"
    ) -> bool:
        """
        Link a message to its ChromaDB embedding.

        Args:
            message_id: Source message ID
            chromadb_id: ChromaDB vector ID
            embedding_model: Model used for embedding

        Returns:
            True if inserted, False if already exists
        """
        now = int(datetime.now().timestamp())

        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO embeddings_reference
                (message_id, chromadb_id, embedding_model, created_at)
                VALUES (?, ?, ?, ?)
            """, (message_id, chromadb_id, embedding_model, now))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def get_embedding_reference(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Get ChromaDB reference for a message.

        Args:
            message_id: Source message ID

        Returns:
            Dictionary with chromadb_id and metadata, or None
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM embeddings_reference WHERE message_id = ?
        """, (message_id,))
        row = cursor.fetchone()

        if row is None:
            return None

        return self._row_to_dict(row)

    def get_messages_without_embeddings(self, min_length: int = 20) -> List[Dict[str, Any]]:
        """
        Get messages that haven't been embedded yet.

        Args:
            min_length: Minimum content length to include

        Returns:
            List of message dictionaries
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT m.* FROM messages m
            LEFT JOIN embeddings_reference e ON m.id = e.message_id
            WHERE e.id IS NULL AND LENGTH(m.content) >= ?
            ORDER BY m.timestamp ASC
        """, (min_length,))
        return [self._row_to_dict(row, parse_metadata=True) for row in cursor.fetchall()]

    # ==================== UTILITY METHODS ====================

    def _row_to_dict(self, row: sqlite3.Row, parse_metadata: bool = False) -> Dict[str, Any]:
        """
        Convert sqlite3.Row to dictionary.

        Args:
            row: Database row
            parse_metadata: If True, parse JSON metadata column

        Returns:
            Dictionary representation
        """
        data = dict(row)

        if parse_metadata and 'metadata' in data and data['metadata']:
            try:
                data['metadata'] = json.loads(data['metadata'])
            except json.JSONDecodeError:
                pass  # Keep as string if invalid JSON

        if parse_metadata and 'extraction_metadata' in data and data['extraction_metadata']:
            try:
                data['extraction_metadata'] = json.loads(data['extraction_metadata'])
            except json.JSONDecodeError:
                pass

        return data

    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive database statistics.

        Returns:
            Dictionary with counts and metrics
        """
        cursor = self.conn.cursor()

        stats = {}

        # Message counts
        cursor.execute("SELECT COUNT(*) FROM messages")
        stats['total_messages'] = cursor.fetchone()[0]

        cursor.execute("SELECT platform, COUNT(*) FROM messages GROUP BY platform")
        stats['messages_by_platform'] = dict(cursor.fetchall())

        # User counts
        cursor.execute("SELECT COUNT(*) FROM users")
        stats['total_users'] = cursor.fetchone()[0]

        cursor.execute("SELECT platform, COUNT(*) FROM users GROUP BY platform")
        stats['users_by_platform'] = dict(cursor.fetchall())

        # Entity counts
        cursor.execute("SELECT COUNT(*) FROM extracted_entities")
        stats['total_entities'] = cursor.fetchone()[0]

        cursor.execute("SELECT entity_type, COUNT(*) FROM extracted_entities GROUP BY entity_type")
        stats['entities_by_type'] = dict(cursor.fetchall())

        # Embedding counts
        cursor.execute("SELECT COUNT(*) FROM embeddings_reference")
        stats['total_embeddings'] = cursor.fetchone()[0]

        # Date range
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM messages")
        min_ts, max_ts = cursor.fetchone()
        if min_ts and max_ts:
            stats['date_range'] = {
                'start': datetime.fromtimestamp(min_ts).isoformat(),
                'end': datetime.fromtimestamp(max_ts).isoformat()
            }

        return stats

    def close(self):
        """Close database connection."""
        self.conn.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
