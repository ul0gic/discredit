"""
Graph Builder - Construct Neo4j Graph from SQLite Data

This module orchestrates building the complete Neo4j knowledge graph
from classified SQLite messages and taxonomy data.

Pipeline:
---------
1. Pull data from SQLite (users, messages, categories, classifications)
2. Create nodes in Neo4j (Category, User, Message)
3. Create core relationships (POSTED, CLASSIFIED_AS, REPLIES_TO)
4. Update category message counts
5. Print statistics

Future: Add entity extraction and derived relationships

Usage:
------
from analysis.graph_builder import GraphBuilder

builder = GraphBuilder()
builder.build_graph(clear_existing=True)
"""

import logging
from typing import List, Dict, Any, Tuple
import sqlite3
from pathlib import Path

from storage.graph_db import GraphDB
from storage.sqlite_db import DiscreditDB

logger = logging.getLogger(__name__)


class GraphBuilder:
    """Builds Neo4j graph from SQLite source data"""

    def __init__(self, db_path: str = None, neo4j_uri: str = None,
                 neo4j_username: str = None, neo4j_password: str = None):
        """
        Initialize graph builder

        Args:
            db_path: Path to SQLite database (defaults to data/discredit.db)
            neo4j_uri: Neo4j connection URI
            neo4j_username: Neo4j username
            neo4j_password: Neo4j password
        """
        self.db_path = db_path or str(Path(__file__).parent.parent / "data" / "discredit.db")
        self.neo4j_uri = neo4j_uri
        self.neo4j_username = neo4j_username
        self.neo4j_password = neo4j_password

        # Will be initialized in build_graph()
        self.sqlite_db = None
        self.graph_db = None

    def build_graph(self, clear_existing: bool = False):
        """
        Build complete Neo4j graph from SQLite data

        Args:
            clear_existing: If True, clears existing graph before building

        Returns:
            Dictionary with build statistics
        """
        print("\n" + "="*70)
        print("🚀 BUILDING NEO4J KNOWLEDGE GRAPH")
        print("="*70)

        stats = {
            'nodes_created': {},
            'relationships_created': {}
        }

        # Initialize connections
        self.sqlite_db = DiscreditDB(self.db_path)
        self.graph_db = GraphDB(self.neo4j_uri, self.neo4j_username, self.neo4j_password)
        self.graph_db.connect()

        try:
            # Optional: Clear existing graph
            if clear_existing:
                print("\n⚠️  Clearing existing graph data...")
                self.graph_db.clear_database()
                print("✅ Graph cleared\n")

            # Step 1: Initialize schema (constraints and indexes)
            self.graph_db.initialize_schema()

            # Step 2: Create Category nodes (10 taxonomy categories)
            categories = self._get_category_data()
            stats['nodes_created']['Category'] = self.graph_db.create_category_nodes(categories)

            # Step 3: Create User nodes
            users = self._get_user_data()
            stats['nodes_created']['User'] = self.graph_db.create_user_nodes(users)

            # Step 4: Create Message nodes (only classified messages)
            messages = self._get_classified_message_data()
            stats['nodes_created']['Message'] = self.graph_db.create_message_nodes(messages)

            # Step 5: Create POSTED relationships (User -> Message)
            posted_rels = self._get_posted_relationships()
            stats['relationships_created']['POSTED'] = self.graph_db.create_posted_relationships(posted_rels)

            # Step 6: Create CLASSIFIED_AS relationships (Message -> Category)
            classified_rels = self._get_classification_relationships()
            stats['relationships_created']['CLASSIFIED_AS'] = self.graph_db.create_classified_as_relationships(classified_rels)

            # Step 7: Create REPLIES_TO relationships (Message -> Message) for threads
            reply_rels = self._get_reply_relationships()
            if reply_rels:
                stats['relationships_created']['REPLIES_TO'] = self.graph_db.create_replies_to_relationships(reply_rels)

            # Step 8: Update category message counts
            self._update_category_counts()

            # Step 9: Print statistics
            self.graph_db.print_stats()

            print("\n" + "="*70)
            print("✅ GRAPH BUILD COMPLETE!")
            print("="*70)
            print(f"\n📊 Summary:")
            print(f"   Nodes Created:         {sum(stats['nodes_created'].values()):,}")
            print(f"   Relationships Created: {sum(stats['relationships_created'].values()):,}")
            print(f"\n💡 Open Neo4j Browser to explore: {self.graph_db.uri.replace('bolt://', 'http://').replace(':7687', ':7474')}")
            print("="*70 + "\n")

            return stats

        finally:
            # Clean up connections
            if self.graph_db:
                self.graph_db.close()

    # ==================== DATA EXTRACTION METHODS ====================

    def _get_category_data(self) -> List[Dict[str, Any]]:
        """
        Get category data from taxonomy with message counts

        Returns:
            List of category dicts
        """
        from analysis.taxonomy import MARKET_TAXONOMY

        # Get actual message counts from database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT category, COUNT(*) as count
            FROM message_taxonomy
            GROUP BY category
        """)

        counts = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()

        # Build category list with counts
        categories = []
        for name, description in MARKET_TAXONOMY.items():
            categories.append({
                'name': name,
                'description': description,
                'message_count': counts.get(name, 0)
            })

        return categories

    def _get_user_data(self) -> List[Dict[str, Any]]:
        """
        Get all users from SQLite

        Returns:
            List of user dicts
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, platform, username, display_name, message_count, first_seen, last_seen
            FROM users
            ORDER BY message_count DESC
        """)

        users = []
        for row in cursor.fetchall():
            users.append({
                'id': row[0],
                'platform': row[1],
                'username': row[2],
                'display_name': row[3],
                'message_count': row[4],
                'first_seen': row[5],
                'last_seen': row[6]
            })

        conn.close()
        return users

    def _get_classified_message_data(self) -> List[Dict[str, Any]]:
        """
        Get only classified messages with their category assignments

        Returns:
            List of message dicts with category field
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Join messages with their classification
        cursor.execute("""
            SELECT DISTINCT
                m.id,
                m.platform,
                m.content,
                m.author_id,
                m.timestamp,
                m.source,
                m.parent_id,
                mt.category
            FROM messages m
            INNER JOIN message_taxonomy mt ON m.id = mt.message_id
            ORDER BY m.timestamp
        """)

        messages = []
        for row in cursor.fetchall():
            messages.append({
                'id': row[0],
                'platform': row[1],
                'content': row[2],
                'author_id': row[3],
                'timestamp': row[4],
                'source': row[5],
                'parent_id': row[6],
                'category': row[7]
            })

        conn.close()
        return messages

    def _get_posted_relationships(self) -> List[Tuple[str, str]]:
        """
        Get User -> Message relationships

        Returns:
            List of (user_id, message_id) tuples
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT DISTINCT m.author_id, m.id
            FROM messages m
            INNER JOIN message_taxonomy mt ON m.id = mt.message_id
            WHERE m.author_id IS NOT NULL
        """)

        relationships = [(row[0], row[1]) for row in cursor.fetchall()]
        conn.close()
        return relationships

    def _get_classification_relationships(self) -> List[Tuple[str, str]]:
        """
        Get Message -> Category relationships

        Returns:
            List of (message_id, category_name) tuples
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT message_id, category
            FROM message_taxonomy
            ORDER BY message_id
        """)

        relationships = [(row[0], row[1]) for row in cursor.fetchall()]
        conn.close()
        return relationships

    def _get_reply_relationships(self) -> List[Tuple[str, str]]:
        """
        Get Message -> Message reply relationships

        Returns:
            List of (child_message_id, parent_message_id) tuples
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT m.id, m.parent_id
            FROM messages m
            INNER JOIN message_taxonomy mt ON m.id = mt.message_id
            WHERE m.parent_id IS NOT NULL
            AND m.parent_id != ''
        """)

        relationships = [(row[0], row[1]) for row in cursor.fetchall()]
        conn.close()
        return relationships

    def _update_category_counts(self):
        """Update category nodes with actual message counts from relationships"""
        print("\n🔄 Updating category message counts...")

        with self.graph_db.driver.session() as session:
            session.run("""
                MATCH (c:Category)
                OPTIONAL MATCH (m:Message)-[:CLASSIFIED_AS]->(c)
                WITH c, count(m) AS message_count
                SET c.message_count = message_count
            """)

        print("✅ Category counts updated\n")


def main():
    """CLI entry point for building the graph"""
    import argparse

    parser = argparse.ArgumentParser(description="Build Neo4j knowledge graph from SQLite data")
    parser.add_argument('--clear', action='store_true', help="Clear existing graph before building")
    parser.add_argument('--db', type=str, help="Path to SQLite database")
    args = parser.parse_args()

    builder = GraphBuilder(db_path=args.db)
    builder.build_graph(clear_existing=args.clear)


if __name__ == "__main__":
    main()
