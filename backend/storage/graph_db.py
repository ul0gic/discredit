"""
Neo4j Graph Database Module - Discredit Visual Intelligence Graph

🎯 Purpose: Build a beautiful, queryable graph of Lovable community intelligence
🔥 Features: Categories, Users, Messages, Entities, Co-occurrence, Visual exploration

Architecture Decisions:
--------------------
1. Local Neo4j: Running on localhost:7687 for simplicity and data locality
2. Node Types: Category, User, Message, Integration, Feature
3. Relationship Types: POSTED, CLASSIFIED_AS, MENTIONS, WANTS, CO_REQUESTED_WITH, SIMILAR_TO
4. Batch Operations: Efficient batch processing with progress bars
5. Derived Relationships: Co-occurrence analysis and user similarity
6. Visual-First: Optimized for Neo4j Browser exploration

Graph Schema:
-------------
Nodes:
- Category: {name, message_count, description} - The 10 taxonomy categories
- User: {id, platform, username, message_count, first_seen, last_seen}
- Message: {id, platform, content, timestamp, source, category}
- Integration: {name, request_count, unique_users, examples}
- Feature: {name, request_count, unique_users, examples}

Relationships:
- (User)-[POSTED]->(Message) - Who posted what
- (Message)-[CLASSIFIED_AS]->(Category) - Taxonomy classification
- (Message)-[REPLIES_TO]->(Message) - Conversation threading
- (Message)-[MENTIONS]->(Integration|Feature) - Entity mentions
- (User)-[WANTS {count}]->(Integration|Feature) - User desires (derived)
- (Integration)-[CO_REQUESTED_WITH {strength, count}]->(Integration) - Co-occurrence patterns
- (User)-[SIMILAR_TO {score}]->(User) - Similar interests (derived)

Visual Colors (configure in Neo4j Browser):
- 🟠 Category: Orange (large hubs)
- 🟢 Integration: Green (opportunity nodes)
- 🔵 Feature: Blue (product ideas)
- 🟣 User: Purple (community)
- ⚪ Message: Gray/White (connections)

Usage:
------
from storage.graph_db import GraphDB

# Initialize and connect
with GraphDB() as graph:
    # Create schema
    graph.initialize_schema()

    # Build graph from SQLite data
    graph.create_category_nodes()
    graph.create_user_nodes(users)
    graph.create_message_nodes(messages)

    # Create relationships
    graph.create_posted_relationships()
    graph.create_category_relationships()

    # Compute derived relationships
    graph.compute_co_occurrence()
    graph.compute_user_similarity()

    # Get stats
    graph.print_stats()
"""

from neo4j import GraphDatabase, Driver
from typing import List, Dict, Any, Optional, Tuple
from tqdm import tqdm
from collections import Counter, defaultdict
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class GraphDB:
    """
    Neo4j graph database wrapper for Discredit.

    Provides schema creation, node/relationship CRUD, and query utilities
    for graph-based analysis.
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initialize Neo4j connection.

        Args:
            uri: Neo4j connection URI (defaults to env var NEO4J_URI)
            username: Neo4j username (defaults to env var NEO4J_USERNAME)
            password: Neo4j password (defaults to env var NEO4J_PASSWORD)
        """
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.username = username or os.getenv("NEO4J_USERNAME", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD")

        if not self.password:
            raise ValueError("Neo4j password not provided. Set NEO4J_PASSWORD env var or pass password parameter.")

        self.driver: Optional[Driver] = None

    def connect(self):
        """Establish connection to Neo4j database."""
        self.driver = GraphDatabase.driver(
            self.uri,
            auth=(self.username, self.password)
        )

        # Test connection
        self.driver.verify_connectivity()
        print(f"✓ Connected to Neo4j at {self.uri}")

    def close(self):
        """Close the driver connection."""
        if self.driver:
            self.driver.close()
            print("✓ Neo4j connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    # ==================== SCHEMA INITIALIZATION ====================

    def initialize_schema(self):
        """
        Create constraints and indexes for graph schema.
        Safe to run multiple times (idempotent).
        """
        print("\n🔧 Initializing graph schema...")

        with self.driver.session() as session:
            # Constraints (enforce uniqueness)
            constraints = [
                "CREATE CONSTRAINT category_name_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",
                "CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
                "CREATE CONSTRAINT message_id_unique IF NOT EXISTS FOR (m:Message) REQUIRE m.id IS UNIQUE",
                "CREATE CONSTRAINT integration_name_unique IF NOT EXISTS FOR (i:Integration) REQUIRE i.name IS UNIQUE",
                "CREATE CONSTRAINT feature_name_unique IF NOT EXISTS FOR (f:Feature) REQUIRE f.name IS UNIQUE"
            ]

            print("   Creating constraints...")
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    if "already exists" not in str(e).lower() and "equivalent" not in str(e).lower():
                        logger.warning(f"Constraint warning: {e}")

            # Indexes for performance
            indexes = [
                "CREATE INDEX user_platform_idx IF NOT EXISTS FOR (u:User) ON (u.platform)",
                "CREATE INDEX user_message_count_idx IF NOT EXISTS FOR (u:User) ON (u.message_count)",
                "CREATE INDEX message_platform_idx IF NOT EXISTS FOR (m:Message) ON (m.platform)",
                "CREATE INDEX message_timestamp_idx IF NOT EXISTS FOR (m:Message) ON (m.timestamp)",
                "CREATE INDEX message_category_idx IF NOT EXISTS FOR (m:Message) ON (m.category)",
                "CREATE INDEX integration_count_idx IF NOT EXISTS FOR (i:Integration) ON (i.request_count)",
                "CREATE INDEX feature_count_idx IF NOT EXISTS FOR (f:Feature) ON (f.request_count)"
            ]

            print("   Creating indexes...")
            for index in indexes:
                try:
                    session.run(index)
                except Exception as e:
                    if "already exists" not in str(e).lower() and "equivalent" not in str(e).lower():
                        logger.warning(f"Index warning: {e}")

        print("✅ Graph schema initialized\n")

    # ==================== UTILITY METHODS ====================

    def query(self, cypher: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query and return results.

        Args:
            cypher: Cypher query string
            parameters: Query parameters dictionary

        Returns:
            List of result records as dictionaries
        """
        with self.driver.session() as session:
            result = session.run(cypher, parameters or {})
            return [dict(record) for record in result]

    def execute(self, cypher: str, parameters: Optional[Dict[str, Any]] = None):
        """
        Execute a Cypher command (no return value).

        Args:
            cypher: Cypher command string
            parameters: Command parameters dictionary
        """
        with self.driver.session() as session:
            session.run(cypher, parameters or {})

    def clear_database(self):
        """Delete all nodes and relationships. USE WITH CAUTION!"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("⚠️  Database cleared (all nodes and relationships deleted)")

    # ==================== NODE CREATION ====================

    def create_category_nodes(self, categories: List[Dict[str, Any]] = None) -> int:
        """
        Create Category nodes (the 10 taxonomy categories).

        Args:
            categories: Optional list of category dicts. If None, creates from taxonomy definitions.
                       Each dict should have: name, description, message_count

        Returns:
            Number of nodes created
        """
        # Default categories from taxonomy if not provided
        if categories is None:
            from analysis.taxonomy import MARKET_TAXONOMY
            categories = [
                {
                    'name': name,
                    'description': description,
                    'message_count': 0  # Will be updated from actual data
                }
                for name, description in MARKET_TAXONOMY.items()
            ]

        print(f"\n📦 Creating {len(categories)} Category nodes...")

        with self.driver.session() as session:
            query = """
            UNWIND $categories AS cat
            MERGE (c:Category {name: cat.name})
            SET c.description = cat.description,
                c.message_count = cat.message_count
            """
            session.run(query, {"categories": categories})

        print(f"✅ Created {len(categories)} Category nodes\n")
        return len(categories)

    def create_user_nodes(self, users: List[Dict[str, Any]], batch_size: int = 500) -> int:
        """
        Create User nodes in batches with progress tracking.

        Args:
            users: List of user dictionaries from SQLite
                   (must have: id, platform, username, message_count, first_seen, last_seen)
            batch_size: Batch size for transactions

        Returns:
            Number of nodes created
        """
        print(f"\n👥 Creating {len(users):,} User nodes...")
        created = 0

        with self.driver.session() as session:
            pbar = tqdm(total=len(users), desc="   Users", unit=" nodes")

            for i in range(0, len(users), batch_size):
                batch = users[i:i + batch_size]

                query = """
                UNWIND $users AS user
                MERGE (u:User {id: user.id})
                SET u.platform = user.platform,
                    u.username = user.username,
                    u.display_name = user.display_name,
                    u.message_count = user.message_count,
                    u.first_seen = user.first_seen,
                    u.last_seen = user.last_seen
                """

                session.run(query, {"users": batch})
                created += len(batch)
                pbar.update(len(batch))

            pbar.close()

        print(f"✅ Created {created:,} User nodes\n")
        return created

    def create_message_nodes(self, messages: List[Dict[str, Any]], batch_size: int = 500) -> int:
        """
        Create Message nodes in batches with progress tracking.

        Args:
            messages: List of message dictionaries from SQLite
                     (must have: id, platform, content, timestamp, source, author_id, category)
            batch_size: Batch size for transactions

        Returns:
            Number of nodes created
        """
        print(f"\n💬 Creating {len(messages):,} Message nodes...")
        created = 0

        with self.driver.session() as session:
            pbar = tqdm(total=len(messages), desc="   Messages", unit=" nodes")

            for i in range(0, len(messages), batch_size):
                batch = messages[i:i + batch_size]

                query = """
                UNWIND $messages AS msg
                MERGE (m:Message {id: msg.id})
                SET m.platform = msg.platform,
                    m.content = msg.content,
                    m.timestamp = msg.timestamp,
                    m.source = msg.source,
                    m.author_id = msg.author_id,
                    m.category = msg.category,
                    m.parent_id = msg.parent_id
                """

                session.run(query, {"messages": batch})
                created += len(batch)
                pbar.update(len(batch))

            pbar.close()

        print(f"✅ Created {created:,} Message nodes\n")
        return created

    def create_entity_nodes(
        self,
        entities: List[Dict[str, Any]],
        node_label: str,
        batch_size: int = 500
    ) -> int:
        """
        Create entity nodes (PainPoint, Integration, Feature, Workaround).

        Args:
            entities: List of entity dictionaries
                     (must have: name, category, and optional: frequency, severity)
            node_label: Node type (e.g., "PainPoint", "Integration")
            batch_size: Batch size for transactions

        Returns:
            Number of nodes created
        """
        created = 0

        with self.driver.session() as session:
            for i in range(0, len(entities), batch_size):
                batch = entities[i:i + batch_size]

                query = f"""
                UNWIND $entities AS entity
                MERGE (e:{node_label} {{name: entity.name}})
                SET e.category = entity.category,
                    e.frequency = COALESCE(entity.frequency, 0),
                    e.severity = COALESCE(entity.severity, 0.0)
                """

                session.run(query, {"entities": batch})
                created += len(batch)

        print(f"✓ Created {created} {node_label} nodes")
        return created

    # ==================== RELATIONSHIP CREATION ====================

    def create_posted_relationships(
        self,
        user_messages: List[Tuple[str, str]],
        batch_size: int = 1000
    ) -> int:
        """
        Create POSTED relationships between Users and Messages.

        Args:
            user_messages: List of (user_id, message_id) tuples
            batch_size: Batch size for transactions

        Returns:
            Number of relationships created
        """
        print(f"\n🔗 Creating {len(user_messages):,} POSTED relationships...")
        created = 0

        with self.driver.session() as session:
            pbar = tqdm(total=len(user_messages), desc="   User→Message", unit=" rels")

            for i in range(0, len(user_messages), batch_size):
                batch = user_messages[i:i + batch_size]

                query = """
                UNWIND $pairs AS pair
                MATCH (u:User {id: pair.user_id})
                MATCH (m:Message {id: pair.message_id})
                MERGE (u)-[:POSTED]->(m)
                """

                params = [{"user_id": uid, "message_id": mid} for uid, mid in batch]
                session.run(query, {"pairs": params})
                created += len(batch)
                pbar.update(len(batch))

            pbar.close()

        print(f"✅ Created {created:,} POSTED relationships\n")
        return created

    def create_classified_as_relationships(
        self,
        message_categories: List[Tuple[str, str]],
        batch_size: int = 1000
    ) -> int:
        """
        Create CLASSIFIED_AS relationships between Messages and Categories.

        Args:
            message_categories: List of (message_id, category_name) tuples
            batch_size: Batch size for transactions

        Returns:
            Number of relationships created
        """
        print(f"\n🏷️  Creating {len(message_categories):,} CLASSIFIED_AS relationships...")
        created = 0

        with self.driver.session() as session:
            pbar = tqdm(total=len(message_categories), desc="   Message→Category", unit=" rels")

            for i in range(0, len(message_categories), batch_size):
                batch = message_categories[i:i + batch_size]

                query = """
                UNWIND $pairs AS pair
                MATCH (m:Message {id: pair.message_id})
                MATCH (c:Category {name: pair.category})
                MERGE (m)-[:CLASSIFIED_AS]->(c)
                """

                params = [{"message_id": mid, "category": cat} for mid, cat in batch]
                session.run(query, {"pairs": params})
                created += len(batch)
                pbar.update(len(batch))

            pbar.close()

        print(f"✅ Created {created:,} CLASSIFIED_AS relationships\n")
        return created

    def create_entity_relationships(
        self,
        message_entities: List[Dict[str, Any]],
        relationship_type: str,
        entity_label: str,
        batch_size: int = 1000
    ) -> int:
        """
        Create relationships between Messages and Entities.

        Args:
            message_entities: List of dicts with message_id, entity_name, confidence, context
            relationship_type: Relationship type (e.g., "EXPRESSES", "REQUESTS")
            entity_label: Entity node label (e.g., "PainPoint", "Integration")
            batch_size: Batch size for transactions

        Returns:
            Number of relationships created
        """
        created = 0

        with self.driver.session() as session:
            for i in range(0, len(message_entities), batch_size):
                batch = message_entities[i:i + batch_size]

                query = f"""
                UNWIND $items AS item
                MATCH (m:Message {{id: item.message_id}})
                MATCH (e:{entity_label} {{name: item.entity_name}})
                MERGE (m)-[r:{relationship_type}]->(e)
                SET r.confidence = item.confidence,
                    r.context = item.context
                """

                session.run(query, {"items": batch})
                created += len(batch)

        print(f"✓ Created {created} {relationship_type} relationships")
        return created

    def create_replies_to_relationships(
        self,
        parent_child_pairs: List[Tuple[str, str]],
        batch_size: int = 1000
    ) -> int:
        """
        Create REPLIES_TO relationships between Messages.

        Args:
            parent_child_pairs: List of (child_message_id, parent_message_id) tuples
            batch_size: Batch size for transactions

        Returns:
            Number of relationships created
        """
        created = 0

        with self.driver.session() as session:
            for i in range(0, len(parent_child_pairs), batch_size):
                batch = parent_child_pairs[i:i + batch_size]

                query = """
                UNWIND $pairs AS pair
                MATCH (child:Message {id: pair.child_id})
                MATCH (parent:Message {id: pair.parent_id})
                MERGE (child)-[:REPLIES_TO]->(parent)
                """

                params = [{"child_id": cid, "parent_id": pid} for cid, pid in batch]
                session.run(query, {"pairs": params})
                created += len(batch)

        print(f"✓ Created {created} REPLIES_TO relationships")
        return created

    # ==================== STATISTICS & VALIDATION ====================

    def get_graph_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive graph statistics.

        Returns:
            Dictionary with node counts, relationship counts, etc.
        """
        with self.driver.session() as session:
            stats = {}

            # Node counts
            node_counts = session.run("""
                MATCH (n)
                RETURN labels(n)[0] AS label, count(*) AS count
                ORDER BY count DESC
            """)
            stats['nodes'] = {record['label']: record['count'] for record in node_counts}
            stats['total_nodes'] = sum(stats['nodes'].values())

            # Relationship counts
            rel_counts = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) AS type, count(*) AS count
                ORDER BY count DESC
            """)
            stats['relationships'] = {record['type']: record['count'] for record in rel_counts}
            stats['total_relationships'] = sum(stats['relationships'].values())

            # Category breakdown
            category_stats = session.run("""
                MATCH (c:Category)
                OPTIONAL MATCH (m:Message)-[:CLASSIFIED_AS]->(c)
                RETURN c.name AS category, count(m) AS message_count
                ORDER BY message_count DESC
            """)
            stats['categories'] = {record['category']: record['message_count'] for record in category_stats}

            # Platform breakdown
            platform_stats = session.run("""
                MATCH (m:Message)
                RETURN m.platform AS platform, count(m) AS count
            """)
            stats['platforms'] = {record['platform']: record['count'] for record in platform_stats}

            return stats

    def print_stats(self):
        """Print beautiful formatted graph statistics"""
        stats = self.get_graph_stats()

        print("\n" + "="*70)
        print("🕸️  NEO4J GRAPH DATABASE STATISTICS")
        print("="*70)

        print(f"\n📊 OVERVIEW")
        print(f"   Total Nodes:         {stats['total_nodes']:>10,}")
        print(f"   Total Relationships: {stats['total_relationships']:>10,}")

        print(f"\n📦 NODES BY TYPE")
        for label, count in sorted(stats['nodes'].items(), key=lambda x: x[1], reverse=True):
            emoji = {
                'Category': '🟠',
                'User': '🟣',
                'Message': '⚪',
                'Integration': '🟢',
                'Feature': '🔵'
            }.get(label, '◽')
            print(f"   {emoji} {label:15} {count:>10,}")

        print(f"\n🔗 RELATIONSHIPS BY TYPE")
        for rel_type, count in sorted(stats['relationships'].items(), key=lambda x: x[1], reverse=True):
            print(f"   • {rel_type:25} {count:>10,}")

        if stats.get('categories'):
            print(f"\n🏷️  MESSAGES BY CATEGORY")
            for category, count in sorted(stats['categories'].items(), key=lambda x: x[1], reverse=True):
                if count > 0:  # Only show categories with messages
                    pct = (count / stats['nodes'].get('Message', 1)) * 100
                    print(f"   • {category:25} {count:>7,} ({pct:>5.1f}%)")

        if stats.get('platforms'):
            print(f"\n🌐 MESSAGES BY PLATFORM")
            for platform, count in stats['platforms'].items():
                print(f"   • {platform:15} {count:>10,}")

        print("\n" + "="*70 + "\n")

    def verify_schema(self) -> Dict[str, List[str]]:
        """
        Verify schema constraints and indexes exist.

        Returns:
            Dictionary with lists of constraints and indexes
        """
        with self.driver.session() as session:
            # Get constraints
            constraints = session.run("SHOW CONSTRAINTS")
            constraint_list = [record['name'] for record in constraints]

            # Get indexes
            indexes = session.run("SHOW INDEXES")
            index_list = [record['name'] for record in indexes]

            return {
                'constraints': constraint_list,
                'indexes': index_list
            }
