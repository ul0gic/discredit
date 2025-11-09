"""
Neo4j Graph Database Module - Discredit Relationship Analysis

This module provides the Neo4j graph database wrapper for relationship analysis,
pattern discovery, and opportunity identification.

Architecture Decisions:
--------------------
1. Local Neo4j: Running on localhost:7687 for simplicity and data locality
2. Node Types: User, Message, PainPoint, Integration, Feature, Workaround
3. Relationship Types: POSTED, MENTIONS, REPLIES_TO, EXPRESSES, REQUESTS, WANTS, etc.
4. Batch Operations: All operations use batch transactions for performance
5. Property-rich model: Nodes and relationships carry metadata for flexible querying

Graph Schema:
-------------
Nodes:
- User: {id, platform, username, message_count, first_seen, last_seen}
- Message: {id, platform, content, timestamp, source}
- PainPoint: {name, category, frequency, severity}
- Integration: {name, category, provider}
- Feature: {name, category, functional_area}
- Workaround: {name, category}

Relationships:
- (User)-[POSTED]->(Message)
- (Message)-[MENTIONS]->(User)
- (Message)-[REPLIES_TO]->(Message)
- (Message)-[EXPRESSES]->(PainPoint) {confidence, context}
- (Message)-[REQUESTS]->(Integration) {confidence, context}
- (Message)-[WANTS]->(Feature) {confidence, context}
- (User)-[INTERESTED_IN]->(Integration) {strength}
- (PainPoint)-[SOLVED_BY]->(Integration) {relevance}

Usage:
------
# Initialize
graph = GraphDB()
graph.connect()

# Create nodes
graph.create_user_nodes(users)
graph.create_message_nodes(messages)

# Create relationships
graph.create_posted_relationships(user_messages)

# Query
results = graph.query("MATCH (u:User)-[:POSTED]->(m:Message) RETURN u, count(m)")
"""

from neo4j import GraphDatabase, Driver
from typing import List, Dict, Any, Optional, Tuple
import os
from pathlib import Path


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
        with self.driver.session() as session:
            # Constraints (enforce uniqueness)
            constraints = [
                "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
                "CREATE CONSTRAINT message_id IF NOT EXISTS FOR (m:Message) REQUIRE m.id IS UNIQUE",
                "CREATE CONSTRAINT painpoint_name IF NOT EXISTS FOR (p:PainPoint) REQUIRE p.name IS UNIQUE",
                "CREATE CONSTRAINT integration_name IF NOT EXISTS FOR (i:Integration) REQUIRE i.name IS UNIQUE",
                "CREATE CONSTRAINT feature_name IF NOT EXISTS FOR (f:Feature) REQUIRE f.name IS UNIQUE",
                "CREATE CONSTRAINT workaround_name IF NOT EXISTS FOR (w:Workaround) REQUIRE w.name IS UNIQUE"
            ]

            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    # Constraint may already exist
                    if "already exists" not in str(e).lower():
                        print(f"⚠️  Constraint warning: {e}")

            # Indexes for performance
            indexes = [
                "CREATE INDEX user_platform IF NOT EXISTS FOR (u:User) ON (u.platform)",
                "CREATE INDEX message_platform IF NOT EXISTS FOR (m:Message) ON (m.platform)",
                "CREATE INDEX message_timestamp IF NOT EXISTS FOR (m:Message) ON (m.timestamp)",
                "CREATE INDEX painpoint_category IF NOT EXISTS FOR (p:PainPoint) ON (p.category)",
                "CREATE INDEX integration_category IF NOT EXISTS FOR (i:Integration) ON (i.category)"
            ]

            for index in indexes:
                try:
                    session.run(index)
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        print(f"⚠️  Index warning: {e}")

        print("✓ Graph schema initialized (constraints and indexes created)")

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

    def create_user_nodes(self, users: List[Dict[str, Any]], batch_size: int = 500) -> int:
        """
        Create User nodes in batches.

        Args:
            users: List of user dictionaries from SQLite
                   (must have: id, platform, username, message_count, first_seen, last_seen)
            batch_size: Batch size for transactions

        Returns:
            Number of nodes created
        """
        created = 0

        with self.driver.session() as session:
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

        print(f"✓ Created {created} User nodes")
        return created

    def create_message_nodes(self, messages: List[Dict[str, Any]], batch_size: int = 500) -> int:
        """
        Create Message nodes in batches.

        Args:
            messages: List of message dictionaries from SQLite
                     (must have: id, platform, content, timestamp, source, author_id)
            batch_size: Batch size for transactions

        Returns:
            Number of nodes created
        """
        created = 0

        with self.driver.session() as session:
            for i in range(0, len(messages), batch_size):
                batch = messages[i:i + batch_size]

                query = """
                UNWIND $messages AS msg
                MERGE (m:Message {id: msg.id})
                SET m.platform = msg.platform,
                    m.content = msg.content,
                    m.timestamp = msg.timestamp,
                    m.source = msg.source,
                    m.author_id = msg.author_id
                """

                session.run(query, {"messages": batch})
                created += len(batch)

        print(f"✓ Created {created} Message nodes")
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
        created = 0

        with self.driver.session() as session:
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

        print(f"✓ Created {created} POSTED relationships")
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

            return stats

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
