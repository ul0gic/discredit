"""
Neo4j Graph Database Test Script

Tests Neo4j connection, schema initialization, node/relationship creation,
and query operations.
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add backend to path
backend_path = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(backend_path))

# Load .env file
env_path = backend_path / ".env"
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key] = value

from storage.graph_db import GraphDB


def test_connection():
    """Test Neo4j connection."""
    print("\n=== Testing Neo4j Connection ===")

    graph = GraphDB()
    graph.connect()

    # Verify connectivity
    assert graph.driver is not None
    print("✓ Connected to Neo4j successfully")

    return graph


def test_schema_initialization(graph):
    """Test schema creation (constraints and indexes)."""
    print("\n=== Testing Schema Initialization ===")

    # Initialize schema
    graph.initialize_schema()

    # Verify schema
    schema = graph.verify_schema()

    print(f"✓ Constraints created: {len(schema['constraints'])}")
    print(f"✓ Indexes created: {len(schema['indexes'])}")

    # Check for expected constraints
    constraint_names = [c.lower() for c in schema['constraints']]
    expected = ['user_id', 'message_id', 'painpoint_name', 'integration_name']

    for exp in expected:
        assert any(exp in c for c in constraint_names), f"Missing constraint: {exp}"

    print("✓ All expected constraints found")


def test_user_nodes(graph):
    """Test User node creation."""
    print("\n=== Testing User Node Creation ===")

    # Create test users
    users = [
        {
            "id": "test_user_001",
            "platform": "discord",
            "username": "testuser1",
            "display_name": "Test User 1",
            "message_count": 5,
            "first_seen": int(datetime.now().timestamp()),
            "last_seen": int(datetime.now().timestamp())
        },
        {
            "id": "test_user_002",
            "platform": "reddit",
            "username": "testuser2",
            "display_name": "Test User 2",
            "message_count": 3,
            "first_seen": int(datetime.now().timestamp()),
            "last_seen": int(datetime.now().timestamp())
        }
    ]

    count = graph.create_user_nodes(users)
    assert count == 2
    print(f"✓ Created {count} user nodes")

    # Verify nodes exist
    result = graph.query("MATCH (u:User) RETURN count(u) as count")
    assert result[0]['count'] >= 2
    print(f"✓ Verified user nodes in database: {result[0]['count']}")


def test_message_nodes(graph):
    """Test Message node creation."""
    print("\n=== Testing Message Node Creation ===")

    now = int(datetime.now().timestamp())

    messages = [
        {
            "id": "test_msg_001",
            "platform": "discord",
            "content": "How do I integrate Supabase?",
            "timestamp": now,
            "source": "general",
            "author_id": "test_user_001"
        },
        {
            "id": "test_msg_002",
            "platform": "discord",
            "content": "The deployment process is confusing",
            "timestamp": now,
            "source": "general",
            "author_id": "test_user_001"
        },
        {
            "id": "test_msg_003",
            "platform": "reddit",
            "content": "Supabase integration guide?",
            "timestamp": now,
            "source": "r/lovable",
            "author_id": "test_user_002"
        }
    ]

    count = graph.create_message_nodes(messages)
    assert count == 3
    print(f"✓ Created {count} message nodes")

    # Verify nodes
    result = graph.query("MATCH (m:Message) RETURN count(m) as count")
    assert result[0]['count'] >= 3
    print(f"✓ Verified message nodes in database: {result[0]['count']}")


def test_entity_nodes(graph):
    """Test entity node creation."""
    print("\n=== Testing Entity Node Creation ===")

    # PainPoints
    pain_points = [
        {"name": "Supabase Integration Complexity", "category": "Integration", "frequency": 10, "severity": 0.8},
        {"name": "Deployment Confusion", "category": "Deployment", "frequency": 5, "severity": 0.6}
    ]

    count = graph.create_entity_nodes(pain_points, "PainPoint")
    assert count == 2
    print(f"✓ Created {count} PainPoint nodes")

    # Integrations
    integrations = [
        {"name": "Supabase", "category": "Database", "frequency": 15, "severity": 0},
        {"name": "Firebase", "category": "Database", "frequency": 8, "severity": 0}
    ]

    count = graph.create_entity_nodes(integrations, "Integration")
    assert count == 2
    print(f"✓ Created {count} Integration nodes")

    # Features
    features = [
        {"name": "Auto-deployment", "category": "DevOps", "frequency": 7, "severity": 0}
    ]

    count = graph.create_entity_nodes(features, "Feature")
    assert count == 1
    print(f"✓ Created {count} Feature nodes")


def test_relationships(graph):
    """Test relationship creation."""
    print("\n=== Testing Relationship Creation ===")

    # POSTED relationships
    user_messages = [
        ("test_user_001", "test_msg_001"),
        ("test_user_001", "test_msg_002"),
        ("test_user_002", "test_msg_003")
    ]

    count = graph.create_posted_relationships(user_messages)
    assert count == 3
    print(f"✓ Created {count} POSTED relationships")

    # EXPRESSES relationships (Message -> PainPoint)
    message_painpoints = [
        {
            "message_id": "test_msg_001",
            "entity_name": "Supabase Integration Complexity",
            "confidence": 0.9,
            "context": "How do I integrate Supabase?"
        },
        {
            "message_id": "test_msg_002",
            "entity_name": "Deployment Confusion",
            "confidence": 0.85,
            "context": "The deployment process is confusing"
        }
    ]

    count = graph.create_entity_relationships(
        message_painpoints,
        "EXPRESSES",
        "PainPoint"
    )
    assert count == 2
    print(f"✓ Created {count} EXPRESSES relationships")

    # REQUESTS relationships (Message -> Integration)
    message_integrations = [
        {
            "message_id": "test_msg_001",
            "entity_name": "Supabase",
            "confidence": 0.95,
            "context": "How do I integrate Supabase?"
        },
        {
            "message_id": "test_msg_003",
            "entity_name": "Supabase",
            "confidence": 0.9,
            "context": "Supabase integration guide?"
        }
    ]

    count = graph.create_entity_relationships(
        message_integrations,
        "REQUESTS",
        "Integration"
    )
    assert count == 2
    print(f"✓ Created {count} REQUESTS relationships")


def test_queries(graph):
    """Test Cypher queries."""
    print("\n=== Testing Cypher Queries ===")

    # Get users with message counts
    results = graph.query("""
        MATCH (u:User)-[:POSTED]->(m:Message)
        RETURN u.username as username, count(m) as msg_count
        ORDER BY msg_count DESC
    """)

    assert len(results) >= 1
    print(f"✓ User message counts query: {len(results)} results")
    for r in results:
        print(f"  - {r['username']}: {r['msg_count']} messages")

    # Get most requested integrations
    results = graph.query("""
        MATCH (m:Message)-[:REQUESTS]->(i:Integration)
        RETURN i.name as integration, count(m) as requests
        ORDER BY requests DESC
        LIMIT 5
    """)

    assert len(results) >= 1
    print(f"✓ Integration requests query: {len(results)} results")
    for r in results:
        print(f"  - {r['integration']}: {r['requests']} requests")

    # Get pain points with context
    results = graph.query("""
        MATCH (m:Message)-[r:EXPRESSES]->(p:PainPoint)
        RETURN p.name as pain_point, r.confidence as confidence, r.context as context
        ORDER BY confidence DESC
    """)

    assert len(results) >= 1
    print(f"✓ Pain points query: {len(results)} results")
    for r in results:
        print(f"  - {r['pain_point']} (confidence: {r['confidence']:.2f})")


def test_graph_stats(graph):
    """Test graph statistics."""
    print("\n=== Testing Graph Statistics ===")

    stats = graph.get_graph_stats()

    print("✓ Graph statistics:")
    print(f"  Total nodes: {stats['total_nodes']}")
    print(f"  Node breakdown: {stats['nodes']}")
    print(f"  Total relationships: {stats['total_relationships']}")
    print(f"  Relationship breakdown: {stats['relationships']}")

    assert stats['total_nodes'] > 0
    assert stats['total_relationships'] > 0


def run_all_tests():
    """Run all graph database tests."""
    print("=" * 60)
    print("DISCREDIT NEO4J GRAPH DATABASE TESTS")
    print("=" * 60)

    try:
        # Connect and test
        graph = test_connection()

        # Clear database for clean test
        print("\n⚠️  Clearing database for clean test...")
        graph.clear_database()

        # Run tests
        test_schema_initialization(graph)
        test_user_nodes(graph)
        test_message_nodes(graph)
        test_entity_nodes(graph)
        test_relationships(graph)
        test_queries(graph)
        test_graph_stats(graph)

        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED!")
        print("=" * 60)

        # Close connection
        graph.close()

        return True

    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
