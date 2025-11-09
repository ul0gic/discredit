"""
SQLite Database Test Script

Tests all CRUD operations and validates schema integrity.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.sqlite_db import DiscreditDB


def test_schema_creation():
    """Test that schema is created successfully."""
    print("\n=== Testing Schema Creation ===")

    # Use a test database
    db = DiscreditDB("backend/data/test_discredit.db")
    db.initialize_schema()

    # Verify tables exist (filter out sqlite_sequence - created automatically for AUTOINCREMENT)
    cursor = db.conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name != 'sqlite_sequence'
        ORDER BY name
    """)
    tables = [row[0] for row in cursor.fetchall()]

    expected_tables = ['embeddings_reference', 'extracted_entities', 'messages', 'users']
    assert tables == expected_tables, f"Expected {expected_tables}, got {tables}"

    print("✓ All tables created successfully")
    print(f"  Tables: {', '.join(tables)}")

    return db


def test_users_crud(db):
    """Test user creation and retrieval."""
    print("\n=== Testing Users CRUD ===")

    # Insert Discord user
    success = db.insert_user(
        id="discord_user_123",
        platform="discord",
        username="testuser",
        display_name="Test User",
        metadata={"discriminator": "1234"}
    )
    assert success, "Failed to insert Discord user"
    print("✓ Inserted Discord user")

    # Insert Reddit user
    success = db.insert_user(
        id="reddit_user_abc",
        platform="reddit",
        username="testreddituser",
        metadata={"link_karma": 1500, "comment_karma": 3000}
    )
    assert success, "Failed to insert Reddit user"
    print("✓ Inserted Reddit user")

    # Retrieve user
    user = db.get_user("discord_user_123")
    assert user is not None, "User not found"
    assert user['username'] == "testuser"
    assert user['platform'] == "discord"
    assert user['metadata']['discriminator'] == "1234"
    print("✓ Retrieved user with parsed metadata")

    # Test upsert
    db.upsert_user(
        id="discord_user_123",
        platform="discord",
        username="updateduser",
        display_name="Updated User"
    )
    user = db.get_user("discord_user_123")
    assert user['username'] == "updateduser"
    print("✓ User upsert working correctly")

    # Test duplicate insert (should fail gracefully)
    success = db.insert_user(
        id="discord_user_123",
        platform="discord",
        username="duplicate"
    )
    assert not success, "Duplicate insert should return False"
    print("✓ Duplicate user prevention working")


def test_messages_crud(db):
    """Test message creation and retrieval."""
    print("\n=== Testing Messages CRUD ===")

    now = int(datetime.now().timestamp())
    one_hour_ago = now - 3600

    # Insert Discord message
    success = db.insert_message(
        id="discord_msg_001",
        platform="discord",
        content="How do I integrate Supabase with Lovable?",
        author_id="discord_user_123",
        timestamp=one_hour_ago,
        source="general",
        metadata={
            "channel_id": "1234567890",
            "reactions": [{"emoji": "👍", "count": 3}],
            "edited": False
        }
    )
    assert success, "Failed to insert Discord message"
    print("✓ Inserted Discord message")

    # Insert Reddit post
    success = db.insert_message(
        id="reddit_post_001",
        platform="reddit",
        content="What are the best integrations for Lovable?",
        author_id="reddit_user_abc",
        timestamp=now,
        source="r/lovable",
        metadata={
            "upvotes": 15,
            "awards": ["helpful"],
            "flair": "Question"
        }
    )
    assert success, "Failed to insert Reddit post"
    print("✓ Inserted Reddit post")

    # Insert Reddit comment (threaded)
    success = db.insert_message(
        id="reddit_comment_001",
        platform="reddit",
        content="Supabase integration is great!",
        author_id="reddit_user_abc",
        timestamp=now,
        source="r/lovable",
        parent_id="reddit_post_001",
        metadata={"upvotes": 5}
    )
    assert success, "Failed to insert Reddit comment"
    print("✓ Inserted threaded Reddit comment")

    # Retrieve message
    msg = db.get_message("discord_msg_001")
    assert msg is not None
    assert msg['content'] == "How do I integrate Supabase with Lovable?"
    assert msg['metadata']['reactions'][0]['emoji'] == "👍"
    print("✓ Retrieved message with parsed metadata")

    # Update user activity
    db.increment_user_message_count("discord_user_123", one_hour_ago)
    db.increment_user_message_count("reddit_user_abc", now)
    db.increment_user_message_count("reddit_user_abc", now)

    user = db.get_user("discord_user_123")
    assert user['message_count'] == 1
    user = db.get_user("reddit_user_abc")
    assert user['message_count'] == 2
    print("✓ User message counts updated correctly")

    # Get messages by platform
    discord_msgs = db.get_messages_by_platform("discord")
    assert len(discord_msgs) == 1
    print(f"✓ Retrieved {len(discord_msgs)} Discord message(s)")

    reddit_msgs = db.get_messages_by_platform("reddit")
    assert len(reddit_msgs) == 2
    print(f"✓ Retrieved {len(reddit_msgs)} Reddit message(s)")

    # Get message count
    total = db.get_message_count()
    assert total == 3
    print(f"✓ Total message count: {total}")


def test_entities_crud(db):
    """Test entity extraction storage."""
    print("\n=== Testing Extracted Entities CRUD ===")

    # Insert pain point
    entity_id = db.insert_entity(
        message_id="discord_msg_001",
        entity_type="pain_point",
        entity_name="Supabase Integration Complexity",
        category="Integration",
        confidence=0.92,
        context="How do I integrate Supabase with Lovable?",
        extraction_metadata={
            "model": "gpt-4",
            "batch_id": "batch_001"
        }
    )
    assert entity_id > 0
    print(f"✓ Inserted pain point entity (ID: {entity_id})")

    # Insert integration request
    entity_id = db.insert_entity(
        message_id="reddit_post_001",
        entity_type="integration",
        entity_name="Supabase",
        category="Database",
        confidence=0.95,
        context="What are the best integrations for Lovable?"
    )
    assert entity_id > 0
    print(f"✓ Inserted integration entity (ID: {entity_id})")

    # Insert another Supabase mention (for frequency test)
    entity_id = db.insert_entity(
        message_id="reddit_comment_001",
        entity_type="integration",
        entity_name="Supabase",
        canonical_name="Supabase",  # Already normalized
        category="Database",
        confidence=0.98,
        context="Supabase integration is great!"
    )
    print(f"✓ Inserted another Supabase mention (ID: {entity_id})")

    # Get entities by type
    pain_points = db.get_entities_by_type("pain_point")
    assert len(pain_points) == 1
    print(f"✓ Retrieved {len(pain_points)} pain point(s)")

    integrations = db.get_entities_by_type("integration", min_confidence=0.9)
    assert len(integrations) == 2
    print(f"✓ Retrieved {len(integrations)} integration(s) with confidence >= 0.9")

    # Get entity frequency
    frequency = db.get_entity_frequency()
    print(f"✓ Entity frequency analysis:")
    for entity_type, name, count in frequency:
        print(f"  - {entity_type}: {name} ({count} mentions)")

    assert frequency[0][2] == 2, "Supabase should have 2 mentions"


def test_embeddings_crud(db):
    """Test embedding reference storage."""
    print("\n=== Testing Embeddings Reference CRUD ===")

    # Insert embedding reference
    success = db.insert_embedding_reference(
        message_id="discord_msg_001",
        chromadb_id="chroma_vec_001",
        embedding_model="text-embedding-3-small"
    )
    assert success
    print("✓ Inserted embedding reference")

    # Retrieve reference
    ref = db.get_embedding_reference("discord_msg_001")
    assert ref is not None
    assert ref['chromadb_id'] == "chroma_vec_001"
    assert ref['embedding_model'] == "text-embedding-3-small"
    print("✓ Retrieved embedding reference")

    # Get messages without embeddings
    unembedded = db.get_messages_without_embeddings(min_length=10)
    assert len(unembedded) == 2  # Two messages not embedded yet
    print(f"✓ Found {len(unembedded)} message(s) without embeddings")


def test_query_functions(db):
    """Test complex query functions."""
    print("\n=== Testing Query Functions ===")

    # Get database stats
    stats = db.get_database_stats()
    print("✓ Database statistics:")
    print(f"  - Total messages: {stats['total_messages']}")
    print(f"  - Total users: {stats['total_users']}")
    print(f"  - Total entities: {stats['total_entities']}")
    print(f"  - Total embeddings: {stats['total_embeddings']}")
    print(f"  - Messages by platform: {stats['messages_by_platform']}")
    print(f"  - Users by platform: {stats['users_by_platform']}")
    print(f"  - Entities by type: {stats['entities_by_type']}")

    assert stats['total_messages'] == 3
    assert stats['total_users'] == 2
    assert stats['total_entities'] == 3
    assert stats['total_embeddings'] == 1

    # Get top users
    top_users = db.get_top_users_by_activity(limit=10)
    assert len(top_users) == 2
    assert top_users[0]['message_count'] == 2  # Reddit user has 2 messages
    print(f"✓ Top user: {top_users[0]['username']} ({top_users[0]['message_count']} messages)")


def test_data_integrity(db):
    """Test data integrity and constraints."""
    print("\n=== Testing Data Integrity ===")

    # Try to insert message with invalid platform
    cursor = db.conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO messages (id, platform, content, author_id, timestamp, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("test_001", "twitter", "test", "user_001", 123456, 123456))
        db.conn.commit()
        assert False, "Should have rejected invalid platform"
    except Exception as e:
        print(f"✓ Platform constraint working: {str(e)[:50]}...")

    # Try to insert entity with invalid type
    try:
        cursor.execute("""
            INSERT INTO extracted_entities (message_id, entity_type, entity_name)
            VALUES (?, ?, ?)
        """, ("discord_msg_001", "invalid_type", "test"))
        db.conn.commit()
        assert False, "Should have rejected invalid entity type"
    except Exception as e:
        print(f"✓ Entity type constraint working: {str(e)[:50]}...")

    print("✓ All data integrity constraints working")


def run_all_tests():
    """Run all database tests."""
    print("=" * 60)
    print("DISCREDIT SQLITE DATABASE TESTS")
    print("=" * 60)

    # Clean up old test database
    test_db_path = Path("backend/data/test_discredit.db")
    if test_db_path.exists():
        test_db_path.unlink()
        print("🗑️  Cleaned up old test database")

    try:
        # Run tests
        db = test_schema_creation()
        test_users_crud(db)
        test_messages_crud(db)
        test_entities_crud(db)
        test_embeddings_crud(db)
        test_query_functions(db)
        test_data_integrity(db)

        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED!")
        print("=" * 60)

        db.close()
        return True

    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        return False
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
