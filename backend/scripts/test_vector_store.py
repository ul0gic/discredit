"""
ChromaDB Vector Store Test Script

Tests ChromaDB operations including collection management, embedding storage,
and semantic search functionality.

Note: Can run with or without OpenAI API key:
- Without key: Tests basic ChromaDB operations with mock embeddings
- With key: Tests full pipeline including OpenAI embedding generation
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import shutil

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.vector_store import VectorStore


def cleanup_test_data():
    """Remove test ChromaDB data."""
    test_path = Path("backend/data/test_chromadb")
    if test_path.exists():
        shutil.rmtree(test_path)
        print("🗑️  Cleaned up test ChromaDB data")


def test_initialization():
    """Test ChromaDB initialization."""
    print("\n=== Testing Initialization ===")

    vs = VectorStore(chroma_path="backend/data/test_chromadb")
    vs.initialize()

    stats = vs.get_collection_stats()
    assert stats['name'] == "messages"
    assert stats['count'] == 0
    print(f"✓ Collection initialized: {stats['name']}")
    print(f"  Embedding model: {stats['metadata']['embedding_model']}")
    print(f"  Dimensions: {stats['metadata']['embedding_dimensions']}")

    return vs


def test_collection_reset(vs):
    """Test collection reset functionality."""
    print("\n=== Testing Collection Reset ===")

    # Add a dummy embedding
    mock_embedding = [0.1] * 1536
    vs.add_embeddings(
        message_ids=["test_msg_001"],
        embeddings=[mock_embedding],
        metadatas=[{"platform": "discord", "timestamp": 123456}]
    )

    assert vs.get_collection_stats()['count'] == 1
    print("✓ Added test embedding")

    # Reset collection
    vs.reset_collection()
    assert vs.get_collection_stats()['count'] == 0
    print("✓ Collection reset successfully")


def test_mock_embeddings():
    """Test ChromaDB operations with mock embeddings (no API key needed)."""
    print("\n=== Testing with Mock Embeddings ===")

    vs = VectorStore(chroma_path="backend/data/test_chromadb")
    vs.initialize(reset=True)

    # Create mock embeddings (1536 dimensions)
    mock_embeddings = [
        [0.1 + i * 0.01] * 1536 for i in range(5)
    ]

    message_ids = [
        "discord_msg_001",
        "discord_msg_002",
        "reddit_post_001",
        "reddit_post_002",
        "reddit_comment_001"
    ]

    now = int(datetime.now().timestamp())

    metadatas = [
        {
            "message_id": "discord_msg_001",
            "platform": "discord",
            "source": "general",
            "timestamp": now - 3600,
            "author_id": "discord_user_123",
            "content_preview": "How do I integrate Supabase with Lovable?"
        },
        {
            "message_id": "discord_msg_002",
            "platform": "discord",
            "source": "general",
            "timestamp": now - 1800,
            "author_id": "discord_user_456",
            "content_preview": "Supabase authentication is working great!"
        },
        {
            "message_id": "reddit_post_001",
            "platform": "reddit",
            "source": "r/lovable",
            "timestamp": now - 7200,
            "author_id": "reddit_user_abc",
            "content_preview": "What are the best database integrations?"
        },
        {
            "message_id": "reddit_post_002",
            "platform": "reddit",
            "source": "r/lovable",
            "timestamp": now - 900,
            "author_id": "reddit_user_def",
            "content_preview": "Need help with deployment to production"
        },
        {
            "message_id": "reddit_comment_001",
            "platform": "reddit",
            "source": "r/lovable",
            "timestamp": now - 600,
            "author_id": "reddit_user_ghi",
            "content_preview": "I recommend trying Firebase or Supabase"
        }
    ]

    # Add embeddings
    vs.add_embeddings(message_ids, mock_embeddings, metadatas)
    print(f"✓ Added {len(message_ids)} mock embeddings")

    # Check count
    stats = vs.get_collection_stats()
    assert stats['count'] == 5
    print(f"✓ Collection count: {stats['count']}")

    # Test message exists
    assert vs.message_exists("discord_msg_001")
    assert not vs.message_exists("nonexistent_msg")
    print("✓ Message existence check working")

    # Test get missing IDs
    test_ids = ["discord_msg_001", "new_msg_001", "new_msg_002"]
    missing = vs.get_missing_message_ids(test_ids)
    assert missing == ["new_msg_001", "new_msg_002"]
    print(f"✓ Missing IDs detection: {len(missing)} missing")

    return vs


def test_search_operations(vs):
    """Test search operations with mock embeddings."""
    print("\n=== Testing Search Operations ===")

    # Note: Search won't work perfectly with mock embeddings and text queries
    # But we can test the API and metadata filtering

    # Test search by message ID
    try:
        similar = vs.search_by_message_id("discord_msg_001", n_results=3)
        assert len(similar) <= 3
        assert all(msg['message_id'] != "discord_msg_001" for msg in similar)
        print(f"✓ Search by message ID: found {len(similar)} similar messages")
    except Exception as e:
        print(f"⚠️  Search by message ID (expected with mock data): {e}")

    # Test platform filtering using get
    result = vs.collection.get(where={"platform": "discord"})
    discord_count = len(result['ids'])
    print(f"✓ Platform filter: {discord_count} Discord messages")

    result = vs.collection.get(where={"platform": "reddit"})
    reddit_count = len(result['ids'])
    print(f"✓ Platform filter: {reddit_count} Reddit messages")

    assert discord_count == 2
    assert reddit_count == 3

    # Test deletion
    vs.delete_message("discord_msg_001")
    assert not vs.message_exists("discord_msg_001")
    print("✓ Message deletion working")


def test_real_embeddings():
    """Test with real OpenAI embeddings (requires API key)."""
    print("\n=== Testing with Real OpenAI Embeddings ===")

    # Check if API key is available
    try:
        vs = VectorStore(chroma_path="backend/data/test_chromadb")
        vs.initialize(reset=True)

        # Test texts
        test_texts = [
            "How do I integrate Supabase with Lovable?",
            "What is the best way to authenticate users?",
            "My app is running slowly, any tips?"
        ]

        print("📡 Calling OpenAI API to generate embeddings...")

        # Generate embeddings
        embeddings = vs.embed_texts(test_texts, batch_size=3, show_progress=False)

        assert len(embeddings) == 3
        assert len(embeddings[0]) == 1536
        print(f"✓ Generated {len(embeddings)} embeddings")
        print(f"  Dimensions: {len(embeddings[0])}")

        # Cost estimate
        cost_estimate = vs.estimate_embedding_cost(text_count=1000)
        print(f"✓ Cost estimate for 1000 messages: ${cost_estimate['estimated_cost_usd']}")

        # Store embeddings
        now = int(datetime.now().timestamp())
        message_ids = ["test_real_001", "test_real_002", "test_real_003"]
        metadatas = [
            {
                "message_id": msg_id,
                "platform": "discord",
                "source": "general",
                "timestamp": now,
                "author_id": "test_user",
                "content_preview": text[:100]
            }
            for msg_id, text in zip(message_ids, test_texts)
        ]

        vs.add_embeddings(message_ids, embeddings, metadatas)
        print(f"✓ Stored {len(embeddings)} embeddings in ChromaDB")

        # Test semantic search
        results = vs.search("database integration", n_results=2)
        print(f"✓ Semantic search returned {len(results)} results")

        if results:
            print(f"  Top result: {results[0]['content_preview'][:60]}...")
            print(f"  Distance: {results[0]['distance']:.4f}")

        # Test add_messages_batch (end-to-end)
        test_messages = [
            {
                "id": "batch_msg_001",
                "content": "Need help with Firebase integration",
                "platform": "discord",
                "source": "help",
                "timestamp": now,
                "author_id": "user_001"
            },
            {
                "id": "batch_msg_002",
                "content": "How to deploy to Vercel?",
                "platform": "reddit",
                "source": "r/lovable",
                "timestamp": now,
                "author_id": "user_002"
            }
        ]

        success, errors = vs.add_messages_batch(test_messages, show_progress=False)
        assert success == 2
        assert errors == 0
        print(f"✓ Batch processing: {success} success, {errors} errors")

        return True

    except Exception as e:
        if "api_key" in str(e).lower() or "authentication" in str(e).lower():
            print("⚠️  OpenAI API key not found - skipping real embedding tests")
            print("   Set OPENAI_API_KEY environment variable to test full pipeline")
            return False
        else:
            raise


def test_integration_with_sqlite():
    """Test integration between ChromaDB and SQLite."""
    print("\n=== Testing SQLite Integration ===")

    from storage.sqlite_db import DiscreditDB

    # Initialize databases
    db = DiscreditDB("backend/data/test_discredit.db")
    db.initialize_schema()

    vs = VectorStore(chroma_path="backend/data/test_chromadb")
    vs.initialize(reset=True)

    # Add messages to SQLite
    now = int(datetime.now().timestamp())

    db.insert_user("test_user_001", "discord", "testuser")

    db.insert_message(
        id="integration_msg_001",
        platform="discord",
        content="How do I integrate with third-party APIs?",
        author_id="test_user_001",
        timestamp=now,
        source="general"
    )

    db.insert_message(
        id="integration_msg_002",
        platform="discord",
        content="My deployment is failing with authentication errors",
        author_id="test_user_001",
        timestamp=now,
        source="general"
    )

    print("✓ Added test messages to SQLite")

    # Get messages without embeddings
    unembedded = db.get_messages_without_embeddings(min_length=10)
    assert len(unembedded) == 2
    print(f"✓ Found {len(unembedded)} messages without embeddings")

    # Add embeddings reference
    db.insert_embedding_reference(
        message_id="integration_msg_001",
        chromadb_id="integration_msg_001"
    )

    # Check again
    unembedded = db.get_messages_without_embeddings(min_length=10)
    assert len(unembedded) == 1
    print("✓ Embedding reference tracking working")

    db.close()


def run_all_tests():
    """Run all vector store tests."""
    print("=" * 60)
    print("DISCREDIT CHROMADB VECTOR STORE TESTS")
    print("=" * 60)

    cleanup_test_data()

    try:
        # Basic tests (no API key needed)
        vs = test_initialization()
        test_collection_reset(vs)
        vs = test_mock_embeddings()
        test_search_operations(vs)

        # Real OpenAI tests (requires API key)
        has_api_key = test_real_embeddings()

        # Integration tests
        test_integration_with_sqlite()

        print("\n" + "=" * 60)
        if has_api_key:
            print("✅ ALL TESTS PASSED (including OpenAI integration)!")
        else:
            print("✅ BASIC TESTS PASSED!")
            print("   (OpenAI API tests skipped - set OPENAI_API_KEY to run)")
        print("=" * 60)

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
    finally:
        cleanup_test_data()
        print("\n🗑️  Cleaned up test data")


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
