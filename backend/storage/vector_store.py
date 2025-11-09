"""
ChromaDB Vector Store Module - Discredit Semantic Search

This module provides the ChromaDB vector store wrapper for semantic search
capabilities on message embeddings.

Architecture Decisions:
--------------------
1. Persistent Storage: ChromaDB data stored in backend/data/chromadb/
2. Collection Design: Single collection 'messages' with platform and source metadata
3. Embedding Model: OpenAI text-embedding-3-small (1536 dimensions, cost-effective)
4. Metadata Schema: platform, source, timestamp, author_id for filtering
5. Integration: Message IDs link to SQLite for full message retrieval

Collection Metadata Schema:
--------------------------
- message_id: Prefixed message ID (links to SQLite)
- platform: 'discord' or 'reddit'
- source: Channel or subreddit name
- timestamp: Unix timestamp for temporal filtering
- author_id: User ID for author-based searches
- content_preview: First 100 chars for quick reference

Usage:
------
# Initialize
vs = VectorStore()
vs.initialize()

# Embed messages
embeddings = vs.embed_texts(["message 1", "message 2"])
vs.add_embeddings(message_ids, embeddings, metadatas)

# Search
results = vs.search("How to integrate Supabase?", n_results=10)
"""

import chromadb
from chromadb.config import Settings
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import openai
from openai import OpenAI
import time
from tqdm import tqdm


class VectorStore:
    """
    ChromaDB vector store wrapper for semantic search on messages.

    Provides embedding generation, storage, and semantic search functionality
    with metadata filtering support.
    """

    COLLECTION_NAME = "messages"
    EMBEDDING_MODEL = "text-embedding-3-small"
    EMBEDDING_DIMENSIONS = 1536

    def __init__(
        self,
        chroma_path: str = "backend/data/chromadb",
        openai_api_key: Optional[str] = None
    ):
        """
        Initialize ChromaDB vector store.

        Args:
            chroma_path: Path to ChromaDB persistent storage
            openai_api_key: OpenAI API key (optional, can be set via OPENAI_API_KEY env var)
        """
        self.chroma_path = Path(chroma_path)
        self.chroma_path.mkdir(parents=True, exist_ok=True)

        # Initialize ChromaDB persistent client
        self.client = chromadb.PersistentClient(
            path=str(self.chroma_path),
            settings=Settings(
                anonymized_telemetry=False,
                allow_reset=True
            )
        )

        # Initialize OpenAI client (lazy-loaded on first use)
        self.openai_api_key = openai_api_key
        self._openai_client = None

        self.collection = None

    @property
    def openai_client(self):
        """Lazy-load OpenAI client on first use."""
        if self._openai_client is None:
            self._openai_client = OpenAI(api_key=self.openai_api_key) if self.openai_api_key else OpenAI()
        return self._openai_client

    def initialize(self, reset: bool = False):
        """
        Initialize or get the messages collection.

        Args:
            reset: If True, delete existing collection and create new one
        """
        if reset and self.COLLECTION_NAME in [c.name for c in self.client.list_collections()]:
            self.client.delete_collection(self.COLLECTION_NAME)
            print(f"🗑️  Deleted existing collection '{self.COLLECTION_NAME}'")

        # Get or create collection
        self.collection = self.client.get_or_create_collection(
            name=self.COLLECTION_NAME,
            metadata={
                "description": "Discredit message embeddings for semantic search",
                "embedding_model": self.EMBEDDING_MODEL,
                "embedding_dimensions": self.EMBEDDING_DIMENSIONS
            }
        )

        print(f"✓ Initialized collection '{self.COLLECTION_NAME}' ({self.collection.count()} vectors)")

    # ==================== EMBEDDING GENERATION ====================

    def embed_texts(
        self,
        texts: List[str],
        batch_size: int = 100,
        show_progress: bool = True,
        retry_on_failure: bool = True
    ) -> List[List[float]]:
        """
        Generate embeddings for a list of texts using OpenAI API.

        Args:
            texts: List of text strings to embed
            batch_size: Number of texts to embed per API call
            show_progress: Show progress bar
            retry_on_failure: Retry failed batches with exponential backoff

        Returns:
            List of embedding vectors (1536 dimensions each)
        """
        if not texts:
            return []

        all_embeddings = []

        # Process in batches
        batches = [texts[i:i + batch_size] for i in range(0, len(texts), batch_size)]

        iterator = tqdm(batches, desc="Generating embeddings") if show_progress else batches

        for batch in iterator:
            embeddings = self._embed_batch(batch, retry_on_failure)
            all_embeddings.extend(embeddings)

        return all_embeddings

    def _embed_batch(
        self,
        texts: List[str],
        retry: bool = True,
        max_retries: int = 3
    ) -> List[List[float]]:
        """
        Embed a single batch with retry logic.

        Args:
            texts: Batch of texts to embed
            retry: Enable retry on failure
            max_retries: Maximum number of retry attempts

        Returns:
            List of embedding vectors
        """
        for attempt in range(max_retries):
            try:
                response = self.openai_client.embeddings.create(
                    model=self.EMBEDDING_MODEL,
                    input=texts,
                    encoding_format="float"
                )

                # Extract embeddings in order
                embeddings = [item.embedding for item in response.data]
                return embeddings

            except openai.RateLimitError as e:
                if not retry or attempt == max_retries - 1:
                    raise

                # Exponential backoff
                wait_time = 2 ** attempt
                print(f"⚠️  Rate limit hit, waiting {wait_time}s before retry...")
                time.sleep(wait_time)

            except openai.APIError as e:
                if not retry or attempt == max_retries - 1:
                    raise

                wait_time = 2 ** attempt
                print(f"⚠️  API error: {e}, waiting {wait_time}s before retry...")
                time.sleep(wait_time)

        raise Exception(f"Failed to embed batch after {max_retries} attempts")

    def estimate_embedding_cost(self, text_count: int, avg_tokens_per_text: int = 50) -> Dict[str, float]:
        """
        Estimate OpenAI API cost for embedding generation.

        Args:
            text_count: Number of texts to embed
            avg_tokens_per_text: Average tokens per text (default: 50)

        Returns:
            Dictionary with cost estimates
        """
        # text-embedding-3-small pricing: $0.020 per 1M tokens
        total_tokens = text_count * avg_tokens_per_text
        cost_per_million = 0.020
        estimated_cost = (total_tokens / 1_000_000) * cost_per_million

        return {
            "text_count": text_count,
            "estimated_tokens": total_tokens,
            "cost_per_million_tokens": cost_per_million,
            "estimated_cost_usd": round(estimated_cost, 4)
        }

    # ==================== STORAGE OPERATIONS ====================

    def add_embeddings(
        self,
        message_ids: List[str],
        embeddings: List[List[float]],
        metadatas: List[Dict[str, Any]]
    ):
        """
        Add embeddings to ChromaDB collection.

        Args:
            message_ids: List of message IDs (used as ChromaDB IDs)
            embeddings: List of embedding vectors
            metadatas: List of metadata dictionaries
        """
        if not message_ids:
            return

        assert len(message_ids) == len(embeddings) == len(metadatas), \
            "message_ids, embeddings, and metadatas must have the same length"

        # ChromaDB requires documents (we'll use content_preview from metadata)
        documents = [meta.get("content_preview", "") for meta in metadatas]

        # Add to collection
        self.collection.add(
            ids=message_ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents
        )

    def add_messages_batch(
        self,
        messages: List[Dict[str, Any]],
        batch_size: int = 100,
        show_progress: bool = True
    ) -> Tuple[int, int]:
        """
        Embed and store a batch of messages.

        Args:
            messages: List of message dictionaries from SQLite
                      (must have: id, content, platform, source, timestamp, author_id)
            batch_size: Embedding batch size
            show_progress: Show progress bar

        Returns:
            Tuple of (success_count, error_count)
        """
        if not messages:
            return 0, 0

        # Prepare data
        message_ids = [msg['id'] for msg in messages]
        texts = [msg['content'] for msg in messages]
        metadatas = [
            {
                "message_id": msg['id'],
                "platform": msg['platform'],
                "source": msg.get('source', ''),
                "timestamp": msg['timestamp'],
                "author_id": msg['author_id'],
                "content_preview": msg['content'][:100]
            }
            for msg in messages
        ]

        try:
            # Generate embeddings
            embeddings = self.embed_texts(texts, batch_size, show_progress)

            # Store in ChromaDB
            self.add_embeddings(message_ids, embeddings, metadatas)

            return len(messages), 0

        except Exception as e:
            print(f"❌ Error adding batch: {e}")
            return 0, len(messages)

    # ==================== SEARCH OPERATIONS ====================

    def search(
        self,
        query_text: str,
        n_results: int = 10,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Semantic search for similar messages.

        Args:
            query_text: Search query text
            n_results: Number of results to return
            where: Metadata filter (e.g., {"platform": "discord"})
            where_document: Document content filter

        Returns:
            List of result dictionaries with message_id, distance, and metadata
        """
        if not self.collection:
            raise ValueError("Collection not initialized. Call initialize() first.")

        # Generate query embedding
        query_embedding = self._embed_batch([query_text], retry=True)[0]

        # Search collection
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results,
            where=where,
            where_document=where_document
        )

        # Format results
        formatted_results = []
        for i in range(len(results['ids'][0])):
            formatted_results.append({
                'message_id': results['ids'][0][i],
                'distance': results['distances'][0][i],
                'metadata': results['metadatas'][0][i],
                'content_preview': results['documents'][0][i]
            })

        return formatted_results

    def search_by_message_id(
        self,
        message_id: str,
        n_results: int = 10,
        where: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Find messages similar to a given message.

        Args:
            message_id: Source message ID
            n_results: Number of similar messages to return
            where: Metadata filter

        Returns:
            List of similar messages (excludes source message)
        """
        if not self.collection:
            raise ValueError("Collection not initialized. Call initialize() first.")

        # Get the message's embedding
        result = self.collection.get(
            ids=[message_id],
            include=["embeddings"]
        )

        if not result['embeddings']:
            raise ValueError(f"Message ID {message_id} not found in collection")

        source_embedding = result['embeddings'][0]

        # Search for similar messages (will include source message)
        results = self.collection.query(
            query_embeddings=[source_embedding],
            n_results=n_results + 1,  # +1 to account for source message
            where=where
        )

        # Format and filter out source message
        formatted_results = []
        for i in range(len(results['ids'][0])):
            msg_id = results['ids'][0][i]
            if msg_id == message_id:
                continue  # Skip source message

            formatted_results.append({
                'message_id': msg_id,
                'distance': results['distances'][0][i],
                'metadata': results['metadatas'][0][i],
                'content_preview': results['documents'][0][i]
            })

        return formatted_results[:n_results]

    def search_by_platform(
        self,
        query_text: str,
        platform: str,
        n_results: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search within a specific platform.

        Args:
            query_text: Search query
            platform: 'discord' or 'reddit'
            n_results: Number of results

        Returns:
            List of results from specified platform
        """
        return self.search(
            query_text=query_text,
            n_results=n_results,
            where={"platform": platform}
        )

    def search_by_timerange(
        self,
        query_text: str,
        start_timestamp: int,
        end_timestamp: int,
        n_results: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search within a time range.

        Args:
            query_text: Search query
            start_timestamp: Start Unix timestamp (inclusive)
            end_timestamp: End Unix timestamp (inclusive)
            n_results: Number of results

        Returns:
            List of results within time range
        """
        return self.search(
            query_text=query_text,
            n_results=n_results,
            where={
                "$and": [
                    {"timestamp": {"$gte": start_timestamp}},
                    {"timestamp": {"$lte": end_timestamp}}
                ]
            }
        )

    # ==================== UTILITY METHODS ====================

    def get_collection_stats(self) -> Dict[str, Any]:
        """
        Get collection statistics.

        Returns:
            Dictionary with count and metadata
        """
        if not self.collection:
            return {"error": "Collection not initialized"}

        count = self.collection.count()
        metadata = self.collection.metadata

        return {
            "name": self.COLLECTION_NAME,
            "count": count,
            "metadata": metadata
        }

    def message_exists(self, message_id: str) -> bool:
        """
        Check if a message has been embedded.

        Args:
            message_id: Message ID to check

        Returns:
            True if message exists in collection
        """
        if not self.collection:
            return False

        result = self.collection.get(ids=[message_id])
        return len(result['ids']) > 0

    def get_missing_message_ids(self, message_ids: List[str]) -> List[str]:
        """
        Get list of message IDs that haven't been embedded yet.

        Args:
            message_ids: List of message IDs to check

        Returns:
            List of message IDs not in collection
        """
        if not self.collection:
            return message_ids

        # Check in batches to avoid API limits
        batch_size = 1000
        missing = []

        for i in range(0, len(message_ids), batch_size):
            batch = message_ids[i:i + batch_size]
            result = self.collection.get(ids=batch)
            existing = set(result['ids'])
            missing.extend([msg_id for msg_id in batch if msg_id not in existing])

        return missing

    def delete_message(self, message_id: str):
        """
        Delete a message embedding from the collection.

        Args:
            message_id: Message ID to delete
        """
        if not self.collection:
            raise ValueError("Collection not initialized")

        self.collection.delete(ids=[message_id])

    def reset_collection(self):
        """
        Delete and recreate the collection (removes all data).
        """
        self.initialize(reset=True)
