"""
Message Clustering Module - Discredit Phase 3.5

This module handles clustering of embedded messages to discover topics, pain points,
and patterns automatically using semantic similarity.

Clustering Methods:
------------------
1. HDBSCAN: Density-based clustering that automatically discovers cluster count
2. K-Means: Centroid-based clustering with specified K
3. UMAP + Clustering: Dimensionality reduction before clustering

Usage:
------
# Basic usage - run HDBSCAN
python -m analysis.clusterer --method hdbscan

# Try K-Means with specific K
python -m analysis.clusterer --method kmeans --k 40

# Compare multiple methods
python -m analysis.clusterer --method all
"""

import sys
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import argparse
import json

# Add backend to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.sqlite_db import DiscreditDB
from storage.vector_store import VectorStore
from config import Config

# Clustering libraries
from sklearn.cluster import KMeans, MiniBatchKMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score
from sklearn.preprocessing import normalize
import hdbscan
import umap


class MessageClusterer:
    """
    Handles clustering of message embeddings using multiple methods.

    Provides comparison, evaluation, and storage of clustering results.
    """

    def __init__(
        self,
        sqlite_path: Optional[str] = None,
        chromadb_path: Optional[str] = None,
    ):
        """
        Initialize clusterer with database connections.

        Args:
            sqlite_path: Path to SQLite database
            chromadb_path: Path to ChromaDB storage
        """
        self.sqlite_path = sqlite_path or str(Config.SQLITE_DB_PATH)
        self.chromadb_path = chromadb_path or str(Config.CHROMADB_PATH)

        self.db = DiscreditDB(self.sqlite_path)
        self.vector_store = VectorStore(self.chromadb_path)

        # Data containers
        self.embeddings = None
        self.message_ids = None
        self.messages = None

        # Results
        self.results = {}

    def load_embeddings(self) -> Tuple[np.ndarray, List[str]]:
        """
        Load all embeddings from ChromaDB.

        Returns:
            Tuple of (embeddings array, message_ids list)
        """
        print("📥 Loading embeddings from ChromaDB...")
        self.vector_store.initialize()

        # Get all vectors from collection
        collection = self.vector_store.collection
        all_data = collection.get(
            include=["embeddings", "metadatas"]
        )

        self.message_ids = all_data['ids']
        self.embeddings = np.array(all_data['embeddings'])

        print(f"   Loaded {len(self.message_ids):,} embeddings")
        print(f"   Embedding dimensions: {self.embeddings.shape[1]}")

        return self.embeddings, self.message_ids

    def load_messages(self, message_ids: List[str]) -> Dict[str, Dict]:
        """
        Load message metadata from SQLite.

        Args:
            message_ids: List of message IDs to load

        Returns:
            Dictionary mapping message_id to message data
        """
        print("📥 Loading message metadata from SQLite...")

        messages = {}
        for msg_id in message_ids:
            msg = self.db.get_message(msg_id)
            if msg:
                messages[msg_id] = msg

        self.messages = messages
        print(f"   Loaded {len(messages):,} messages")
        return messages

    # ==================== CLUSTERING METHODS ====================

    def cluster_hdbscan(
        self,
        min_cluster_size: int = 25,
        min_samples: int = 10,
        metric: str = 'euclidean'
    ) -> Dict[str, Any]:
        """
        Cluster using HDBSCAN (density-based, auto-discovers cluster count).

        Args:
            min_cluster_size: Minimum messages per cluster
            min_samples: How conservative clustering is
            metric: Distance metric (euclidean after normalization = cosine)

        Returns:
            Clustering results dictionary
        """
        print(f"\n🔬 Running HDBSCAN clustering...")
        print(f"   min_cluster_size={min_cluster_size}, min_samples={min_samples}")
        print(f"   metric={metric} (on normalized embeddings = cosine similarity)")

        # Normalize embeddings for cosine distance (use euclidean on normalized = cosine)
        embeddings_normalized = normalize(self.embeddings, norm='l2')

        # Cluster
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            metric=metric,
            cluster_selection_method='eom',
            prediction_data=True
        )

        labels = clusterer.fit_predict(embeddings_normalized)

        # Calculate metrics (excluding noise points labeled as -1)
        mask = labels != -1
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise = np.sum(labels == -1)

        results = {
            'method': 'hdbscan',
            'parameters': {
                'min_cluster_size': min_cluster_size,
                'min_samples': min_samples,
                'metric': metric
            },
            'labels': labels,
            'n_clusters': n_clusters,
            'n_noise': n_noise,
            'n_samples': len(labels),
            'clusterer': clusterer
        }

        # Quality metrics (only for non-noise points)
        if n_clusters > 1 and np.sum(mask) > 0:
            results['silhouette_score'] = silhouette_score(
                embeddings_normalized[mask],
                labels[mask],
                metric='euclidean',
                sample_size=min(10000, np.sum(mask))
            )
            results['calinski_harabasz_score'] = calinski_harabasz_score(
                embeddings_normalized[mask],
                labels[mask]
            )
            results['davies_bouldin_score'] = davies_bouldin_score(
                embeddings_normalized[mask],
                labels[mask]
            )

        # Cluster size distribution
        unique, counts = np.unique(labels[labels != -1], return_counts=True)
        results['cluster_sizes'] = dict(zip(unique.tolist(), counts.tolist()))

        print(f"   ✅ Found {n_clusters} clusters, {n_noise} noise points")
        if 'silhouette_score' in results:
            print(f"   📊 Silhouette: {results['silhouette_score']:.3f}")

        return results

    def cluster_kmeans(
        self,
        k: int = 30,
        use_minibatch: bool = True
    ) -> Dict[str, Any]:
        """
        Cluster using K-Means (requires specifying K).

        Args:
            k: Number of clusters
            use_minibatch: Use MiniBatchKMeans for speed

        Returns:
            Clustering results dictionary
        """
        print(f"\n🔬 Running K-Means clustering (k={k})...")

        # Normalize embeddings
        embeddings_normalized = normalize(self.embeddings, norm='l2')

        # Cluster
        if use_minibatch and len(embeddings_normalized) > 10000:
            clusterer = MiniBatchKMeans(
                n_clusters=k,
                random_state=42,
                batch_size=1000,
                n_init=10
            )
        else:
            clusterer = KMeans(
                n_clusters=k,
                random_state=42,
                n_init=10
            )

        labels = clusterer.fit_predict(embeddings_normalized)

        # Calculate metrics
        results = {
            'method': 'kmeans',
            'parameters': {
                'k': k,
                'use_minibatch': use_minibatch
            },
            'labels': labels,
            'n_clusters': k,
            'n_noise': 0,
            'n_samples': len(labels),
            'clusterer': clusterer,
            'inertia': clusterer.inertia_
        }

        # Quality metrics
        sample_size = min(10000, len(embeddings_normalized))
        results['silhouette_score'] = silhouette_score(
            embeddings_normalized,
            labels,
            metric='cosine',
            sample_size=sample_size
        )
        results['calinski_harabasz_score'] = calinski_harabasz_score(
            embeddings_normalized,
            labels
        )
        results['davies_bouldin_score'] = davies_bouldin_score(
            embeddings_normalized,
            labels
        )

        # Cluster size distribution
        unique, counts = np.unique(labels, return_counts=True)
        results['cluster_sizes'] = dict(zip(unique.tolist(), counts.tolist()))

        print(f"   ✅ Created {k} clusters")
        print(f"   📊 Silhouette: {results['silhouette_score']:.3f}")
        print(f"   📊 Inertia: {results['inertia']:.2f}")

        return results

    def cluster_umap_hdbscan(
        self,
        n_components: int = 50,
        min_cluster_size: int = 25,
        min_samples: int = 10
    ) -> Dict[str, Any]:
        """
        Reduce dimensionality with UMAP, then cluster with HDBSCAN.

        Args:
            n_components: UMAP target dimensions
            min_cluster_size: HDBSCAN parameter
            min_samples: HDBSCAN parameter

        Returns:
            Clustering results dictionary
        """
        print(f"\n🔬 Running UMAP + HDBSCAN clustering...")
        print(f"   UMAP: {self.embeddings.shape[1]}D → {n_components}D")

        # UMAP dimensionality reduction
        print("   Running UMAP...")
        reducer = umap.UMAP(
            n_components=n_components,
            metric='cosine',
            random_state=42,
            n_neighbors=15,
            min_dist=0.0
        )
        embeddings_reduced = reducer.fit_transform(self.embeddings)

        print(f"   Running HDBSCAN on reduced embeddings...")
        # Cluster in reduced space
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            metric='euclidean',  # Use euclidean after UMAP
            cluster_selection_method='eom'
        )

        labels = clusterer.fit_predict(embeddings_reduced)

        # Calculate metrics
        mask = labels != -1
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise = np.sum(labels == -1)

        results = {
            'method': 'umap_hdbscan',
            'parameters': {
                'n_components': n_components,
                'min_cluster_size': min_cluster_size,
                'min_samples': min_samples
            },
            'labels': labels,
            'n_clusters': n_clusters,
            'n_noise': n_noise,
            'n_samples': len(labels),
            'clusterer': clusterer,
            'reducer': reducer,
            'embeddings_reduced': embeddings_reduced
        }

        # Quality metrics (only for non-noise points)
        if n_clusters > 1 and np.sum(mask) > 0:
            results['silhouette_score'] = silhouette_score(
                embeddings_reduced[mask],
                labels[mask],
                metric='euclidean',
                sample_size=min(10000, np.sum(mask))
            )
            results['calinski_harabasz_score'] = calinski_harabasz_score(
                embeddings_reduced[mask],
                labels[mask]
            )
            results['davies_bouldin_score'] = davies_bouldin_score(
                embeddings_reduced[mask],
                labels[mask]
            )

        # Cluster size distribution
        unique, counts = np.unique(labels[labels != -1], return_counts=True)
        results['cluster_sizes'] = dict(zip(unique.tolist(), counts.tolist()))

        print(f"   ✅ Found {n_clusters} clusters, {n_noise} noise points")
        if 'silhouette_score' in results:
            print(f"   📊 Silhouette: {results['silhouette_score']:.3f}")

        return results

    # ==================== ANALYSIS & EVALUATION ====================

    def compare_k_values(self, k_values: List[int] = [20, 30, 40, 50, 75]):
        """
        Run K-Means with multiple K values and compare.

        Args:
            k_values: List of K values to try
        """
        print(f"\n🔬 Comparing K-Means with K = {k_values}")

        for k in k_values:
            result = self.cluster_kmeans(k=k)
            self.results[f'kmeans_k{k}'] = result

        # Print comparison
        print("\n" + "="*80)
        print("K-MEANS COMPARISON")
        print("="*80)
        print(f"{'K':<8} {'Silhouette':<12} {'Calinski-H':<15} {'Davies-B':<12} {'Inertia':<15}")
        print("-"*80)

        for k in k_values:
            result = self.results[f'kmeans_k{k}']
            print(f"{k:<8} {result['silhouette_score']:<12.4f} "
                  f"{result['calinski_harabasz_score']:<15.2f} "
                  f"{result['davies_bouldin_score']:<12.4f} "
                  f"{result['inertia']:<15.2f}")

        print("="*80)
        print("📊 Higher Silhouette & Calinski-Harabasz = Better")
        print("📊 Lower Davies-Bouldin = Better")
        print("="*80)

    def analyze_cluster_samples(
        self,
        result_key: str,
        samples_per_cluster: int = 5,
        output_file: Optional[str] = None
    ):
        """
        Extract sample messages from each cluster for inspection.

        Args:
            result_key: Which clustering result to analyze
            samples_per_cluster: Number of samples per cluster
            output_file: Optional JSON output file
        """
        if result_key not in self.results:
            print(f"❌ Result '{result_key}' not found")
            return

        result = self.results[result_key]
        labels = result['labels']

        print(f"\n📊 Analyzing clusters from '{result_key}'")

        cluster_samples = {}

        for cluster_id in sorted(set(labels)):
            if cluster_id == -1:
                continue  # Skip noise

            # Get message IDs in this cluster
            mask = labels == cluster_id
            cluster_msg_ids = [self.message_ids[i] for i in np.where(mask)[0]]

            # Sample random messages
            sample_size = min(samples_per_cluster, len(cluster_msg_ids))
            sampled_ids = np.random.choice(cluster_msg_ids, sample_size, replace=False)

            # Get message content
            samples = []
            for msg_id in sampled_ids:
                if msg_id in self.messages:
                    msg = self.messages[msg_id]
                    samples.append({
                        'id': msg_id,
                        'content': msg['content'][:200],  # First 200 chars
                        'platform': msg['platform'],
                        'source': msg.get('source', '')
                    })

            cluster_samples[int(cluster_id)] = {
                'size': int(np.sum(mask)),
                'samples': samples
            }

        # Print summary
        print(f"\n{'Cluster':<10} {'Size':<10} {'Sample Messages'}")
        print("-"*80)

        for cluster_id in sorted(cluster_samples.keys())[:10]:  # Show first 10
            cluster = cluster_samples[cluster_id]
            print(f"\nCluster {cluster_id:<3} ({cluster['size']} messages)")
            for i, sample in enumerate(cluster['samples'][:3], 1):
                print(f"  {i}. [{sample['platform']}] {sample['content'][:80]}...")

        # Save to file if requested
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, 'w') as f:
                json.dump(cluster_samples, f, indent=2)

            print(f"\n💾 Saved cluster samples to: {output_file}")

        return cluster_samples

    def save_clustering_to_db(
        self,
        result_key: str,
        table_suffix: str = ''
    ):
        """
        Save clustering results to SQLite.

        Creates a new table with message_id and cluster_id mapping.

        Args:
            result_key: Which clustering result to save
            table_suffix: Optional suffix for table name
        """
        if result_key not in self.results:
            print(f"❌ Result '{result_key}' not found")
            return

        result = self.results[result_key]
        labels = result['labels']

        table_name = f"message_clusters{table_suffix}"

        print(f"\n💾 Saving clustering to SQLite table '{table_name}'...")

        # Create table
        cursor = self.db.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                message_id TEXT PRIMARY KEY,
                cluster_id INTEGER NOT NULL,
                method TEXT,
                parameters TEXT,
                created_at INTEGER,
                FOREIGN KEY (message_id) REFERENCES messages(id)
            )
        """)

        # Insert cluster assignments
        now = int(datetime.now().timestamp())
        params_json = json.dumps(result['parameters'])

        data = [
            (self.message_ids[i], int(labels[i]), result['method'], params_json, now)
            for i in range(len(labels))
        ]

        cursor.executemany(f"""
            INSERT OR REPLACE INTO {table_name}
            (message_id, cluster_id, method, parameters, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, data)

        self.db.conn.commit()

        print(f"   ✅ Saved {len(data):,} cluster assignments")
        print(f"   Table: {table_name}")


def main():
    """Command-line interface for clustering."""
    parser = argparse.ArgumentParser(
        description="Cluster embedded messages to discover topics and patterns"
    )
    parser.add_argument(
        '--method',
        type=str,
        choices=['hdbscan', 'kmeans', 'umap_hdbscan', 'compare_kmeans', 'all'],
        default='hdbscan',
        help='Clustering method to use'
    )
    parser.add_argument(
        '--min-cluster-size',
        type=int,
        default=25,
        help='HDBSCAN: minimum cluster size'
    )
    parser.add_argument(
        '--min-samples',
        type=int,
        default=10,
        help='HDBSCAN: minimum samples'
    )
    parser.add_argument(
        '--k',
        type=int,
        default=30,
        help='K-Means: number of clusters'
    )
    parser.add_argument(
        '--k-values',
        type=str,
        default='20,30,40,50,75',
        help='Comma-separated K values for comparison'
    )
    parser.add_argument(
        '--save',
        type=str,
        default=None,
        help='Save clustering to database with table suffix'
    )
    parser.add_argument(
        '--export-samples',
        type=str,
        default=None,
        help='Export cluster samples to JSON file'
    )

    args = parser.parse_args()

    print("🎯 DISCREDIT MESSAGE CLUSTERING")
    print("="*80 + "\n")

    # Initialize
    clusterer = MessageClusterer()

    # Load data
    embeddings, message_ids = clusterer.load_embeddings()
    messages = clusterer.load_messages(message_ids)

    # Run clustering
    if args.method == 'hdbscan' or args.method == 'all':
        result = clusterer.cluster_hdbscan(
            min_cluster_size=args.min_cluster_size,
            min_samples=args.min_samples
        )
        clusterer.results['hdbscan'] = result

    if args.method == 'kmeans':
        result = clusterer.cluster_kmeans(k=args.k)
        clusterer.results[f'kmeans_k{args.k}'] = result

    if args.method == 'compare_kmeans' or args.method == 'all':
        k_values = [int(k.strip()) for k in args.k_values.split(',')]
        clusterer.compare_k_values(k_values)

    if args.method == 'umap_hdbscan' or args.method == 'all':
        result = clusterer.cluster_umap_hdbscan(
            min_cluster_size=args.min_cluster_size,
            min_samples=args.min_samples
        )
        clusterer.results['umap_hdbscan'] = result

    # Export samples
    if args.export_samples and clusterer.results:
        best_result = list(clusterer.results.keys())[0]
        clusterer.analyze_cluster_samples(
            best_result,
            samples_per_cluster=10,
            output_file=args.export_samples
        )

    # Save to database
    if args.save and clusterer.results:
        best_result = list(clusterer.results.keys())[0]
        clusterer.save_clustering_to_db(best_result, table_suffix=args.save)

    # Clean up
    clusterer.db.close()

    print("\n✅ Clustering complete!")


if __name__ == "__main__":
    main()
