"""
Quick test of HDBSCAN clustering on a small sample of messages.
"""
import sys
import json
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.clusterer import MessageClusterer

def main():
    print("🎯 HDBSCAN CLUSTERING - LIGHT TEST (100 messages)")
    print("="*80 + "\n")

    # Initialize clusterer
    clusterer = MessageClusterer()

    # Load embeddings (limit to 100)
    print("📥 Loading first 100 embeddings...")
    embeddings, message_ids = clusterer.load_embeddings()

    # Take only first 100
    embeddings = embeddings[:100]
    message_ids = message_ids[:100]

    clusterer.embeddings = embeddings
    clusterer.message_ids = message_ids

    print(f"   Loaded {len(embeddings)} embeddings")
    print(f"   Embedding dimensions: {embeddings.shape[1]}")

    # Load corresponding messages
    print("\n📥 Loading message metadata...")
    messages = clusterer.load_messages(message_ids)

    # Run HDBSCAN with smaller min_cluster_size for 100 samples
    print("\n🔬 Running HDBSCAN clustering...")
    print("   Using min_cluster_size=5 (smaller for 100 samples)")

    result = clusterer.cluster_hdbscan(
        min_cluster_size=5,  # Smaller for test
        min_samples=3
    )

    # Store result for saving
    clusterer.results['hdbscan_test'] = result

    # Print results
    print("\n" + "="*80)
    print("📊 CLUSTERING RESULTS")
    print("="*80)
    print(f"Total messages:     {result['n_samples']}")
    print(f"Clusters found:     {result['n_clusters']}")
    print(f"Noise points:       {result['n_noise']} ({result['n_noise']/result['n_samples']*100:.1f}%)")
    print(f"Silhouette score:   {result.get('silhouette_score', 'N/A')}")

    # Organize results by cluster
    labels = result['labels']
    export_data = {
        'summary': {
            'total_messages': int(result['n_samples']),
            'clusters_found': int(result['n_clusters']),
            'noise_points': int(result['n_noise']),
            'noise_percentage': float(result['n_noise']/result['n_samples']*100),
            'silhouette_score': float(result.get('silhouette_score')) if result.get('silhouette_score') else None
        },
        'noise_points': [],
        'clusters': {}
    }

    # Collect noise points
    for i, label in enumerate(labels):
        msg_id = message_ids[i]
        msg = messages.get(msg_id, {})

        msg_data = {
            'message_id': msg_id,
            'platform': msg.get('platform'),
            'content': msg.get('content', '').strip(),
            'author_id': msg.get('author_id'),
            'timestamp': int(msg.get('timestamp')) if msg.get('timestamp') else None
        }

        if label == -1:
            export_data['noise_points'].append(msg_data)
        else:
            cluster_key = f'cluster_{label}'
            if cluster_key not in export_data['clusters']:
                export_data['clusters'][cluster_key] = []
            export_data['clusters'][cluster_key].append(msg_data)

    # Save to JSON
    output_path = Path(__file__).parent.parent / 'reports' / 'test_clustering_100.json'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)

    print(f"\n💾 Results saved to: {output_path}")
    print(f"   - {len(export_data['noise_points'])} noise points")
    print(f"   - {len(export_data['clusters'])} clusters")
    for cluster_key, cluster_msgs in export_data['clusters'].items():
        print(f"     • {cluster_key}: {len(cluster_msgs)} messages")

    if result['n_clusters'] > 0:
        print("\n✅ Clustering succeeded! Ready to run on full dataset.")
    else:
        print("\n⚠️  No clusters found - may need to adjust parameters")

    print("="*80)

    # Save to database
    print("\n💾 Saving to database for validation...")
    clustering_run_id = clusterer.save_clustering_to_db('hdbscan_test')

    # Validate database save
    print("\n🔍 Validating database save...")

    # Check clustering_runs table
    run_data = clusterer.db.get_clustering_run(clustering_run_id)
    print(f"   ✓ clustering_runs: Run #{clustering_run_id}")
    print(f"     Method: {run_data['method']}")
    print(f"     Parameters: {run_data['parameters']}")
    print(f"     Clusters: {run_data['n_clusters']}, Noise: {run_data['n_noise']}")

    # Check message_clusters table
    cursor = clusterer.db.conn.cursor()
    cursor.execute("""
        SELECT cluster_id, COUNT(*) as count
        FROM message_clusters
        WHERE clustering_run_id = ?
        GROUP BY cluster_id
        ORDER BY cluster_id
    """, (clustering_run_id,))

    cluster_counts = cursor.fetchall()
    print(f"\n   ✓ message_clusters: {len(cluster_counts)} cluster groups")
    for cluster_id, count in cluster_counts:
        label = "NOISE" if cluster_id == -1 else f"Cluster {cluster_id}"
        print(f"     {label}: {count} messages")

    # Test retrieval of cluster messages
    print(f"\n   ✓ Testing message retrieval from cluster 1...")
    cluster_messages = clusterer.db.get_cluster_messages(clustering_run_id, 1, limit=3)
    print(f"     Retrieved {len(cluster_messages)} messages (limited to 3)")
    for msg in cluster_messages:
        preview = msg['content'][:60] + "..." if len(msg['content']) > 60 else msg['content']
        print(f"       - {msg['platform']}: {preview}")

    print("\n" + "="*80)
    print("✅ DATABASE VALIDATION COMPLETE")
    print("="*80)
    print(f"Clustering run ID: {clustering_run_id}")
    print(f"To delete this test: DELETE FROM clustering_runs WHERE id = {clustering_run_id};")
    print("="*80)

    # Clean up
    clusterer.db.close()

if __name__ == "__main__":
    main()
