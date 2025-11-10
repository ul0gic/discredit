"""
Inspect noise points to validate clustering quality.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.clusterer import MessageClusterer

def main():
    print("🔍 INSPECTING CLUSTERING NOISE POINTS")
    print("="*80 + "\n")

    # Initialize and load
    clusterer = MessageClusterer()
    embeddings, message_ids = clusterer.load_embeddings()

    # Take first 100
    embeddings = embeddings[:100]
    message_ids = message_ids[:100]
    clusterer.embeddings = embeddings
    clusterer.message_ids = message_ids

    messages = clusterer.load_messages(message_ids)

    # Cluster
    result = clusterer.cluster_hdbscan(min_cluster_size=5, min_samples=3)
    labels = result['labels']

    # Separate noise vs clustered
    noise_indices = [i for i, label in enumerate(labels) if label == -1]
    clustered_indices = [i for i, label in enumerate(labels) if label != -1]

    print(f"\n📊 Distribution:")
    print(f"   Clustered: {len(clustered_indices)} ({len(clustered_indices)/len(labels)*100:.1f}%)")
    print(f"   Noise:     {len(noise_indices)} ({len(noise_indices)/len(labels)*100:.1f}%)")

    # Show sample noise points
    print(f"\n🔍 Sample NOISE points (first 10):")
    print("="*80)
    for i in noise_indices[:10]:
        msg_id = message_ids[i]
        msg = messages.get(msg_id, {})
        content = msg.get('content', '').strip()[:150]  # First 150 chars
        platform = msg.get('platform', 'unknown')
        print(f"\n[{platform.upper()}] {msg_id}")
        print(f"   {content}...")

    # Show sample clustered points
    print(f"\n\n✅ Sample CLUSTERED points (first 10):")
    print("="*80)
    for i in clustered_indices[:10]:
        msg_id = message_ids[i]
        msg = messages.get(msg_id, {})
        content = msg.get('content', '').strip()[:150]
        platform = msg.get('platform', 'unknown')
        cluster = labels[i]
        print(f"\n[{platform.upper()}] Cluster {cluster} - {msg_id}")
        print(f"   {content}...")

    print("\n" + "="*80)
    print("🤔 Analysis:")
    print("   - Are noise points spam/off-topic/short? → Good filtering")
    print("   - Are noise points valuable content? → May need parameter tuning")
    print("="*80)

if __name__ == "__main__":
    main()
