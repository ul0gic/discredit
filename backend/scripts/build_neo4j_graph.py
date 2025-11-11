#!/usr/bin/env python3
"""
Build Neo4j Knowledge Graph

Constructs the complete Neo4j graph from classified SQLite messages.
Includes Categories, Users, Messages, and their relationships.

Usage:
------
# Build graph (preserve existing if any)
poetry run python scripts/build_neo4j_graph.py

# Clear existing graph and rebuild from scratch
poetry run python scripts/build_neo4j_graph.py --clear

# Use custom database path
poetry run python scripts/build_neo4j_graph.py --db /path/to/discredit.db
"""

import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from analysis.graph_builder import GraphBuilder
import argparse
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)


def main():
    parser = argparse.ArgumentParser(
        description="Build Neo4j knowledge graph from SQLite data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build graph
  poetry run python scripts/build_neo4j_graph.py

  # Clear and rebuild
  poetry run python scripts/build_neo4j_graph.py --clear

  # Custom database
  poetry run python scripts/build_neo4j_graph.py --db /path/to/db.sqlite

After building, open Neo4j Browser at http://localhost:7474 to explore!
        """
    )

    parser.add_argument(
        '--clear',
        action='store_true',
        help="Clear existing graph before building (DESTRUCTIVE)"
    )

    parser.add_argument(
        '--db',
        type=str,
        help="Path to SQLite database (default: backend/data/discredit.db)"
    )

    parser.add_argument(
        '--yes',
        action='store_true',
        help="Skip confirmation prompt (for automation)"
    )

    args = parser.parse_args()

    # Confirmation for clearing (unless --yes flag)
    if args.clear and not args.yes:
        print("\n⚠️  WARNING: You are about to CLEAR the existing Neo4j graph!")
        try:
            response = input("   Are you sure? Type 'yes' to continue: ")
            if response.lower() != 'yes':
                print("   Cancelled.")
                return
        except EOFError:
            print("\n   Non-interactive mode detected. Use --yes flag to confirm.")
            return

    # Build the graph
    builder = GraphBuilder(db_path=args.db)
    stats = builder.build_graph(clear_existing=args.clear)

    # Success message
    print("\n🎉 Graph build complete!")
    print(f"   {sum(stats['nodes_created'].values()):,} nodes created")
    print(f"   {sum(stats['relationships_created'].values()):,} relationships created")
    print(f"\n💡 Next steps:")
    print(f"   1. Open Neo4j Browser: http://localhost:7474")
    print(f"   2. Try visualization queries in backend/queries/viz_queries.cypher")
    print(f"   3. Explore categories, users, and message patterns\n")


if __name__ == "__main__":
    main()
