#!/usr/bin/env python3
"""
Test all API credentials and connections before data collection.
"""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import requests
import praw
import openai
from neo4j import GraphDatabase
from config import config


def test_discord():
    """Test Discord authentication token."""
    print("\n🔵 Testing Discord API...")

    headers = {
        'Authorization': config.DISCORD_AUTH_TOKEN,
        'Content-Type': 'application/json'
    }

    # Test with a simple API call to get current user info
    url = 'https://discord.com/api/v10/users/@me'

    try:
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            user_data = response.json()
            username = user_data.get('username', 'Unknown')
            user_id = user_data.get('id', 'Unknown')
            print(f"   ✅ Discord auth successful!")
            print(f"   → Logged in as: {username} (ID: {user_id})")
            return True
        elif response.status_code == 401:
            print(f"   ❌ Discord auth failed: Invalid token")
            print(f"   → Extract a fresh token from browser DevTools")
            return False
        else:
            print(f"   ❌ Discord API error: {response.status_code}")
            print(f"   → Response: {response.text}")
            return False

    except Exception as e:
        print(f"   ❌ Discord connection failed: {e}")
        return False


def test_reddit():
    """Test Reddit API credentials."""
    print("\n🟠 Testing Reddit API...")

    try:
        reddit = praw.Reddit(
            client_id=config.REDDIT_CLIENT_ID,
            client_secret=config.REDDIT_CLIENT_SECRET,
            user_agent=config.REDDIT_USER_AGENT
        )

        # Test with read-only access to configured subreddit
        subreddit = reddit.subreddit(config.REDDIT_SUBREDDIT)

        # Fetch just one post to validate
        posts = list(subreddit.new(limit=1))

        if posts:
            print(f"   ✅ Reddit API connection successful!")
            print(f"   → Subreddit: r/{subreddit.display_name}")
            print(f"   → Subscribers: {subreddit.subscribers:,}")
            return True
        else:
            print(f"   ⚠️  Reddit API connected but no posts found in r/{config.REDDIT_SUBREDDIT}")
            return True  # Still valid, just empty

    except Exception as e:
        print(f"   ❌ Reddit API failed: {e}")
        print(f"   → Check client_id and client_secret in .env")
        return False


def test_openai():
    """Test OpenAI API key."""
    print("\n🟢 Testing OpenAI API...")

    try:
        client = openai.OpenAI(api_key=config.OPENAI_API_KEY)

        # Test with a minimal API call (list models)
        models = client.models.list()

        # Check if we can access GPT models
        model_ids = [m.id for m in models.data]
        has_gpt = any('gpt' in m for m in model_ids)

        if has_gpt:
            print(f"   ✅ OpenAI API connection successful!")
            print(f"   → API key valid, GPT models accessible")
            return True
        else:
            print(f"   ⚠️  OpenAI API connected but no GPT models found")
            return True  # Still valid

    except openai.AuthenticationError:
        print(f"   ❌ OpenAI authentication failed: Invalid API key")
        return False
    except Exception as e:
        print(f"   ❌ OpenAI API failed: {e}")
        return False


def test_neo4j():
    """Test Neo4j database connection."""
    print("\n🔴 Testing Neo4j Database...")

    try:
        driver = GraphDatabase.driver(
            config.NEO4J_URI,
            auth=(config.NEO4J_USERNAME, config.NEO4J_PASSWORD)
        )

        # Test connection with a simple query
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            record = result.single()

            if record and record['test'] == 1:
                # Get node count
                result = session.run("MATCH (n) RETURN count(n) as count")
                node_count = result.single()['count']

                print(f"   ✅ Neo4j connection successful!")
                print(f"   → URI: {config.NEO4J_URI}")
                print(f"   → Nodes in database: {node_count}")

                driver.close()
                return True

        driver.close()
        return False

    except Exception as e:
        print(f"   ❌ Neo4j connection failed: {e}")
        print(f"   → Check if Neo4j is running: systemctl status neo4j")
        return False


def main():
    """Run all credential tests."""
    print("=" * 60)
    print("🔐 DISCREDIT - API Credential Validation")
    print("=" * 60)

    results = {
        'Discord': test_discord(),
        'Reddit': test_reddit(),
        'OpenAI': test_openai(),
        'Neo4j': test_neo4j()
    }

    print("\n" + "=" * 60)
    print("📊 VALIDATION SUMMARY")
    print("=" * 60)

    for service, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {service:.<20} {status}")

    all_passed = all(results.values())

    if all_passed:
        print("\n🎉 All credentials validated! Ready for data collection.")
        return 0
    else:
        print("\n⚠️  Some credentials failed. Fix the issues above before proceeding.")
        return 1


if __name__ == '__main__':
    sys.exit(main())
