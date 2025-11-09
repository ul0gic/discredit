#!/usr/bin/env python3
"""
Lightweight test of Discord scraper - fetches only 10 messages.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from config import config
from storage.sqlite_db import DiscreditDB
from scrapers.discord import DiscordScraper


def main():
    """Run a light test of the Discord scraper."""
    print("=" * 60)
    print("🧪 DISCORD SCRAPER - LIGHT TEST (10 messages)")
    print("=" * 60)

    # Initialize database
    db = DiscreditDB(config.SQLITE_DB_PATH)
    db.initialize_schema()

    # Create scraper
    scraper = DiscordScraper(
        auth_token=config.DISCORD_AUTH_TOKEN,
        server_id=config.DISCORD_SERVER_ID,
        channel_id=config.DISCORD_CHANNEL_ID,
        db=db
    )

    # Get channel info
    print("\n📡 Fetching channel info...")
    channel_info = scraper.get_channel_info()
    if channel_info:
        channel_name = channel_info.get('name', 'Unknown')
        print(f"   ✅ Channel: #{channel_name}")
        print(f"   ✅ Channel ID: {scraper.channel_id}")

    # Manually fetch and parse 10 messages
    print("\n📥 Fetching 10 most recent messages...")

    endpoint = f"/channels/{scraper.channel_id}/messages"
    params = {'limit': 10}

    response = scraper._make_request(endpoint, params)

    if not response:
        print("❌ Failed to fetch messages")
        return 1

    messages = response.json()
    print(f"   ✅ Fetched {len(messages)} raw messages from API")

    # Parse and save messages
    print("\n🔄 Parsing and saving messages...")
    parsed_messages = []
    users_dict = {}

    for msg_data in messages:
        parsed = scraper._parse_message(msg_data)

        if not parsed:
            continue

        parsed_messages.append(parsed)

        # Track user
        user_id = parsed['author_id']
        if user_id not in users_dict:
            users_dict[user_id] = {
                'user_id': user_id,
                'platform': 'discord',
                'username': parsed['author_name'],
                'message_count': 1
            }
        else:
            users_dict[user_id]['message_count'] += 1

    print(f"   ✅ Parsed {len(parsed_messages)} valid messages")
    print(f"   ✅ Found {len(users_dict)} unique users")

    # Save to database
    if parsed_messages:
        scraper._save_batch(parsed_messages, users_dict)
        print(f"   ✅ Saved to database")

    # Query and display results
    print("\n" + "=" * 60)
    print("📊 DATABASE CONTENTS")
    print("=" * 60)

    stats = db.get_database_stats()
    print(f"Total messages: {stats['total_messages']}")
    print(f"Discord messages: {stats['messages_by_platform'].get('discord', 0)}")
    print(f"Total users: {stats['total_users']}")

    # Show sample messages
    print("\n📨 Sample Messages:")
    print("-" * 60)

    sample_messages = db.get_messages_by_platform('discord', limit=5)

    for i, msg in enumerate(sample_messages, 1):
        print(f"\n{i}. ID: {msg['id']}")
        print(f"   Author: {msg['author_id']}")
        print(f"   Content: {msg['content'][:80]}{'...' if len(msg['content']) > 80 else ''}")
        print(f"   Platform: {msg['platform']}")
        print(f"   Source: {msg['source']}")

    # Show users
    print("\n" + "-" * 60)
    print("👥 Users:")
    print("-" * 60)

    cursor = db.conn.cursor()
    cursor.execute("SELECT id, username, message_count FROM users LIMIT 5")
    users = cursor.fetchall()

    for user in users:
        print(f"   {user[1]}: {user[2]} messages (ID: {user[0]})")

    print("\n" + "=" * 60)
    print("✅ Light test complete!")
    print("=" * 60)

    db.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
