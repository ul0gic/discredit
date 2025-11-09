#!/usr/bin/env python3
"""
Lightweight test of Reddit scraper - fetches only 3 posts with comments.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from config import config
from storage.sqlite_db import DiscreditDB
from scrapers.reddit import RedditScraper
import praw


def main():
    """Run a light test of the Reddit scraper."""
    print("=" * 60)
    print("🧪 REDDIT SCRAPER - LIGHT TEST (3 posts)")
    print("=" * 60)

    # Initialize database
    db = DiscreditDB(config.SQLITE_DB_PATH)
    db.initialize_schema()

    # Create scraper
    scraper = RedditScraper(
        client_id=config.REDDIT_CLIENT_ID,
        client_secret=config.REDDIT_CLIENT_SECRET,
        user_agent=config.REDDIT_USER_AGENT,
        subreddit_name=config.REDDIT_SUBREDDIT,
        db=db
    )

    print(f"\n📡 Connected to r/{scraper.subreddit_name}")
    print(f"   Subscribers: {scraper.subreddit.subscribers:,}")

    # Manually fetch 3 posts
    print("\n📥 Fetching 3 most recent posts with comments...")

    posts_buffer = []
    comments_buffer = []
    users_dict = {}

    post_count = 0
    for submission in scraper.subreddit.new(limit=3):
        post_count += 1

        # Parse post
        parsed_post = scraper._parse_submission(submission)

        if parsed_post:
            posts_buffer.append(parsed_post)
            scraper._track_user(parsed_post, users_dict)

            print(f"\n{post_count}. {submission.title[:70]}")
            print(f"   Author: {submission.author}")
            print(f"   Score: {submission.score} | Comments: {submission.num_comments}")

            # Get comments
            if submission.num_comments > 0:
                all_comments = scraper._get_all_comments(submission)

                for comment in all_comments[:10]:  # Limit to 10 comments per post for test
                    parsed_comment = scraper._parse_comment(comment, parsed_post['message_id'])

                    if parsed_comment:
                        comments_buffer.append(parsed_comment)
                        scraper._track_user(parsed_comment, users_dict)

                print(f"   📨 Parsed {len([c for c in comments_buffer if c['parent_message_id'] == parsed_post['message_id']])} comments")

    print(f"\n✅ Fetched {len(posts_buffer)} posts, {len(comments_buffer)} comments")
    print(f"✅ Found {len(users_dict)} unique users")

    # Save to database
    if posts_buffer or comments_buffer:
        print("\n💾 Saving to database...")
        scraper._save_batch(posts_buffer, comments_buffer, users_dict)
        print("   ✅ Saved successfully")

    # Query and display results
    print("\n" + "=" * 60)
    print("📊 DATABASE CONTENTS")
    print("=" * 60)

    stats = db.get_database_stats()
    print(f"Total messages: {stats['total_messages']}")
    print(f"  Discord: {stats['messages_by_platform'].get('discord', 0)}")
    print(f"  Reddit: {stats['messages_by_platform'].get('reddit', 0)}")
    print(f"Total users: {stats['total_users']}")

    # Show Reddit posts
    print("\n📨 Reddit Posts:")
    print("-" * 60)

    cursor = db.conn.cursor()
    cursor.execute("""
        SELECT id, author_id, content, metadata
        FROM messages
        WHERE platform = 'reddit' AND id LIKE 'reddit_t3_%'
        ORDER BY timestamp DESC
        LIMIT 3
    """)

    posts = cursor.fetchall()
    for i, post in enumerate(posts, 1):
        # Get first line of content
        first_line = post[2].split('\n')[0][:70]
        print(f"\n{i}. {first_line}...")
        print(f"   ID: {post[0]}")
        print(f"   Author: {post[1]}")

    # Show Reddit comments
    print("\n💬 Sample Comments:")
    print("-" * 60)

    cursor.execute("""
        SELECT id, parent_id, content
        FROM messages
        WHERE platform = 'reddit' AND id LIKE 'reddit_t1_%'
        ORDER BY timestamp DESC
        LIMIT 5
    """)

    comments = cursor.fetchall()
    for i, comment in enumerate(comments, 1):
        content_preview = comment[2][:60]
        print(f"\n{i}. {content_preview}...")
        print(f"   ID: {comment[0]}")
        print(f"   Parent: {comment[1]}")

    print("\n" + "=" * 60)
    print("✅ Light test complete!")
    print("=" * 60)

    db.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
