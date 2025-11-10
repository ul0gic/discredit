#!/usr/bin/env python3
"""
Reddit scraper using PRAW (Python Reddit API Wrapper).

Scrapes posts and comments from any subreddit with:
- Full comment tree traversal
- Bot/mod filtering
- Deleted/removed content skipping
- SQLite storage integration
- Rate limit handling
"""

import time
import praw
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from tqdm import tqdm

import sys
import os
# Add backend directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config import config
from storage.sqlite_db import DiscreditDB


class RedditScraper:
    """Reddit scraper using PRAW for any subreddit."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        user_agent: str,
        subreddit_name: str,
        db: DiscreditDB
    ):
        """
        Initialize Reddit scraper.

        Args:
            client_id: Reddit API client ID
            client_secret: Reddit API client secret
            user_agent: Reddit API user agent
            subreddit_name: Subreddit to scrape (e.g., "python", "webdev")
            db: DiscreditDB instance for storage
        """
        self.subreddit_name = subreddit_name
        self.db = db

        # Initialize PRAW
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )

        self.subreddit = self.reddit.subreddit(subreddit_name)

        # Statistics
        self.stats = {
            'posts_scraped': 0,
            'comments_scraped': 0,
            'users_found': set(),
            'api_calls': 0,
            'skipped_bots': 0,
            'skipped_deleted': 0,
            'skipped_mods': 0,
            'errors': []
        }

    def _is_bot_or_mod(self, author) -> bool:
        """Check if author is a bot or moderator."""
        if author is None:
            return False

        author_name = str(author).lower()

        # Common bot patterns
        bot_patterns = ['bot', 'automod', 'automoderator', 'moderator']
        if any(pattern in author_name for pattern in bot_patterns):
            return True

        return False

    def _is_deleted_or_removed(self, content: str, author) -> bool:
        """Check if content is deleted or removed."""
        # Deleted author
        if author is None or str(author) == '[deleted]':
            return True

        # Removed content
        if content in ['[removed]', '[deleted]', '']:
            return True

        return False

    def _parse_submission(self, submission) -> Optional[Dict]:
        """
        Parse Reddit submission (post) into normalized format.

        Args:
            submission: PRAW Submission object

        Returns:
            Parsed submission dict or None if invalid
        """
        try:
            # Check if deleted/removed
            if self._is_deleted_or_removed(submission.selftext, submission.author):
                self.stats['skipped_deleted'] += 1
                return None

            # Check if bot/mod
            if self._is_bot_or_mod(submission.author):
                self.stats['skipped_bots'] += 1
                return None

            # Reddit post ID format: reddit_t3_abc123 (explicit type prefix)
            post_id = f"reddit_t3_{submission.id}"

            # Get author info
            author_name = str(submission.author)
            author_id = f"reddit_{submission.author.name}" if submission.author else "reddit_deleted"

            # Combine title and selftext for content
            # Title is most important, put it first
            content_parts = [submission.title]
            if submission.selftext and submission.selftext.strip():
                content_parts.append(submission.selftext)
            content = "\n\n".join(content_parts)

            # Parse timestamp
            created_utc = int(submission.created_utc)

            # Build metadata
            metadata = {
                'type': 'post',
                'upvotes': submission.score,
                'upvote_ratio': submission.upvote_ratio,
                'num_comments': submission.num_comments,
                'flair': submission.link_flair_text,
                'is_self_post': submission.is_self,
                'url': submission.url if not submission.is_self else None,
                'permalink': f"https://reddit.com{submission.permalink}",
                'awards': [award['name'] for award in submission.all_awardings] if hasattr(submission, 'all_awardings') else [],
                'is_stickied': submission.stickied,
                'is_locked': submission.locked
            }

            return {
                'message_id': post_id,
                'platform': 'reddit',
                'author_id': author_id,
                'author_name': author_name,
                'content': content,
                'timestamp': created_utc,
                'parent_message_id': None,  # Posts have no parent
                'metadata': metadata
            }

        except Exception as e:
            error = f"Failed to parse submission {submission.id}: {str(e)}"
            self.stats['errors'].append(error)
            print(f"⚠️  {error}")
            return None

    def _parse_comment(self, comment, post_id: str) -> Optional[Dict]:
        """
        Parse Reddit comment into normalized format.

        Args:
            comment: PRAW Comment object
            post_id: Parent post ID (prefixed)

        Returns:
            Parsed comment dict or None if invalid
        """
        try:
            # Skip MoreComments objects
            if isinstance(comment, praw.models.MoreComments):
                return None

            # Check if deleted/removed
            if self._is_deleted_or_removed(comment.body, comment.author):
                self.stats['skipped_deleted'] += 1
                return None

            # Check if bot/mod
            if self._is_bot_or_mod(comment.author):
                self.stats['skipped_bots'] += 1
                return None

            # Reddit comment ID format: reddit_t1_xyz789 (explicit type prefix)
            comment_id = f"reddit_t1_{comment.id}"

            # Get author info
            author_name = str(comment.author)
            author_id = f"reddit_{comment.author.name}" if comment.author else "reddit_deleted"

            # Content
            content = comment.body.strip()

            if not content:
                return None

            # Parse timestamp
            created_utc = int(comment.created_utc)

            # Determine parent - could be post or another comment
            # PRAW gives parent_id with type prefix (t3_xxx or t1_xxx)
            parent_id = f"reddit_{comment.parent_id}"  # e.g., reddit_t3_abc123 or reddit_t1_xyz789

            # Calculate depth (0 = top-level comment on post)
            depth = 0
            try:
                # Walk up the parent chain
                current = comment
                while hasattr(current, 'parent') and not current.parent_id.startswith('t3_'):
                    depth += 1
                    current = current.parent()
                    if depth > 50:  # Safety limit
                        break
            except:
                depth = 0  # Fallback

            # Build metadata
            metadata = {
                'type': 'comment',
                'upvotes': comment.score,
                'depth': depth,
                'is_top_level': comment.parent_id.startswith('t3_'),
                'is_submitter': comment.is_submitter,
                'permalink': f"https://reddit.com{comment.permalink}",
                'awards': [award['name'] for award in comment.all_awardings] if hasattr(comment, 'all_awardings') else []
            }

            return {
                'message_id': comment_id,
                'platform': 'reddit',
                'author_id': author_id,
                'author_name': author_name,
                'content': content,
                'timestamp': created_utc,
                'parent_message_id': parent_id,
                'metadata': metadata
            }

        except Exception as e:
            error = f"Failed to parse comment: {str(e)}"
            self.stats['errors'].append(error)
            return None

    def _get_all_comments(self, submission) -> List:
        """
        Recursively get all comments from a submission, flattening the tree.

        Args:
            submission: PRAW Submission object

        Returns:
            Flat list of all Comment objects
        """
        # Replace MoreComments objects to get all comments
        submission.comments.replace_more(limit=None)

        # Flatten comment tree
        all_comments = submission.comments.list()

        return all_comments

    def scrape_subreddit(
        self,
        months_back: int = 3,
        batch_size: int = 100
    ) -> Dict:
        """
        Scrape posts and comments from subreddit.

        Args:
            months_back: How many months back to scrape
            batch_size: Posts to process before committing to DB

        Returns:
            Statistics dictionary
        """
        print(f"\n{'='*60}")
        print(f"🟠 REDDIT SCRAPER - Starting")
        print(f"{'='*60}")
        print(f"Subreddit: r/{self.subreddit_name}")
        print(f"Time range: Last {months_back} months")
        print(f"{'='*60}\n")

        # Calculate cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=months_back * 30)
        cutoff_timestamp = int(cutoff_date.timestamp())

        print(f"📅 Cutoff date: {cutoff_date.strftime('%Y-%m-%d')}")
        print(f"📥 Fetching posts...\n")

        # Track data
        posts_buffer = []
        comments_buffer = []
        users_dict = {}

        # Fetch submissions from the last N months
        # PRAW doesn't have a direct time filter, so we'll iterate until we hit the cutoff
        processed_posts = 0
        total_comments = 0

        try:
            # Iterate through new submissions
            for submission in self.subreddit.new(limit=None):
                self.stats['api_calls'] += 1

                # Check if before cutoff
                if submission.created_utc < cutoff_timestamp:
                    print(f"\n✅ Reached {months_back}-month cutoff date")
                    break

                # Parse submission
                parsed_post = self._parse_submission(submission)

                if parsed_post:
                    posts_buffer.append(parsed_post)
                    self._track_user(parsed_post, users_dict)
                    processed_posts += 1

                    # Get all comments for this post
                    print(f"📨 Post {processed_posts}: {submission.title[:60]}... ({submission.num_comments} comments)")

                    if submission.num_comments > 0:
                        try:
                            all_comments = self._get_all_comments(submission)
                            self.stats['api_calls'] += 1

                            # Parse each comment
                            for comment in all_comments:
                                parsed_comment = self._parse_comment(comment, parsed_post['message_id'])

                                if parsed_comment:
                                    comments_buffer.append(parsed_comment)
                                    self._track_user(parsed_comment, users_dict)
                                    total_comments += 1

                        except Exception as e:
                            error = f"Failed to fetch comments for {submission.id}: {str(e)}"
                            self.stats['errors'].append(error)
                            print(f"   ⚠️  {error}")

                # Save batch periodically
                if len(posts_buffer) >= batch_size:
                    self._save_batch(posts_buffer, comments_buffer, users_dict)
                    posts_buffer = []
                    comments_buffer = []
                    print(f"\n💾 Checkpoint: {processed_posts} posts, {total_comments} comments\n")

                # Rate limiting - be respectful to Reddit (60 req/min, well under 100 req/min limit)
                time.sleep(1.0)

        except Exception as e:
            error = f"Scraping error: {str(e)}"
            self.stats['errors'].append(error)
            print(f"\n❌ {error}")

        # Save remaining data
        if posts_buffer or comments_buffer:
            self._save_batch(posts_buffer, comments_buffer, users_dict)

        return self._finalize_stats()

    def _track_user(self, message: Dict, users_dict: Dict):
        """Track user in temporary dictionary."""
        user_id = message['author_id']

        if user_id not in users_dict:
            users_dict[user_id] = {
                'user_id': user_id,
                'platform': 'reddit',
                'username': message['author_name'],
                'message_count': 0
            }

        users_dict[user_id]['message_count'] += 1

    def _save_batch(self, posts: List[Dict], comments: List[Dict], users_dict: Dict):
        """Save batch of posts, comments, and users to database."""
        source = f"r/{self.subreddit_name}"

        # Save posts first (so parent_id references exist for comments)
        for post in posts:
            inserted = self.db.insert_message(
                id=post['message_id'],
                platform=post['platform'],
                content=post['content'],
                author_id=post['author_id'],
                timestamp=post['timestamp'],
                source=source,
                parent_id=post.get('parent_message_id'),
                metadata=post.get('metadata')
            )

            if inserted:
                self.stats['posts_scraped'] += 1
                self.db.increment_user_message_count(
                    user_id=post['author_id'],
                    timestamp=post['timestamp']
                )

        # Save comments
        for comment in comments:
            inserted = self.db.insert_message(
                id=comment['message_id'],
                platform=comment['platform'],
                content=comment['content'],
                author_id=comment['author_id'],
                timestamp=comment['timestamp'],
                source=source,
                parent_id=comment.get('parent_message_id'),
                metadata=comment.get('metadata')
            )

            if inserted:
                self.stats['comments_scraped'] += 1
                self.db.increment_user_message_count(
                    user_id=comment['author_id'],
                    timestamp=comment['timestamp']
                )

        # Upsert users
        for user_id, user_data in users_dict.items():
            self.db.upsert_user(
                id=user_id,
                platform=user_data['platform'],
                username=user_data['username']
            )

        self.db.conn.commit()
        self.stats['users_found'].update(users_dict.keys())

    def _finalize_stats(self) -> Dict:
        """Finalize and return scraping statistics."""
        return {
            'posts_scraped': self.stats['posts_scraped'],
            'comments_scraped': self.stats['comments_scraped'],
            'total_messages': self.stats['posts_scraped'] + self.stats['comments_scraped'],
            'unique_users': len(self.stats['users_found']),
            'api_calls': self.stats['api_calls'],
            'skipped_bots': self.stats['skipped_bots'],
            'skipped_deleted': self.stats['skipped_deleted'],
            'errors': self.stats['errors']
        }


def main():
    """Run Reddit scraper as standalone script."""
    from storage.sqlite_db import DiscreditDB

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

    # Run scraper
    stats = scraper.scrape_subreddit(months_back=config.SCRAPE_MONTHS_BACK)

    # Print summary
    print(f"\n{'='*60}")
    print("📊 SCRAPING SUMMARY")
    print(f"{'='*60}")
    print(f"Posts scraped: {stats['posts_scraped']:,}")
    print(f"Comments scraped: {stats['comments_scraped']:,}")
    print(f"Total messages: {stats['total_messages']:,}")
    print(f"Unique users: {stats['unique_users']:,}")
    print(f"API calls: {stats['api_calls']:,}")
    print(f"Skipped (bots): {stats['skipped_bots']}")
    print(f"Skipped (deleted): {stats['skipped_deleted']}")
    print(f"Errors: {len(stats['errors'])}")
    if stats['errors']:
        print("\nErrors encountered:")
        for error in stats['errors'][:5]:
            print(f"  - {error}")
    print(f"{'='*60}\n")

    # Close database
    db.close()


if __name__ == '__main__':
    main()
