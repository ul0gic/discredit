#!/usr/bin/env python3
"""
Discord message scraper using Discord API v10.

Scrapes messages from specified Discord channels with:
- Pagination support
- Rate limiting and exponential backoff
- Checkpoint/resume capability
- SQLite storage integration
"""

import time
import json
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm

import sys
import os
# Add backend directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config import config
from storage.sqlite_db import DiscreditDB


class DiscordScraper:
    """Discord message scraper with checkpoint support."""

    API_BASE = "https://discord.com/api/v10"

    def __init__(
        self,
        auth_token: str,
        server_id: str,
        channel_id: str,
        db: DiscreditDB,
        checkpoint_path: Optional[str] = None
    ):
        """
        Initialize Discord scraper.

        Args:
            auth_token: Discord authorization token
            server_id: Discord server (guild) ID
            channel_id: Channel ID to scrape
            db: DiscreditDB instance for storage
            checkpoint_path: Path to checkpoint file (default: data/discord_checkpoint.json)
        """
        self.auth_token = auth_token
        self.server_id = server_id
        self.channel_id = channel_id
        self.db = db

        # Setup checkpoint
        if checkpoint_path is None:
            checkpoint_path = config.DATA_DIR / "discord_checkpoint.json"
        self.checkpoint_path = Path(checkpoint_path)
        self.checkpoint = self._load_checkpoint()

        # Rate limiting
        self.rate_limit = config.DISCORD_RATE_LIMIT  # requests per second
        self.last_request_time = 0

        # Statistics
        self.stats = {
            'messages_scraped': 0,
            'users_found': set(),
            'api_calls': 0,
            'rate_limit_hits': 0,
            'errors': []
        }

    def _load_checkpoint(self) -> Dict:
        """Load checkpoint from disk if exists."""
        if self.checkpoint_path.exists():
            with open(self.checkpoint_path, 'r') as f:
                checkpoint = json.load(f)
                print(f"📂 Loaded checkpoint: {checkpoint['messages_saved']} messages saved, "
                      f"last ID: {checkpoint.get('last_message_id', 'none')}")
                return checkpoint

        return {
            'channel_id': self.channel_id,
            'last_message_id': None,
            'messages_saved': 0,
            'started_at': None,
            'last_updated': None
        }

    def _save_checkpoint(self):
        """Save current progress to checkpoint file."""
        self.checkpoint['last_updated'] = datetime.utcnow().isoformat()

        # Ensure parent directory exists
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.checkpoint_path, 'w') as f:
            json.dump(self.checkpoint, f, indent=2)

    def _rate_limit_wait(self):
        """Ensure we don't exceed rate limit."""
        min_interval = 1.0 / self.rate_limit
        elapsed = time.time() - self.last_request_time

        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        self.last_request_time = time.time()

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        max_retries: int = 5
    ) -> Optional[requests.Response]:
        """
        Make HTTP request to Discord API with rate limiting and retries.

        Args:
            endpoint: API endpoint (relative to API_BASE)
            params: Query parameters
            max_retries: Maximum retry attempts

        Returns:
            Response object or None on failure
        """
        url = f"{self.API_BASE}{endpoint}"
        headers = {
            'Authorization': self.auth_token,
            'Content-Type': 'application/json'
        }

        for attempt in range(max_retries):
            self._rate_limit_wait()
            self.stats['api_calls'] += 1

            try:
                response = requests.get(url, headers=headers, params=params, timeout=15)

                # Success
                if response.status_code == 200:
                    return response

                # Rate limited
                if response.status_code == 429:
                    self.stats['rate_limit_hits'] += 1
                    retry_after = response.json().get('retry_after', 1.0)
                    print(f"⏳ Rate limited, waiting {retry_after:.2f}s...")
                    time.sleep(retry_after)
                    continue

                # Unauthorized
                if response.status_code == 401:
                    error = f"Unauthorized (401) - check Discord token"
                    self.stats['errors'].append(error)
                    print(f"❌ {error}")
                    return None

                # Not found
                if response.status_code == 404:
                    error = f"Channel not found (404) - check channel ID"
                    self.stats['errors'].append(error)
                    print(f"❌ {error}")
                    return None

                # Other errors
                print(f"⚠️  API error {response.status_code}, attempt {attempt + 1}/{max_retries}")

                # Exponential backoff
                if attempt < max_retries - 1:
                    backoff = min(2 ** attempt, 32)
                    time.sleep(backoff)

            except requests.RequestException as e:
                error = f"Request failed: {str(e)}"
                self.stats['errors'].append(error)
                print(f"⚠️  {error}, attempt {attempt + 1}/{max_retries}")

                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)

        return None

    def _parse_message(self, msg_data: Dict) -> Optional[Dict]:
        """
        Parse Discord message data into our normalized format.

        Args:
            msg_data: Raw message data from Discord API

        Returns:
            Parsed message dict or None if invalid
        """
        try:
            # Extract author info
            author = msg_data.get('author', {})
            author_id = author.get('id')
            author_name = author.get('username', 'Unknown')

            # Skip bot messages
            if author.get('bot', False):
                return None

            # Parse timestamp
            timestamp_str = msg_data.get('timestamp')
            timestamp_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            timestamp_unix = int(timestamp_dt.timestamp())

            # Extract content
            content = msg_data.get('content', '').strip()

            # Skip empty messages (could be just embeds/attachments)
            if not content and not msg_data.get('attachments') and not msg_data.get('embeds'):
                return None

            # Parse thread/reply info
            referenced_message = msg_data.get('referenced_message')
            parent_id = None
            if referenced_message:
                parent_id = f"discord_{referenced_message.get('id')}"  # Prefix parent ID

            # Build metadata
            metadata = {
                'type': msg_data.get('type', 0),
                'edited_timestamp': msg_data.get('edited_timestamp'),
                'mentions': [f"discord_{u.get('id')}" for u in msg_data.get('mentions', [])],
                'mention_roles': msg_data.get('mention_roles', []),
                'attachments': [
                    {
                        'url': a.get('url'),
                        'filename': a.get('filename'),
                        'content_type': a.get('content_type')
                    }
                    for a in msg_data.get('attachments', [])
                ],
                'embeds': len(msg_data.get('embeds', [])),
                'reactions': [
                    {
                        'emoji': r.get('emoji', {}).get('name'),
                        'count': r.get('count', 0)
                    }
                    for r in msg_data.get('reactions', [])
                ]
            }

            return {
                'message_id': f"discord_{msg_data.get('id')}",  # Prefix message ID
                'platform': 'discord',
                'author_id': f"discord_{author_id}",  # Prefix user ID
                'author_name': author_name,
                'content': content,
                'timestamp': timestamp_unix,  # Unix timestamp integer
                'timestamp_dt': timestamp_dt,  # Keep datetime for comparison
                'parent_message_id': parent_id,
                'channel_id': self.channel_id,
                'metadata': metadata
            }

        except Exception as e:
            error = f"Failed to parse message: {str(e)}"
            self.stats['errors'].append(error)
            print(f"⚠️  {error}")
            return None

    def scrape_messages(
        self,
        months_back: int = 3,
        batch_size: int = 100,
        checkpoint_interval: int = 1000
    ) -> Dict:
        """
        Scrape messages from Discord channel.

        Args:
            months_back: How many months back to scrape
            batch_size: Messages per API request (max 100)
            checkpoint_interval: Save checkpoint every N messages

        Returns:
            Statistics dictionary
        """
        print(f"\n{'='*60}")
        print(f"🔵 DISCORD SCRAPER - Starting")
        print(f"{'='*60}")
        print(f"Channel ID: {self.channel_id}")
        print(f"Time range: Last {months_back} months")
        print(f"Batch size: {batch_size}")
        print(f"{'='*60}\n")

        # Calculate cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=months_back * 30)
        cutoff_timestamp = int(cutoff_date.timestamp())

        # Set checkpoint start time if new
        if not self.checkpoint['started_at']:
            self.checkpoint['started_at'] = datetime.utcnow().isoformat()

        # Determine starting point
        before_id = self.checkpoint.get('last_message_id')

        # Progress tracking
        total_messages = 0
        batch_count = 0
        messages_buffer = []

        # Track users
        users_dict = {}

        endpoint = f"/channels/{self.channel_id}/messages"

        with tqdm(desc="Scraping messages", unit=" msg") as pbar:
            while True:
                # Build query params
                params = {'limit': batch_size}
                if before_id:
                    params['before'] = before_id

                # Fetch batch
                response = self._make_request(endpoint, params)

                if not response:
                    print("❌ Failed to fetch messages, stopping.")
                    break

                messages = response.json()

                # No more messages
                if not messages:
                    print("\n✅ Reached end of channel history")
                    break

                batch_count += 1
                batch_parsed = 0

                # Process each message
                for msg_data in messages:
                    # Parse message
                    parsed = self._parse_message(msg_data)

                    if not parsed:
                        continue

                    # Check if before cutoff (use datetime for comparison)
                    if parsed['timestamp_dt'] < cutoff_date:
                        print(f"\n✅ Reached {months_back}-month cutoff date: {cutoff_date.date()}")
                        # Save remaining buffer
                        if messages_buffer:
                            self._save_batch(messages_buffer, users_dict)
                            messages_buffer = []
                        return self._finalize_stats()

                    # Add to buffer
                    messages_buffer.append(parsed)
                    batch_parsed += 1

                    # Track user (use timestamp for storage, timestamp_dt for comparison)
                    user_id = parsed['author_id']
                    if user_id not in users_dict:
                        users_dict[user_id] = {
                            'user_id': user_id,
                            'platform': 'discord',
                            'username': parsed['author_name'],
                            'message_count': 0,
                            'first_seen': parsed['timestamp'],
                            'last_seen': parsed['timestamp'],
                            'first_seen_dt': parsed['timestamp_dt'],
                            'last_seen_dt': parsed['timestamp_dt']
                        }

                    users_dict[user_id]['message_count'] += 1
                    if parsed['timestamp_dt'] < users_dict[user_id]['first_seen_dt']:
                        users_dict[user_id]['first_seen'] = parsed['timestamp']
                        users_dict[user_id]['first_seen_dt'] = parsed['timestamp_dt']
                    if parsed['timestamp_dt'] > users_dict[user_id]['last_seen_dt']:
                        users_dict[user_id]['last_seen'] = parsed['timestamp']
                        users_dict[user_id]['last_seen_dt'] = parsed['timestamp_dt']

                total_messages += batch_parsed
                pbar.update(batch_parsed)

                # Save checkpoint periodically
                if total_messages % checkpoint_interval == 0 and messages_buffer:
                    self._save_batch(messages_buffer, users_dict)
                    messages_buffer = []

                    # Update checkpoint
                    self.checkpoint['messages_saved'] = total_messages
                    self._save_checkpoint()

                # Get oldest message ID for next iteration
                before_id = messages[-1]['id']
                self.checkpoint['last_message_id'] = before_id

                # Small delay between batches
                time.sleep(0.1)

        # Save any remaining messages
        if messages_buffer:
            self._save_batch(messages_buffer, users_dict)

        return self._finalize_stats()

    def _save_batch(self, messages: List[Dict], users_dict: Dict):
        """Save batch of messages and users to database."""
        if not messages:
            return

        # Get channel name for source field
        source = f"#{self.channel_id}"  # Will be enhanced with actual channel name if available

        # Save messages
        saved_count = 0
        for msg in messages:
            # Insert message (database handles deduplication via unique ID constraint)
            inserted = self.db.insert_message(
                id=msg['message_id'],
                platform=msg['platform'],
                content=msg['content'],
                author_id=msg['author_id'],
                timestamp=msg['timestamp'],
                source=source,
                parent_id=msg.get('parent_message_id'),
                metadata=msg.get('metadata')
            )

            if inserted:
                saved_count += 1
                # Increment user message count
                self.db.increment_user_message_count(
                    user_id=msg['author_id'],
                    timestamp=msg['timestamp']
                )

        # Upsert users (ensure they exist with basic info)
        for user_id, user_data in users_dict.items():
            self.db.upsert_user(
                id=user_id,
                platform=user_data['platform'],
                username=user_data['username']
            )

        self.db.conn.commit()

        self.stats['messages_scraped'] += saved_count
        self.stats['users_found'].update(users_dict.keys())

    def _finalize_stats(self) -> Dict:
        """Finalize and return scraping statistics."""
        # Save final checkpoint
        self.checkpoint['messages_saved'] = self.stats['messages_scraped']
        self.checkpoint['completed_at'] = datetime.utcnow().isoformat()
        self._save_checkpoint()

        return {
            'messages_scraped': self.stats['messages_scraped'],
            'unique_users': len(self.stats['users_found']),
            'api_calls': self.stats['api_calls'],
            'rate_limit_hits': self.stats['rate_limit_hits'],
            'errors': self.stats['errors'],
            'checkpoint_path': str(self.checkpoint_path)
        }

    def get_channel_info(self) -> Optional[Dict]:
        """Fetch channel information from Discord API."""
        endpoint = f"/channels/{self.channel_id}"
        response = self._make_request(endpoint)

        if response:
            return response.json()
        return None


def main():
    """Run Discord scraper as standalone script."""
    from storage.sqlite_db import DiscreditDB

    # Initialize database
    db = DiscreditDB()

    # Create scraper
    scraper = DiscordScraper(
        auth_token=config.DISCORD_AUTH_TOKEN,
        server_id=config.DISCORD_SERVER_ID,
        channel_id=config.DISCORD_CHANNEL_ID,
        db=db
    )

    # Get channel info
    channel_info = scraper.get_channel_info()
    if channel_info:
        print(f"Channel: #{channel_info.get('name', 'Unknown')}")

    # Run scraper
    stats = scraper.scrape_messages(months_back=config.SCRAPE_MONTHS_BACK)

    # Print summary
    print(f"\n{'='*60}")
    print("📊 SCRAPING SUMMARY")
    print(f"{'='*60}")
    print(f"Messages scraped: {stats['messages_scraped']:,}")
    print(f"Unique users: {stats['unique_users']:,}")
    print(f"API calls: {stats['api_calls']:,}")
    print(f"Rate limit hits: {stats['rate_limit_hits']}")
    print(f"Errors: {len(stats['errors'])}")
    if stats['errors']:
        print("\nErrors encountered:")
        for error in stats['errors'][:5]:  # Show first 5
            print(f"  - {error}")
    print(f"{'='*60}\n")

    # Close database
    db.close()


if __name__ == '__main__':
    main()
