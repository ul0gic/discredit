"""
Centralized configuration management for Discredit.
Loads environment variables and validates required credentials.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
BACKEND_DIR = Path(__file__).parent
load_dotenv(BACKEND_DIR / ".env")


class Config:
    """Central configuration class for all Discredit settings."""

    # Discord Configuration
    DISCORD_AUTH_TOKEN = os.getenv("DISCORD_AUTH_TOKEN")
    DISCORD_SERVER_ID = os.getenv("DISCORD_SERVER_ID")
    DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")
    DISCORD_RATE_LIMIT = int(os.getenv("DISCORD_RATE_LIMIT", "5"))

    # Reddit Configuration
    REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
    REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
    REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "Discredit/1.0")
    REDDIT_SUBREDDIT = os.getenv("REDDIT_SUBREDDIT", "python")

    # OpenAI Configuration
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_RATE_LIMIT = int(os.getenv("OPENAI_RATE_LIMIT", "50"))

    # Neo4j Configuration
    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

    # Database Paths
    DATA_DIR = BACKEND_DIR / "data"
    SQLITE_DB_PATH = BACKEND_DIR / os.getenv("SQLITE_DB_PATH", "data/discredit.db")
    CHROMADB_PATH = BACKEND_DIR / os.getenv("CHROMADB_PATH", "data/chromadb")

    # Scraping Configuration
    SCRAPE_MONTHS_BACK = int(os.getenv("SCRAPE_MONTHS_BACK", "3"))

    @classmethod
    def validate_discord_credentials(cls):
        """Validate Discord configuration is set."""
        missing = []
        if not cls.DISCORD_AUTH_TOKEN or cls.DISCORD_AUTH_TOKEN == "your_discord_token_here":
            missing.append("DISCORD_AUTH_TOKEN")
        if not cls.DISCORD_SERVER_ID or cls.DISCORD_SERVER_ID == "your_server_id":
            missing.append("DISCORD_SERVER_ID")
        if not cls.DISCORD_CHANNEL_ID or cls.DISCORD_CHANNEL_ID == "your_channel_id":
            missing.append("DISCORD_CHANNEL_ID")

        if missing:
            raise ValueError(
                f"Missing Discord credentials: {', '.join(missing)}\n"
                f"Please update your .env file with valid Discord credentials."
            )
        return True

    @classmethod
    def validate_reddit_credentials(cls):
        """Validate Reddit configuration is set."""
        missing = []
        if not cls.REDDIT_CLIENT_ID or cls.REDDIT_CLIENT_ID == "your_reddit_client_id":
            missing.append("REDDIT_CLIENT_ID")
        if not cls.REDDIT_CLIENT_SECRET or cls.REDDIT_CLIENT_SECRET == "your_reddit_client_secret":
            missing.append("REDDIT_CLIENT_SECRET")

        if missing:
            raise ValueError(
                f"Missing Reddit credentials: {', '.join(missing)}\n"
                f"Please update your .env file with valid Reddit API credentials."
            )
        return True

    @classmethod
    def validate_openai_credentials(cls):
        """Validate OpenAI configuration is set."""
        if not cls.OPENAI_API_KEY or cls.OPENAI_API_KEY == "your_openai_api_key_here":
            raise ValueError(
                "Missing OpenAI API key.\n"
                "Please set OPENAI_API_KEY in your .env file."
            )
        return True

    @classmethod
    def validate_neo4j_credentials(cls):
        """Validate Neo4j configuration is set."""
        missing = []
        if not cls.NEO4J_URI or cls.NEO4J_URI == "neo4j+s://your-instance.databases.neo4j.io":
            missing.append("NEO4J_URI")
        if not cls.NEO4J_PASSWORD or cls.NEO4J_PASSWORD == "your_neo4j_password_here":
            missing.append("NEO4J_PASSWORD")

        if missing:
            raise ValueError(
                f"Missing Neo4j credentials: {', '.join(missing)}\n"
                f"Please update your .env file with valid Neo4j AuraDB credentials."
            )
        return True

    @classmethod
    def validate_all(cls):
        """Validate all credentials are configured."""
        cls.validate_discord_credentials()
        cls.validate_reddit_credentials()
        cls.validate_openai_credentials()
        cls.validate_neo4j_credentials()
        return True


# Convenience instance for importing
config = Config()
