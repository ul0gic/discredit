<div align="center">

# 🎯 Discredit

### *Discord and Reddit Community Intelligence Mining for Market Discovery*

**Extract actionable insights from Discord servers and Reddit communities**

[![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Poetry](https://img.shields.io/badge/Poetry-Dependency%20Management-60A5FA?style=for-the-badge&logo=poetry&logoColor=white)](https://python-poetry.org)
[![SQLite](https://img.shields.io/badge/SQLite-Source%20of%20Truth-003B57?style=for-the-badge&logo=sqlite&logoColor=white)](https://sqlite.org)
[![Neo4j](https://img.shields.io/badge/Neo4j-Graph%20DB-008CC1?style=for-the-badge&logo=neo4j&logoColor=white)](https://neo4j.com)
[![OpenAI](https://img.shields.io/badge/OpenAI-GPT--5-412991?style=for-the-badge&logo=openai&logoColor=white)](https://openai.com)

---

</div>

## 📖 Overview

Discredit is a **community intelligence mining system** that analyzes conversations from any Discord server and Reddit community to identify pain points, feature requests, and integration opportunities. By processing thousands of real user conversations, it surfaces actionable insights backed by actual demand data.

## ✨ Features

- 🔄 **Automated Scraping**: Collect messages from Discord channels and Reddit with full comment tree traversal
- 🗄️ **Unified Database**: Single source of truth normalizing Discord and Reddit data with smart ID prefixing
- 🧠 **Semantic Search**: Vector embeddings for finding similar pain points and requests
- 🔬 **Topic Clustering**: HDBSCAN automatically discovers natural topic groupings from embeddings
- 🕸️ **Graph Analysis**: Relationship mapping to identify patterns and communities
- 🤖 **AI-Powered Extraction**: GPT-5 extracts pain points, integrations, and features from conversations
- 📊 **Opportunity Reports**: Data-driven insights with user quotes and frequency metrics
- ⚡ **Production Ready**: Rate limiting, checkpointing, and resumable scraping

## 🏗️ Architecture

### 🔍 Overview

Discredit uses a **multi-database architecture** to handle different aspects of data analysis:

- 🗄️ **SQLite**: Source of truth for all scraped messages, users, and extracted entities
- 🧠 **ChromaDB**: Vector embeddings for semantic search and clustering
- 🕸️ **Neo4j**: Graph database for relationship analysis and pattern discovery
- 🐍 **Python 3.13** with **Poetry** for dependency management

```mermaid
graph TB
    subgraph "Data Sources"
        Discord[Discord API<br/>Any Server/Channels]
        Reddit[Reddit API<br/>Any Subreddit]
    end

    subgraph "Scrapers"
        DS[Discord Scraper<br/>discord.py]
        RS[Reddit Scraper<br/>reddit.py]
    end

    subgraph "Storage Layer"
        SQLite[(SQLite<br/>Source of Truth)]
        ChromaDB[(ChromaDB<br/>Vectors)]
        Neo4j[(Neo4j<br/>Graph)]
    end

    subgraph "Analysis Layer"
        Embedder[Embedder<br/>OpenAI API]
        Clusterer[Topic Clustering<br/>HDBSCAN]
        Extractor[Entity Extractor<br/>GPT-5]
        GraphBuilder[Graph Builder]
    end

    subgraph "Output"
        Reports[Opportunity Reports]
        Insights[Market Insights]
    end

    Discord --> DS
    Reddit --> RS
    DS --> SQLite
    RS --> SQLite

    SQLite --> Embedder
    Embedder --> ChromaDB

    ChromaDB --> Clusterer
    Clusterer --> SQLite

    SQLite --> Extractor
    Extractor --> SQLite

    SQLite --> GraphBuilder
    GraphBuilder --> Neo4j

    ChromaDB --> Reports
    Neo4j --> Reports
    SQLite --> Insights

    style SQLite fill:#0ea5e9,stroke:#38bdf8,stroke-width:3px,color:#fff
    style ChromaDB fill:#f59e0b,stroke:#fbbf24,stroke-width:3px,color:#000
    style Neo4j fill:#ef4444,stroke:#f87171,stroke-width:3px,color:#fff
    style Discord fill:#5865f2,stroke:#7289da,stroke-width:2px,color:#fff
    style Reddit fill:#ff4500,stroke:#ff6b35,stroke-width:2px,color:#fff
    style DS fill:#818cf8,stroke:#a5b4fc,stroke-width:2px,color:#fff
    style RS fill:#fb923c,stroke:#fdba74,stroke-width:2px,color:#fff
    style Embedder fill:#8b5cf6,stroke:#a78bfa,stroke-width:2px,color:#fff
    style Clusterer fill:#8b5cf6,stroke:#a78bfa,stroke-width:2px,color:#fff
    style Extractor fill:#8b5cf6,stroke:#a78bfa,stroke-width:2px,color:#fff
    style GraphBuilder fill:#8b5cf6,stroke:#a78bfa,stroke-width:2px,color:#fff
    style Reports fill:#10b981,stroke:#34d399,stroke-width:2px,color:#fff
    style Insights fill:#10b981,stroke:#34d399,stroke-width:2px,color:#fff
```

### 💾 Database Architecture

#### 📊 SQLite Schema

SQLite serves as the **single source of truth** with a unified table design that normalizes both Discord and Reddit data:

```mermaid
erDiagram
    messages ||--o{ extracted_entities : "has"
    messages ||--o{ embeddings_reference : "has"
    users ||--o{ messages : "authors"
    messages ||--o{ messages : "replies to"

    messages {
        string id PK "discord_XXX or reddit_t3_XXX/reddit_t1_XXX"
        string platform "discord | reddit"
        string content "Message text"
        string author_id FK "Prefixed user ID"
        integer timestamp "Unix timestamp"
        string source "Channel name or r/subreddit"
        string parent_id FK "For threading"
        json metadata "Platform-specific data"
        integer scraped_at "When scraped"
    }

    users {
        string id PK "Prefixed: discord_XXX or reddit_XXX"
        string platform "discord | reddit"
        string username "Username"
        string display_name "Display name"
        integer message_count "Total messages"
        integer first_seen "Unix timestamp"
        integer last_seen "Unix timestamp"
        json metadata "Platform-specific"
    }

    extracted_entities {
        integer id PK
        string message_id FK "Source message"
        string entity_type "pain_point | integration | feature"
        string entity_name "Raw extracted name"
        string canonical_name "Normalized name"
        string category "Entity category"
        real confidence "0-1 score"
        string context "Text snippet"
        json extraction_metadata "Model, timestamp, etc"
    }

    embeddings_reference {
        integer id PK
        string message_id FK "Message reference"
        string chromadb_id "ChromaDB vector ID"
        string embedding_model "e.g. text-embedding-3-small"
        integer created_at "Unix timestamp"
    }
```

**🎯 Key Design Decisions:**

1. 🔗 **Unified Messages Table**: Single table for Discord and Reddit with `platform` column
2. 🏷️ **ID Prefixing**: Prevents collisions (`discord_123`, `reddit_t3_abc`, `reddit_t1_xyz`)
3. 📝 **Type Prefixes for Reddit**: `t3_` = posts, `t1_` = comments
4. 🧵 **Parent-Child Threading**: `parent_id` field supports Discord replies and Reddit comment trees
5. 📦 **JSON Metadata**: Platform-specific fields (reactions, upvotes, awards) stored as JSON
6. ⚡ **No ORM**: Raw SQL for performance and transparency

### ⚙️ Application Architecture

#### 🤖 Scraper Architecture

Both scrapers follow a common pattern: fetch → parse → normalize → store with deduplication.

```mermaid
sequenceDiagram
    participant API as External API
    participant Scraper as Scraper Module
    participant Parser as Message Parser
    participant DB as SQLite Database
    participant Checkpoint as Checkpoint System

    Note over Scraper: Initialize with credentials
    Scraper->>API: Fetch batch of messages
    API-->>Scraper: Raw message data

    loop For each message
        Scraper->>Parser: Parse raw message
        Parser->>Parser: Extract author, content, metadata
        Parser->>Parser: Add platform prefix to IDs
        Parser->>Parser: Filter bots/deleted
        Parser-->>Scraper: Normalized message dict
    end

    Scraper->>DB: Insert batch (with dedup check)
    DB-->>Scraper: Confirmation

    Scraper->>Checkpoint: Save progress
    Note over Checkpoint: Resumable on failure

    alt More messages available
        Scraper->>API: Fetch next batch
    else Reached cutoff date
        Scraper->>Scraper: Finalize statistics
    end
```

**💬 Discord Scraper (`scrapers/discord.py`):**
- 🌐 Uses Discord API v10 (HTTP/REST)
- 📄 Pagination via `before` parameter
- ⏱️ Rate limiting: 5 req/sec with exponential backoff
- 💾 Checkpoint every 1000 messages
- 🚫 Filters: bots, empty messages
- 🏷️ Prefixes: `discord_MESSAGEID`, `discord_USERID`

**🔴 Reddit Scraper (`scrapers/reddit.py`):**
- 🔧 Uses PRAW (Python Reddit API Wrapper)
- 🌲 Fetches posts with full comment trees
- ♾️ No depth limit on comment traversal
- 🚫 Filters: bots, mods, deleted/removed content
- 🏷️ Prefixes: `reddit_t3_POSTID` (posts), `reddit_t1_COMMENTID` (comments)
- ⏱️ Rate limiting: 0.5s between requests

#### 📊 Data Flow

```mermaid
flowchart LR
    subgraph "Phase 1: Collection"
        D[Discord<br/>Scraper] --> S[(SQLite)]
        R[Reddit<br/>Scraper] --> S
    end

    subgraph "Phase 2: Semantic"
        S --> E[Embedder<br/>OpenAI]
        E --> C[(ChromaDB)]
        C -.link.-> S
    end

    subgraph "Phase 3: Clustering"
        C --> CL[Topic<br/>Clustering<br/>HDBSCAN]
        CL --> S
    end

    subgraph "Phase 4: Extraction"
        S --> X[Entity<br/>Extractor<br/>GPT-5]
        X --> S
    end

    subgraph "Phase 4: Graph"
        S --> G[Graph<br/>Builder]
        G --> N[(Neo4j)]
    end

    subgraph "Phase 5: Analysis"
        S --> A[Analyzers]
        C --> A
        N --> A
        A --> O[Reports]
    end

    style S fill:#0ea5e9,stroke:#38bdf8,stroke-width:3px,color:#fff
    style C fill:#f59e0b,stroke:#fbbf24,stroke-width:3px,color:#000
    style N fill:#ef4444,stroke:#f87171,stroke-width:3px,color:#fff
    style D fill:#818cf8,stroke:#a5b4fc,stroke-width:2px,color:#fff
    style R fill:#fb923c,stroke:#fdba74,stroke-width:2px,color:#fff
    style E fill:#8b5cf6,stroke:#a78bfa,stroke-width:2px,color:#fff
    style X fill:#8b5cf6,stroke:#a78bfa,stroke-width:2px,color:#fff
    style G fill:#8b5cf6,stroke:#a78bfa,stroke-width:2px,color:#fff
    style A fill:#06b6d4,stroke:#22d3ee,stroke-width:2px,color:#fff
    style O fill:#10b981,stroke:#34d399,stroke-width:2px,color:#fff
```

**🔄 Pipeline Stages:**

1. 📥 **Collection**: Scrapers → SQLite (unified storage)
2. 🧠 **Semantic Processing**: SQLite → OpenAI → ChromaDB (vector embeddings)
3. 🔍 **Entity Extraction**: SQLite → GPT-4 → SQLite (pain points, integrations, features)
4. 🕸️ **Graph Construction**: SQLite → Neo4j (relationships, communities, patterns)
5. 📊 **Analysis**: Multi-database queries → Opportunity reports

---

## 📁 Project Structure

```
discredit/
├── backend/
│   ├── scrapers/       # Discord and Reddit scraping modules
│   ├── storage/        # Database wrapper modules (SQLite, ChromaDB, Neo4j)
│   ├── analysis/       # Embedding and entity extraction logic
│   ├── queries/        # Cypher queries and analysis scripts
│   ├── scripts/        # Runnable entry points
│   ├── data/           # All database files (gitignored)
│   ├── reports/        # Generated analysis reports (gitignored)
│   ├── config.py       # Centralized configuration
│   ├── .env            # Environment variables and secrets (gitignored)
│   └── pyproject.toml  # Poetry dependencies
├── .project/           # Project documentation and planning
└── README.md           # This file
```

## 🚀 Setup

### 1️⃣ Install Poetry (if not already installed)

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

### 2️⃣ Install Dependencies

```bash
cd backend
poetry install
```

### 3️⃣ Configure Environment Variables

Edit `backend/.env` with your credentials and target communities:

- 💬 **Discord**: Auth token, server ID, channel IDs (your target server)
- 🔴 **Reddit**: Client ID, client secret, user agent, subreddit name (your target community)
- 🤖 **OpenAI**: API key for embeddings and extraction
- 🕸️ **Neo4j**: Connection URI, username, password

**Example `.env`:**
```bash
# Discord
DISCORD_AUTH_TOKEN=your_token_here
DISCORD_SERVER_ID=123456789
DISCORD_CHANNEL_ID=987654321

# Reddit
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_secret
REDDIT_USER_AGENT=Discredit/1.0
REDDIT_SUBREDDIT=python  # Change to your target subreddit

# OpenAI
OPENAI_API_KEY=sk-...

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
```

### 4️⃣ Activate Poetry Shell

```bash
poetry shell
```

---

## 💻 Usage

Run scripts from the `backend/` directory using Poetry:

```bash
# Test all API credentials
poetry run python scripts/test_credentials.py

# Light test of Discord scraper (10 messages)
poetry run python scripts/test_discord_scraper_light.py

# Light test of Reddit scraper (3 posts)
poetry run python scripts/test_reddit_scraper_light.py

# Run full Discord scraper
poetry run python scrapers/discord.py

# Run full Reddit scraper
poetry run python scrapers/reddit.py
```

---

## 🛠️ Development

- 📂 All backend code lives in `backend/`
- 📦 Use `poetry add <package>` to add new dependencies
- 🐚 Use `poetry shell` to activate the virtual environment
- ⚙️ Configuration is managed through `backend/config.py`

---

## 🎯 Use Cases

- **Market Research**: Identify pain points and feature requests in your target community
- **Product Development**: Discover what users actually want based on real conversations
- **Competitive Intelligence**: Understand integration needs and workflow patterns
- **Community Analysis**: Track sentiment, engagement, and emerging trends
- **SaaS Opportunity Discovery**: Find high-value problems worth solving

**Output**: Actionable reports backed by real user quotes, frequency analysis, and demand metrics

