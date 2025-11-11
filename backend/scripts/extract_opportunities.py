#!/usr/bin/env python3
"""
Extract Integration Opportunities to Structured Formats

Exports all integration_requests to:
1. JSON - structured data for analysis
2. Markdown - organized by patterns for deep reading
3. Summary - high-level patterns and stats
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import sqlite3
import json
from datetime import datetime
from collections import Counter, defaultdict
import re

# Load environment
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")


def extract_keywords(content):
    """Extract potential integration names and keywords"""
    content_lower = content.lower()

    # Common integrations/tools to look for
    keywords = {
        'supabase', 'stripe', 'auth0', 'firebase', 'shopify', 'zapier',
        'sendgrid', 'mailgun', 'resend', 'vercel', 'cloudflare', 'github',
        'jira', 'slack', 'discord', 'webhook', 'api', 'oauth', 'sso',
        'payment', 'email', 'database', 'auth', 'ssl', 'domain', 'dns',
        'prerender', 'seo', 'analytics', 'google', 'facebook', 'meta',
        'postgres', 'mysql', 'mongodb', 'redis', 'n8n', 'make', 'cursor',
        'nextjs', 'react', 'tailwind', 'graphql', 'rest', 'alexa', 'twilio',
        'aws', 'gcp', 'azure', 'heroku', 'railway', 'render'
    }

    found = []
    for keyword in keywords:
        if keyword in content_lower:
            found.append(keyword)

    return found


def categorize_message(content, keywords):
    """Attempt to categorize the request"""
    content_lower = content.lower()
    categories = []

    # Payment related
    if any(k in keywords for k in ['stripe', 'payment', 'shopify']):
        categories.append('payment')

    # Database/Backend
    if any(k in keywords for k in ['supabase', 'firebase', 'postgres', 'mysql', 'mongodb', 'database']):
        categories.append('database')

    # Authentication
    if any(k in keywords for k in ['auth0', 'oauth', 'sso', 'auth', 'authentication']):
        categories.append('authentication')

    # Email
    if any(k in keywords for k in ['sendgrid', 'mailgun', 'resend', 'email', 'smtp']):
        categories.append('email')

    # Migration/Export
    if 'migrat' in content_lower or 'export' in content_lower or 'dump' in content_lower:
        categories.append('migration_export')

    # Automation
    if any(k in keywords for k in ['zapier', 'n8n', 'webhook', 'automation', 'make']):
        categories.append('automation')

    # SEO/Analytics
    if any(k in keywords for k in ['prerender', 'seo', 'analytics', 'google', 'facebook']):
        categories.append('seo_analytics')

    # Domain/SSL
    if any(k in keywords for k in ['domain', 'ssl', 'dns', 'cloudflare']):
        categories.append('domain_ssl')

    # API Integration
    if 'api' in keywords or 'rest' in keywords or 'graphql' in keywords:
        categories.append('api')

    return categories if categories else ['uncategorized']


def export_to_json():
    """Export to structured JSON"""

    db_path = Path(__file__).parent.parent / "data" / "discredit.db"
    output_dir = Path(__file__).parent.parent / "reports"
    output_dir.mkdir(exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\n🔍 Extracting integration requests...")
    cursor.execute("""
        SELECT
            m.id,
            m.content,
            m.platform,
            m.timestamp,
            m.source,
            u.username,
            u.message_count
        FROM messages m
        JOIN message_taxonomy mt ON m.id = mt.message_id
        JOIN users u ON m.author_id = u.id
        WHERE mt.category = 'integration_requests'
        ORDER BY m.timestamp DESC
    """)

    rows = cursor.fetchall()

    # Structure the data
    messages = []
    for msg_id, content, platform, timestamp, source, username, msg_count in rows:
        keywords = extract_keywords(content)
        categories = categorize_message(content, keywords)

        messages.append({
            'id': msg_id,
            'content': content,
            'platform': platform,
            'timestamp': timestamp,
            'date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
            'source': source,
            'user': {
                'username': username,
                'total_messages': msg_count
            },
            'extracted_keywords': keywords,
            'auto_categories': categories
        })

    # Export JSON
    json_path = output_dir / "integration_requests.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump({
            'total_messages': len(messages),
            'export_date': datetime.now().isoformat(),
            'messages': messages
        }, f, indent=2, ensure_ascii=False)

    print(f"✅ JSON exported: {json_path}")

    conn.close()
    return messages


def export_to_organized_markdown(messages):
    """Export to organized markdown grouped by patterns"""

    output_dir = Path(__file__).parent.parent / "reports"
    md_path = output_dir / "integration_opportunities_analysis.md"

    # Group messages by auto-category
    by_category = defaultdict(list)
    for msg in messages:
        for cat in msg['auto_categories']:
            by_category[cat].append(msg)

    # Count keywords
    keyword_counter = Counter()
    for msg in messages:
        keyword_counter.update(msg['extracted_keywords'])

    # Platform breakdown
    platform_counter = Counter(msg['platform'] for msg in messages)

    with open(md_path, 'w', encoding='utf-8') as f:
        f.write("# Integration Opportunities - Deep Analysis\n\n")
        f.write(f"**Total Messages:** {len(messages)}\n")
        f.write(f"**Export Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write("---\n\n")

        # Platform breakdown
        f.write("## 📊 Platform Distribution\n\n")
        for platform, count in platform_counter.most_common():
            pct = (count / len(messages)) * 100
            f.write(f"- **{platform}**: {count} messages ({pct:.1f}%)\n")
        f.write("\n---\n\n")

        # Top keywords
        f.write("## 🔑 Top Keywords Mentioned\n\n")
        for keyword, count in keyword_counter.most_common(30):
            pct = (count / len(messages)) * 100
            f.write(f"- **{keyword}**: {count} mentions ({pct:.1f}%)\n")
        f.write("\n---\n\n")

        # Category breakdown
        f.write("## 📁 Auto-Categorized Patterns\n\n")
        category_names = {
            'migration_export': '🚨 Migration/Export (CRITICAL PAIN)',
            'payment': '💳 Payment Integration',
            'database': '🗄️ Database/Backend',
            'authentication': '🔐 Authentication/Auth',
            'email': '📧 Email/Contact Forms',
            'automation': '⚡ Automation/Webhooks',
            'seo_analytics': '📈 SEO/Analytics',
            'domain_ssl': '🌐 Domain/SSL',
            'api': '🔌 API Integration',
            'uncategorized': '❓ Uncategorized'
        }

        for cat, name in category_names.items():
            if cat in by_category:
                f.write(f"### {name}\n\n")
                f.write(f"**Count:** {len(by_category[cat])} messages\n\n")

        f.write("\n---\n\n")

        # Messages by category
        f.write("## 📝 Messages by Category\n\n")

        for cat, name in category_names.items():
            if cat not in by_category:
                continue

            f.write(f"## {name}\n\n")
            f.write(f"**{len(by_category[cat])} messages**\n\n")

            for i, msg in enumerate(by_category[cat][:50], 1):  # Top 50 per category
                f.write(f"### [{i}] {msg['platform'].upper()} | {msg['date']} | @{msg['user']['username']}\n\n")

                if msg['extracted_keywords']:
                    f.write(f"**Keywords:** {', '.join(msg['extracted_keywords'])}\n\n")

                f.write(f"```\n{msg['content']}\n```\n\n")
                f.write("---\n\n")

            if len(by_category[cat]) > 50:
                f.write(f"*... and {len(by_category[cat]) - 50} more messages in this category*\n\n")

            f.write("\n\n")

    print(f"✅ Organized Markdown exported: {md_path}")


def export_summary(messages):
    """Export executive summary"""

    output_dir = Path(__file__).parent.parent / "reports"
    summary_path = output_dir / "integration_opportunities_summary.md"

    # Analyze patterns
    keyword_counter = Counter()
    for msg in messages:
        keyword_counter.update(msg['extracted_keywords'])

    # Category counts
    category_counter = Counter()
    for msg in messages:
        category_counter.update(msg['auto_categories'])

    # Platform breakdown
    platform_counter = Counter(msg['platform'] for msg in messages)

    # User activity
    user_msgs = Counter(msg['user']['username'] for msg in messages)

    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("# Integration Opportunities - Executive Summary\n\n")
        f.write(f"**Analysis Date:** {datetime.now().strftime('%Y-%m-%d')}\n")
        f.write(f"**Total Messages Analyzed:** {len(messages)}\n\n")

        f.write("---\n\n")

        f.write("## 🎯 TOP 10 OPPORTUNITY PATTERNS\n\n")

        for i, (cat, count) in enumerate(category_counter.most_common(10), 1):
            pct = (count / len(messages)) * 100
            f.write(f"{i}. **{cat.replace('_', ' ').title()}**: {count} messages ({pct:.1f}%)\n")

        f.write("\n---\n\n")

        f.write("## 🔥 TOP 20 TOOLS/SERVICES MENTIONED\n\n")

        for i, (keyword, count) in enumerate(keyword_counter.most_common(20), 1):
            pct = (count / len(messages)) * 100
            f.write(f"{i}. **{keyword}**: {count} mentions ({pct:.1f}%)\n")

        f.write("\n---\n\n")

        f.write("## 📊 PLATFORM BREAKDOWN\n\n")

        for platform, count in platform_counter.items():
            pct = (count / len(messages)) * 100
            f.write(f"- **{platform}**: {count} messages ({pct:.1f}%)\n")

        f.write("\n---\n\n")

        f.write("## 👥 TOP 10 REQUESTERS\n\n")

        for i, (user, count) in enumerate(user_msgs.most_common(10), 1):
            f.write(f"{i}. **@{user}**: {count} integration requests\n")

        f.write("\n---\n\n")

        f.write("## 💡 KEY INSIGHTS\n\n")

        # Auto-generate insights
        migration_count = category_counter.get('migration_export', 0)
        supabase_count = keyword_counter.get('supabase', 0)
        stripe_count = keyword_counter.get('stripe', 0)

        f.write(f"1. **Migration/Export Pain**: {migration_count} messages about data export/migration\n")
        f.write(f"2. **Supabase Lock-in**: {supabase_count} mentions of Supabase (likely related to Lovable Cloud)\n")
        f.write(f"3. **Payment Integration**: {stripe_count} Stripe mentions + payment-related requests\n")
        f.write(f"4. **Platform Preference**: {platform_counter['discord']} Discord vs {platform_counter['reddit']} Reddit messages\n")

        f.write("\n---\n\n")

        f.write("## 🚀 RECOMMENDED NEXT STEPS\n\n")
        f.write("1. Deep dive into top 3 patterns\n")
        f.write("2. Interview users in each category\n")
        f.write("3. Validate willingness to pay\n")
        f.write("4. Build MVP for highest-demand opportunity\n")

    print(f"✅ Summary exported: {summary_path}")


def main():
    print("\n" + "="*80)
    print("🚀 EXTRACTING INTEGRATION OPPORTUNITIES")
    print("="*80)

    # Export to JSON
    messages = export_to_json()

    # Export to organized Markdown
    export_to_organized_markdown(messages)

    # Export summary
    export_summary(messages)

    print("\n" + "="*80)
    print("✅ EXTRACTION COMPLETE")
    print("="*80)
    print("\n📁 Files created:")
    print("   1. reports/integration_requests.json - Full structured data")
    print("   2. reports/integration_opportunities_analysis.md - Organized by category")
    print("   3. reports/integration_opportunities_summary.md - Executive summary")
    print("\n💡 Next: Open the markdown files to explore patterns!\n")


if __name__ == "__main__":
    main()
