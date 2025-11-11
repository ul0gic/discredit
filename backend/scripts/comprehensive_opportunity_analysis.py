#!/usr/bin/env python3
"""
Comprehensive Opportunity Analysis - All Actionable Categories

Analyzes ALL actionable categories to find common themes, patterns,
and monetizable opportunities. Excludes pricing_complaints, spam_noise, questions.

Actionable Categories:
- integration_requests (908)
- feature_requests (742)
- bug_reports (1,794)
- performance_issues (804)
- usability_problems (698)
- authentication_needs (257)
- customization_requests (46)

Total: 7,090 actionable messages
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import sqlite3
import json
from datetime import datetime
from collections import Counter, defaultdict
import re

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")


def extract_all_keywords(content):
    """Extract comprehensive keywords and entities"""
    content_lower = content.lower()

    # Tools & Services
    tools = {
        'supabase', 'stripe', 'auth0', 'firebase', 'shopify', 'zapier',
        'sendgrid', 'mailgun', 'resend', 'vercel', 'cloudflare', 'github',
        'jira', 'slack', 'discord', 'webhook', 'api', 'oauth', 'sso',
        'postgres', 'mysql', 'mongodb', 'redis', 'n8n', 'make', 'cursor',
        'nextjs', 'react', 'tailwind', 'graphql', 'rest', 'alexa', 'twilio',
        'aws', 'gcp', 'azure', 'heroku', 'railway', 'render', 'netlify',
        'paypal', 'square', 'braintree', 'plaid', 'dwolla',
        'sendbird', 'pusher', 'socket.io', 'ably',
        'algolia', 'elasticsearch', 'typesense',
        'clerk', 'magic', 'passage', 'stytch',
        'postmark', 'ses', 'sparkpost',
        'segment', 'mixpanel', 'amplitude', 'hotjar',
        'intercom', 'zendesk', 'crisp', 'drift',
        'calendly', 'cal.com', 'chili piper',
        'airtable', 'notion', 'coda',
        'webflow', 'framer', 'bubble',
        'retool', 'appsmith', 'budibase',
        'sanity', 'contentful', 'strapi',
        'lemonsqueezy', 'paddle', 'chargebee',
        'uploadcare', 'cloudinary', 'imgix',
        'mux', 'cloudflare stream', 'vimeo',
        'openai', 'anthropic', 'cohere', 'replicate',
        'pdf', 'csv', 'excel', 'export', 'import'
    }

    # Pain points & needs
    pain_keywords = {
        'migrate', 'migration', 'export', 'import', 'backup', 'dump',
        'slow', 'performance', 'speed', 'lag', 'timeout', 'crash',
        'bug', 'error', 'broken', 'fail', 'issue', 'problem',
        'complicated', 'confusing', 'difficult', 'hard', 'unclear',
        'missing', 'need', 'want', 'wish', 'would love',
        'cant', "can't", 'unable', 'impossible', 'wont', "won't",
        'stuck', 'blocked', 'frustrated', 'annoying',
        'ssl', 'https', 'certificate', 'domain', 'dns', 'custom domain',
        'cors', 'security', 'authentication', 'login', 'signup',
        'payment', 'subscription', 'billing', 'checkout',
        'email', 'smtp', 'contact form', 'notification',
        'mobile', 'responsive', 'ios', 'android',
        'seo', 'meta', 'sitemap', 'robot', 'crawl',
        'analytics', 'tracking', 'metrics', 'dashboard',
        'webhook', 'automation', 'workflow', 'trigger',
        'template', 'component', 'ui', 'design',
        'database', 'table', 'schema', 'query',
        'deploy', 'deployment', 'hosting', 'production',
        'test', 'testing', 'debug', 'debugging',
        'documentation', 'docs', 'tutorial', 'guide',
        'integration', 'connect', 'sync', 'link'
    }

    found_tools = [k for k in tools if k in content_lower]
    found_pains = [k for k in pain_keywords if k in content_lower]

    return {
        'tools': found_tools,
        'pain_points': found_pains,
        'all': found_tools + found_pains
    }


def analyze_cross_category_patterns(messages_by_category):
    """Find patterns that appear across multiple categories"""

    print("\n🔍 Analyzing cross-category patterns...")

    # Track keywords across categories
    keyword_categories = defaultdict(lambda: defaultdict(int))

    for category, messages in messages_by_category.items():
        for msg in messages:
            for keyword in msg['keywords']['all']:
                keyword_categories[keyword][category] += 1

    # Find keywords that appear in 3+ categories (cross-cutting concerns)
    cross_cutting = {}
    for keyword, categories in keyword_categories.items():
        if len(categories) >= 3:
            cross_cutting[keyword] = {
                'categories': dict(categories),
                'total_mentions': sum(categories.values()),
                'category_count': len(categories)
            }

    return cross_cutting


def identify_opportunity_themes(messages_by_category, cross_cutting):
    """Identify high-level opportunity themes"""

    print("\n💡 Identifying opportunity themes...")

    themes = defaultdict(lambda: {
        'messages': [],
        'categories': set(),
        'keywords': Counter(),
        'users': set()
    })

    # Define themes based on patterns
    theme_keywords = {
        'data_migration': ['migrate', 'migration', 'export', 'import', 'backup', 'dump', 'supabase'],
        'payment_processing': ['stripe', 'payment', 'checkout', 'subscription', 'billing', 'paypal'],
        'email_communication': ['email', 'smtp', 'sendgrid', 'resend', 'mailgun', 'contact form'],
        'authentication': ['auth0', 'oauth', 'sso', 'login', 'signup', 'authentication', 'clerk'],
        'automation_webhooks': ['webhook', 'zapier', 'n8n', 'automation', 'workflow', 'trigger'],
        'api_integration': ['api', 'rest', 'graphql', 'endpoint', 'integration', 'connect'],
        'hosting_deployment': ['vercel', 'netlify', 'deploy', 'hosting', 'production', 'render'],
        'domain_ssl': ['domain', 'ssl', 'https', 'certificate', 'dns', 'cloudflare'],
        'database_backend': ['database', 'supabase', 'firebase', 'postgres', 'mysql', 'mongodb'],
        'seo_analytics': ['seo', 'analytics', 'tracking', 'meta', 'sitemap', 'google'],
        'mobile_responsive': ['mobile', 'responsive', 'ios', 'android', 'app'],
        'performance': ['slow', 'performance', 'speed', 'lag', 'timeout', 'optimization'],
        'ui_components': ['component', 'template', 'ui', 'design', 'tailwind', 'style'],
        'ecommerce': ['shopify', 'product', 'cart', 'inventory', 'ecommerce'],
        'cms_content': ['cms', 'contentful', 'sanity', 'strapi', 'blog', 'content'],
        'file_handling': ['upload', 'file', 'pdf', 'csv', 'image', 'cloudinary'],
        'real_time': ['real-time', 'websocket', 'pusher', 'socket.io', 'live'],
        'ai_ml': ['openai', 'ai', 'gpt', 'llm', 'machine learning', 'anthropic'],
        'testing_debugging': ['test', 'debug', 'error', 'bug', 'issue', 'fix']
    }

    # Categorize messages into themes
    for category, messages in messages_by_category.items():
        for msg in messages:
            msg_keywords = set(msg['keywords']['all'])

            for theme_name, theme_kw in theme_keywords.items():
                if any(kw in msg_keywords for kw in theme_kw):
                    themes[theme_name]['messages'].append(msg)
                    themes[theme_name]['categories'].add(category)
                    themes[theme_name]['keywords'].update(msg_keywords)
                    themes[theme_name]['users'].add(msg['user']['username'])

    return themes


def export_comprehensive_analysis(messages_by_category, cross_cutting, themes):
    """Export comprehensive analysis to multiple formats"""

    output_dir = Path(__file__).parent.parent / "reports"
    output_dir.mkdir(exist_ok=True)

    # 1. JSON Export - Full data
    json_data = {
        'export_date': datetime.now().isoformat(),
        'total_messages': sum(len(msgs) for msgs in messages_by_category.values()),
        'categories': {
            cat: {
                'count': len(msgs),
                'messages': msgs
            }
            for cat, msgs in messages_by_category.items()
        },
        'cross_cutting_patterns': cross_cutting,
        'opportunity_themes': {
            name: {
                'message_count': len(data['messages']),
                'categories': list(data['categories']),
                'top_keywords': data['keywords'].most_common(20),
                'unique_users': len(data['users'])
            }
            for name, data in themes.items()
        }
    }

    json_path = output_dir / "comprehensive_opportunities.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    print(f"✅ JSON exported: {json_path}")

    # 2. Executive Summary
    export_executive_summary(messages_by_category, cross_cutting, themes, output_dir)

    # 3. Detailed Theme Analysis
    export_theme_analysis(themes, output_dir)

    # 4. Cross-Category Patterns
    export_cross_patterns(cross_cutting, output_dir)


def export_executive_summary(messages_by_category, cross_cutting, themes, output_dir):
    """Export executive summary"""

    summary_path = output_dir / "OPPORTUNITY_SUMMARY.md"

    total_messages = sum(len(msgs) for msgs in messages_by_category.values())

    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("# 🚀 DISCREDIT OPPORTUNITY ANALYSIS - EXECUTIVE SUMMARY\n\n")
        f.write(f"**Analysis Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Total Actionable Messages:** {total_messages:,}\n\n")

        f.write("---\n\n")

        # Category breakdown
        f.write("## 📊 MESSAGE DISTRIBUTION BY CATEGORY\n\n")
        sorted_cats = sorted(messages_by_category.items(), key=lambda x: len(x[1]), reverse=True)
        for cat, msgs in sorted_cats:
            pct = (len(msgs) / total_messages) * 100
            f.write(f"- **{cat.replace('_', ' ').title()}**: {len(msgs):,} messages ({pct:.1f}%)\n")

        f.write("\n---\n\n")

        # Top themes by message count
        f.write("## 🎯 TOP 15 OPPORTUNITY THEMES\n\n")
        sorted_themes = sorted(themes.items(), key=lambda x: len(x[1]['messages']), reverse=True)

        for i, (theme_name, data) in enumerate(sorted_themes[:15], 1):
            msg_count = len(data['messages'])
            cat_count = len(data['categories'])
            user_count = len(data['users'])

            f.write(f"### {i}. {theme_name.replace('_', ' ').upper()}\n\n")
            f.write(f"- **Messages:** {msg_count:,}\n")
            f.write(f"- **Unique Users:** {user_count:,}\n")
            f.write(f"- **Categories:** {cat_count} ({', '.join(data['categories'])})\n")
            f.write(f"- **Top Keywords:** {', '.join([kw for kw, _ in data['keywords'].most_common(10)])}\n\n")

        f.write("\n---\n\n")

        # Cross-cutting concerns
        f.write("## 🔗 CROSS-CATEGORY PATTERNS (appear in 3+ categories)\n\n")
        sorted_cross = sorted(cross_cutting.items(),
                            key=lambda x: x[1]['total_mentions'],
                            reverse=True)[:20]

        for keyword, data in sorted_cross:
            f.write(f"### {keyword}\n")
            f.write(f"- **Total mentions:** {data['total_mentions']}\n")
            f.write(f"- **Categories:** {data['category_count']}\n")
            breakdown = ', '.join([f"{cat}({count})" for cat, count in data['categories'].items()])
            f.write(f"- **Breakdown:** {breakdown}\n\n")

        f.write("\n---\n\n")

        # Key insights
        f.write("## 💡 KEY INSIGHTS & OPPORTUNITIES\n\n")

        # Analyze top themes
        top_3_themes = sorted_themes[:3]

        f.write("### Highest-Demand Opportunities:\n\n")
        for i, (theme_name, data) in enumerate(top_3_themes, 1):
            f.write(f"{i}. **{theme_name.replace('_', ' ').title()}**\n")
            f.write(f"   - {len(data['messages'])} messages from {len(data['users'])} users\n")
            f.write(f"   - Spans {len(data['categories'])} categories\n")
            f.write(f"   - Opportunity: Build {theme_name.replace('_', ' ')} solution\n\n")

        f.write("\n### Cross-Cutting Pain Points:\n\n")

        # Find most painful cross-cutting issues
        migration_total = cross_cutting.get('migrate', {}).get('total_mentions', 0) + \
                         cross_cutting.get('export', {}).get('total_mentions', 0)

        f.write(f"1. **Data Migration/Export**: {migration_total} mentions across categories\n")
        f.write(f"   - Users are STUCK and need data portability\n\n")

        f.write("2. **Integration Complexity**: API, webhook, and service connection issues\n")
        f.write("   - Need simpler integration tooling\n\n")

        f.write("3. **Performance Issues**: Speed, lag, timeout complaints\n")
        f.write("   - Opportunity for optimization tools\n\n")

        f.write("\n---\n\n")

        # Recommendations
        f.write("## 🚀 RECOMMENDED NEXT STEPS\n\n")
        f.write("1. **Deep dive into top 3 themes** - Read actual messages for nuance\n")
        f.write("2. **Validate willingness to pay** - Survey users in each theme\n")
        f.write("3. **Assess competition** - Research existing solutions\n")
        f.write("4. **Prioritize by feasibility** - What can we build fastest?\n")
        f.write("5. **Build MVPs** - Start with highest demand + lowest complexity\n\n")

        f.write("---\n\n")
        f.write("📁 **Additional Reports:**\n")
        f.write("- `THEME_ANALYSIS.md` - Deep dive into each theme with messages\n")
        f.write("- `CROSS_PATTERNS.md` - Cross-category patterns analysis\n")
        f.write("- `comprehensive_opportunities.json` - Full structured data\n")

    print(f"✅ Summary exported: {summary_path}")


def export_theme_analysis(themes, output_dir):
    """Export detailed theme analysis"""

    theme_path = output_dir / "THEME_ANALYSIS.md"

    sorted_themes = sorted(themes.items(), key=lambda x: len(x[1]['messages']), reverse=True)

    with open(theme_path, 'w', encoding='utf-8') as f:
        f.write("# 🎯 OPPORTUNITY THEMES - DETAILED ANALYSIS\n\n")
        f.write(f"**Export Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("This report contains detailed analysis of each opportunity theme with sample messages.\n\n")

        f.write("---\n\n")

        for theme_name, data in sorted_themes:
            if len(data['messages']) < 10:  # Skip themes with <10 messages
                continue

            f.write(f"## {theme_name.replace('_', ' ').upper()}\n\n")
            f.write(f"**Message Count:** {len(data['messages']):,}\n")
            f.write(f"**Unique Users:** {len(data['users']):,}\n")
            f.write(f"**Categories:** {', '.join(sorted(data['categories']))}\n\n")

            f.write(f"**Top Keywords:** {', '.join([kw for kw, _ in data['keywords'].most_common(15)])}\n\n")

            f.write("### Sample Messages (Top 20):\n\n")

            # Show top 20 messages
            for i, msg in enumerate(data['messages'][:20], 1):
                f.write(f"#### [{i}] {msg['platform'].upper()} | {msg['date']} | @{msg['user']['username']}\n\n")
                f.write(f"**Category:** {msg['category']}\n\n")
                if msg['keywords']['tools']:
                    f.write(f"**Tools:** {', '.join(msg['keywords']['tools'])}\n\n")
                f.write(f"```\n{msg['content'][:500]}\n```\n\n")
                f.write("---\n\n")

            if len(data['messages']) > 20:
                f.write(f"*... and {len(data['messages']) - 20} more messages in this theme*\n\n")

            f.write("\n\n")

    print(f"✅ Theme analysis exported: {theme_path}")


def export_cross_patterns(cross_cutting, output_dir):
    """Export cross-category patterns"""

    cross_path = output_dir / "CROSS_PATTERNS.md"

    sorted_cross = sorted(cross_cutting.items(),
                         key=lambda x: x[1]['total_mentions'],
                         reverse=True)

    with open(cross_path, 'w', encoding='utf-8') as f:
        f.write("# 🔗 CROSS-CATEGORY PATTERNS\n\n")
        f.write("Keywords and themes that appear across multiple categories\n")
        f.write("(Minimum 3 categories)\n\n")

        f.write("---\n\n")

        for keyword, data in sorted_cross:
            f.write(f"## {keyword.upper()}\n\n")
            f.write(f"**Total Mentions:** {data['total_mentions']}\n")
            f.write(f"**Categories:** {data['category_count']}\n\n")

            f.write("**Distribution:**\n")
            sorted_cats = sorted(data['categories'].items(), key=lambda x: x[1], reverse=True)
            for cat, count in sorted_cats:
                f.write(f"- {cat.replace('_', ' ').title()}: {count}\n")

            f.write("\n---\n\n")

    print(f"✅ Cross-patterns exported: {cross_path}")


def main():
    print("\n" + "="*80)
    print("🚀 COMPREHENSIVE OPPORTUNITY ANALYSIS")
    print("="*80)

    db_path = Path(__file__).parent.parent / "data" / "discredit.db"

    # Actionable categories (exclude pricing_complaints, spam_noise, questions)
    actionable_categories = [
        'integration_requests',
        'feature_requests',
        'bug_reports',
        'performance_issues',
        'usability_problems',
        'authentication_needs',
        'customization_requests'
    ]

    print(f"\n📊 Analyzing {len(actionable_categories)} actionable categories...")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Extract all messages by category
    messages_by_category = {}

    for category in actionable_categories:
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
            WHERE mt.category = ?
            ORDER BY m.timestamp DESC
        """, (category,))

        rows = cursor.fetchall()

        messages = []
        for msg_id, content, platform, timestamp, source, username, msg_count in rows:
            keywords = extract_all_keywords(content)

            messages.append({
                'id': msg_id,
                'content': content,
                'platform': platform,
                'timestamp': timestamp,
                'date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                'source': source,
                'category': category,
                'user': {
                    'username': username,
                    'total_messages': msg_count
                },
                'keywords': keywords
            })

        messages_by_category[category] = messages
        print(f"   ✅ {category}: {len(messages)} messages")

    conn.close()

    total = sum(len(msgs) for msgs in messages_by_category.values())
    print(f"\n📊 Total actionable messages: {total:,}")

    # Analyze cross-category patterns
    cross_cutting = analyze_cross_category_patterns(messages_by_category)
    print(f"   ✅ Found {len(cross_cutting)} cross-cutting patterns")

    # Identify opportunity themes
    themes = identify_opportunity_themes(messages_by_category, cross_cutting)
    print(f"   ✅ Identified {len(themes)} opportunity themes")

    # Export comprehensive analysis
    print("\n📁 Exporting analysis...")
    export_comprehensive_analysis(messages_by_category, cross_cutting, themes)

    print("\n" + "="*80)
    print("✅ ANALYSIS COMPLETE")
    print("="*80)
    print("\n📁 Files created:")
    print("   1. reports/OPPORTUNITY_SUMMARY.md - Executive overview")
    print("   2. reports/THEME_ANALYSIS.md - Deep dive into each theme")
    print("   3. reports/CROSS_PATTERNS.md - Cross-category patterns")
    print("   4. reports/comprehensive_opportunities.json - Full data")
    print("\n💡 Start with OPPORTUNITY_SUMMARY.md for high-level insights!\n")


if __name__ == "__main__":
    main()
