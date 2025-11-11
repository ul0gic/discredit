"""
Market Intelligence Taxonomy - Discredit

This module defines the taxonomy categories for classifying messages
in the market intelligence pipeline. Categories are designed to identify
monetizable opportunities through focused single-category classification.

Each message MUST be assigned to exactly ONE category.
"""

# Focused 10-category taxonomy for opportunity discovery
MARKET_TAXONOMY = {
    "integration_requests": "User needs to connect/integrate with external tools or services (Stripe, Auth0, APIs, webhooks, etc)",
    "feature_requests": "User wants new functionality, improvements, or enhancements that don't exist",
    "pricing_complaints": "Concerns about cost, affordability, expensive pricing, need for cheaper plans or tiers",
    "performance_issues": "Speed problems, slow responses, reliability issues, downtime, latency, scalability concerns",
    "usability_problems": "UX issues, confusing interface, learning curve, complexity, poor documentation",
    "authentication_needs": "Login, SSO, OAuth, permissions, security, user management, access control",
    "bug_reports": "Technical errors, broken features, crashes, unexpected behavior, things not working",
    "customization_requests": "Need for more control, configuration options, theming, branding, white-labeling",
    "questions": "User asking how to do something, seeking help, learning, informational queries",
    "spam_noise": "Low-quality content, off-topic messages, GIFs, memes, promotional spam, gibberish, not actionable"
}


def get_taxonomy_prompt() -> str:
    """
    Generate the GPT-5 prompt for taxonomy classification.

    Returns:
        Formatted prompt string with taxonomy and instructions
    """
    prompt = """You are a market intelligence classifier analyzing user messages to identify monetizable opportunities.

CRITICAL RULES:
- Each message gets EXACTLY ONE category
- Choose the PRIMARY intent/purpose of the message
- If a message fits multiple categories, pick the MOST IMPORTANT one
- Focus on actionable business intelligence

TAXONOMY (10 Categories):
"""

    for category, description in MARKET_TAXONOMY.items():
        prompt += f"\n{category}:\n  {description}\n"

    prompt += """

CLASSIFICATION INSTRUCTIONS:
1. Read the message and identify its PRIMARY purpose
2. Assign it to the ONE category that best represents the core intent
3. When in doubt between two categories, prioritize in this order:
   - integration_requests > feature_requests > customization_requests
   - bug_reports > performance_issues > usability_problems
   - pricing_complaints (specific) > questions (general)
   - authentication_needs > integration_requests (if specifically about auth)
4. Mark obvious spam/noise as "spam_noise"

RESPONSE FORMAT:
Return ONLY valid JSON with this exact structure:
{
  "message_id": "category_name",
  "message_id": "category_name",
  ...
}

EXAMPLES:
- "Lovable is too expensive, need cheaper plan" → "pricing_complaints"
- "How do I integrate Auth0 with my app?" → "authentication_needs"
- "Would love to see dark mode feature!" → "feature_requests"
- "Getting 500 errors when connecting to Supabase" → "bug_reports"
- "The UI is confusing, hard to find settings" → "usability_problems"
- "Can you add Stripe payment integration?" → "integration_requests"
- "Need more customization options for branding" → "customization_requests"
- "App is really slow, takes forever to load" → "performance_issues"
- "How do I deploy my app?" → "questions"
- "https://tenor.com/view/gif-12345" → "spam_noise"

Now classify the following messages. Return ONLY the JSON object, nothing else:
"""

    return prompt


def get_flat_categories() -> list[str]:
    """
    Get a flat list of all categories.

    Returns:
        List of category names
    """
    return list(MARKET_TAXONOMY.keys())


def get_taxonomy_summary() -> str:
    """
    Get a human-readable summary of the taxonomy.

    Returns:
        Formatted string describing all categories
    """
    summary = "Market Intelligence Taxonomy - Single Category Classification\n"
    summary += "=" * 80 + "\n\n"
    summary += "Each message is assigned to EXACTLY ONE category.\n\n"

    for i, (category, description) in enumerate(MARKET_TAXONOMY.items(), 1):
        summary += f"{i:2d}. {category:25s} - {description}\n"

    summary += f"\nTotal: {len(MARKET_TAXONOMY)} categories\n"

    return summary


if __name__ == "__main__":
    # Print taxonomy summary when run directly
    print(get_taxonomy_summary())
    print("\n" + "=" * 50)
    print("FLAT CATEGORIES:")
    for cat in get_flat_categories():
        print(f"  - {cat}")
