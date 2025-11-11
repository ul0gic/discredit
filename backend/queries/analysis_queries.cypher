// ============================================================================
// ADVANCED ANALYSIS QUERIES - Find Opportunities & Patterns
// ============================================================================
// These queries dig deep into user behavior, platform differences,
// and opportunity patterns. Copy-paste into Neo4j Browser.
// ============================================================================


// ============================================================================
// 👥 TOP COMPLAINERS & POWER USERS
// ============================================================================

// 1. TOP COMPLAINERS - Users who post most pricing complaints
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'pricing_complaints'})
WITH u, count(m) AS complaint_count, collect(m.content)[0..3] AS sample_complaints
WHERE u.message_count > 0
RETURN u.username AS user,
       u.platform AS platform,
       complaint_count AS complaints,
       u.message_count AS total_messages,
       round(complaint_count * 100.0 / u.message_count, 1) AS complaint_percentage,
       sample_complaints
ORDER BY complaint_count DESC
LIMIT 20

// 2. TOP INTEGRATION REQUESTERS - Who wants integrations the most?
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'integration_requests'})
WITH u, count(m) AS integration_requests, collect(m.content)[0..3] AS sample_requests
WHERE u.message_count > 0
RETURN u.username AS user,
       u.platform AS platform,
       integration_requests AS requests,
       u.message_count AS total_messages,
       sample_requests
ORDER BY integration_requests DESC
LIMIT 20

// 3. SERIAL BUG REPORTERS - Users reporting the most bugs
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'bug_reports'})
WITH u, count(m) AS bug_count, collect(m.content)[0..3] AS sample_bugs
WHERE u.message_count > 0
RETURN u.username AS user,
       u.platform AS platform,
       bug_count AS bugs_reported,
       u.message_count AS total_messages,
       sample_bugs
ORDER BY bug_count DESC
LIMIT 20

// 4. MULTI-CATEGORY COMPLAINERS - Users complaining about multiple things
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category)
WHERE c.name IN ['pricing_complaints', 'bug_reports', 'performance_issues', 'usability_problems']
  AND u.message_count > 0
WITH u, collect(DISTINCT c.name) AS complaint_types, count(m) AS total_complaints
WHERE size(complaint_types) >= 3
RETURN u.username AS user,
       u.platform AS platform,
       complaint_types,
       total_complaints,
       size(complaint_types) AS categories_complained_about
ORDER BY total_complaints DESC
LIMIT 20


// ============================================================================
// 📊 DISCORD VS REDDIT COMPARISONS
// ============================================================================

// 5. PRICING COMPLAINTS: Discord vs Reddit
MATCH (m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'pricing_complaints'})
WITH m.platform AS platform, count(m) AS complaint_count
RETURN platform,
       complaint_count,
       round(complaint_count * 100.0 / 1840, 1) AS percentage_of_pricing_complaints
ORDER BY complaint_count DESC

// 6. INTEGRATION REQUESTS: Discord vs Reddit
MATCH (m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'integration_requests'})
WITH m.platform AS platform, count(m) AS request_count
RETURN platform,
       request_count,
       round(request_count * 100.0 / 908, 1) AS percentage_of_integration_requests
ORDER BY request_count DESC

// 7. ALL CATEGORIES: Platform Breakdown
MATCH (m:Message)-[:CLASSIFIED_AS]->(c:Category)
WITH c.name AS category, m.platform AS platform, count(m) AS count
RETURN category,
       sum(CASE WHEN platform = 'discord' THEN count ELSE 0 END) AS discord_count,
       sum(CASE WHEN platform = 'reddit' THEN count ELSE 0 END) AS reddit_count,
       sum(count) AS total,
       round(sum(CASE WHEN platform = 'discord' THEN count ELSE 0 END) * 100.0 / sum(count), 1) AS discord_pct
ORDER BY total DESC

// 8. PLATFORM USER BEHAVIOR - Average messages per user
MATCH (u:User)
WITH u.platform AS platform,
     count(u) AS user_count,
     avg(u.message_count) AS avg_messages,
     max(u.message_count) AS max_messages
RETURN platform,
       user_count,
       round(avg_messages, 1) AS avg_msgs_per_user,
       max_messages AS most_active_user_msgs
ORDER BY user_count DESC


// ============================================================================
// 🔥 OPPORTUNITY HUNTING
// ============================================================================

// 9. USERS WHO WANT BOTH INTEGRATIONS AND AUTH - Potential bundle opportunity
MATCH (u:User)-[:POSTED]->(m1:Message)-[:CLASSIFIED_AS]->(c1:Category {name: 'integration_requests'}),
      (u)-[:POSTED]->(m2:Message)-[:CLASSIFIED_AS]->(c2:Category {name: 'authentication_needs'})
RETURN u.username AS user,
       u.platform AS platform,
       count(DISTINCT m1) AS integration_mentions,
       count(DISTINCT m2) AS auth_mentions,
       u.message_count AS total_messages
ORDER BY (count(DISTINCT m1) + count(DISTINCT m2)) DESC
LIMIT 20

// 10. FRUSTRATED USERS - Reporting bugs AND performance issues
MATCH (u:User)-[:POSTED]->(m1:Message)-[:CLASSIFIED_AS]->(c1:Category {name: 'bug_reports'}),
      (u)-[:POSTED]->(m2:Message)-[:CLASSIFIED_AS]->(c2:Category {name: 'performance_issues'})
RETURN u.username AS user,
       u.platform AS platform,
       count(DISTINCT m1) AS bugs,
       count(DISTINCT m2) AS performance_complaints,
       u.message_count AS total_messages,
       collect(DISTINCT m1.content)[0..2] AS sample_issues
ORDER BY (count(DISTINCT m1) + count(DISTINCT m2)) DESC
LIMIT 20

// 11. FEATURE WISHLIST CHAMPIONS - Users requesting most features
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'feature_requests'})
WITH u, count(m) AS feature_count, collect(m.content) AS all_requests
RETURN u.username AS user,
       u.platform AS platform,
       feature_count AS features_requested,
       all_requests[0..5] AS sample_requests
ORDER BY feature_count DESC
LIMIT 15

// 12. CUSTOMIZATION SEEKERS - Small but valuable segment
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'customization_requests'})
RETURN u.username AS user,
       u.platform AS platform,
       count(m) AS customization_requests,
       collect(m.content) AS all_requests
ORDER BY count(m) DESC


// ============================================================================
// 💬 CONVERSATION ANALYSIS
// ============================================================================

// 13. MOST DISCUSSED TOPICS (by conversation thread depth)
MATCH path = (m1:Message)-[:REPLIES_TO*1..5]->(m2:Message)
WHERE (m2)-[:CLASSIFIED_AS]->(:Category)
MATCH (m2)-[:CLASSIFIED_AS]->(c:Category)
WITH c.name AS topic, m2.id AS original_message, length(path) AS depth
RETURN topic,
       count(DISTINCT original_message) AS thread_starters,
       max(depth) AS longest_thread,
       avg(depth) AS avg_thread_depth
ORDER BY thread_starters DESC

// 14. CONVERSATIONS THAT ESCALATE TO COMPLAINTS
MATCH (m1:Message)-[:REPLIES_TO]->(m2:Message)
WHERE (m1)-[:CLASSIFIED_AS]->(:Category {name: 'pricing_complaints'})
  AND NOT (m2)-[:CLASSIFIED_AS]->(:Category {name: 'pricing_complaints'})
MATCH (m2)-[:CLASSIFIED_AS]->(c:Category)
RETURN c.name AS started_as,
       count(*) AS escalated_to_pricing_complaint,
       collect(m1.content)[0..3] AS sample_escalations
ORDER BY escalated_to_pricing_complaint DESC

// 15. INFLUENTIAL THREAD STARTERS - Messages that generate most replies
MATCH (original:Message)<-[:REPLIES_TO]-(reply:Message)
MATCH (original)-[:CLASSIFIED_AS]->(c:Category)
MATCH (original)<-[:POSTED]-(u:User)
WITH original, c.name AS category, u.username AS author, count(reply) AS reply_count
RETURN category,
       author,
       original.content AS message,
       reply_count,
       original.platform AS platform
ORDER BY reply_count DESC
LIMIT 20


// ============================================================================
// 📈 USER SEGMENTATION
// ============================================================================

// 16. POWER USERS BY CATEGORY - Who dominates each category?
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category)
WHERE c.name IN ['integration_requests', 'feature_requests', 'authentication_needs', 'customization_requests']
WITH c.name AS category, u, count(m) AS messages_in_category
ORDER BY category, messages_in_category DESC
WITH category, collect({username: u.username, platform: u.platform, count: messages_in_category})[0..5] AS top_users
RETURN category, top_users

// 17. CASUAL VS HARDCORE USERS - Message distribution
MATCH (u:User)
WITH CASE
    WHEN u.message_count >= 50 THEN 'hardcore'
    WHEN u.message_count >= 20 THEN 'power_user'
    WHEN u.message_count >= 5 THEN 'regular'
    ELSE 'casual'
END AS user_type, count(u) AS user_count, avg(u.message_count) AS avg_messages
RETURN user_type,
       user_count,
       round(avg_messages, 1) AS avg_msgs,
       round(user_count * 100.0 / 5902, 1) AS percentage_of_users
ORDER BY
  CASE user_type
    WHEN 'hardcore' THEN 1
    WHEN 'power_user' THEN 2
    WHEN 'regular' THEN 3
    ELSE 4
  END

// 18. USERS WHO SWITCHED FROM QUESTIONS TO COMPLAINTS - Journey analysis
MATCH (u:User)-[:POSTED]->(m1:Message)-[:CLASSIFIED_AS]->(c1:Category {name: 'questions'}),
      (u)-[:POSTED]->(m2:Message)-[:CLASSIFIED_AS]->(c2:Category)
WHERE c2.name IN ['pricing_complaints', 'bug_reports', 'performance_issues']
  AND m2.timestamp > m1.timestamp
WITH u, c2.name AS complaint_type, count(DISTINCT m2) AS complaints
RETURN u.username AS user,
       u.platform AS platform,
       complaint_type,
       complaints,
       u.message_count AS total_messages
ORDER BY complaints DESC
LIMIT 20


// ============================================================================
// 🎯 SPECIFIC OPPORTUNITY QUERIES
// ============================================================================

// 19. REDDIT-SPECIFIC PRICING COMPLAINERS (might be different audience)
MATCH (u:User {platform: 'reddit'})-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'pricing_complaints'})
RETURN u.username AS user,
       count(m) AS complaints,
       collect(m.content)[0..3] AS sample_complaints
ORDER BY complaints DESC
LIMIT 15

// 20. DISCORD POWER USERS REQUESTING FEATURES
MATCH (u:User {platform: 'discord'})-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'feature_requests'})
WHERE u.message_count > 20
RETURN u.username AS user,
       count(m) AS feature_requests,
       u.message_count AS total_messages,
       collect(m.content)[0..3] AS sample_requests
ORDER BY feature_requests DESC
LIMIT 15

// 21. AUTHENTICATION NEEDS COMBINED WITH INTEGRATION REQUESTS - Security-focused users
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category)
WHERE c.name IN ['authentication_needs', 'integration_requests']
WITH u, collect(DISTINCT c.name) AS interests, count(m) AS total_security_messages
WHERE size(interests) = 2
RETURN u.username AS user,
       u.platform AS platform,
       total_security_messages AS security_related_messages,
       u.message_count AS total_messages
ORDER BY total_security_messages DESC

// 22. EARLY ADOPTERS WHO BECAME COMPLAINERS - Timeline analysis
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category)
WHERE c.name IN ['pricing_complaints', 'bug_reports', 'performance_issues']
WITH u, min(m.timestamp) AS first_complaint, count(m) AS complaint_count
WHERE u.first_seen < first_complaint - (30 * 24 * 3600) // Users active 30+ days before complaining
RETURN u.username AS user,
       u.platform AS platform,
       complaint_count AS complaints,
       u.message_count AS total_messages,
       datetime({epochSeconds: u.first_seen}) AS joined,
       datetime({epochSeconds: first_complaint}) AS first_complaint_date
ORDER BY complaint_count DESC
LIMIT 20


// ============================================================================
// 🔍 CONTENT ANALYSIS HELPERS
// ============================================================================

// 23. SAMPLE MESSAGES BY CATEGORY - Get actual content for analysis
MATCH (m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'integration_requests'})
RETURN m.content AS message,
       m.platform AS platform,
       datetime({epochSeconds: m.timestamp}) AS posted_date
ORDER BY m.timestamp DESC
LIMIT 50

// 24. FIND MESSAGES CONTAINING SPECIFIC KEYWORDS (change 'stripe' to search)
MATCH (m:Message)-[:CLASSIFIED_AS]->(c:Category)
WHERE toLower(m.content) CONTAINS 'stripe'
RETURN m.content AS message,
       c.name AS category,
       m.platform AS platform,
       datetime({epochSeconds: m.timestamp}) AS date
LIMIT 30

// 25. PRICING COMPLAINTS WITH NUMBERS (likely mentioning price points)
MATCH (m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'pricing_complaints'})
WHERE m.content =~ '.*\\$[0-9]+.*' OR m.content =~ '.*[0-9]+/mo.*'
RETURN m.content AS complaint,
       m.platform AS platform,
       datetime({epochSeconds: m.timestamp}) AS date
LIMIT 30


// ============================================================================
// 📊 VISUAL QUERIES (Great for Browser Graph View)
// ============================================================================

// 26. VISUALIZE: Top 5 pricing complainers and their messages
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'pricing_complaints'})
WITH u, count(m) AS complaint_count
ORDER BY complaint_count DESC
LIMIT 5
MATCH (u)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'pricing_complaints'})
RETURN u, m, c

// 27. VISUALIZE: Integration requesters network
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'integration_requests'})
WITH u, count(m) AS requests
WHERE requests >= 3
MATCH (u)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'integration_requests'})
RETURN u, m, c
LIMIT 100

// 28. VISUALIZE: Multi-complainers (users with 3+ complaint types)
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category)
WHERE c.name IN ['pricing_complaints', 'bug_reports', 'performance_issues', 'usability_problems']
WITH u, collect(DISTINCT c) AS categories, collect(m) AS messages
WHERE size(categories) >= 3
RETURN u, categories, messages[0..10]
LIMIT 50


// ============================================================================
// 💡 EXPORT-READY QUERIES (Table view, CSV-friendly)
// ============================================================================

// 29. EXPORT: Top 100 integration requesters with contact info
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'integration_requests'})
WITH u, count(m) AS requests, collect(m.content) AS all_requests
RETURN u.username AS username,
       u.platform AS platform,
       u.message_count AS total_messages,
       requests AS integration_requests,
       all_requests
ORDER BY requests DESC
LIMIT 100

// 30. EXPORT: Category summary for reporting
MATCH (c:Category)
OPTIONAL MATCH (c)<-[:CLASSIFIED_AS]-(m:Message)
OPTIONAL MATCH (m)<-[:POSTED]-(u:User)
WITH c, count(DISTINCT m) AS message_count, count(DISTINCT u) AS unique_users
RETURN c.name AS category,
       c.description AS description,
       message_count,
       unique_users,
       round(message_count * 100.0 / 26545, 2) AS percentage
ORDER BY message_count DESC


// ============================================================================
// 🎨 BONUS: Platform-Specific Deep Dives
// ============================================================================

// 31. REDDIT SUPER-USERS - Most active Reddit contributors
MATCH (u:User {platform: 'reddit'})-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category)
WHERE c.name IN ['integration_requests', 'feature_requests', 'authentication_needs']
RETURN u.username AS redditor,
       count(m) AS actionable_messages,
       u.message_count AS total_messages,
       collect(DISTINCT c.name) AS interested_in
ORDER BY actionable_messages DESC
LIMIT 20

// 32. DISCORD-SPECIFIC PAIN POINTS
MATCH (m:Message {platform: 'discord'})-[:CLASSIFIED_AS]->(c:Category)
WHERE c.name IN ['bug_reports', 'performance_issues', 'usability_problems']
WITH c.name AS pain_point, count(m) AS mentions
RETURN pain_point,
       mentions,
       round(mentions * 100.0 / 18653, 2) AS pct_of_discord_messages
ORDER BY mentions DESC


// ============================================================================
// 🔥 THE MONEY QUERY - Best Opportunities Summary
// ============================================================================

// 33. OPPORTUNITY HEATMAP - What people want most, by platform
MATCH (m:Message)-[:CLASSIFIED_AS]->(c:Category)
WHERE c.name IN ['integration_requests', 'feature_requests', 'authentication_needs', 'customization_requests']
MATCH (m)<-[:POSTED]-(u:User)
WITH c.name AS opportunity_category,
     m.platform AS platform,
     count(DISTINCT m) AS message_count,
     count(DISTINCT u) AS unique_users,
     collect(DISTINCT u.username)[0..10] AS sample_users
RETURN opportunity_category,
       platform,
       message_count,
       unique_users,
       round(message_count * 1.0 / unique_users, 1) AS avg_msgs_per_user,
       sample_users
ORDER BY message_count DESC
