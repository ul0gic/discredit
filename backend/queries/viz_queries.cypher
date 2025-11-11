// ============================================================================
// NEO4J BROWSER VISUALIZATION QUERIES
// ============================================================================
// Copy-paste these queries into Neo4j Browser (http://localhost:7474)
// to explore the Discredit knowledge graph
//
// Recommended: Configure node colors in Browser settings:
// - Category: Orange
// - User: Purple
// - Message: Gray/White
// - Integration: Green (when added)
// - Feature: Blue (when added)
// ============================================================================


// ----------------------------------------------------------------------------
// 🟠 CATEGORY OVERVIEW
// ----------------------------------------------------------------------------

// 1. View all 10 categories with their message counts
MATCH (c:Category)
RETURN c
ORDER BY c.message_count DESC

// 2. Show category distribution as a hub visualization
MATCH (c:Category)<-[:CLASSIFIED_AS]-(m:Message)
RETURN c, m
LIMIT 200


// ----------------------------------------------------------------------------
// 🔍 TOP CATEGORIES DEEP DIVE
// ----------------------------------------------------------------------------

// 3. Explore integration_requests category (908 messages)
MATCH (c:Category {name: 'integration_requests'})<-[:CLASSIFIED_AS]-(m:Message)<-[:POSTED]-(u:User)
RETURN c, m, u
LIMIT 100

// 4. Explore feature_requests category (742 messages)
MATCH (c:Category {name: 'feature_requests'})<-[:CLASSIFIED_AS]-(m:Message)<-[:POSTED]-(u:User)
RETURN c, m, u
LIMIT 100

// 5. Sample messages from each actionable category
MATCH (c:Category)<-[:CLASSIFIED_AS]-(m:Message)
WHERE c.name IN ['integration_requests', 'feature_requests', 'authentication_needs',
                 'usability_problems', 'performance_issues', 'customization_requests']
RETURN c, m
LIMIT 150


// ----------------------------------------------------------------------------
// 👥 USER ANALYSIS
// ----------------------------------------------------------------------------

// 6. Find power users (high message count)
MATCH (u:User)
WHERE u.message_count > 20
RETURN u
ORDER BY u.message_count DESC
LIMIT 50

// 7. Show power users and their messages
MATCH (u:User)-[:POSTED]->(m:Message)
WHERE u.message_count > 30
RETURN u, m
LIMIT 100

// 8. Most active users by platform
MATCH (u:User)
RETURN u.platform AS platform,
       u.username AS user,
       u.message_count AS messages
ORDER BY u.message_count DESC
LIMIT 20

// 9. Users posting to specific categories
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'integration_requests'})
RETURN u, m, c
LIMIT 50


// ----------------------------------------------------------------------------
// 💬 MESSAGE PATTERNS
// ----------------------------------------------------------------------------

// 10. Messages by platform
MATCH (m:Message)
RETURN m.platform AS platform, count(m) AS count
ORDER BY count DESC

// 11. Sample messages from Discord vs Reddit
MATCH (m:Message)
WHERE m.platform IN ['discord', 'reddit']
RETURN m.platform, m.content, m.source
LIMIT 20

// 12. Message threading (conversations)
MATCH path = (m1:Message)-[:REPLIES_TO]->(m2:Message)
RETURN path
LIMIT 50

// 13. Deep conversation threads (3+ messages)
MATCH path = (m1:Message)-[:REPLIES_TO*1..3]->(m2:Message)
RETURN path
LIMIT 30


// ----------------------------------------------------------------------------
// 🎯 OPPORTUNITY DISCOVERY QUERIES
// ----------------------------------------------------------------------------

// 14. Find most vocal users about integrations
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'integration_requests'})
WITH u, count(m) AS integration_mentions
WHERE integration_mentions > 3
RETURN u.username, u.platform, integration_mentions
ORDER BY integration_mentions DESC

// 15. Find users requesting both features AND integrations
MATCH (u:User)-[:POSTED]->(m1:Message)-[:CLASSIFIED_AS]->(c1:Category {name: 'feature_requests'}),
      (u)-[:POSTED]->(m2:Message)-[:CLASSIFIED_AS]->(c2:Category {name: 'integration_requests'})
RETURN u, m1, m2, c1, c2
LIMIT 50

// 16. Users with authentication AND integration needs (potential bundle)
MATCH (u:User)-[:POSTED]->(m1:Message)-[:CLASSIFIED_AS]->(c1:Category {name: 'authentication_needs'}),
      (u)-[:POSTED]->(m2:Message)-[:CLASSIFIED_AS]->(c2:Category {name: 'integration_requests'})
RETURN u.username, count(DISTINCT m1) AS auth_mentions, count(DISTINCT m2) AS integration_mentions
ORDER BY auth_mentions + integration_mentions DESC


// ----------------------------------------------------------------------------
// 📊 STATISTICS & ANALYSIS
// ----------------------------------------------------------------------------

// 17. Category distribution (table view)
MATCH (c:Category)
OPTIONAL MATCH (m:Message)-[:CLASSIFIED_AS]->(c)
RETURN c.name AS category,
       c.description AS description,
       count(m) AS message_count,
       round(count(m) * 100.0 / 26545, 1) AS percentage
ORDER BY message_count DESC

// 18. Platform distribution by category
MATCH (m:Message)-[:CLASSIFIED_AS]->(c:Category)
RETURN c.name AS category,
       m.platform AS platform,
       count(m) AS count
ORDER BY category, count DESC

// 19. Timeline analysis (messages over time)
MATCH (m:Message)
WITH date(datetime({epochSeconds: m.timestamp})) AS date, count(m) AS messages
RETURN date, messages
ORDER BY date

// 20. User activity distribution
MATCH (u:User)
RETURN u.message_count AS messages,
       count(u) AS user_count
ORDER BY messages


// ----------------------------------------------------------------------------
// 🌟 VISUAL EXPLORATION QUERIES
// ----------------------------------------------------------------------------

// 21. Random sample of the graph (great for overview)
MATCH (c:Category)<-[:CLASSIFIED_AS]-(m:Message)<-[:POSTED]-(u:User)
RETURN c, m, u
LIMIT 200

// 22. Show only actionable categories (no spam/questions)
MATCH (c:Category)<-[:CLASSIFIED_AS]-(m:Message)
WHERE NOT c.name IN ['spam_noise', 'questions']
RETURN c, m
LIMIT 200

// 23. Focus on small actionable categories
MATCH (c:Category)<-[:CLASSIFIED_AS]-(m:Message)<-[:POSTED]-(u:User)
WHERE c.name IN ['authentication_needs', 'customization_requests']
RETURN c, m, u

// 24. Network of users discussing integrations
MATCH (u:User)-[:POSTED]->(m:Message)-[:CLASSIFIED_AS]->(c:Category {name: 'integration_requests'})
RETURN u, m, c
LIMIT 150


// ----------------------------------------------------------------------------
// 🔥 ADVANCED ANALYSIS (Future - After Entity Extraction)
// ----------------------------------------------------------------------------

// These queries will work after you add Integration/Feature entity nodes

// Find top integrations requested
// MATCH (i:Integration)<-[:MENTIONS]-(m:Message)
// RETURN i.name, i.request_count, i.unique_users
// ORDER BY i.request_count DESC
// LIMIT 20

// Show integration co-occurrence patterns
// MATCH (i1:Integration)-[r:CO_REQUESTED_WITH]->(i2:Integration)
// WHERE r.strength > 0.3
// RETURN i1, r, i2

// Find users similar by interests
// MATCH (u1:User)-[r:SIMILAR_TO]->(u2:User)
// WHERE r.score > 0.5
// RETURN u1, r, u2
// LIMIT 50


// ============================================================================
// 💡 TIPS FOR NEO4J BROWSER
// ============================================================================
//
// 1. Configure node colors in Browser settings (gear icon):
//    - Click a node type → Choose color → Apply to all
//
// 2. Adjust graph layout:
//    - Click and drag nodes to reorganize
//    - Use physics settings (bottom panel) to spread nodes
//
// 3. Expand relationships:
//    - Double-click a node to see its connections
//    - Right-click → Expand
//
// 4. Filter results:
//    - Add WHERE clauses to queries
//    - Adjust LIMIT values
//
// 5. Export visualizations:
//    - Bottom panel → Export as PNG/SVG
//
// ============================================================================
