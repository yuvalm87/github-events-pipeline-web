-- Top Repositories Analytical Query
-- Groups events by repository and computes aggregated metrics.
-- 
-- This query is parameterized by the Python query function:
--   - min_timestamp: computed as NOW() - INTERVAL days
--   - limit: number of rows to return
--
-- Metrics computed:
--   - total_events: count of all events in the time window
--   - unique_users: count of distinct actor_login values
--   - push_events: count of events where event_type='PushEvent'
--   - first_event_at: earliest created_at timestamp for the repository
--   - last_event_at: latest created_at timestamp for the repository
--   - processed_at: current query execution timestamp

SELECT
    repo_name,
    COUNT(*) AS total_events,
    COUNT(DISTINCT actor_login) AS unique_users,
    COUNT(CASE WHEN event_type = 'PushEvent' THEN 1 END) AS push_events,
    MIN(created_at) AS first_event_at,
    MAX(created_at) AS last_event_at,
    NOW() AS processed_at
FROM events
WHERE created_at > ?  -- parameter 1: min_timestamp
GROUP BY repo_name
ORDER BY total_events DESC
LIMIT ?  -- parameter 2: limit
