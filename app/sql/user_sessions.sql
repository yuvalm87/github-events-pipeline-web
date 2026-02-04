-- User Sessions Modeling
-- 
-- A session starts when a user becomes active after 30 minutes of inactivity.
-- 
-- Logic:
-- 1. Expand query window backward 30 minutes to capture pre-boundary activity
-- 2. Use window functions to detect session breaks (gaps > 30 min)
-- 3. Assign deterministic session IDs via cumulative sum
-- 4. Aggregate to session level
-- 5. Filter results to requested time window
-- 
-- Parameters:
-- - start_timestamp: filter sessions to session_end_at >= start_timestamp
-- - limit_count: max number of sessions to return
-- 
-- Output fields:
-- - actor_login: GitHub user login
-- - session_id: deterministic session number per actor (starts at 1, increments per gap > 30 min)
-- - session_start_at: timestamp of first event in session
-- - session_end_at: timestamp of last event in session
-- - events_in_session: count of events in session

WITH expanded_window AS (
    -- Load events from expanded window: start_timestamp - 30 minutes
    SELECT 
        event_id,
        actor_login,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY actor_login ORDER BY created_at, event_id) as event_sequence
    FROM events
    WHERE actor_login IS NOT NULL
        AND created_at >= $1::TIMESTAMP - INTERVAL '30 minutes'
    ORDER BY actor_login, created_at, event_id
),

session_breaks AS (
    -- Detect session boundaries using LAG() to get previous event timestamp
    SELECT 
        event_id,
        actor_login,
        created_at,
        event_sequence,
        LAG(created_at) OVER (PARTITION BY actor_login ORDER BY created_at, event_id) as prev_created_at,
        CASE 
            WHEN LAG(created_at) OVER (PARTITION BY actor_login ORDER BY created_at, event_id) IS NULL THEN 1
            WHEN EXTRACT(EPOCH FROM (created_at - LAG(created_at) OVER (PARTITION BY actor_login ORDER BY created_at, event_id))) / 60 > 30 THEN 1
            ELSE 0
        END as is_new_session
    FROM expanded_window
),

session_ids AS (
    -- Assign session IDs using cumulative sum of session breaks
    SELECT 
        event_id,
        actor_login,
        created_at,
        SUM(is_new_session) OVER (PARTITION BY actor_login ORDER BY created_at, event_id) as session_id
    FROM session_breaks
),

aggregated_sessions AS (
    -- Aggregate events to session level
    SELECT 
        actor_login,
        session_id,
        MIN(created_at) as session_start_at,
        MAX(created_at) as session_end_at,
        COUNT(*) as events_in_session
    FROM session_ids
    GROUP BY actor_login, session_id
)

-- Final result: filter to requested time window and apply limit
SELECT 
    actor_login,
    session_id,
    session_start_at,
    session_end_at,
    events_in_session
FROM aggregated_sessions
WHERE session_end_at >= $1::TIMESTAMP
ORDER BY actor_login, session_start_at DESC
LIMIT $2::INTEGER;
