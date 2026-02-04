-- repo_events_nested.sql
-- DuckDB views for nested repo event modeling and schema evolution

-- v1: repo_name, events as list of struct(event_id, actor_login, event_type, created_at)
CREATE OR REPLACE VIEW repo_events_nested_v1 AS
SELECT
  repo_name,
  list(
    struct_pack(
      event_id := event_id,
      actor_login := actor_login,
      event_type := event_type,
      created_at := created_at
    ) ORDER BY created_at, event_id
  ) AS events
FROM events
GROUP BY repo_name;

-- v2: adds repo_id to struct
CREATE OR REPLACE VIEW repo_events_nested_v2 AS
SELECT
  repo_name,
  list(
    struct_pack(
      event_id := event_id,
      actor_login := actor_login,
      event_type := event_type,
      created_at := created_at,
      repo_id := repo_id
    ) ORDER BY created_at, event_id
  ) AS events
FROM events
GROUP BY repo_name;

-- Optionally, alias latest version
CREATE OR REPLACE VIEW repo_events_nested AS SELECT * FROM repo_events_nested_v2;
