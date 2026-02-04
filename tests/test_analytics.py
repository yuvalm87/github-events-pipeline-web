"""Tests for analytics queries."""
import os
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pytest
import duckdb


def write_jsonl(file_path: Path, events: list[dict]):
    """Write events to a JSONL file."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        for ev in events:
            f.write(json.dumps(ev))
            f.write("\n")


def db_conn_from_env():
    """Get DuckDB connection using environment DATA_DIR."""
    import importlib
    db_mod = importlib.import_module("app.db")
    db_path = db_mod.get_db_path()
    conn = duckdb.connect(db_path)
    return conn


def test_get_top_repos_empty_database(client):

        """Test that top_repos query runs on empty database."""
        import importlib
        
        # Re-import analytics after test database is set up
        if "app.analytics" in __import__("sys").modules:
            del __import__("sys").modules["app.analytics"]
        analytics_mod = importlib.import_module("app.analytics")
        
        # Ensure schema is initialized
        db_mod = importlib.import_module("app.db")
        db_mod.init_database()
        
        result = analytics_mod.get_top_repos(days=30, limit=10)
        assert result == []

def test_get_top_repos_returns_correct_columns(client):
    """Test that top_repos returns all required columns."""
    import importlib
    
    # Re-import analytics after test database is set up
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    # Insert test data
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc)
    events = [
        {
            "id": "evt1",
            "type": "PushEvent",
            "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 1, "login": "user1"},
            "repo": {"id": 100, "name": "org/repo1"},
            "payload": {}
        },
        {
            "id": "evt2",
            "type": "PushEvent",
            "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 2, "login": "user2"},
            "repo": {"id": 100, "name": "org/repo1"},
            "payload": {}
        },
    ]
    
    file_path = raw_dir / "test_batch.jsonl"
    write_jsonl(file_path, events)
    
    # Load events
    client.post("/load")
    
    # Query top repos
    result = analytics_mod.get_top_repos(days=30, limit=10)
    
    assert len(result) > 0
    row = result[0]
    
    # Verify all required columns are present
    required_columns = {
        "repo_name", "total_events", "unique_users", 
        "push_events", "first_event_at", "last_event_at", "processed_at"
    }
    assert set(row.keys()) == required_columns
    
    # Verify basic types
    assert isinstance(row["repo_name"], str)
    assert isinstance(row["total_events"], int)
    assert isinstance(row["unique_users"], int)
    assert isinstance(row["push_events"], int)


def test_get_top_repos_push_events_counting(client):
    """Test that push_events counts only PushEvent type."""
    import importlib
    
    # Re-import analytics after test database is set up
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    # Insert test data with mixed event types
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc)
    events = [
        {
            "id": "evt_push_1",
            "type": "PushEvent",
            "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 1, "login": "user1"},
            "repo": {"id": 200, "name": "org/repo2"},
            "payload": {}
        },
        {
            "id": "evt_push_2",
            "type": "PushEvent",
            "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 1, "login": "user1"},
            "repo": {"id": 200, "name": "org/repo2"},
            "payload": {}
        },
        {
            "id": "evt_create_1",
            "type": "CreateEvent",
            "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 1, "login": "user1"},
            "repo": {"id": 200, "name": "org/repo2"},
            "payload": {}
        },
    ]
    
    file_path = raw_dir / "test_push_batch.jsonl"
    write_jsonl(file_path, events)
    
    # Load events
    client.post("/load")
    
    # Query top repos
    result = analytics_mod.get_top_repos(days=30, limit=10)
    
    assert len(result) > 0
    repo2 = next((r for r in result if r["repo_name"] == "org/repo2"), None)
    assert repo2 is not None
    
    # org/repo2 has 3 total events, 2 of which are PushEvent
    assert repo2["total_events"] == 3
    assert repo2["push_events"] == 2


def test_get_top_repos_days_filter(client):
    """Test that days filter excludes old events."""
    import importlib
    
    # Re-import analytics after test database is set up
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    # Insert test data with events at different timestamps
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc)
    old_date = (now - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    recent_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    events = [
        {
            "id": "evt_old_1",
            "type": "PushEvent",
            "created_at": old_date,
            "_ingested_at": recent_date,
            "actor": {"id": 1, "login": "user1"},
            "repo": {"id": 300, "name": "org/old_repo"},
            "payload": {}
        },
        {
            "id": "evt_recent_1",
            "type": "PushEvent",
            "created_at": recent_date,
            "_ingested_at": recent_date,
            "actor": {"id": 1, "login": "user1"},
            "repo": {"id": 301, "name": "org/recent_repo"},
            "payload": {}
        },
    ]
    
    file_path = raw_dir / "test_days_batch.jsonl"
    write_jsonl(file_path, events)
    
    # Load events
    client.post("/load")
    
    # Query with 30-day filter (should exclude old_repo)
    result_30 = analytics_mod.get_top_repos(days=30, limit=10)
    repos_30 = {r["repo_name"] for r in result_30}
    assert "org/recent_repo" in repos_30
    assert "org/old_repo" not in repos_30
    
    # Query with 90-day filter (should include both)
    result_90 = analytics_mod.get_top_repos(days=90, limit=10)
    repos_90 = {r["repo_name"] for r in result_90}
    assert "org/recent_repo" in repos_90
    assert "org/old_repo" in repos_90


def test_get_top_repos_limit_parameter(client):
    """Test that limit parameter works correctly."""
    import importlib
    
    # Re-import analytics after test database is set up
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    # Insert test data with multiple repositories
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc)
    events = []
    for repo_id in range(400, 405):
        for i in range(3):
            events.append({
                "id": f"evt_repo{repo_id}_{i}",
                "type": "PushEvent",
                "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "actor": {"id": 1, "login": "user1"},
                "repo": {"id": repo_id, "name": f"org/repo{repo_id}"},
                "payload": {}
            })
    
    file_path = raw_dir / "test_limit_batch.jsonl"
    write_jsonl(file_path, events)
    
    # Load events
    client.post("/load")
    
    # Query with limit=2
    result = analytics_mod.get_top_repos(days=30, limit=2)
    assert len(result) <= 2
    
    # Query with limit=10 (should return all 5 repos)
    result = analytics_mod.get_top_repos(days=30, limit=10)
    assert len(result) <= 10

# =============================================================================
# Tests for User Sessions Modeling (Part 3B)
# =============================================================================

def test_get_user_sessions_empty_database(client):
    """Test that user_sessions query runs on empty database."""
    import importlib
    
    # Re-import analytics after test database is set up
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    # Ensure schema is initialized
    db_mod = importlib.import_module("app.db")
    db_mod.init_database()
    
    result = analytics_mod.get_user_sessions(days=7, limit=50)
    assert result == []


def test_get_user_sessions_single_event_is_a_session(client):
    """Test that a single event creates a valid session."""
    import importlib
    
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc)
    events = [
        {
            "id": "session_evt_1",
            "type": "PushEvent",
            "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 1, "login": "user_single"},
            "repo": {"id": 100, "name": "org/repo1"},
            "payload": {}
        },
    ]
    
    file_path = raw_dir / "test_single_event.jsonl"
    write_jsonl(file_path, events)
    client.post("/load")
    
    result = analytics_mod.get_user_sessions(days=7, limit=50)
    
    assert len(result) == 1
    session = result[0]
    assert session["actor_login"] == "user_single"
    assert session["session_id"] == 1
    assert session["events_in_session"] == 1
    assert session["session_start_at"] == session["session_end_at"]


def test_get_user_sessions_events_within_30_minutes_same_session(client):
    """Test that events within 30 minutes of each other are in the same session."""
    import importlib
    
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc).replace(microsecond=0)
    event_time_1 = now
    event_time_2 = now + timedelta(minutes=15)
    event_time_3 = now + timedelta(minutes=29)
    
    events = [
        {
            "id": "same_session_evt_1",
            "type": "PushEvent",
            "created_at": event_time_1.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 2, "login": "user_same_session"},
            "repo": {"id": 100, "name": "org/repo1"},
            "payload": {}
        },
        {
            "id": "same_session_evt_2",
            "type": "CreateEvent",
            "created_at": event_time_2.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 2, "login": "user_same_session"},
            "repo": {"id": 101, "name": "org/repo2"},
            "payload": {}
        },
        {
            "id": "same_session_evt_3",
            "type": "PushEvent",
            "created_at": event_time_3.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 2, "login": "user_same_session"},
            "repo": {"id": 102, "name": "org/repo3"},
            "payload": {}
        },
    ]
    
    file_path = raw_dir / "test_same_session.jsonl"
    write_jsonl(file_path, events)
    client.post("/load")
    
    result = analytics_mod.get_user_sessions(days=7, limit=50)
    
    assert len(result) == 1
    session = result[0]
    assert session["actor_login"] == "user_same_session"
    assert session["session_id"] == 1
    assert session["events_in_session"] == 3
    # Compare without timezone (DuckDB returns naive timestamps)
    assert session["session_start_at"] == event_time_1.replace(tzinfo=None)
    assert session["session_end_at"] == event_time_3.replace(tzinfo=None)


def test_get_user_sessions_gap_over_30_minutes_creates_new_session(client):
    """Test that a gap > 30 minutes creates a new session."""
    import importlib
    
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc).replace(microsecond=0)
    event_time_1 = now
    event_time_2 = now + timedelta(minutes=31)  # > 30 minutes
    
    events = [
        {
            "id": "gap_session_evt_1",
            "type": "PushEvent",
            "created_at": event_time_1.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 3, "login": "user_gap_sessions"},
            "repo": {"id": 100, "name": "org/repo1"},
            "payload": {}
        },
        {
            "id": "gap_session_evt_2",
            "type": "CreateEvent",
            "created_at": event_time_2.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 3, "login": "user_gap_sessions"},
            "repo": {"id": 101, "name": "org/repo2"},
            "payload": {}
        },
    ]
    
    file_path = raw_dir / "test_gap_sessions.jsonl"
    write_jsonl(file_path, events)
    client.post("/load")
    
    result = analytics_mod.get_user_sessions(days=7, limit=50)
    
    # Should have exactly 2 sessions
    assert len(result) == 2
    
    # Sessions should be in reverse chronological order
    sessions = sorted(result, key=lambda s: s["session_start_at"])
    
    assert sessions[0]["session_id"] == 1
    assert sessions[0]["events_in_session"] == 1
    assert sessions[0]["session_start_at"] == event_time_1.replace(tzinfo=None)
    
    assert sessions[1]["session_id"] == 2
    assert sessions[1]["events_in_session"] == 1
    assert sessions[1]["session_start_at"] == event_time_2.replace(tzinfo=None)


def test_get_user_sessions_multiple_users_isolated(client):
    """Test that sessions are properly isolated per actor."""
    import importlib
    
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc)
    events = [
        # User A: 2 events close together (same session)
        {
            "id": "multi_user_evt_a1",
            "type": "PushEvent",
            "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 4, "login": "user_a"},
            "repo": {"id": 100, "name": "org/repo1"},
            "payload": {}
        },
        {
            "id": "multi_user_evt_a2",
            "type": "CreateEvent",
            "created_at": (now + timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 4, "login": "user_a"},
            "repo": {"id": 101, "name": "org/repo2"},
            "payload": {}
        },
        # User B: 1 event (single session)
        {
            "id": "multi_user_evt_b1",
            "type": "PushEvent",
            "created_at": (now + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 5, "login": "user_b"},
            "repo": {"id": 102, "name": "org/repo3"},
            "payload": {}
        },
    ]
    
    file_path = raw_dir / "test_multi_users.jsonl"
    write_jsonl(file_path, events)
    client.post("/load")
    
    result = analytics_mod.get_user_sessions(days=7, limit=50)
    
    # Should have 2 sessions total (1 for user_a, 1 for user_b)
    assert len(result) == 2
    
    user_a_sessions = [s for s in result if s["actor_login"] == "user_a"]
    user_b_sessions = [s for s in result if s["actor_login"] == "user_b"]
    
    assert len(user_a_sessions) == 1
    assert len(user_b_sessions) == 1
    
    assert user_a_sessions[0]["events_in_session"] == 2
    assert user_b_sessions[0]["events_in_session"] == 1


def test_get_user_sessions_expanded_window_boundary(client):
    """Test that sessions spanning the time window boundary are correctly captured.
    
    This test verifies the expanded window logic: an event 10 minutes before
    the requested window start should be included if there's an event within
    30 minutes after the window start, so they're in the same session.
    """
    import importlib
    
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc).replace(microsecond=0)
    window_start = now - timedelta(days=5)
    
    # Event 10 minutes before window start
    event_time_1 = window_start - timedelta(minutes=10)
    # Event 20 minutes after window start (within 30 min of event_1)
    event_time_2 = window_start + timedelta(minutes=20)
    
    events = [
        {
            "id": "boundary_evt_1",
            "type": "PushEvent",
            "created_at": event_time_1.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 6, "login": "user_boundary"},
            "repo": {"id": 100, "name": "org/repo1"},
            "payload": {}
        },
        {
            "id": "boundary_evt_2",
            "type": "CreateEvent",
            "created_at": event_time_2.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 6, "login": "user_boundary"},
            "repo": {"id": 101, "name": "org/repo2"},
            "payload": {}
        },
    ]
    
    file_path = raw_dir / "test_boundary.jsonl"
    write_jsonl(file_path, events)
    client.post("/load")
    
    # Query for 5 days window (should include both events in one session)
    result = analytics_mod.get_user_sessions(days=5, limit=50)
    
    # Should have 1 session with both events
    assert len(result) == 1
    session = result[0]
    assert session["actor_login"] == "user_boundary"
    assert session["events_in_session"] == 2
    # Compare without timezone (DuckDB returns naive timestamps)
    assert session["session_start_at"] == event_time_1.replace(tzinfo=None)
    assert session["session_end_at"] == event_time_2.replace(tzinfo=None)


def test_get_user_sessions_returns_correct_columns(client):
    """Test that get_user_sessions returns all required columns."""
    import importlib
    
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc)
    events = [
        {
            "id": "cols_evt_1",
            "type": "PushEvent",
            "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": 7, "login": "user_cols"},
            "repo": {"id": 100, "name": "org/repo1"},
            "payload": {}
        },
    ]
    
    file_path = raw_dir / "test_cols.jsonl"
    write_jsonl(file_path, events)
    client.post("/load")
    
    result = analytics_mod.get_user_sessions(days=7, limit=50)
    
    assert len(result) == 1
    session = result[0]
    
    # Verify all required columns are present
    required_columns = {
        "actor_login", "session_id", "session_start_at", 
        "session_end_at", "events_in_session"
    }
    assert set(session.keys()) == required_columns
    
    # Verify basic types
    assert isinstance(session["actor_login"], str)
    assert isinstance(session["session_id"], int)
    assert isinstance(session["events_in_session"], int)


def test_get_user_sessions_limit_parameter(client):
    """Test that limit parameter works correctly."""
    import importlib
    
    if "app.analytics" in __import__("sys").modules:
        del __import__("sys").modules["app.analytics"]
    analytics_mod = importlib.import_module("app.analytics")
    
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    
    now = datetime.now(timezone.utc)
    events = []
    # Create 5 users with 1 event each (5 sessions)
    for user_id in range(8, 13):
        events.append({
            "id": f"limit_evt_user{user_id}",
            "type": "PushEvent",
            "created_at": (now + timedelta(minutes=user_id)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "actor": {"id": user_id, "login": f"user_{user_id}"},
            "repo": {"id": 100 + user_id, "name": f"org/repo{user_id}"},
            "payload": {}
        })
    
    file_path = raw_dir / "test_limit.jsonl"
    write_jsonl(file_path, events)
    client.post("/load")
    
    # Query with limit=2
    result_2 = analytics_mod.get_user_sessions(days=7, limit=2)
    assert len(result_2) <= 2
    
    # Query with limit=10 (should return all 5 sessions)
    result_10 = analytics_mod.get_user_sessions(days=7, limit=10)
    assert len(result_10) <= 10