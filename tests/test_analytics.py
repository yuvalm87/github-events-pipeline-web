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
