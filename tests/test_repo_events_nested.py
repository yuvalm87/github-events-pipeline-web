import os
import json
from pathlib import Path
from datetime import datetime, timezone
import duckdb
import pytest


def write_jsonl(file_path: Path, events: list[dict]):
    """Write events to a JSONL file."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        for ev in events:
            f.write(json.dumps(ev))
            f.write("\n")

def db_conn_from_env():
    import importlib
    db_mod = importlib.import_module("app.db")
    db_path = db_mod.get_db_path()
    conn = duckdb.connect(db_path)
    return conn

def test_repo_events_nested_row_count(client):
    """repo_events_nested returns one row per repo."""
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    events = [
        {"id": "evt1", "type": "PushEvent", "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "actor": {"id": 1, "login": "user1"}, "repo": {"id": 100, "name": "org/repo1"}, "payload": {}},
        {"id": "evt2", "type": "PushEvent", "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "actor": {"id": 2, "login": "user2"}, "repo": {"id": 200, "name": "org/repo2"}, "payload": {}}
    ]
    write_jsonl(raw_dir / "batch1.jsonl", events)
    client.post("/load")
    conn = db_conn_from_env()
    count = conn.execute("SELECT COUNT(*) FROM repo_events_nested").fetchone()[0]
    assert count == 2
    conn.close()

def test_repo_events_nested_list_length(client):
    """list length equals event count per repo."""
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    events = [
        {"id": f"evt{i}", "type": "PushEvent", "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "actor": {"id": i, "login": f"user{i}"}, "repo": {"id": 100, "name": "org/repo1"}, "payload": {}} for i in range(1, 4)
    ]
    write_jsonl(raw_dir / "batch2.jsonl", events)
    client.post("/load")
    conn = db_conn_from_env()
    list_len = conn.execute("SELECT len(events) FROM repo_events_nested WHERE repo_name='org/repo1'").fetchone()[0]
    assert list_len == 3
    event_count = conn.execute("SELECT COUNT(*) FROM events WHERE repo_name='org/repo1'").fetchone()[0]
    assert event_count == 3
    conn.close()

def test_repo_events_nested_deterministic_ordering(client):
    """deterministic ordering inside list."""
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    t1 = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    t2 = (now.replace(second=now.second + 1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    events = [
        {"id": "evtB", "type": "PushEvent", "created_at": t2, "_ingested_at": t2, "actor": {"id": 2, "login": "user2"}, "repo": {"id": 100, "name": "org/repo1"}, "payload": {}},
        {"id": "evtA", "type": "PushEvent", "created_at": t1, "_ingested_at": t1, "actor": {"id": 1, "login": "user1"}, "repo": {"id": 100, "name": "org/repo1"}, "payload": {}}
    ]
    write_jsonl(raw_dir / "batch3.jsonl", events)
    client.post("/load")
    conn = db_conn_from_env()
    row = conn.execute("SELECT events[1].created_at, events[2].created_at, events[1].event_id, events[2].event_id FROM repo_events_nested WHERE repo_name='org/repo1'").fetchone()
    created1, created2, eid1, eid2 = row
    assert created1 <= created2
    if created1 == created2:
        assert eid1 <= eid2
    conn.close()

def test_repo_events_nested_schema_evolution(client):
    """schema evolution field exists (repo_id)."""
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    events = [
        {"id": "evt1", "type": "PushEvent", "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "_ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "actor": {"id": 1, "login": "user1"}, "repo": {"id": 100, "name": "org/repo1"}, "payload": {}}
    ]
    write_jsonl(raw_dir / "batch4.jsonl", events)
    client.post("/load")
    conn = db_conn_from_env()
    repo_id = conn.execute("SELECT events[1].repo_id FROM repo_events_nested WHERE repo_name='org/repo1'").fetchone()[0]
    assert repo_id == 100
    conn.close()
