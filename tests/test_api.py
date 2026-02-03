import os
import json
from pathlib import Path

import duckdb


def write_jsonl(file_path: Path, events: list[dict]):
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


def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_load_idempotent_on_same_file(client, tmp_path):
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"

    # Create 3 fake events with _ingested_at
    events = [
        {
            "id": "e1",
            "type": "PushEvent",
            "created_at": "2026-02-03T10:00:00Z",
            "_ingested_at": "2026-02-03T10:00:01Z",
            "actor": {"id": 11, "login": "alice"},
            "repo": {"id": 101, "name": "alice/repo"},
            "payload": {}
        },
        {
            "id": "e2",
            "type": "CreateEvent",
            "created_at": "2026-02-03T10:01:00Z",
            "_ingested_at": "2026-02-03T10:01:01Z",
            "actor": {"id": 12, "login": "bob"},
            "repo": {"id": 102, "name": "bob/repo"},
            "payload": {}
        },
        {
            "id": "e3",
            "type": "IssuesEvent",
            "created_at": "2026-02-03T10:02:00Z",
            "_ingested_at": "2026-02-03T10:02:01Z",
            "actor": {"id": 13, "login": "carol"},
            "repo": {"id": 103, "name": "carol/repo"},
            "payload": {}
        }
    ]

    file_path = raw_dir / "events_batch_000.jsonl"
    write_jsonl(file_path, events)

    # First load
    r1 = client.post("/load")
    assert r1.status_code == 200
    body1 = r1.json()
    assert body1["loaded_files"] == 1
    assert body1["inserted_events"] == 3

    # Second load (idempotent)
    r2 = client.post("/load")
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["skipped_files"] == 1
    assert body2["inserted_events"] == 0

    # Verify DB has 3 events
    conn = db_conn_from_env()
    try:
        cnt = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        assert cnt == 3
    finally:
        conn.close()


def test_event_dedup_within_different_files(client, tmp_path):
    data_dir = Path(os.environ["DATA_DIR"])
    raw_dir = data_dir / "raw"

    # File A: events e1, e2
    events_a = [
        {
            "id": "e1",
            "type": "PushEvent",
            "created_at": "2026-02-03T11:00:00Z",
            "_ingested_at": "2026-02-03T11:00:01Z",
            "actor": {"id": 21, "login": "dan"},
            "repo": {"id": 201, "name": "dan/repo"},
            "payload": {}
        },
        {
            "id": "e2",
            "type": "CreateEvent",
            "created_at": "2026-02-03T11:01:00Z",
            "_ingested_at": "2026-02-03T11:01:01Z",
            "actor": {"id": 22, "login": "eva"},
            "repo": {"id": 202, "name": "eva/repo"},
            "payload": {}
        }
    ]

    # File B: events e2 (duplicate), e3
    events_b = [
        {
            "id": "e2",
            "type": "CreateEvent",
            "created_at": "2026-02-03T11:01:00Z",
            "_ingested_at": "2026-02-03T11:01:01Z",
            "actor": {"id": 22, "login": "eva"},
            "repo": {"id": 202, "name": "eva/repo"},
            "payload": {}
        },
        {
            "id": "e3",
            "type": "IssuesEvent",
            "created_at": "2026-02-03T11:02:00Z",
            "_ingested_at": "2026-02-03T11:02:01Z",
            "actor": {"id": 23, "login": "frank"},
            "repo": {"id": 203, "name": "frank/repo"},
            "payload": {}
        }
    ]

    file_a = raw_dir / "events_a.jsonl"
    file_b = raw_dir / "events_b.jsonl"
    write_jsonl(file_a, events_a)
    write_jsonl(file_b, events_b)

    # Run load
    r = client.post("/load")
    assert r.status_code == 200
    body = r.json()

    # inserted_events should equal unique event ids (e1,e2,e3) => 3
    assert body["inserted_events"] == 3

    # events table should have 3 rows
    conn = db_conn_from_env()
    try:
        cnt = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        assert cnt == 3
    finally:
        conn.close()
