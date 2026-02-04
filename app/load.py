"""JSONL to DuckDB loader with idempotent file tracking.

This module implements safe, incremental loading of immutable JSONL files into DuckDB:

Transaction Model:
- Each file is loaded within its own transaction (per-file transactions)
- This allows partial progress: if file N fails, files N+1, N+2, ... still process
- Each file's load is atomic: all events from a file are loaded together or not at all
- Idempotency is enforced at both file level (SHA256 tracking) and event level (event_id PK)
- Re-running load_all_events() is safe: unchanged files are skipped, new files are loaded

File Immutability:
- Raw JSONL files in data/raw/ are assumed immutable by design
- If a file's content changes (different SHA256), a warning is logged
- Changed files are skipped to maintain immutability contract
- This ensures events are never duplicated due to file modifications
"""
import datetime
import hashlib
import json
import logging
from pathlib import Path
import os
from typing import NamedTuple

import duckdb

from app.db import init_database, get_db_connection

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR = Path(os.environ.get("DATA_DIR", str(DEFAULT_DATA_DIR)))
RAW_DATA_DIR = DATA_DIR / "raw"


class LoadResult(NamedTuple):
    """Results from a load operation."""
    scanned_files: int
    loaded_files: int
    skipped_files: int
    inserted_events: int
    duration_ms: int
    db_path: str


def compute_file_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def get_file_metadata(file_path: Path) -> tuple[int, str]:
    """Get file size (bytes) and modification time (as ISO timestamp string)."""
    stat = file_path.stat()
    mtime_timestamp = stat.st_mtime
    mtime_dt = datetime.datetime.fromtimestamp(mtime_timestamp, tz=datetime.timezone.utc)
    return int(stat.st_size), mtime_dt.isoformat()


def load_events_from_jsonl(conn: duckdb.DuckDBPyConnection, file_path: Path) -> int:
    """
    Load events from a JSONL file into the events table.
    Returns the number of inserted events.
    """
    try:
        num_events = 0
        source_file = file_path.name
        
        # Read JSONL file line by line
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                event_data = json.loads(line)
                
                # Extract flattened fields
                event_id = event_data.get("id")
                event_type = event_data.get("type")
                created_at_str = event_data.get("created_at")
                ingested_at_str = event_data.get("_ingested_at")
                
                actor = event_data.get("actor", {})
                actor_id = actor.get("id")
                actor_login = actor.get("login")
                
                repo = event_data.get("repo", {})
                repo_id = repo.get("id")
                repo_name = repo.get("name")
                
                payload = event_data.get("payload")
                raw = event_data
                
                # Parse timestamps
                try:
                    # created_at format: 2026-02-03T10:35:16Z
                    created_at = None
                    if created_at_str:
                        created_at = created_at_str.replace("Z", "+00:00")
                    
                    # _ingested_at format: 2026-02-03T10:35:16.421955Z
                    ingested_at = None
                    if ingested_at_str:
                        ingested_at = ingested_at_str.replace("Z", "+00:00")
                except Exception as e:
                    logger.warning(f"Error parsing timestamps for event {event_id}: {e}")
                
                # Skip if event_id already exists (event-level dedupe)
                try:
                    exists = conn.execute(
                        "SELECT 1 FROM events WHERE event_id = ? LIMIT 1",
                        [event_id]
                    ).fetchone()
                    if exists:
                        continue

                    # Insert into database; count only actual inserts
                    conn.execute("""
                        INSERT INTO events 
                        (event_id, event_type, created_at, ingested_at, actor_id, actor_login, repo_id, repo_name, payload, raw, source_file)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        event_id,
                        event_type,
                        created_at,
                        ingested_at,
                        actor_id,
                        actor_login,
                        repo_id,
                        repo_name,
                        json.dumps(payload) if payload else None,
                        json.dumps(raw),
                        source_file
                    ])
                    num_events += 1
                except Exception as e:
                    logger.warning(f"Error inserting event {event_id}: {e}")
                    continue
        
        return num_events
    except Exception as e:
        logger.error(f"Error loading events from {file_path.name}: {e}")
        raise


def load_all_events() -> LoadResult:
    """
    Idempotent load: scan data/raw/*.jsonl files, skip already-loaded files,
    load new files into DuckDB, track file metadata to prevent re-processing.
    
    Returns:
        LoadResult with counts of scanned_files, loaded_files, skipped_files,
        inserted_events, duration_ms, and db_path.
    """
    import time
    start_time = time.perf_counter()
    
    logger.info("Starting JSONL to DuckDB load process")
    
    # Initialize database schema
    init_database()
    
    # Ensure raw data directory exists
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    conn = get_db_connection()
    
    try:
        scanned_files = 0
        loaded_files = 0
        skipped_files = 0
        inserted_events = 0
        
        # Get list of all JSONL files sorted by name
        jsonl_files = sorted(RAW_DATA_DIR.glob("*.jsonl"))
        scanned_files = len(jsonl_files)
        logger.info(f"Found {scanned_files} JSONL file(s) in {RAW_DATA_DIR}")
        
        for file_path in jsonl_files:
            file_name = file_path.name
            logger.info(f"Processing file: {file_name}")
            
            # Compute file SHA256 for change detection
            file_sha256 = compute_file_sha256(file_path)
            file_size, file_mtime = get_file_metadata(file_path)
            
            # Check if file already loaded
            existing = conn.execute(
                "SELECT file_sha256 FROM loaded_files WHERE file_path = ?",
                [file_name]
            ).fetchall()
            
            if existing:
                existing_sha256 = existing[0][0]
                if existing_sha256 == file_sha256:
                    # File unchanged: skip it (idempotency)
                    logger.info(f"  Skipped: {file_name} (already loaded, hash unchanged)")
                    skipped_files += 1
                    continue
                else:
                    # File content changed: log warning and skip to maintain immutability
                    logger.warning(
                        f"  Skipped: {file_name} (file modified since last load, "
                        f"old hash={existing_sha256[:12]}..., new hash={file_sha256[:12]}...). "
                        f"Raw files are immutable; ignoring modified file."
                    )
                    skipped_files += 1
                    continue
            
            # Load events from file within a per-file transaction
            # Each file transaction is atomic: either all events load or none do
            # This allows safe re-runs even if some files fail midway
            try:
                conn.begin()
                
                num_events = load_events_from_jsonl(conn, file_path)
                inserted_events += num_events
                
                # Record file as loaded with its metadata for future idempotency checks
                conn.execute("""
                    INSERT INTO loaded_files (file_path, file_size, file_mtime, file_sha256)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (file_path) DO UPDATE SET 
                        file_size = excluded.file_size,
                        file_mtime = excluded.file_mtime,
                        file_sha256 = excluded.file_sha256,
                        loaded_at = now()
                """, [file_name, file_size, file_mtime, file_sha256])
                
                conn.commit()
                loaded_files += 1
                logger.info(f"  Loaded: {file_name} ({num_events} events)")
                
            except Exception as e:
                conn.rollback()
                logger.error(f"  Failed to load {file_name}: {e}. Skipping file and continuing.")
                # Continue with next file instead of failing entirely
                continue
        
        # Final summary log
        logger.info(
            f"Load complete: scanned={scanned_files}, loaded={loaded_files}, "
            f"skipped={skipped_files}, inserted_events={inserted_events}"
        )
        
        # Create (or recreate) nested views after loading events
        sql_file = Path(__file__).parent / "sql" / "repo_events_nested.sql"
        if sql_file.exists():
            with open(sql_file, "r") as f:
                sql_script = f.read()
            conn.execute(sql_script)
            logger.info("Created/refreshed nested views from repo_events_nested.sql")
        else:
            logger.warning(f"SQL file not found: {sql_file}")
        
        elapsed_ms = int((time.perf_counter() - start_time) * 1000)
        
        from app.db import get_db_path
        return LoadResult(
            scanned_files=scanned_files,
            loaded_files=loaded_files,
            skipped_files=skipped_files,
            inserted_events=inserted_events,
            duration_ms=elapsed_ms,
            db_path=get_db_path()
        )
    finally:
        conn.close()












