# GitHub Events Pipeline

A FastAPI service that ingests GitHub public events and loads them into DuckDB for analytics.

## Project Structure

```
app/
  main.py           # FastAPI application with endpoints
  ingest.py         # GitHub API event fetching and enrichment
  load.py           # JSONL to DuckDB loader with idempotent deduplication
  db.py             # DuckDB connection and initialization helpers
  sql/
    schema.sql      # Database schema (events, loaded_files tables)
  __pycache__/
data/
  raw/              # Immutable JSONL batch files (events_YYYYMMDD_HHMMSS_batch_NNN.jsonl)
  duckdb/           # DuckDB database file (created on first load)
requirements.txt    # Python dependencies
README.md           # This file
```

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Database location:**
   - DuckDB file will be created at `data/duckdb/github_events.duckdb` on first load
   - Database directory is created automatically if it doesn't exist

## Running the Service

Start the FastAPI server with Uvicorn:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Or with auto-reload during development:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000`.

## API Endpoints

### Health Check

```bash
GET /health
```

Returns: `{"status": "ok"}`

### Ingest Events

Fetches the latest GitHub public events from `https://api.github.com/events`, enriches them with ingestion metadata, and writes JSONL batch files to `data/raw/`.

```bash
POST /ingest
```

**Request:**
```json
(no body)
```

**Response:**
```json
{
  "status": "accepted",
  "message": "Ingestion started in background"
}
```

**Details:**
- Runs asynchronously in the background
- Fetches events from GitHub's public events API
- Enriches each event with `_ingested_at` timestamp
- Saves events in JSONL format (~150 events per batch file)
- Files are immutable and stored at: `data/raw/events_YYYYMMDD_HHMMSS_batch_NNN.jsonl`

### Load Events to DuckDB

Loads JSONL files from `data/raw/` into DuckDB with idempotent deduplication using file content hashing.

```bash
POST /load
```

**Request:**
```json
(no body)
```

**Response:**
```json
{
  "scanned_files": 10,
  "loaded_files": 5,
  "skipped_files": 5,
  "inserted_events": 750,
  "db_path": "data/duckdb/github_events.duckdb"
}
```

**Details:**
- Scans `data/raw/` for `.jsonl` files (sorted by filename)
- Computes SHA256 hash of each file to detect changes
- Skips files already loaded with identical hash (idempotent)
- Reads JSONL using DuckDB's `read_json_auto()`
- Inserts events into `events` table with deduplication by `event_id`
- Tracks loaded files in `loaded_files` table (prevents re-processing)
- Uses transactions per file (rollback on failure, continues with next file)

## Database Schema

### `events` Table

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING (PK) | Unique GitHub event ID |
| `event_type` | STRING | Event type (e.g., "PushEvent", "CreateEvent") |
| `created_at` | TIMESTAMP | Event creation time (from GitHub) |
| `ingested_at` | TIMESTAMP | When event was collected by ingest service |
| `actor_id` | INTEGER | GitHub user/bot ID who triggered the event |
| `actor_login` | STRING | GitHub login of the actor |
| `repo_id` | INTEGER | GitHub repository ID |
| `repo_name` | STRING | Repository name (format: "owner/repo") |
| `payload` | JSON | Event-specific payload (varies by event type) |
| `raw` | JSON | Full event JSON from GitHub API |
| `source_file` | STRING | Name of JSONL file this event came from |
| `loaded_at` | TIMESTAMP | When event was loaded into DuckDB |

**Primary Key:** `event_id` (guarantees no duplicate events)

**Indexes:**
- `idx_events_type` on `event_type` (for filtering by event type)
- `idx_events_created_at` on `created_at` (for time-range queries)

### `loaded_files` Table

| Column | Type | Description |
|--------|------|-------------|
| `file_path` | STRING (PK) | JSONL filename from `data/raw/` |
| `file_size` | BIGINT | File size in bytes |
| `file_mtime` | TIMESTAMP | Last modified time |
| `file_sha256` | STRING | SHA256 hash of file content (for change detection) |
| `loaded_at` | TIMESTAMP | When file was loaded |

**Primary Key:** `file_path` (one entry per source file)

**Index:**
- `idx_loaded_files_sha256` on `file_sha256` (for duplicate detection)

## Example Workflow

1. **Start the service:**
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```

2. **Ingest events from GitHub:**
   ```bash
   curl -X POST http://localhost:8000/ingest
   ```

3. **Load into DuckDB:**
   ```bash
   curl -X POST http://localhost:8000/load
   ```

4. **Query the database directly:**
   ```python
   import duckdb
   conn = duckdb.connect('data/duckdb/github_events.duckdb')
   events = conn.execute('SELECT event_type, COUNT(*) FROM events GROUP BY event_type').fetchall()
   print(events)
   ```

## Idempotency Guarantees

The load process is fully idempotent:

1. **File-level deduplication:** Each file's SHA256 hash is computed and stored in `loaded_files`. If the file hasn't changed (same hash), it's skipped.

2. **Event-level deduplication:** The `events` table uses `event_id` as primary key. Attempting to insert a duplicate event_id results in `ON CONFLICT ... DO NOTHING`, preventing duplicates even if files are re-processed.

3. **Transaction safety:** Each file load is wrapped in a transaction. If any error occurs, changes for that file are rolled back. Subsequent files continue processing normally.

Because raw JSONL files are immutable, re-running `/load` multiple times is safe and will only insert new events from new files.

## Example Analytical Queries

Once events are loaded into DuckDB, you can run various analytical queries. Examples:

### Events Per Event Type

```sql
SELECT event_type, COUNT(*) as count
FROM events
GROUP BY event_type
ORDER BY count DESC;
```

Example output:
```
PushEvent          | 250
CreateEvent        | 120
PullRequestEvent   | 85
IssuesEvent        | 45
...
```

### Top 10 Repositories by Event Count

```sql
SELECT repo_name, COUNT(*) as event_count
FROM events
WHERE repo_name IS NOT NULL
GROUP BY repo_name
ORDER BY event_count DESC
LIMIT 10;
```

### Events Per Hour

```sql
SELECT 
  DATE_TRUNC('hour', created_at) as hour,
  COUNT(*) as event_count
FROM events
GROUP BY DATE_TRUNC('hour', created_at)
ORDER BY hour DESC
LIMIT 24;
```

### Most Active Users

```sql
SELECT actor_login, COUNT(*) as event_count
FROM events
WHERE actor_login IS NOT NULL
GROUP BY actor_login
ORDER BY event_count DESC
LIMIT 10;
```

### Recent Events in a Specific Repository

```sql
SELECT event_id, event_type, actor_login, created_at
FROM events
WHERE repo_name = 'owner/repo_name'
ORDER BY created_at DESC
LIMIT 20;
```

Connect to the database and run these queries:

```python
import duckdb

conn = duckdb.connect('data/duckdb/github_events.duckdb')

# Example: Get event distribution
result = conn.execute(
    'SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY COUNT(*) DESC'
).fetchall()

for event_type, count in result:
    print(f"{event_type}: {count}")

conn.close()
```
