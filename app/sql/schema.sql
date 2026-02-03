-- DuckDB schema for GitHub Events analytics

-- Create events table with primary key
CREATE TABLE IF NOT EXISTS events (
    event_id STRING PRIMARY KEY,
    event_type STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    actor_id INTEGER,
    actor_login STRING,
    repo_id INTEGER,
    repo_name STRING,
    payload JSON,
    raw JSON NOT NULL,
    source_file STRING NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Create loaded_files table to track loaded files for idempotent loading
CREATE TABLE IF NOT EXISTS loaded_files (
    file_path STRING PRIMARY KEY,
    file_size BIGINT NOT NULL,
    file_mtime TIMESTAMP NOT NULL,
    file_sha256 STRING NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Create index on event_type for faster filtering
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);

-- Create index on created_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);

-- Create index on loaded_files sha256 for duplicate detection
CREATE INDEX IF NOT EXISTS idx_loaded_files_sha256 ON loaded_files(file_sha256);
