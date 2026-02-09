import ReactMarkdown from 'react-markdown'
import { MermaidDiagram } from '../../components/MermaidDiagram'

const mermaid = `
flowchart LR
  subgraph raw [data/raw]
    F1[file1.jsonl]
    F2[file2.jsonl]
  end
  subgraph loader [Load]
    SHA[SHA256 per file]
    PK[event_id PK]
  end
  subgraph db [DuckDB]
    EV[events]
    LF[loaded_files]
  end
  F1 --> SHA
  F2 --> SHA
  SHA --> LF
  SHA --> PK
  PK --> EV
`

const doc = `
## POST /load

Loads JSONL files from \`data/raw/\` into DuckDB with **idempotent** behavior:

- **Table schema:** \`events\` (event_id PK, event_type, created_at, ingested_at, actor_*, repo_*, payload, raw, source_file, loaded_at) and \`loaded_files\` (file_path PK, file_size, file_mtime, file_sha256, loaded_at).
- **Idempotency:** Each file’s **SHA256** is stored in \`loaded_files\`. If the same path already exists with the same hash, the file is skipped. Per-event deduplication uses \`event_id\` as primary key (\`ON CONFLICT DO NOTHING\` or skip-if-exists in code).
- **Raw JSON:** The full event JSON is kept in the \`raw\` column for replay and debugging.

### Schema (app/sql/schema.sql)

\`\`\`sql
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

CREATE TABLE IF NOT EXISTS loaded_files (
    file_path STRING PRIMARY KEY,
    file_size BIGINT NOT NULL,
    file_mtime TIMESTAMP NOT NULL,
    file_sha256 STRING NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT now()
);
\`\`\`

### How to run / debug

1. Ensure \`data/raw/\` has some \`events_*.jsonl\` (run \`POST /ingest\` first).
2. Call \`POST http://localhost:8000/load\`. Response includes \`scanned_files\`, \`loaded_files\`, \`skipped_files\`, \`inserted_events\`, \`duration_ms\`, \`db_path\`.
3. Re-calling \`/load\` should show \`skipped_files\` for already-loaded files (idempotency).
4. Inspect DB: \`duckdb data/duckdb/github_events.duckdb\` then \`SELECT * FROM events LIMIT 5;\`, \`SELECT * FROM loaded_files;\`.
`

export function Loading() {
  return (
    <article className="doc-page">
      <h1>Deep Dive: Loading</h1>
      <div className="doc-content">
        <MermaidDiagram chart={mermaid} />
        <ReactMarkdown>{doc}</ReactMarkdown>
      </div>
    </article>
  )
}
