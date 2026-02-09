import ReactMarkdown from 'react-markdown'
import { MermaidDiagram } from '../components/MermaidDiagram'

const overviewMermaid = `
flowchart LR
  subgraph ext [External]
    GitHub[GitHub API]
  end
  subgraph ingest [Ingestion]
    JSONL[JSONL batches]
  end
  subgraph store [Storage]
    DuckDB[(DuckDB)]
  end
  subgraph out [Output]
    API[FastAPI endpoints]
  end
  GitHub --> JSONL
  JSONL --> DuckDB
  DuckDB --> API
`

const overviewDoc = `
This system collects **GitHub public events**, stores them in **DuckDB**, and exposes **analytics** via a REST API.

## End-to-end flow

1. **Ingestion** — \`POST /ingest\` fetches events from \`https://api.github.com/events\`, enriches them with an ingestion timestamp, and writes immutable JSONL batch files to \`data/raw/\` (e.g. \`events_20250109_120000_batch_001.jsonl\`).

2. **Loading** — \`POST /load\` scans \`data/raw/\`, computes a SHA256 hash per file, and loads new files into DuckDB. Idempotency is enforced by tracking loaded files (by hash) and deduplicating events by \`event_id\` primary key. Raw JSON is preserved in the \`events\` table.

3. **Analytics** — DuckDB holds \`events\` and \`loaded_files\` tables. Analytics include:
   - **Top repos** — repositories ranked by event count over a time window.
   - **User sessions** — sequences of events per user with a 30-minute inactivity rule.
   - **Nested views** — \`ARRAY<STRUCT>\` views for repo-level event lists and schema evolution.

4. **API** — The FastAPI app exposes \`/health\`, \`/ingest\`, \`/load\`, \`/top-repos\`, and \`/user-sessions\`. Use the **Playground** to call them from the browser.

## How to run locally

- **Backend:** \`uvicorn app.main:app --host 0.0.0.0 --port 8000\` (from repo root).
- **Frontend:** \`cd frontend && npm run dev\` — dev server runs with a proxy so \`/api/*\` forwards to the backend.
- Open the frontend URL (e.g. http://localhost:5173), use **Playground** to trigger ingest/load and query top-repos and user-sessions.
`

export function Overview() {
  return (
    <article className="doc-page">
      <h1>Overview</h1>
      <div className="doc-content">
        <MermaidDiagram chart={overviewMermaid} id="overview-flow" />
        <ReactMarkdown>{overviewDoc}</ReactMarkdown>
      </div>
    </article>
  )
}
