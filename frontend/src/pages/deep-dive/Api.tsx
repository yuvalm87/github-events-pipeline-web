import ReactMarkdown from 'react-markdown'

const doc = `
## API endpoints

All endpoints are served at the backend root (e.g. \`http://localhost:8000\`). When using the frontend dev server, use the **/api** prefix so the Vite proxy forwards to the backend.

| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Liveness check |
| POST | /ingest | Start background ingestion (GitHub → JSONL) |
| POST | /load | Load JSONL from data/raw/ into DuckDB |
| GET | /top-repos | Top repositories by event count (query: days, limit) |
| GET | /user-sessions | User sessions with 30-min rule (query: days, limit) |

### Examples

**Health**
\`\`\`bash
curl http://localhost:8000/health
# {"status":"ok"}
\`\`\`

**Ingest**
\`\`\`bash
curl -X POST http://localhost:8000/ingest
# {"status":"accepted","message":"Ingestion started in background"}
\`\`\`

**Load**
\`\`\`bash
curl -X POST http://localhost:8000/load
# {"scanned_files":5,"loaded_files":2,"skipped_files":3,"inserted_events":300,"duration_ms":120,"db_path":"..."}
\`\`\`

**Top repos**
\`\`\`bash
curl "http://localhost:8000/top-repos?days=7&limit=10"
# [{"repo_name":"owner/repo","total_events":100,"unique_users":20,...}, ...]
\`\`\`

**User sessions**
\`\`\`bash
curl "http://localhost:8000/user-sessions?days=7&limit=50"
# [{"actor_login":"alice","session_id":1,"session_start_at":"...","session_end_at":"...","events_in_session":5}, ...]
\`\`\`

### How to run / debug

- Use the **Playground** in this app to send requests and inspect responses.
- Backend OpenAPI docs: \`http://localhost:8000/docs\`.
`

export function Api() {
  return (
    <article className="doc-page">
      <h1>Deep Dive: API</h1>
      <div className="doc-content">
        <ReactMarkdown>{doc}</ReactMarkdown>
      </div>
    </article>
  )
}
