import ReactMarkdown from 'react-markdown'
import { MermaidDiagram } from '../../components/MermaidDiagram'

const mermaid = `
sequenceDiagram
  participant Client
  participant API as FastAPI
  participant BG as Background task
  participant GitHub as GitHub API
  participant FS as data/raw/

  Client->>API: POST /ingest
  API->>API: add_task(ingest_events)
  API-->>Client: 202 accepted
  BG->>GitHub: GET /events
  GitHub-->>BG: JSON array
  BG->>BG: add _ingested_at, batch(150)
  BG->>FS: write events_YYYYMMDD_HHMMSS_batch_NNN.jsonl
`

const doc = `
## POST /ingest

Triggers **background** ingestion: the API returns immediately with \`202 Accepted\` while the actual fetch and write runs in a FastAPI \`BackgroundTasks\` task.

- **Source:** \`https://api.github.com/events\` (one page per run).
- **Enrichment:** Each event gets \`_ingested_at\` (UTC ISO timestamp).
- **Batches:** Events are written in JSONL files with ~150 events per file.
- **File naming:** \`events_YYYYMMDD_HHMMSS_batch_NNN.jsonl\` (e.g. \`events_20250209_120000_batch_001.jsonl\`).
- **Immutability:** Files are written once and not modified; re-running ingest creates new files.

### Code (app/ingest.py)

\`\`\`python
def save_events_batch(batch: list[dict], batch_number: int) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = DATA_DIR / f"events_{timestamp}_batch_{batch_number:03d}.jsonl"
    with open(filename, "w", encoding="utf-8") as f:
        for event in batch:
            f.write(json.dumps(event, ensure_ascii=False) + "\\n")
\`\`\`

### How to run / debug

1. Start backend: \`uvicorn app.main:app --host 0.0.0.0 --port 8000\`.
2. Call \`POST http://localhost:8000/ingest\` (or use Playground).
3. Check \`data/raw/\` for new \`events_*.jsonl\` files.
4. Logs: ingestion progress and errors are logged by \`app.ingest\` (logger).
`

export function Ingestion() {
  return (
    <article className="doc-page">
      <h1>Deep Dive: Ingestion</h1>
      <div className="doc-content">
        <MermaidDiagram chart={mermaid} />
        <ReactMarkdown>{doc}</ReactMarkdown>
      </div>
    </article>
  )
}
