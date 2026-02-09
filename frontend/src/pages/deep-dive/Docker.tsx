import ReactMarkdown from 'react-markdown'
import { MermaidDiagram } from '../../components/MermaidDiagram'

const mermaid = `
flowchart LR
  subgraph host [Host]
    data[./data]
  end
  subgraph container [Container]
    app[/app/data]
    api[FastAPI :8000]
  end
  data -->|volume mount| app
  app --> api
`

const doc = `
## Docker

- **docker-compose.yml** defines one service (e.g. \`api\`): build from repo root, port \`8000:8000\`, volume \`./data:/app/data\`.
- **Data persistence:** \`data/raw/\` and \`data/duckdb/\` live on the host under \`./data\`. Restarting the container keeps all JSONL files and the DuckDB database.
- **Dockerfile:** \`FROM python:3.12-slim\`, install deps, copy app, \`CMD uvicorn app.main:app --host 0.0.0.0 --port 8000\`.

### How to run

\`\`\`bash
docker compose up --build
\`\`\`

Then: API at \`http://localhost:8000\`, docs at \`http://localhost:8000/docs\`. To use the frontend Playground, run the frontend locally (\`cd frontend && npm run dev\`) and keep the proxy pointing at \`http://localhost:8000\` (backend in Docker).
`

export function Docker() {
  return (
    <article className="doc-page">
      <h1>Deep Dive: Docker</h1>
      <div className="doc-content">
        <MermaidDiagram chart={mermaid} />
        <ReactMarkdown>{doc}</ReactMarkdown>
      </div>
    </article>
  )
}
