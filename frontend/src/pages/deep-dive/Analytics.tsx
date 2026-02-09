import ReactMarkdown from 'react-markdown'
import { MermaidDiagram } from '../../components/MermaidDiagram'

const mermaid = `
flowchart TB
  subgraph models [Analytics models]
    TR[Top Repos by event count]
    US[User Sessions 30-min rule]
    Nested[ARRAY of STRUCT per repo]
  end
  events[(events table)]
  events --> TR
  events --> US
  events --> Nested
`

const doc = `
## Analytics

- **Top Repos:** \`GET /top-repos?days=7&limit=10\` — aggregates by \`repo_name\`, counts events and distinct users, filters by \`created_at > NOW() - days\`, returns top \`limit\` repos.
- **User Sessions:** \`GET /user-sessions?days=7&limit=50\` — a session is a sequence of events per \`actor_login\` with **no more than 30 minutes** between consecutive events. The SQL uses a 30-minute expanded window and \`LAG()\` to detect session breaks, then assigns session IDs and aggregates.
- **Nested view (ARRAY<STRUCT>):** \`repo_events_nested\` (and v1/v2) — one row per repo with \`events\` as a list of structs (\`event_id\`, \`actor_login\`, \`event_type\`, \`created_at\`, and in v2 \`repo_id\`). Used for schema evolution: add columns in a new view without changing the base table.

### User sessions (30-min rule) — app/sql/user_sessions.sql

\`\`\`sql
-- Detect session boundaries: gap > 30 min => new session
CASE
  WHEN LAG(created_at) OVER (...) IS NULL THEN 1
  WHEN EXTRACT(EPOCH FROM (created_at - LAG(created_at) OVER (...))) / 60 > 30 THEN 1
  ELSE 0
END as is_new_session
\`\`\`

### Nested view (app/sql/repo_events_nested.sql)

\`\`\`sql
SELECT repo_name,
  list(struct_pack(
    event_id := event_id,
    actor_login := actor_login,
    event_type := event_type,
    created_at := created_at,
    repo_id := repo_id
  ) ORDER BY created_at, event_id) AS events
FROM events
GROUP BY repo_name;
\`\`\`

### How to run / debug

1. Load data first: \`POST /ingest\` then \`POST /load\`.
2. \`GET /top-repos?days=7&limit=10\` and \`GET /user-sessions?days=7&limit=50\` from Playground or \`curl\`.
3. For nested view: connect to DuckDB and run \`SELECT * FROM repo_events_nested LIMIT 5;\`.
`

export function Analytics() {
  return (
    <article className="doc-page">
      <h1>Deep Dive: Analytics</h1>
      <div className="doc-content">
        <MermaidDiagram chart={mermaid} />
        <ReactMarkdown>{doc}</ReactMarkdown>
      </div>
    </article>
  )
}
