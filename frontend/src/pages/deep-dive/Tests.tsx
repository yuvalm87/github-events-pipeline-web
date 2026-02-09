import ReactMarkdown from 'react-markdown'

const doc = `
## Tests (pytest)

Tests live in \`tests/\` and cover:

- **test_api.py** — API endpoints: health, load idempotency (same file skipped), ingest (mocked or real), top-repos and user-sessions with test data.
- **test_analytics.py** — Analytics logic: top repos and user sessions with seeded events.
- **test_repo_events_nested.py** — Nested view (ARRAY<STRUCT>) and schema evolution (v1 vs v2).

Fixtures (e.g. in \`conftest.py\`) set \`DATA_DIR\` to a temp directory so the real \`data/\` is not touched.

### How to run

\`\`\`bash
pip install -r requirements-dev.txt
pytest -q
\`\`\`

Verbose:
\`\`\`bash
pytest -v
\`\`\`

Single file:
\`\`\`bash
pytest tests/test_api.py -v
\`\`\`

### Coverage

To see what is tested, run with coverage:

\`\`\`bash
pip install pytest-cov
pytest --cov=app --cov-report=term-missing
\`\`\`
`

export function Tests() {
  return (
    <article className="doc-page">
      <h1>Deep Dive: Tests</h1>
      <div className="doc-content">
        <ReactMarkdown>{doc}</ReactMarkdown>
      </div>
    </article>
  )
}
