import { useState, useCallback } from 'react'

type EndpointId = 'health' | 'ingest' | 'load' | 'top-repos' | 'user-sessions'

interface EndpointConfig {
  id: EndpointId
  label: string
  method: string
  path: string
  queryParams: { name: string; default: string; placeholder?: string }[]
  hasBody: boolean
}

const ENDPOINTS: EndpointConfig[] = [
  { id: 'health', label: 'GET /health', method: 'GET', path: '/health', queryParams: [], hasBody: false },
  { id: 'ingest', label: 'POST /ingest', method: 'POST', path: '/ingest', queryParams: [], hasBody: false },
  { id: 'load', label: 'POST /load', method: 'POST', path: '/load', queryParams: [], hasBody: false },
  {
    id: 'top-repos',
    label: 'GET /top-repos',
    method: 'GET',
    path: '/top-repos',
    queryParams: [
      { name: 'days', default: '7', placeholder: 'e.g. 7' },
      { name: 'limit', default: '10', placeholder: 'e.g. 10' },
    ],
    hasBody: false,
  },
  {
    id: 'user-sessions',
    label: 'GET /user-sessions',
    method: 'GET',
    path: '/user-sessions',
    queryParams: [
      { name: 'days', default: '7', placeholder: 'e.g. 7' },
      { name: 'limit', default: '50', placeholder: 'e.g. 50' },
    ],
    hasBody: false,
  },
]

interface ResponseState {
  status: number | null
  timeMs: number | null
  data: unknown
  error: string | null
}

const STORAGE_KEY = 'playground-params'
const HISTORY_KEY = 'playground-history'
const HISTORY_MAX = 10

interface HistoryEntry {
  method: string
  url: string
  status: number | null
  timeMs: number | null
  ts: number
}

function loadStoredParams(): Record<string, string> {
  try {
    const s = localStorage.getItem(STORAGE_KEY)
    return s ? (JSON.parse(s) as Record<string, string>) : {}
  } catch {
    return {}
  }
}

function saveStoredParams(params: Record<string, string>) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(params))
  } catch {
    // ignore
  }
}

function loadHistory(): HistoryEntry[] {
  try {
    const s = localStorage.getItem(HISTORY_KEY)
    return s ? (JSON.parse(s) as HistoryEntry[]) : []
  } catch {
    return []
  }
}

function saveHistory(entries: HistoryEntry[]) {
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(entries.slice(0, HISTORY_MAX)))
  } catch {
    // ignore
  }
}

export function Playground() {
  const [activeId, setActiveId] = useState<EndpointId>('health')
  const [baseUrl, setBaseUrl] = useState('/api')
  const [path, setPath] = useState('/health')
  const [method, setMethod] = useState('GET')
  const [queryValues, setQueryValues] = useState<Record<string, string>>({})
  const [body, setBody] = useState('')
  const [loading, setLoading] = useState(false)
  const [response, setResponse] = useState<ResponseState>({ status: null, timeMs: null, data: null, error: null })
  const [history, setHistory] = useState<HistoryEntry[]>(() => loadHistory())

  const active = ENDPOINTS.find((e) => e.id === activeId) ?? ENDPOINTS[0]

  const selectEndpoint = useCallback(
    (ep: EndpointConfig) => {
      setActiveId(ep.id)
      setPath(ep.path)
      setMethod(ep.method)
      const stored = loadStoredParams()
      const next: Record<string, string> = {}
      for (const q of ep.queryParams) {
        next[q.name] = stored[`${ep.id}.${q.name}`] ?? q.default
      }
      setQueryValues(next)
      setBody('')
    },
    []
  )

  const setQuery = useCallback((name: string, value: string) => {
    setQueryValues((prev) => {
      const next = { ...prev, [name]: value }
      const merged = { ...loadStoredParams(), ...Object.fromEntries(Object.entries(next).map(([k, v]) => [`${activeId}.${k}`, v])) }
      saveStoredParams(merged)
      return next
    })
  }, [activeId])

  const buildUrl = useCallback(() => {
    const base = baseUrl.replace(/\/$/, '')
    const p = path.startsWith('/') ? path : `/${path}`
    const params = active.queryParams
      .map((q) => {
        const v = queryValues[q.name]?.trim()
        return v ? `${encodeURIComponent(q.name)}=${encodeURIComponent(v)}` : null
      })
      .filter(Boolean) as string[]
    const query = params.length ? `?${params.join('&')}` : ''
    return `${base}${p}${query}`
  }, [baseUrl, path, active.queryParams, queryValues])

  const loadFromHistory = useCallback((entry: HistoryEntry) => {
    const [pathname, search = ''] = entry.url.split('?')
    const parts = pathname.split('/').filter(Boolean)
    const base = parts.length >= 1 ? `/${parts[0]}` : '/api'
    const path = parts.length > 1 ? `/${parts.slice(1).join('/')}` : '/'
    setBaseUrl(base)
    setPath(path)
    const params = new URLSearchParams(search)
    const ep = ENDPOINTS.find((e) => e.path === path || path.startsWith(e.path))
    if (ep) {
      setActiveId(ep.id)
      setMethod(ep.method)
      const next: Record<string, string> = {}
      ep.queryParams.forEach((q) => {
        next[q.name] = params.get(q.name) ?? q.default
      })
      setQueryValues(next)
    }
  }, [])

  const send = useCallback(async () => {
    const url = buildUrl()
    setLoading(true)
    setResponse({ status: null, timeMs: null, data: null, error: null })
    const start = performance.now()
    try {
      const opts: RequestInit = { method }
      if (method !== 'GET' && body.trim()) {
        try {
          opts.body = JSON.stringify(JSON.parse(body))
        } catch {
          opts.body = body
        }
      }
      const res = await fetch(url, opts)
      const timeMs = Math.round(performance.now() - start)
      let data: unknown
      const text = await res.text()
      try {
        data = text ? (JSON.parse(text) as unknown) : null
      } catch {
        data = text
      }
      if (!res.ok) {
        setResponse({
          status: res.status,
          timeMs,
          data,
          error: `HTTP ${res.status}: ${res.statusText}`,
        })
      } else {
        setResponse({ status: res.status, timeMs, data, error: null })
      }
      const entry: HistoryEntry = { method, url, status: res.status, timeMs, ts: Date.now() }
      setHistory((prev) => {
        const next = [entry, ...prev.filter((h) => h.url !== url || h.method !== method)].slice(0, HISTORY_MAX)
        saveHistory(next)
        return next
      })
      // Persist query params
      const toStore: Record<string, string> = {}
      active.queryParams.forEach((q) => {
        toStore[`${activeId}.${q.name}`] = queryValues[q.name] ?? q.default
      })
      saveStoredParams({ ...loadStoredParams(), ...toStore })
    } catch (err) {
      const timeMs = Math.round(performance.now() - start)
      setResponse({
        status: null,
        timeMs,
        data: null,
        error: err instanceof Error ? err.message : String(err),
      })
    } finally {
      setLoading(false)
    }
  }, [buildUrl, method, body, active.queryParams, activeId, queryValues])

  return (
    <article className="doc-page playground-page">
      <h1>API Playground</h1>

      <div className="playground-tabs">
        {ENDPOINTS.map((ep) => (
          <button
            key={ep.id}
            type="button"
            className={activeId === ep.id ? 'playground-tab active' : 'playground-tab'}
            onClick={() => selectEndpoint(ep)}
          >
            {ep.label}
          </button>
        ))}
      </div>

      <section className="playground-request section">
        <h2>Request</h2>
        <div className="request-row">
          <label>
            Base URL
            <input
              type="text"
              value={baseUrl}
              onChange={(e) => setBaseUrl(e.target.value)}
              className="request-input"
            />
          </label>
          <label>
            Method
            <input type="text" value={method} readOnly className="request-input method-readonly" />
          </label>
          <label>
            Path
            <input
              type="text"
              value={path}
              onChange={(e) => setPath(e.target.value)}
              className="request-input"
            />
          </label>
        </div>
        {active.queryParams.length > 0 && (
          <div className="request-row query-row">
            {active.queryParams.map((q) => (
              <label key={q.name}>
                {q.name}
                <input
                  type="text"
                  value={queryValues[q.name] ?? q.default}
                  onChange={(e) => setQuery(q.name, e.target.value)}
                  placeholder={q.placeholder ?? q.default}
                  className="request-input query-input"
                />
              </label>
            ))}
          </div>
        )}
        {active.hasBody && (
          <label className="body-label">
            Body (JSON)
            <textarea
              value={body}
              onChange={(e) => setBody(e.target.value)}
              className="request-body"
              rows={4}
            />
          </label>
        )}
        <button type="button" onClick={send} disabled={loading} className="send-btn">
          {loading ? 'Sending…' : 'Send'}
        </button>
      </section>

      <section className="playground-response section">
        <h2>Response</h2>
        {response.error && (
          <div className="status error" role="alert">
            {response.error}
          </div>
        )}
        {(response.status != null || response.timeMs != null) && (
          <div className="response-meta">
            {response.status != null && (
              <span className="response-status">Status: {response.status}</span>
            )}
            {response.timeMs != null && (
              <span className="response-time">Time: {response.timeMs} ms</span>
            )}
          </div>
        )}
        {response.data != null && (
          <div className="response-json-wrap">
            <button
              type="button"
              className="copy-btn"
              onClick={() => {
                const str =
                  typeof response.data === 'string'
                    ? response.data
                    : JSON.stringify(response.data, null, 2)
                navigator.clipboard.writeText(str)
              }}
            >
              Copy
            </button>
            <pre className="json-block">
              {typeof response.data === 'string'
                ? response.data
                : JSON.stringify(response.data, null, 2)}
            </pre>
          </div>
        )}
      </section>

      {history.length > 0 && (
        <section className="playground-history section">
          <h2>History (last 10)</h2>
          <ul className="history-list">
            {history.map((h, i) => (
              <li key={`${h.ts}-${i}`} className="history-item">
                <span className="history-summary">
                  {h.method} {h.url}
                  {h.status != null && ` — ${h.status}`}
                  {h.timeMs != null && ` — ${h.timeMs} ms`}
                </span>
                <button type="button" className="history-load-btn" onClick={() => loadFromHistory(h)}>
                  Load
                </button>
              </li>
            ))}
          </ul>
        </section>
      )}
    </article>
  )
}
