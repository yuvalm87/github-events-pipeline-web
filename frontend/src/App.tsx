import { useState } from 'react'
import { getHealth } from './api/client'
import './App.css'

function App() {
  const [healthStatus, setHealthStatus] = useState<string | null>(null)
  const [healthJson, setHealthJson] = useState<string | null>(null)
  const [healthError, setHealthError] = useState<string | null>(null)
  const [healthLoading, setHealthLoading] = useState(false)

  async function handleCheckHealth() {
    setHealthLoading(true)
    setHealthError(null)
    setHealthStatus(null)
    setHealthJson(null)
    try {
      const data = await getHealth()
      setHealthStatus(data.status ?? 'OK')
      setHealthJson(JSON.stringify(data, null, 2))
    } catch (err) {
      setHealthError(err instanceof Error ? err.message : String(err))
    } finally {
      setHealthLoading(false)
    }
  }

  return (
    <div className="app">
      <h1>GitHub Events Pipeline</h1>

      <section className="section health-section">
        <h2>Health</h2>
        <button
          type="button"
          onClick={handleCheckHealth}
          disabled={healthLoading}
        >
          {healthLoading ? 'Checking…' : 'Check health'}
        </button>
        {healthError && (
          <div className="status error" role="alert">
            {healthError}
          </div>
        )}
        {healthStatus != null && !healthError && (
          <>
            <div className="status success">Status: {healthStatus}</div>
            {healthJson && (
              <pre className="json-block">{healthJson}</pre>
            )}
          </>
        )}
      </section>
    </div>
  )
}

export default App
