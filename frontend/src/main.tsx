import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import { initMermaid } from './lib/mermaid'
import App from './App.tsx'

initMermaid()

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
