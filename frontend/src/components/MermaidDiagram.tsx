import { useEffect, useRef } from 'react'
import mermaid from 'mermaid'
import { initMermaid, applyOverviewSectionColors } from '../lib/mermaid'

interface MermaidDiagramProps {
  chart: string
  id?: string
}

export function MermaidDiagram({ chart, id: idProp }: MermaidDiagramProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const id = idProp ?? `mermaid-${Math.random().toString(36).slice(2)}`

  useEffect(() => {
    if (!containerRef.current || !chart.trim()) return
    initMermaid()
    containerRef.current.innerHTML = ''
    const pre = document.createElement('pre')
    pre.className = 'mermaid'
    pre.textContent = chart
    containerRef.current.appendChild(pre)
    mermaid
      .run({ nodes: [pre], suppressErrors: true })
      .then(() => {
        if (id === 'overview-flow') applyOverviewSectionColors(containerRef.current)
      })
      .catch(() => {
        if (containerRef.current) containerRef.current.innerHTML = '<p class="mermaid-error">Diagram failed to render.</p>'
      })
  }, [chart])

  return <div ref={containerRef} className="mermaid-container" data-diagram-id={id} />
}
