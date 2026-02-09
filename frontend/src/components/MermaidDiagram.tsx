import { useEffect, useRef } from 'react'
import mermaid from 'mermaid'

mermaid.initialize({
  startOnLoad: false,
  theme: 'dark',
  securityLevel: 'loose',
})

interface MermaidDiagramProps {
  chart: string
  id?: string
}

export function MermaidDiagram({ chart, id: idProp }: MermaidDiagramProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const id = idProp ?? `mermaid-${Math.random().toString(36).slice(2)}`

  useEffect(() => {
    if (!containerRef.current || !chart.trim()) return
    containerRef.current.innerHTML = ''
    const pre = document.createElement('pre')
    pre.className = 'mermaid'
    pre.textContent = chart
    containerRef.current.appendChild(pre)
    mermaid.run({ nodes: [pre], suppressErrors: true }).catch(() => {
      if (containerRef.current) containerRef.current.innerHTML = '<p class="mermaid-error">Diagram failed to render.</p>'
    })
  }, [chart])

  return <div ref={containerRef} className="mermaid-container" data-diagram-id={id} />
}
