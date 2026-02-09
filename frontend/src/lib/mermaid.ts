import mermaid from 'mermaid'

/** Dashboard palette (hex) – aligned with CSS variables in index.css */
const palette = {
  text: '#0f172a',
  textMuted: '#64748b',
  primary: '#2563eb',
  primarySubtle: '#eff6ff',
  primaryBorder: '#1d4ed8',
  secondary: '#7c3aed',
  secondarySubtle: '#f5f3ff',
  tertiary: '#4f46e5',
  tertiarySubtle: '#eef2ff',
  line: '#475569',
  border: '#334155',
  surface: '#ffffff',
  noteBkg: '#fef9c3',
  noteText: '#1a202c',
  clusterBkg: '#f1f5f9',
  clusterBorder: '#94a3b8',
} as const

let initialized = false

/**
 * Initialize Mermaid once at app startup with a global theme aligned to the
 * dashboard palette. Stronger contrast, clearer arrows/labels, better spacing.
 */
export function initMermaid(): void {
  if (initialized) return
  initialized = true

  mermaid.initialize({
    startOnLoad: false,
    theme: 'base',
    securityLevel: 'strict',
    fontFamily: 'system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
    flowchart: {
      curve: 'basis',
      padding: 12,
    },
    themeVariables: {
      fontSize: '14px',
      background: palette.surface,
      primaryColor: palette.primarySubtle,
      primaryBorderColor: palette.primaryBorder,
      primaryTextColor: palette.text,
      secondaryColor: palette.secondarySubtle,
      secondaryBorderColor: palette.secondary,
      secondaryTextColor: palette.text,
      tertiaryColor: palette.tertiarySubtle,
      tertiaryBorderColor: palette.tertiary,
      tertiaryTextColor: palette.text,
      lineColor: palette.line,
      textColor: palette.text,
      mainBkg: palette.primarySubtle,
      nodeBorder: palette.primaryBorder,
      nodeTextColor: palette.text,
      clusterBkg: palette.clusterBkg,
      clusterBorder: palette.clusterBorder,
      noteBkgColor: palette.noteBkg,
      noteTextColor: palette.noteText,
      titleColor: palette.text,
      edgeLabelBackground: palette.surface,
    },
    themeCSS: `
      .mermaid .node rect,
      .mermaid .node circle,
      .mermaid .node polygon {
        stroke-width: 1.5px;
        rx: 6px;
        ry: 6px;
      }
      .mermaid .node .label {
        color: ${palette.text} !important;
        font-size: 14px;
      }
      .mermaid .edgePath path {
        stroke: ${palette.line};
        stroke-width: 1.5px;
      }
      .mermaid .arrowheadPath {
        fill: ${palette.line};
        stroke: ${palette.line};
      }
      .mermaid .edgeLabel {
        background: ${palette.surface} !important;
        color: ${palette.text} !important;
        font-size: 13px;
      }
      .mermaid .cluster rect {
        rx: 8px;
        ry: 8px;
        stroke-width: 1.5px;
      }
      .mermaid .cluster .cluster-label {
        font-weight: 600;
        fill: ${palette.text} !important;
      }
    `,
  })
}

/** Cluster id → section role for Overview diagram color-coding */
const OVERVIEW_CLUSTER_SECTIONS: Record<string, string> = {
  ext: 'external',
  ingest: 'ingestion',
  store: 'storage',
  out: 'output',
}

/**
 * Post-process rendered SVG for the Overview diagram: add data-section to
 * clusters so CSS can apply section identity colors. Safe no-op if container
 * or structure doesn't match.
 */
export function applyOverviewSectionColors(container: HTMLElement | null): void {
  if (!container) return
  const svg = container.querySelector('svg')
  if (!svg) return
  for (const [clusterId, section] of Object.entries(OVERVIEW_CLUSTER_SECTIONS)) {
    const g = svg.querySelector(`[id="cluster_${clusterId}"]`) as SVGGElement | null
    if (g) g.setAttribute('data-section', section)
  }
}
