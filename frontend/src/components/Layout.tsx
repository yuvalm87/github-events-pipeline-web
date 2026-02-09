import { Link, Outlet, useLocation } from 'react-router-dom'

const iconOverview = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z" />
    <polyline points="9 22 9 12 15 12 15 22" />
  </svg>
)
const iconPlayground = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <polygon points="5 3 19 12 5 21 5 3" />
  </svg>
)
const iconDeepDive = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20" />
    <path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z" />
    <line x1="12" y1="8" x2="12" y2="14" />
    <line x1="9" y1="11" x2="15" y2="11" />
  </svg>
)
const iconDoc = (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
    <polyline points="14 2 14 8 20 8" />
    <line x1="16" y1="13" x2="8" y2="13" />
    <line x1="16" y1="17" x2="8" y2="17" />
    <polyline points="10 9 9 9 8 9" />
  </svg>
)

const nav = [
  { to: '/', label: 'Overview', icon: iconOverview },
  { to: '/playground', label: 'Playground', icon: iconPlayground },
  {
    label: 'Deep Dives',
    icon: iconDeepDive,
    children: [
      { to: '/deep-dive/ingestion', label: 'Ingestion' },
      { to: '/deep-dive/loading', label: 'Loading' },
      { to: '/deep-dive/analytics', label: 'Analytics' },
      { to: '/deep-dive/api', label: 'API' },
      { to: '/deep-dive/tests', label: 'Tests' },
      { to: '/deep-dive/docker', label: 'Docker' },
    ],
  },
]

export function Layout() {
  const location = useLocation()

  return (
    <div className="layout">
      <aside className="sidebar">
        <h1 className="sidebar-title">GitHub Events Pipeline</h1>
        <nav className="sidebar-nav">
          {nav.map((item) =>
            'children' in item && item.children ? (
              <div key={item.label} className="nav-group">
                <span className="nav-group-label">
                  <span className="nav-group-icon">{item.icon}</span>
                  {item.label}
                </span>
                {item.children.map((c) => (
                  <Link
                    key={c.to}
                    to={c.to}
                    className={location.pathname === c.to ? 'nav-link active' : 'nav-link'}
                  >
                    <span className="nav-link-icon">{iconDoc}</span>
                    {c.label}
                  </Link>
                ))}
              </div>
            ) : (
              <Link
                key={item.to}
                to={item.to}
                className={location.pathname === item.to ? 'nav-link active' : 'nav-link'}
              >
                <span className="nav-link-icon">{item.icon}</span>
                {item.label}
              </Link>
            )
          )}
        </nav>
      </aside>
      <main className="main">
        <Outlet />
      </main>
    </div>
  )
}
