import { Link, Outlet, useLocation } from 'react-router-dom'

const nav = [
  { to: '/', label: 'Overview' },
  { to: '/playground', label: 'Playground' },
  {
    label: 'Deep Dives',
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
                <span className="nav-group-label">{item.label}</span>
                {item.children.map((c) => (
                  <Link
                    key={c.to}
                    to={c.to}
                    className={location.pathname === c.to ? 'nav-link active' : 'nav-link'}
                  >
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
