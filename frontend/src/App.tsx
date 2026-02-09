import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { Layout } from './components/Layout'
import { Overview } from './pages/Overview'
import { Playground } from './pages/Playground'
import { Ingestion } from './pages/deep-dive/Ingestion'
import { Loading } from './pages/deep-dive/Loading'
import { Analytics } from './pages/deep-dive/Analytics'
import { Api } from './pages/deep-dive/Api'
import { Tests } from './pages/deep-dive/Tests'
import { Docker } from './pages/deep-dive/Docker'
import './App.css'

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Overview />} />
          <Route path="playground" element={<Playground />} />
          <Route path="deep-dive/ingestion" element={<Ingestion />} />
          <Route path="deep-dive/loading" element={<Loading />} />
          <Route path="deep-dive/analytics" element={<Analytics />} />
          <Route path="deep-dive/api" element={<Api />} />
          <Route path="deep-dive/tests" element={<Tests />} />
          <Route path="deep-dive/docker" element={<Docker />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

export default App
