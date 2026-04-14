import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom';
import { Database, Building2, FlaskConical, Zap, Shield, Code, Rocket, Activity } from 'lucide-react';
import Home from './pages/Home';
import DatasetList from './pages/DatasetList';
import DatasetDetail from './pages/DatasetDetail';
import FeatureStore from './pages/FeatureStore';
import ModelDevelopment from './pages/ModelDevelopment';
import ModelFactory from './pages/ModelFactory';
import ModelFactoryRun from './pages/ModelFactoryRun';
import ModelDeployment from './pages/ModelDeployment';
import Monitoring from './pages/Monitoring';
import Governance from './pages/Governance';

function Nav() {
  const { pathname } = useLocation();

  const tabs = [
    { to: '/', label: 'Home', icon: Database, match: (p: string) => p === '/' },
    { to: '/datasets', label: 'Data Ingestion', icon: Database, match: (p: string) => p.startsWith('/dataset') },
    { to: '/features', label: 'Feature Store', icon: Zap, match: (p: string) => p.startsWith('/features') },
    { to: '/development', label: 'Model Dev', icon: Code, match: (p: string) => p.startsWith('/development') },
    { to: '/models', label: 'Model Factory', icon: FlaskConical, match: (p: string) => p.startsWith('/models') },
    { to: '/deployment', label: 'Deployment', icon: Rocket, match: (p: string) => p.startsWith('/deployment') },
    { to: '/monitoring', label: 'Monitoring', icon: Activity, match: (p: string) => p.startsWith('/monitoring') },
    { to: '/governance', label: 'Governance', icon: Shield, match: (p: string) => p.startsWith('/governance') },
  ];

  return (
    <header className="bg-[#1e293b] text-white">
      <div className="max-w-7xl mx-auto px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-6">
          <Link to="/" className="flex items-center gap-3 hover:opacity-90 transition-opacity">
            <Database className="w-6 h-6 text-blue-400" />
            <div>
              <h1 className="text-lg font-bold tracking-tight">Pricing Governance</h1>
              <p className="text-xs text-gray-400">Data & Model Review</p>
            </div>
          </Link>
          <nav className="flex items-center gap-0.5">
            {tabs.map(({ to, label, icon: Icon, match }) => (
              <Link key={to} to={to}
                className={`px-2.5 py-1.5 rounded text-xs font-medium transition-colors ${
                  match(pathname) ? 'bg-white/10 text-white' : 'text-gray-400 hover:text-white'
                }`}
              >
                <Icon className="w-3 h-3 inline mr-1" />
                {label}
              </Link>
            ))}
          </nav>
        </div>
        <div className="flex items-center gap-2 text-sm text-gray-400">
          <Building2 className="w-4 h-4" />
          <span className="font-medium text-gray-300">Bricksurance SE</span>
        </div>
      </div>
    </header>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-100 font-[system-ui]">
        <Nav />
        <main>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/datasets" element={<DatasetList />} />
            <Route path="/dataset/:datasetId" element={<DatasetDetail />} />
            <Route path="/features" element={<FeatureStore />} />
            <Route path="/development" element={<ModelDevelopment />} />
            <Route path="/models" element={<ModelFactory />} />
            <Route path="/models/:runId" element={<ModelFactoryRun />} />
            <Route path="/deployment" element={<ModelDeployment />} />
            <Route path="/monitoring" element={<Monitoring />} />
            <Route path="/governance" element={<Governance />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
