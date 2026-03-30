import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom';
import { Database, Building2 } from 'lucide-react';
import DatasetList from './pages/DatasetList';
import DatasetDetail from './pages/DatasetDetail';

function Nav() {
  return (
    <header className="bg-[#1e293b] text-white">
      <div className="max-w-7xl mx-auto px-6 py-3 flex items-center justify-between">
        <Link to="/" className="flex items-center gap-3 hover:opacity-90 transition-opacity">
          <Database className="w-6 h-6 text-blue-400" />
          <div>
            <h1 className="text-lg font-bold tracking-tight">Pricing Data Ingestion</h1>
            <p className="text-xs text-gray-400">Review & Approval</p>
          </div>
        </Link>
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
            <Route path="/" element={<DatasetList />} />
            <Route path="/dataset/:datasetId" element={<DatasetDetail />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
