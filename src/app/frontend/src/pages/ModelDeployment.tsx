import { useEffect, useState } from 'react';
import { Rocket, ExternalLink, CheckCircle2, Clock, XCircle, User, Calendar, Tag } from 'lucide-react';
import { api } from '../lib/api';

export default function ModelDeployment() {
  const [models, setModels] = useState<any[]>([]);
  const [endpoints, setEndpoints] = useState<any[]>([]);
  const [latency, setLatency] = useState<any>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.getRegisteredModels(),
      api.getServingEndpoints(),
      api.getEndpointLatency(),
    ]).then(([m, e, l]) => {
      setModels(m);
      setEndpoints(e);
      setLatency(l);
    }).finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="p-8 text-center text-gray-500">Loading deployment status...</div>;

  const deployedModelNames = new Set(
    endpoints.flatMap(e => e.entities?.map((ent: any) => ent.model) || [])
  );

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-gray-900">Model Deployment</h2>
        <p className="text-gray-500 mt-1">Registered models in Unity Catalog and active serving endpoints</p>
      </div>

      {/* Context */}
      <div className="grid grid-cols-2 gap-4 mb-6">
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <h4 className="text-xs font-semibold text-blue-800 uppercase tracking-wide mb-1">Databricks features demonstrated</h4>
          <div className="flex flex-wrap gap-1.5">
            {['Unity Catalog model registry', 'Mosaic AI Model Serving', 'Champion/challenger routing', 'Scale-to-zero endpoints', 'Auto feature lookup'].map(f => (
              <span key={f} className="px-2 py-0.5 rounded text-[10px] font-medium bg-blue-100 text-blue-700">{f}</span>
            ))}
          </div>
        </div>
        <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
          <h4 className="text-xs font-semibold text-amber-800 uppercase tracking-wide mb-1">Why it matters for actuaries</h4>
          <p className="text-xs text-amber-700">
            Models are versioned, governed, and deployable with one click. Champion/challenger
            routing enables safe rollouts — test a new model on 10% of traffic before full deployment.
            Every version is traceable to its training data via Delta Time Travel.
          </p>
        </div>
      </div>

      {/* Serving Endpoints */}
      <h3 className="text-lg font-semibold text-gray-900 mb-3">Active Serving Endpoints</h3>
      {endpoints.length > 0 ? (
        <div className="grid gap-3 mb-8">
          {endpoints.map((ep) => (
            <div key={ep.name} className="bg-white border border-gray-200 rounded-lg p-5">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-3">
                  <Rocket className="w-5 h-5 text-green-600" />
                  <h4 className="font-semibold text-gray-900">{ep.name}</h4>
                  <StatusBadge state={ep.state} />
                </div>
                <a href={ep.url} target="_blank" rel="noopener noreferrer"
                  className="flex items-center gap-1 px-3 py-1 bg-gray-100 rounded text-xs font-medium text-gray-700 hover:bg-gray-200">
                  Open in Databricks <ExternalLink className="w-3 h-3" />
                </a>
              </div>
              {ep.entities?.length > 0 && (
                <div className="grid grid-cols-2 gap-3 mb-3">
                  {ep.entities.map((ent: any, i: number) => {
                    const trafficPct = ep.traffic?.find((t: any) => t.model === ent.name)?.traffic_pct || 0;
                    return (
                      <div key={i} className="bg-gray-50 rounded-lg p-3">
                        <div className="flex items-center justify-between mb-1">
                          <span className="font-medium text-sm text-gray-800">{ent.name}</span>
                          <span className="text-xs text-blue-600 font-medium">{trafficPct}% traffic</span>
                        </div>
                        <div className="text-xs text-gray-500">
                          <code>{ent.model}</code> v{ent.version}
                        </div>
                        <div className="text-xs text-gray-400 mt-1">
                          Size: {ent.workload_size} | Scale to zero: {ent.scale_to_zero ? 'Yes' : 'No'}
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
              {/* Latency */}
              {Object.keys(latency).length > 0 && (
                <div className="flex gap-4 text-xs text-gray-500 border-t pt-2">
                  {latency.endpoint_avg_ms != null && <span>Avg: <strong>{Number(latency.endpoint_avg_ms).toFixed(0)}ms</strong></span>}
                  {latency.endpoint_p50_ms != null && <span>P50: <strong>{Number(latency.endpoint_p50_ms).toFixed(0)}ms</strong></span>}
                  {latency.endpoint_p95_ms != null && <span>P95: <strong>{Number(latency.endpoint_p95_ms).toFixed(0)}ms</strong></span>}
                  {latency.endpoint_p99_ms != null && <span>P99: <strong>{Number(latency.endpoint_p99_ms).toFixed(0)}ms</strong></span>}
                </div>
              )}
            </div>
          ))}
        </div>
      ) : (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center mb-8">
          <Rocket className="w-8 h-8 text-gray-300 mx-auto mb-2" />
          <p className="text-gray-500 text-sm">No serving endpoints deployed yet</p>
          <p className="text-gray-400 text-xs mt-1">Run the deploy_model_endpoint job to create one</p>
        </div>
      )}

      {/* Registered Models */}
      <h3 className="text-lg font-semibold text-gray-900 mb-3">Registered Models in Unity Catalog</h3>
      {models.length > 0 ? (
        <div className="grid gap-3">
          {models.map((m) => {
            const isDeployed = deployedModelNames.has(m.full_name);
            const latest = m.latest_version;
            return (
              <div key={m.name} className={`bg-white border rounded-lg p-4 ${isDeployed ? 'border-green-200' : 'border-gray-200'}`}>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <h4 className="font-semibold text-gray-900 font-mono text-sm">{m.name}</h4>
                    {isDeployed && (
                      <span className="px-2 py-0.5 rounded text-[10px] font-medium bg-green-50 text-green-700 border border-green-200">
                        DEPLOYED
                      </span>
                    )}
                  </div>
                  <a href={m.catalog_url} target="_blank" rel="noopener noreferrer"
                    className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 font-medium">
                    View in UC <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
                <div className="grid grid-cols-4 gap-4 text-xs text-gray-500">
                  <div className="flex items-center gap-1">
                    <Tag className="w-3 h-3" />
                    Versions: <strong className="text-gray-700">{m.versions?.length || 0}</strong>
                  </div>
                  <div className="flex items-center gap-1">
                    <User className="w-3 h-3" />
                    {m.created_by?.split('@')[0] || '?'}
                  </div>
                  <div className="flex items-center gap-1">
                    <Calendar className="w-3 h-3" />
                    {m.created_at ? new Date(m.created_at).toLocaleDateString() : '?'}
                  </div>
                  <div>
                    Latest: <strong className="text-gray-700">v{latest?.version || '?'}</strong>
                    {latest?.status && <span className="ml-1 text-gray-400">({latest.status})</span>}
                  </div>
                </div>
                {/* Version history */}
                {m.versions?.length > 1 && (
                  <div className="mt-2 border-t pt-2">
                    <div className="flex gap-2 overflow-x-auto">
                      {m.versions.map((v: any) => (
                        <div key={v.version} className="shrink-0 px-2 py-1 bg-gray-50 rounded text-[10px] text-gray-500">
                          v{v.version} — {v.created_by?.split('@')[0]} — {v.created_at ? new Date(v.created_at).toLocaleDateString() : '?'}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ) : (
        <p className="text-gray-400 text-sm">No models registered yet — run model training first</p>
      )}
    </div>
  );
}

function StatusBadge({ state }: { state: string }) {
  if (state === 'READY') return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-green-50 text-green-700 border border-green-200">
      <CheckCircle2 className="w-3 h-3" /> Ready
    </span>
  );
  if (state === 'NOT_READY') return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-amber-50 text-amber-700 border border-amber-200">
      <Clock className="w-3 h-3" /> Starting
    </span>
  );
  return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-gray-50 text-gray-600 border border-gray-200">
      {state}
    </span>
  );
}
