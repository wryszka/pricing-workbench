import { useEffect, useState } from 'react';
import { Database, Zap, Clock, ExternalLink, CheckCircle2, AlertTriangle, Tag, MessageCircle } from 'lucide-react';
import { api } from '../lib/api';

export default function FeatureStore() {
  const [data, setData] = useState<any>(null);
  const [config, setConfig] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.getFeatureStoreStatus(),
      api.getConfig(),
    ]).then(([d, c]) => { setData(d); setConfig(c); }).finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="p-8 text-center text-gray-500">Loading feature store status...</div>;
  if (!data) return <div className="p-8 text-center text-red-500">Failed to load feature store status</div>;

  const upt = data.upt || {};
  const os = data.online_store || {};
  const lat = data.latency || {};
  const tags = upt.tags || {};
  const storeActive = os.state === 'AVAILABLE' || os.state === 'ACTIVE';

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-gray-900">Unified Pricing Table</h2>
        <p className="text-gray-500 mt-1">Single wide table with all pricing features — offline (Delta Lake) and online (Lakebase) serving</p>
      </div>

      {/* What this demonstrates */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-5 mb-6">
        <h3 className="font-semibold text-blue-800 mb-2">What this demonstrates</h3>
        <div className="grid grid-cols-2 gap-4 text-sm text-blue-700">
          <div>
            <h4 className="font-medium mb-1">Online vs Offline Feature Stores</h4>
            <p className="text-blue-600">
              The <strong>offline store</strong> (Delta Lake) is used for model training and batch scoring — full SQL access,
              Time Travel, and table scans. The <strong>online store</strong> (Lakebase) provides sub-10ms key-value lookups
              for real-time pricing — same data, different access pattern.
            </p>
          </div>
          <div>
            <h4 className="font-medium mb-1">Why this matters for real-time pricing</h4>
            <p className="text-blue-600">
              A quote engine needs 100+ features in under 30ms. Delta Lake is too slow for this.
              The online store solves it — and models logged with FeatureLookup <strong>automatically
              resolve features at serving time</strong>, zero additional integration needed.
            </p>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-6">
        {/* Offline Store */}
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="px-5 py-3 bg-gray-50 border-b flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Database className="w-4 h-4 text-blue-600" />
              <h3 className="font-semibold text-gray-800">Offline Store (Delta Lake)</h3>
            </div>
            <span className="px-2 py-0.5 rounded text-xs font-medium bg-green-50 text-green-700 border border-green-200">
              Active
            </span>
          </div>
          <div className="p-5 space-y-3">
            <div className="grid grid-cols-2 gap-3">
              <Stat label="Rows" value={Number(upt.row_count || 0).toLocaleString()} />
              <Stat label="Columns" value={String(upt.column_count || 0)} />
              <Stat label="Delta Version" value={`v${upt.delta_version}`} />
              <Stat label="Primary Key" value={upt.primary_key || 'policy_id'} />
            </div>
            <div className="text-xs text-gray-500">
              Last modified: {upt.last_modified || '—'}
            </div>
            {upt.catalog_url && (
              <a href={upt.catalog_url} target="_blank" rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 font-medium">
                <ExternalLink className="w-3 h-3" /> View in Catalog Explorer
              </a>
            )}
          </div>
        </div>

        {/* Online Store */}
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="px-5 py-3 bg-gray-50 border-b flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Zap className="w-4 h-4 text-amber-500" />
              <h3 className="font-semibold text-gray-800">Online Store (Lakebase)</h3>
            </div>
            <span className={`px-2 py-0.5 rounded text-xs font-medium ${
              storeActive ? 'bg-green-50 text-green-700 border border-green-200'
                          : 'bg-amber-50 text-amber-700 border border-amber-200'
            }`}>
              {os.state || 'Unknown'}
            </span>
          </div>
          <div className="p-5 space-y-3">
            {storeActive ? (
              <>
                <div className="grid grid-cols-2 gap-3">
                  <Stat label="Store Name" value={os.name} />
                  <Stat label="Capacity" value={os.capacity || '—'} />
                </div>
                <div className="text-xs text-gray-500">
                  Created: {os.created || '—'}
                </div>
              </>
            ) : (
              <div className="flex items-center gap-2 text-amber-600 text-sm">
                <AlertTriangle className="w-4 h-4" />
                {os.state === 'NOT_CREATED'
                  ? 'Online store not created yet — run the setup_online_store notebook'
                  : `Store state: ${os.state}`}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Latency */}
      {Object.keys(lat).length > 0 && (
        <div className="mt-6 bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="px-5 py-3 bg-gray-50 border-b flex items-center gap-2">
            <Clock className="w-4 h-4 text-purple-600" />
            <h3 className="font-semibold text-gray-800">Latency Test Results</h3>
          </div>
          <div className="p-5">
            <p className="text-sm text-gray-500 mb-4">
              Measured latency for feature lookups from the online store. In production, these lookups
              happen automatically inside the Model Serving endpoint.
            </p>
            <div className="grid grid-cols-4 gap-4">
              {lat.single_lookup_avg_ms != null && (
                <MetricCard label="Single Lookup (avg)" value={`${Number(lat.single_lookup_avg_ms).toFixed(1)}ms`}
                  color={lat.single_lookup_avg_ms < 30 ? 'green' : 'amber'} />
              )}
              {lat.single_lookup_p50_ms != null && (
                <MetricCard label="Single Lookup (P50)" value={`${Number(lat.single_lookup_p50_ms).toFixed(1)}ms`}
                  color={lat.single_lookup_p50_ms < 20 ? 'green' : 'amber'} />
              )}
              {lat.single_lookup_p99_ms != null && (
                <MetricCard label="Single Lookup (P99)" value={`${Number(lat.single_lookup_p99_ms).toFixed(1)}ms`}
                  color={lat.single_lookup_p99_ms < 100 ? 'green' : 'amber'} />
              )}
              {lat.batch_100_ms != null && (
                <MetricCard label="Batch 100 Keys" value={`${Number(lat.batch_100_ms).toFixed(0)}ms`}
                  color={lat.batch_100_ms < 500 ? 'green' : 'amber'} />
              )}
            </div>
          </div>
        </div>
      )}

      {/* Tags */}
      {Object.keys(tags).length > 0 && (
        <div className="mt-6 bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="px-5 py-3 bg-gray-50 border-b flex items-center gap-2">
            <Tag className="w-4 h-4 text-gray-600" />
            <h3 className="font-semibold text-gray-800">Feature Table Tags</h3>
          </div>
          <div className="p-5 flex flex-wrap gap-2">
            {Object.entries(tags).map(([k, v]) => (
              <span key={k} className="px-3 py-1 bg-gray-100 rounded-full text-xs text-gray-700">
                <strong>{k}:</strong> {v as string}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* How it compares */}
      <div className="mt-6 bg-gray-50 border border-gray-200 rounded-lg p-5">
        <h3 className="font-semibold text-gray-800 mb-3">How this compares to building custom API layers</h3>
        <div className="grid grid-cols-3 gap-4 text-sm">
          <div className="bg-white rounded-lg p-4 border">
            <h4 className="font-medium text-red-700 mb-2">Custom REST API</h4>
            <ul className="text-gray-600 space-y-1">
              <li>Build and maintain API service</li>
              <li>Manage caching layer (Redis/DynamoDB)</li>
              <li>Custom sync from warehouse to cache</li>
              <li>Handle schema evolution manually</li>
              <li>Separate monitoring and alerting</li>
            </ul>
          </div>
          <div className="bg-white rounded-lg p-4 border">
            <h4 className="font-medium text-amber-700 mb-2">Third-party Feature Store</h4>
            <ul className="text-gray-600 space-y-1">
              <li>Additional vendor and cost</li>
              <li>Data movement between platforms</li>
              <li>Separate governance and lineage</li>
              <li>Integration overhead</li>
              <li>Feature/training skew risk</li>
            </ul>
          </div>
          <div className="bg-white rounded-lg p-4 border border-green-200">
            <h4 className="font-medium text-green-700 mb-2">Databricks Online Store</h4>
            <ul className="text-gray-600 space-y-1">
              <li>Native — same platform, zero movement</li>
              <li>Auto-synced from Delta Lake</li>
              <li>UC lineage and governance built in</li>
              <li>Schema evolution handled</li>
              <li>Model Serving auto-resolves features</li>
            </ul>
          </div>
        </div>
      </div>

      {/* Genie — query the pricing table */}
      {config?.genie_embed_url && (
        <div className="mt-6">
          <div className="bg-purple-50 border border-purple-200 rounded-t-lg px-5 py-3 flex items-center justify-between">
            <div className="flex items-center gap-3">
              <MessageCircle className="w-5 h-5 text-purple-600" />
              <div>
                <h3 className="font-semibold text-purple-800">Ask questions about the pricing data</h3>
                <p className="text-xs text-purple-600">
                  Query the Unified Pricing Table in natural language — powered by Databricks Genie
                </p>
              </div>
            </div>
            <a href={config.genie_url} target="_blank" rel="noopener noreferrer"
              className="text-xs text-purple-500 hover:text-purple-700 flex items-center gap-1">
              Open full screen <ExternalLink className="w-3 h-3" />
            </a>
          </div>
          <div className="bg-white border border-t-0 border-purple-200 rounded-b-lg overflow-hidden">
            <iframe
              src={config.genie_embed_url}
              className="w-full border-0"
              style={{ height: '500px' }}
              title="Genie — Pricing Data Explorer"
              allow="clipboard-write"
            />
          </div>
          <div className="mt-2 flex flex-wrap gap-2">
            <span className="text-xs text-gray-400">Try:</span>
            {["What is the average premium by industry?", "Which postcodes have the highest flood risk?", "Show me loss ratio by construction type", "How many policies have claims?"].map((q, i) => (
              <span key={i} className="text-xs text-purple-600 bg-purple-50 border border-purple-200 rounded px-2 py-0.5">"{q}"</span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-xs text-gray-500">{label}</div>
      <div className="text-sm font-medium text-gray-900 font-mono">{value}</div>
    </div>
  );
}

function MetricCard({ label, value, color }: { label: string; value: string; color: string }) {
  const colorMap: Record<string, string> = {
    green: 'border-green-200 bg-green-50',
    amber: 'border-amber-200 bg-amber-50',
    red: 'border-red-200 bg-red-50',
  };
  const textMap: Record<string, string> = {
    green: 'text-green-700',
    amber: 'text-amber-700',
    red: 'text-red-700',
  };
  return (
    <div className={`rounded-lg border p-3 ${colorMap[color] || ''}`}>
      <div className="text-xs text-gray-500">{label}</div>
      <div className={`text-xl font-bold mt-1 ${textMap[color] || ''}`}>{value}</div>
    </div>
  );
}
