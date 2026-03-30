import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { ArrowLeft, GitCompare, TrendingUp, ShieldCheck, CheckCircle2, XCircle, Loader2 } from 'lucide-react';
import { api } from '../lib/api';

type Tab = 'diff' | 'impact' | 'quality' | 'approval';

export default function DatasetDetail() {
  const { datasetId } = useParams<{ datasetId: string }>();
  const [tab, setTab] = useState<Tab>('diff');
  const [datasets, setDatasets] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getDatasets().then(setDatasets).finally(() => setLoading(false));
  }, []);

  const ds = datasets.find((d) => d.id === datasetId);

  if (loading) return <div className="p-8 text-center text-gray-500">Loading...</div>;
  if (!ds) return <div className="p-8 text-center text-red-500">Dataset not found</div>;

  const tabs: { id: Tab; label: string; icon: any }[] = [
    { id: 'diff', label: 'Data Changes', icon: GitCompare },
    { id: 'impact', label: 'Impact Analysis', icon: TrendingUp },
    { id: 'quality', label: 'Data Quality', icon: ShieldCheck },
    { id: 'approval', label: 'Approve / Reject', icon: CheckCircle2 },
  ];

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <Link to="/" className="inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-blue-600 mb-4">
        <ArrowLeft className="w-4 h-4" /> Back to datasets
      </Link>

      <div className="mb-6">
        <h2 className="text-2xl font-bold text-gray-900">{ds.display_name}</h2>
        <p className="text-gray-500 mt-1">{ds.description} &middot; Source: {ds.source} &middot; Join key: <code className="bg-gray-100 px-1.5 py-0.5 rounded text-xs">{ds.join_key}</code></p>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-gray-200 mb-6">
        {tabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              tab === t.id
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            <t.icon className="w-4 h-4" />
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'diff' && <DiffTab datasetId={datasetId!} />}
      {tab === 'impact' && <ImpactTab datasetId={datasetId!} />}
      {tab === 'quality' && <QualityTab datasetId={datasetId!} />}
      {tab === 'approval' && <ApprovalTab datasetId={datasetId!} />}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab 1: Data Changes (Diff)
// ---------------------------------------------------------------------------

function DiffTab({ datasetId }: { datasetId: string }) {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getDatasetDiff(datasetId).then(setData).finally(() => setLoading(false));
  }, [datasetId]);

  if (loading) return <Spinner />;
  if (!data) return <ErrorMsg msg="Failed to load diff" />;

  const s = data.summary;

  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid grid-cols-4 gap-4">
        <MetricCard label="Raw (Pending)" value={Number(s.raw_total).toLocaleString()} color="blue" />
        <MetricCard label="Silver (Current)" value={Number(s.silver_total).toLocaleString()} color="gray" />
        <MetricCard label="New Rows" value={Number(s.new_rows).toLocaleString()} color="green" />
        <MetricCard label="Removed Rows" value={Number(s.removed_rows).toLocaleString()} color="red" />
      </div>

      {/* Changed rows */}
      {data.changed_rows.length > 0 && (
        <Section title={`Changed Records (${data.changed_rows.length} shown)`}>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="bg-gray-50">
                  <th className="px-3 py-2 text-left font-medium text-gray-600">{data.key_column}</th>
                  {data.compare_columns.map((col: string) => (
                    <th key={col} colSpan={2} className="px-3 py-2 text-center font-medium text-gray-600">{col}</th>
                  ))}
                </tr>
                <tr className="bg-gray-50 border-b">
                  <th></th>
                  {data.compare_columns.map((col: string) => (
                    <>
                      <th key={`old_${col}`} className="px-3 py-1 text-center text-xs text-gray-400">Old</th>
                      <th key={`new_${col}`} className="px-3 py-1 text-center text-xs text-blue-500">New</th>
                    </>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.changed_rows.slice(0, 20).map((row: any, i: number) => (
                  <tr key={i} className="border-b hover:bg-blue-50/30">
                    <td className="px-3 py-2 font-mono text-xs">{row[data.key_column]}</td>
                    {data.compare_columns.map((col: string) => {
                      const oldVal = row[`old_${col}`];
                      const newVal = row[`new_${col}`];
                      const changed = String(oldVal) !== String(newVal);
                      return (
                        <>
                          <td key={`old_${col}_${i}`} className="px-3 py-2 text-center text-gray-500">{formatVal(oldVal)}</td>
                          <td key={`new_${col}_${i}`} className={`px-3 py-2 text-center font-medium ${changed ? 'text-blue-600 bg-blue-50' : ''}`}>
                            {formatVal(newVal)}
                          </td>
                        </>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Section>
      )}

      {/* New rows */}
      {data.new_rows.length > 0 && (
        <Section title={`New Records (${data.new_rows.length} shown)`}>
          <SimpleTable rows={data.new_rows} />
        </Section>
      )}

      {/* Removed rows */}
      {data.removed_rows.length > 0 && (
        <Section title={`Removed Records (${data.removed_rows.length} shown)`}>
          <SimpleTable rows={data.removed_rows} />
        </Section>
      )}

      {data.changed_rows.length === 0 && data.new_rows.length === 0 && data.removed_rows.length === 0 && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-6 text-center">
          <CheckCircle2 className="w-8 h-8 text-green-500 mx-auto mb-2" />
          <p className="text-green-700 font-medium">No differences detected between raw and silver versions.</p>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab 2: Impact Analysis
// ---------------------------------------------------------------------------

function ImpactTab({ datasetId }: { datasetId: string }) {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getDatasetImpact(datasetId).then(setData).finally(() => setLoading(false));
  }, [datasetId]);

  if (loading) return <Spinner />;
  if (!data) return <ErrorMsg msg="Failed to load impact analysis" />;

  const portfolio = data.summary || data.portfolio || {};

  return (
    <div className="space-y-6">
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <h3 className="font-semibold text-blue-800">{data.impact_type}</h3>
        <p className="text-sm text-blue-600 mt-1">Simulated impact of this dataset on the current portfolio</p>
      </div>

      {/* Portfolio summary */}
      <div className="grid grid-cols-3 gap-4">
        {portfolio.total_policies && (
          <MetricCard label="Total Policies" value={Number(portfolio.total_policies).toLocaleString()} color="blue" />
        )}
        {portfolio.total_gwp && (
          <MetricCard label="Total GWP" value={`£${formatGwp(portfolio.total_gwp)}`} color="blue" />
        )}
        {portfolio.overpriced_count && (
          <MetricCard label="Overpriced Policies" value={Number(portfolio.overpriced_count).toLocaleString()} color="amber" />
        )}
        {portfolio.overpriced_gwp && (
          <MetricCard label="Overpriced GWP" value={`£${formatGwp(portfolio.overpriced_gwp)}`} color="amber" />
        )}
        {portfolio.underpriced_count && (
          <MetricCard label="Underpriced Policies" value={Number(portfolio.underpriced_count).toLocaleString()} color="red" />
        )}
        {portfolio.underpriced_gwp && (
          <MetricCard label="Underpriced GWP" value={`£${formatGwp(portfolio.underpriced_gwp)}`} color="red" />
        )}
        {portfolio.high_risk_count && (
          <MetricCard label="High Risk Policies" value={Number(portfolio.high_risk_count).toLocaleString()} color="red" />
        )}
        {portfolio.high_risk_gwp && (
          <MetricCard label="High Risk GWP" value={`£${formatGwp(portfolio.high_risk_gwp)}`} color="red" />
        )}
        {portfolio.high_risk_policies && (
          <MetricCard label="High Risk Policies" value={Number(portfolio.high_risk_policies).toLocaleString()} color="red" />
        )}
        {portfolio.potentially_underpriced && (
          <MetricCard label="Potentially Underpriced" value={Number(portfolio.potentially_underpriced).toLocaleString()} color="amber" />
        )}
        {portfolio.renewals_next_90d && (
          <MetricCard label="Renewals (90 days)" value={Number(portfolio.renewals_next_90d).toLocaleString()} color="gray" />
        )}
        {portfolio.renewal_gwp_next_90d && (
          <MetricCard label="Renewal GWP (90d)" value={`£${formatGwp(portfolio.renewal_gwp_next_90d)}`} color="gray" />
        )}
      </div>

      {/* Tier breakdown */}
      {data.by_tier && data.by_tier.length > 0 && (
        <Section title="Breakdown by Tier">
          <SimpleTable rows={data.by_tier} />
        </Section>
      )}

      {/* Insights */}
      <Section title="Key Insights">
        <div className="space-y-3">
          {data.insights?.map((insight: any, i: number) => (
            <div
              key={i}
              className={`rounded-lg p-4 border ${
                insight.severity === 'high'
                  ? 'bg-red-50 border-red-200'
                  : insight.severity === 'medium'
                  ? 'bg-amber-50 border-amber-200'
                  : 'bg-blue-50 border-blue-200'
              }`}
            >
              <h4 className={`font-semibold ${
                insight.severity === 'high' ? 'text-red-800' : insight.severity === 'medium' ? 'text-amber-800' : 'text-blue-800'
              }`}>
                {insight.title}
              </h4>
              <p className={`text-sm mt-1 ${
                insight.severity === 'high' ? 'text-red-600' : insight.severity === 'medium' ? 'text-amber-600' : 'text-blue-600'
              }`}>
                {insight.description}
              </p>
            </div>
          ))}
        </div>
      </Section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab 3: Data Quality
// ---------------------------------------------------------------------------

function QualityTab({ datasetId }: { datasetId: string }) {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getDatasetQuality(datasetId).then(setData).finally(() => setLoading(false));
  }, [datasetId]);

  if (loading) return <Spinner />;
  if (!data) return <ErrorMsg msg="Failed to load quality metrics" />;

  return (
    <div className="space-y-6">
      {/* Top-line metrics */}
      <div className="grid grid-cols-4 gap-4">
        <MetricCard label="Raw Rows" value={Number(data.raw_row_count).toLocaleString()} color="blue" />
        <MetricCard label="Silver Rows (passed DQ)" value={Number(data.silver_row_count).toLocaleString()} color="green" />
        <MetricCard label="Rows Dropped" value={Number(data.rows_dropped).toLocaleString()} color="red" />
        <MetricCard
          label="DQ Pass Rate"
          value={`${data.dq_pass_rate}%`}
          color={data.dq_pass_rate >= 95 ? 'green' : data.dq_pass_rate >= 85 ? 'amber' : 'red'}
        />
      </div>

      {/* Freshness */}
      <div className={`rounded-lg p-4 border ${data.freshness_status === 'fresh' ? 'bg-green-50 border-green-200' : 'bg-amber-50 border-amber-200'}`}>
        <div className="flex items-center justify-between">
          <div>
            <h4 className="font-semibold text-gray-800">Data Freshness</h4>
            <p className="text-sm text-gray-600">Last ingested: {data.last_ingested || 'Never'}</p>
          </div>
          <span className={`px-3 py-1 rounded-full text-xs font-medium ${
            data.freshness_status === 'fresh' ? 'bg-green-100 text-green-700' : 'bg-amber-100 text-amber-700'
          }`}>
            {data.freshness_status === 'fresh' ? 'Fresh' : 'Stale'}
          </span>
        </div>
      </div>

      {/* DQ Expectations */}
      <Section title="Data Quality Expectations">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b">
                <th className="px-3 py-2 text-left font-medium text-gray-600">Expectation</th>
                <th className="px-3 py-2 text-left font-medium text-gray-600">Rule</th>
                <th className="px-3 py-2 text-left font-medium text-gray-600">Action</th>
                <th className="px-3 py-2 text-left font-medium text-gray-600">Status</th>
              </tr>
            </thead>
            <tbody>
              {data.expectations?.map((exp: any, i: number) => (
                <tr key={i} className="border-b hover:bg-gray-50">
                  <td className="px-3 py-2 font-mono text-xs">{exp.name}</td>
                  <td className="px-3 py-2 text-gray-700">{exp.rule}</td>
                  <td className="px-3 py-2">
                    <span className="px-2 py-0.5 rounded text-xs bg-red-50 text-red-600 border border-red-200">{exp.action}</span>
                  </td>
                  <td className="px-3 py-2">
                    <span className="px-2 py-0.5 rounded text-xs bg-green-50 text-green-600 border border-green-200">{exp.status}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      {/* Column completeness */}
      <Section title="Column Completeness (% non-null in Silver)">
        <div className="grid grid-cols-3 gap-3">
          {Object.entries(data.completeness || {}).map(([col, pct]: [string, any]) => (
            <div key={col} className="flex items-center justify-between bg-white border rounded-lg px-3 py-2">
              <span className="text-sm font-mono text-gray-700">{col}</span>
              <div className="flex items-center gap-2">
                <div className="w-24 h-2 bg-gray-100 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full ${Number(pct) >= 99 ? 'bg-green-500' : Number(pct) >= 90 ? 'bg-amber-400' : 'bg-red-400'}`}
                    style={{ width: `${pct}%` }}
                  />
                </div>
                <span className={`text-xs font-medium w-12 text-right ${Number(pct) >= 99 ? 'text-green-600' : Number(pct) >= 90 ? 'text-amber-600' : 'text-red-600'}`}>
                  {pct}%
                </span>
              </div>
            </div>
          ))}
        </div>
      </Section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab 4: Approve / Reject
// ---------------------------------------------------------------------------

function ApprovalTab({ datasetId }: { datasetId: string }) {
  const [notes, setNotes] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<any>(null);
  const [history, setHistory] = useState<any[]>([]);

  useEffect(() => {
    api.getApprovalHistory(datasetId).then(setHistory).catch(() => {});
  }, [datasetId, result]);

  const handleDecision = async (decision: string) => {
    setSubmitting(true);
    try {
      const res = await api.approveDataset(datasetId, decision, notes);
      setResult(res);
      setNotes('');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="space-y-6">
      {result && (
        <div className={`rounded-lg p-4 border ${result.decision === 'approved' ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'}`}>
          <p className={`font-semibold ${result.decision === 'approved' ? 'text-green-800' : 'text-red-800'}`}>
            {result.message}
          </p>
          <p className="text-sm text-gray-600 mt-1">Reviewer: {result.reviewer}</p>
        </div>
      )}

      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Review Decision</h3>
        <p className="text-sm text-gray-600 mb-4">
          Confirm that this dataset version has been reviewed and is suitable for merging into the Unified Pricing Table,
          or reject it with notes explaining why.
        </p>

        <div className="mb-4">
          <label className="block text-sm font-medium text-gray-700 mb-1">Reviewer Notes</label>
          <textarea
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            rows={3}
            placeholder="Optional: add notes about this review decision..."
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
          />
        </div>

        <div className="flex gap-3">
          <button
            onClick={() => handleDecision('approved')}
            disabled={submitting}
            className="flex items-center gap-2 px-6 py-2.5 bg-green-600 text-white rounded-lg font-medium hover:bg-green-700 disabled:opacity-50 transition-colors"
          >
            {submitting ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle2 className="w-4 h-4" />}
            Approve & Merge
          </button>
          <button
            onClick={() => handleDecision('rejected')}
            disabled={submitting}
            className="flex items-center gap-2 px-6 py-2.5 bg-red-600 text-white rounded-lg font-medium hover:bg-red-700 disabled:opacity-50 transition-colors"
          >
            {submitting ? <Loader2 className="w-4 h-4 animate-spin" /> : <XCircle className="w-4 h-4" />}
            Reject
          </button>
        </div>
      </div>

      {/* Approval history */}
      {history.length > 0 && (
        <Section title="Approval History">
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b">
                  <th className="px-3 py-2 text-left font-medium text-gray-600">Date</th>
                  <th className="px-3 py-2 text-left font-medium text-gray-600">Decision</th>
                  <th className="px-3 py-2 text-left font-medium text-gray-600">Reviewer</th>
                  <th className="px-3 py-2 text-left font-medium text-gray-600">Notes</th>
                  <th className="px-3 py-2 text-left font-medium text-gray-600">Raw/Silver</th>
                </tr>
              </thead>
              <tbody>
                {history.map((h: any, i: number) => (
                  <tr key={i} className="border-b hover:bg-gray-50">
                    <td className="px-3 py-2 text-gray-600">{h.reviewed_at}</td>
                    <td className="px-3 py-2">
                      <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                        h.decision === 'approved' ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'
                      }`}>
                        {h.decision}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-gray-700">{h.reviewer}</td>
                    <td className="px-3 py-2 text-gray-500">{h.reviewer_notes || '—'}</td>
                    <td className="px-3 py-2 text-gray-600">{h.raw_row_count} / {h.silver_row_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Section>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Shared components
// ---------------------------------------------------------------------------

function MetricCard({ label, value, color }: { label: string; value: string; color: string }) {
  const colorMap: Record<string, string> = {
    blue: 'border-blue-200 bg-blue-50',
    green: 'border-green-200 bg-green-50',
    red: 'border-red-200 bg-red-50',
    amber: 'border-amber-200 bg-amber-50',
    gray: 'border-gray-200 bg-gray-50',
  };
  const textMap: Record<string, string> = {
    blue: 'text-blue-700',
    green: 'text-green-700',
    red: 'text-red-700',
    amber: 'text-amber-700',
    gray: 'text-gray-700',
  };

  return (
    <div className={`rounded-lg border p-4 ${colorMap[color] || colorMap.gray}`}>
      <div className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</div>
      <div className={`text-2xl font-bold mt-1 ${textMap[color] || textMap.gray}`}>{value}</div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="px-4 py-3 bg-gray-50 border-b">
        <h3 className="font-semibold text-gray-800">{title}</h3>
      </div>
      <div className="p-4">{children}</div>
    </div>
  );
}

function SimpleTable({ rows }: { rows: any[] }) {
  if (!rows.length) return <p className="text-gray-500 text-sm">No data</p>;
  const cols = Object.keys(rows[0]).filter((c) => !c.startsWith('_'));
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="bg-gray-50 border-b">
            {cols.map((c) => (
              <th key={c} className="px-3 py-2 text-left font-medium text-gray-600 whitespace-nowrap">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 20).map((row, i) => (
            <tr key={i} className="border-b hover:bg-gray-50">
              {cols.map((c) => (
                <td key={c} className="px-3 py-2 text-gray-700 whitespace-nowrap">{formatVal(row[c])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Spinner() {
  return (
    <div className="flex items-center justify-center p-12">
      <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
    </div>
  );
}

function ErrorMsg({ msg }: { msg: string }) {
  return <div className="p-8 text-center text-red-500">{msg}</div>;
}

function formatVal(v: any): string {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'number') return v.toLocaleString();
  return String(v);
}

function formatGwp(v: any): string {
  const num = Number(v);
  if (isNaN(num)) return String(v);
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(0)}K`;
  return num.toLocaleString();
}
