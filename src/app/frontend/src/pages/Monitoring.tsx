import { useEffect, useState } from 'react';
import { Activity, AlertTriangle, TrendingUp, Clock, Database, Shield, BarChart3 } from 'lucide-react';
import { api } from '../lib/api';

export default function Monitoring() {
  const [governance, setGovernance] = useState<any>(null);
  const [featureStore, setFeatureStore] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.getGovernanceSummary(),
      api.getFeatureStoreStatus(),
    ]).then(([g, f]) => {
      setGovernance(g);
      setFeatureStore(f);
    }).finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="p-8 text-center text-gray-500">Loading monitoring data...</div>;

  const upt = featureStore?.upt || {};
  const dq = governance?.data_quality || [];

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-gray-900">Monitoring</h2>
        <p className="text-gray-500 mt-1">Data health, model performance, and operational metrics for pricing actuaries</p>
      </div>

      <div className="bg-blue-50 border border-blue-200 rounded-lg p-5 mb-6">
        <h3 className="font-semibold text-blue-800 mb-2">What actuaries monitor</h3>
        <p className="text-sm text-blue-600">
          This page answers the questions a Head of Pricing asks every morning: Is the data fresh?
          Are the models stable? Has anything drifted? Are there anomalies that need investigation?
          In production, these would be backed by Lakehouse Monitoring and automated alerts.
        </p>
      </div>

      <div className="grid grid-cols-2 gap-6">
        {/* Data Freshness */}
        <Section title="Data Freshness" icon={Clock} color="blue">
          <p className="text-xs text-gray-500 mb-3">
            <strong>Actuary question:</strong> "Is my data up to date? When was the last ingestion?"
          </p>
          <div className="space-y-2">
            <MetricRow label="Unified Pricing Table" value={`v${upt.delta_version || '?'}`} detail={`Last modified: ${upt.last_modified || '?'}`} status="ok" />
            <MetricRow label="Policies" value={`${Number(upt.row_count || 0).toLocaleString()} rows`} detail={`${upt.unique_policies || '?'} unique`} status="ok" />
            <MetricRow label="Feature columns" value={`${upt.column_count || '?'} columns`} detail="Including synthetic features" status="ok" />
          </div>
        </Section>

        {/* Data Quality */}
        <Section title="Data Quality Pass Rates" icon={Shield} color="green">
          <p className="text-xs text-gray-500 mb-3">
            <strong>Actuary question:</strong> "How clean is the data? How many records failed DQ?"
          </p>
          {dq.map((d: any, i: number) => (
            <div key={i} className="mb-3">
              <div className="flex justify-between text-sm mb-1">
                <span className="font-medium text-gray-700">{d.dataset}</span>
                <span className={Number(d.pass_rate) >= 95 ? 'text-green-600' : 'text-amber-600'}>{d.pass_rate}%</span>
              </div>
              <div className="w-full h-2 bg-gray-100 rounded-full">
                <div className={`h-full rounded-full ${Number(d.pass_rate) >= 95 ? 'bg-green-500' : 'bg-amber-400'}`}
                     style={{ width: `${d.pass_rate}%` }} />
              </div>
              <div className="text-[10px] text-gray-400 mt-0.5">{d.dropped} rows dropped by DQ expectations</div>
            </div>
          ))}
        </Section>

        {/* Model Stability */}
        <Section title="Model Stability" icon={TrendingUp} color="purple">
          <p className="text-xs text-gray-500 mb-3">
            <strong>Actuary question:</strong> "Are the models still performing as expected? Has anything drifted?"
          </p>
          <div className="space-y-3">
            <div className="bg-gray-50 rounded-lg p-3">
              <h5 className="text-xs font-semibold text-gray-600 mb-1">What to monitor in production</h5>
              <ul className="text-xs text-gray-500 space-y-1">
                <li>• <strong>PSI (Population Stability Index)</strong> — detects if the input distribution has shifted since training</li>
                <li>• <strong>Prediction drift</strong> — are model outputs changing over time?</li>
                <li>• <strong>Feature drift</strong> — have key features (flood scores, credit scores) shifted?</li>
                <li>• <strong>Loss ratio tracking</strong> — are actual claims matching model predictions?</li>
              </ul>
            </div>
            <div className="bg-purple-50 border border-purple-200 rounded-lg p-3">
              <p className="text-xs text-purple-700">
                <strong>Databricks Lakehouse Monitoring</strong> provides automated drift detection,
                statistical profiling, and alerting — configured per table or model endpoint.
                In this demo, the Model Factory computes PSI during evaluation.
              </p>
            </div>
          </div>
        </Section>

        {/* Portfolio Health */}
        <Section title="Portfolio Health Indicators" icon={BarChart3} color="amber">
          <p className="text-xs text-gray-500 mb-3">
            <strong>Actuary question:</strong> "What does my book look like? Any concentration risk?"
          </p>
          <div className="space-y-3">
            <div className="bg-gray-50 rounded-lg p-3">
              <h5 className="text-xs font-semibold text-gray-600 mb-1">Key metrics for pricing actuaries</h5>
              <ul className="text-xs text-gray-500 space-y-1">
                <li>• <strong>Avg loss ratio by tier</strong> — is pricing adequate across segments?</li>
                <li>• <strong>Renewal pipeline</strong> — what GWP is renewing in the next 90 days?</li>
                <li>• <strong>Market position</strong> — are we competitive or overpriced by segment?</li>
                <li>• <strong>Churn forecast</strong> — which policies are at risk of non-renewal?</li>
                <li>• <strong>Geographic concentration</strong> — is flood/subsidence exposure balanced?</li>
              </ul>
            </div>
            <div className="bg-amber-50 border border-amber-200 rounded-lg p-3">
              <p className="text-xs text-amber-700">
                <strong>AI/BI Dashboards</strong> and <strong>Genie Spaces</strong> enable actuaries
                to build custom views and ask ad-hoc questions in natural language. Dashboard queries
                are available in <code>src/08_governance/dashboard_queries.sql</code>.
              </p>
            </div>
          </div>
        </Section>

        {/* Operational */}
        <Section title="Operational Health" icon={Activity} color="red">
          <p className="text-xs text-gray-500 mb-3">
            <strong>Actuary question:</strong> "Are all systems running? Any failures?"
          </p>
          <div className="space-y-2">
            <MetricRow label="Audit events (total)" value={String(governance?.events_by_type?.reduce((s: number, e: any) => s + Number(e.event_count || 0), 0) || 0)} detail="Across all event types" status="ok" />
            <MetricRow label="Event types tracked" value={String(governance?.events_by_type?.length || 0)} detail="Approvals, uploads, agent calls, etc." status="ok" />
            <MetricRow label="DLT pipeline" value="Active" detail="Bronze → Silver with expectations" status="ok" />
            <MetricRow label="Online store" value={featureStore?.online_store?.state || '?'} detail={featureStore?.online_store?.name || ''} status={featureStore?.online_store?.state === 'AVAILABLE' ? 'ok' : 'warn'} />
          </div>
        </Section>

        {/* Alerting */}
        <Section title="Alerting & Notifications" icon={AlertTriangle} color="gray">
          <p className="text-xs text-gray-500 mb-3">
            <strong>Actuary question:</strong> "Will I be told if something goes wrong?"
          </p>
          <div className="bg-gray-50 rounded-lg p-3">
            <h5 className="text-xs font-semibold text-gray-600 mb-1">Configurable alerts for production</h5>
            <ul className="text-xs text-gray-500 space-y-1">
              <li>• <strong>Data arrival SLA</strong> — alert if vendor data doesn't arrive on schedule</li>
              <li>• <strong>DQ threshold breach</strong> — alert if pass rate drops below 90%</li>
              <li>• <strong>Model drift</strong> — alert if PSI exceeds 0.25 (unstable)</li>
              <li>• <strong>Serving latency</strong> — alert if P99 exceeds 200ms</li>
              <li>• <strong>Endpoint errors</strong> — alert on 5xx error rate spike</li>
              <li>• <strong>Approval backlog</strong> — alert if datasets await review for &gt;48h</li>
            </ul>
          </div>
          <div className="mt-2 text-xs text-gray-400">
            Implemented via Databricks SQL Alerts, Workflows notifications, or external integrations (PagerDuty, Slack, email).
          </div>
        </Section>
      </div>
    </div>
  );
}

function Section({ title, icon: Icon, color, children }: { title: string; icon: any; color: string; children: React.ReactNode }) {
  const iconColors: Record<string, string> = {
    blue: 'text-blue-600', green: 'text-green-600', purple: 'text-purple-600',
    amber: 'text-amber-600', red: 'text-red-600', gray: 'text-gray-600',
  };
  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="px-4 py-3 bg-gray-50 border-b flex items-center gap-2">
        <Icon className={`w-4 h-4 ${iconColors[color] || 'text-gray-600'}`} />
        <h3 className="font-semibold text-gray-800">{title}</h3>
      </div>
      <div className="p-4">{children}</div>
    </div>
  );
}

function MetricRow({ label, value, detail, status }: { label: string; value: string; detail: string; status: 'ok' | 'warn' | 'error' }) {
  return (
    <div className="flex items-center justify-between py-1.5 border-b border-gray-100 last:border-0">
      <div>
        <span className="text-sm font-medium text-gray-700">{label}</span>
        <span className="text-xs text-gray-400 ml-2">{detail}</span>
      </div>
      <div className="flex items-center gap-1.5">
        <span className="text-sm font-medium text-gray-900">{value}</span>
        <div className={`w-2 h-2 rounded-full ${status === 'ok' ? 'bg-green-500' : status === 'warn' ? 'bg-amber-400' : 'bg-red-500'}`} />
      </div>
    </div>
  );
}
