import { useEffect, useMemo, useState } from 'react';
import {
  Search, AlertTriangle, Download, Copy, CheckCircle2, PlayCircle,
  FileText, BarChart3, MessageCircle, ExternalLink, ArrowUpRight,
  Phone, Sparkles, Zap,
} from 'lucide-react';
import { api } from '../lib/api';

type Recent = {
  transaction_id: string; company_name: string; postcode: string; region: string;
  sic_description: string; gross_premium: number | null; quote_status: string;
  is_outlier: boolean; model_version: string; created_at: string;
};

type TxDetail = {
  meta: any;
  payloads: {
    sales: any | null;
    engine_request: any | null;
    engine_response: any | null;
  };
};

type Replay = {
  transaction_id: string;
  stored_premium: number;
  stored_model: string;
  replay_premium: number;
  replay_model: string;
  delta_pct: number;
  notes: string;
  is_outlier: boolean;
  replay_response: any;
};

export default function QuoteReview() {
  const [tab, setTab] = useState<'lookup' | 'analytics' | 'genie'>('lookup');
  const [config, setConfig] = useState<any>(null);
  useEffect(() => { api.getConfig().then(setConfig).catch(() => {}); }, []);

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <div className="mb-5">
        <h2 className="text-2xl font-bold text-gray-900">Quote Review</h2>
        <p className="text-gray-500 mt-1">
          Investigate individual commercial quotes end-to-end. From customer call to pricing-engine replay.
        </p>
      </div>

      {/* Scenario banner — always visible, explains what this thing is for */}
      <div className="mb-6 bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200 rounded-lg p-5">
        <div className="flex items-start gap-3">
          <div className="shrink-0 mt-0.5 w-9 h-9 rounded-full bg-blue-600 text-white flex items-center justify-center">
            <Phone className="w-5 h-5" />
          </div>
          <div className="flex-1">
            <h3 className="font-semibold text-blue-900 mb-1">
              Scenario — a customer calls: <em>"why was I charged so much?"</em>
            </h3>
            <p className="text-sm text-blue-800 leading-relaxed">
              Every quote that flows through the rating engine is captured as three JSON payloads
              into Unity Catalog — the sales-channel request, the call sent to the engine, and
              the response it returned. An operator pulls up the transaction here, inspects what
              was sent and received, and <strong>re-runs it against today's pricing model</strong> to
              decide whether the problem is inside the engine or somewhere upstream.
            </p>
            <p className="text-xs text-blue-700 mt-2 leading-relaxed">
              <strong>The other role this table plays:</strong> every row in <code className="bg-white px-1 rounded">quotes</code> is
              also a training example for the Demand GBM — it learns why quotes convert or drop out.
              The JSON payloads are the exact serving-time feature vector the pricing model consumed.
            </p>
            <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-blue-700">
              <span className="inline-flex items-center gap-1">
                <span className="w-1.5 h-1.5 bg-blue-500 rounded-full" />
                Data: <code className="bg-white px-1 rounded">quotes</code> + three
                <code className="bg-white px-1 rounded">quote_payload_*</code> tables
              </span>
              <span className="inline-flex items-center gap-1">
                <span className="w-1.5 h-1.5 bg-blue-500 rounded-full" />
                Replay: simulated against the latest model version
              </span>
              <span className="inline-flex items-center gap-1">
                <span className="w-1.5 h-1.5 bg-blue-500 rounded-full" />
                Ask Genie for broader pattern analysis
              </span>
            </div>
          </div>
        </div>
      </div>

      <div className="border-b border-gray-200 mb-6">
        <nav className="flex gap-6">
          <TabButton label="Transaction lookup" icon={Search}     active={tab === 'lookup'}    onClick={() => setTab('lookup')} />
          <TabButton label="Analytics"          icon={BarChart3}  active={tab === 'analytics'} onClick={() => setTab('analytics')} />
          <TabButton label="Ask Genie"          icon={MessageCircle} active={tab === 'genie'}  onClick={() => setTab('genie')} />
        </nav>
      </div>

      {tab === 'lookup'    && <LookupTab />}
      {tab === 'analytics' && <AnalyticsTab />}
      {tab === 'genie'     && <GenieTab config={config} />}
    </div>
  );
}

function TabButton({ label, icon: Icon, active, onClick }: {
  label: string; icon: any; active: boolean; onClick: () => void;
}) {
  return (
    <button onClick={onClick} className={`py-2.5 px-1 border-b-2 -mb-px text-sm font-medium flex items-center gap-2 ${
      active ? 'border-blue-600 text-blue-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}>
      <Icon className="w-4 h-4" />
      {label}
    </button>
  );
}

// --------------------------------------------------------------------------
// Tab 1 — Transaction lookup
// --------------------------------------------------------------------------

function LookupTab() {
  const [recent, setRecent] = useState<Recent[] | null>(null);
  const [input, setInput]   = useState('');
  const [txId, setTxId]     = useState('');
  const [detail, setDetail] = useState<TxDetail | null>(null);
  const [detailErr, setDetailErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [replay, setReplay] = useState<Replay | null>(null);
  const [replayLoading, setReplayLoading] = useState(false);

  useEffect(() => {
    api.getQuoteStreamRecent(50).then(setRecent).catch(() => setRecent([]));
  }, []);

  const load = async (id: string) => {
    setTxId(id); setDetail(null); setReplay(null); setDetailErr(null);
    if (!id) return;
    setLoading(true);
    try {
      const d = await api.getQuoteStreamTransaction(id);
      setDetail(d);
    } catch (e: any) {
      setDetailErr(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  };

  const runReplay = async () => {
    if (!txId) return;
    setReplayLoading(true);
    try {
      const r = await api.replayQuote(txId);
      setReplay(r);
    } catch (e) {
      console.error('Replay failed', e);
    } finally {
      setReplayLoading(false);
    }
  };

  return (
    <div className="grid grid-cols-3 gap-6">
      {/* Left: find */}
      <div className="col-span-1 space-y-4">
        <div className="bg-white rounded-lg border border-gray-200 p-4">
          <h3 className="font-semibold text-gray-800 mb-2 text-sm">Find a quote</h3>
          <p className="text-xs text-gray-500 mb-3">
            Paste a transaction ID (e.g. <code className="text-[10px] bg-gray-100 px-1 rounded">TX-BAKERY-48M-2026Q2</code>) or pick from the recent list below.
          </p>
          <div className="flex gap-2">
            <input value={input} onChange={e => setInput(e.target.value.toUpperCase())}
              placeholder="TX-..." className="flex-1 px-3 py-1.5 border border-gray-300 rounded text-sm font-mono" />
            <button onClick={() => load(input.trim())}
              className="px-3 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-700">
              Lookup
            </button>
          </div>
        </div>

        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="px-3 py-2 border-b text-xs font-medium text-gray-600 uppercase tracking-wide bg-gray-50">
            Recent quotes
          </div>
          <div className="max-h-[640px] overflow-y-auto divide-y">
            {recent === null && <div className="p-4 text-xs text-gray-400">Loading…</div>}
            {recent !== null && recent.length === 0 && (
              <div className="p-4 text-xs text-gray-400">
                No transactions found. Run <code className="bg-gray-100 px-1">setup_quote_stream</code>.
              </div>
            )}
            {recent?.map(r => (
              <button key={r.transaction_id} onClick={() => { setInput(r.transaction_id); load(r.transaction_id); }}
                className={`block w-full text-left px-3 py-2 text-xs hover:bg-blue-50 transition-colors ${
                  txId === r.transaction_id ? 'bg-blue-50' : ''}`}>
                <div className="flex items-center justify-between gap-2">
                  <span className="font-mono text-gray-800 truncate">{r.transaction_id}</span>
                  {asBool(r.is_outlier) && <AlertTriangle className="w-3.5 h-3.5 text-red-500 shrink-0" />}
                </div>
                <div className="text-gray-500 text-[11px] truncate">{r.company_name}</div>
                <div className="flex items-center gap-2 mt-0.5">
                  <StatusBadge status={r.quote_status} />
                  <span className="text-gray-600">{r.gross_premium ? `£${fmtInt(r.gross_premium)}` : '—'}</span>
                </div>
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Right: detail */}
      <div className="col-span-2 space-y-4">
        {!txId && (
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-sm text-blue-700">
            Pick a transaction on the left to see its payloads and run a simulated replay.
          </div>
        )}
        {loading && <div className="p-6 text-sm text-gray-500">Loading transaction…</div>}
        {detailErr && <div className="p-4 bg-red-50 border border-red-200 rounded text-sm text-red-700">{detailErr}</div>}

        {detail && !loading && (
          <>
            <DetailHeader detail={detail} />
            <ReplayPanel detail={detail} replay={replay} loading={replayLoading} onRun={runReplay} />
            <AgentAnalystPanel detail={detail} replay={replay} />
            <FlatSummary detail={detail} />
            <PayloadTabs detail={detail} />
          </>
        )}
      </div>
    </div>
  );
}

function DetailHeader({ detail }: { detail: TxDetail }) {
  const m = detail.meta;
  const outlier = asBool(m.is_outlier);
  return (
    <div className={`rounded-lg border p-5 ${outlier ? 'bg-red-50 border-red-200' : 'bg-white border-gray-200'}`}>
      <div className="flex items-center justify-between mb-3">
        <div>
          <div className="text-xs text-gray-500">Transaction</div>
          <div className="font-mono text-sm font-medium text-gray-900">{m.transaction_id}</div>
        </div>
        {outlier && (
          <span className="inline-flex items-center gap-1.5 px-3 py-1 bg-red-600 text-white text-xs font-medium rounded-full">
            <AlertTriangle className="w-3.5 h-3.5" /> Outlier flagged
          </span>
        )}
      </div>
      <div className="grid grid-cols-4 gap-3">
        <Stat label="Status" value={m.quote_status} />
        <Stat label="Gross premium" value={m.gross_premium ? `£${fmtInt(m.gross_premium)}` : '—'} mono />
        <Stat label="Postcode" value={m.postcode} mono />
        <Stat label="Model" value={m.model_version} mono />
      </div>
      {outlier && (
        <div className="mt-3 text-sm text-red-700">
          This quote sits far outside the peer-group distribution. Re-run against the rating engine
          below to confirm whether the problem is in the pricing engine or upstream in the flow.
        </div>
      )}
    </div>
  );
}

function ReplayPanel({ detail, replay, loading, onRun }: {
  detail: TxDetail; replay: Replay | null; loading: boolean; onRun: () => void;
}) {
  const m = detail.meta;
  const createdAt = m.created_at_str || m.created_at || '';
  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="px-5 py-3 border-b bg-gray-50 flex items-center justify-between">
        <div>
          <h3 className="font-semibold text-gray-800 text-sm flex items-center gap-2">
            <Zap className="w-4 h-4 text-amber-500" />
            Re-run against the rating engine
          </h3>
          <p className="text-xs text-gray-500">
            Compare the price stored at quote time against what today's model would return.
          </p>
        </div>
        <button onClick={onRun} disabled={loading}
          className="px-3 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50 flex items-center gap-1.5">
          <PlayCircle className="w-4 h-4" />
          {loading ? 'Replaying…' : 'Re-run'}
        </button>
      </div>

      <div className="p-5">
        {/* Side-by-side: when the quote was generated vs today's replay */}
        <div className="grid grid-cols-2 gap-3 mb-3">
          <ComparisonBlock
            heading="At quote time"
            date={createdAt}
            model={m.model_version}
            premium={m.gross_premium ? `£${fmtInt(Number(m.gross_premium))}` : '—'}
            tone={asBool(m.is_outlier) ? 'red' : 'neutral'}
          />
          <ComparisonBlock
            heading={replay ? 'Replay (today)' : 'Replay (today) — not run yet'}
            date={replay ? 'Just now' : '—'}
            model={replay?.replay_model}
            premium={replay ? `£${fmtInt(replay.replay_premium)}` : '—'}
            tone={
              replay && Math.abs(replay.delta_pct) > 50 ? 'green' :
              replay && Math.abs(replay.delta_pct) > 5  ? 'amber' :
              replay                                    ? 'green' : 'neutral'
            }
          />
        </div>

        {replay && (
          <div className={`rounded-lg border px-4 py-3 text-sm flex items-start gap-3 ${
            Math.abs(replay.delta_pct) > 50 ? 'bg-red-50 border-red-200 text-red-800' :
            Math.abs(replay.delta_pct) > 5  ? 'bg-amber-50 border-amber-200 text-amber-800' :
                                              'bg-green-50 border-green-200 text-green-800'}`}>
            <div className="shrink-0 font-mono text-xl leading-none mt-0.5">
              {replay.delta_pct >= 0 ? '+' : ''}{replay.delta_pct.toFixed(1)}%
            </div>
            <div>
              <div className="font-medium text-[13px] mb-0.5">
                {Math.abs(replay.delta_pct) > 50 ? 'Large discrepancy' :
                 Math.abs(replay.delta_pct) > 5  ? 'Moderate drift'   :
                                                   'Price reproduces'}
              </div>
              <div className="text-xs opacity-90">{replay.notes}</div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function ComparisonBlock({ heading, date, model, premium, tone }: {
  heading: string; date?: string; model?: string; premium: string;
  tone: 'red' | 'amber' | 'green' | 'neutral';
}) {
  const toneMap = {
    red:     'bg-red-50 border-red-200',
    amber:   'bg-amber-50 border-amber-200',
    green:   'bg-green-50 border-green-200',
    neutral: 'bg-gray-50 border-gray-200',
  };
  const textMap = {
    red:     'text-red-700',
    amber:   'text-amber-700',
    green:   'text-green-700',
    neutral: 'text-gray-800',
  };
  return (
    <div className={`rounded-lg border p-3 ${toneMap[tone]}`}>
      <div className="text-[11px] font-medium text-gray-600 uppercase tracking-wide">{heading}</div>
      <div className={`text-xl font-bold mt-1 font-mono ${textMap[tone]}`}>{premium}</div>
      <div className="text-[11px] text-gray-500 mt-1 space-y-0.5">
        {date  && <div>Generated: <span className="font-mono">{date}</span></div>}
        {model && <div>Model: <span className="font-mono">{model}</span></div>}
      </div>
    </div>
  );
}

function AgentAnalystPanel({ detail, replay }: { detail: TxDetail; replay: Replay | null }) {
  const [running, setRunning] = useState(false);
  const [shown,   setShown]   = useState(false);
  const m = detail.meta;
  const outlier = asBool(m.is_outlier);

  const runMock = () => {
    setRunning(true);
    setTimeout(() => { setShown(true); setRunning(false); }, 1400);
  };

  // Build a plausible mock narrative based on what we know about the transaction
  const mockNarrative = outlier ? (
    <>
      <p>
        The £{fmtInt(Number(m.gross_premium))} gross premium for <strong>{m.company_name}</strong>{' '}
        is <strong>~{replay ? Math.abs(replay.delta_pct).toFixed(0) : '4.5'}× higher</strong> than
        the peer p99 for {m.region} × {m.construction_type} construction. Breaking it down
        factor-by-factor:
      </p>
      <ul className="list-disc list-inside space-y-1 mt-2 text-[13px]">
        <li>Base building + contents + liability premium is consistent with peers (£~{fmtInt((m.buildings_si * 0.0011 + m.contents_si * 0.01 + m.liability_si * 0.0004))}).</li>
        <li>Loadings carry a <code className="bg-white px-1 rounded text-[11px]">factor_override_anomaly</code> entry of £48M — this is not a legitimate rating factor. It was injected upstream.</li>
        <li>The replay against <code className="bg-white px-1 rounded text-[11px]">{replay?.replay_model || 'the latest model'}</code> returns a clean price — the current engine does not reproduce the anomaly.</li>
      </ul>
      <p className="mt-2 font-medium">Recommended approach:</p>
      <ol className="list-decimal list-inside space-y-1 mt-1 text-[13px]">
        <li>Trace the <code className="bg-white px-1 rounded text-[11px]">factor_override_anomaly</code> key in the engine request payload — check whether a factor override was sent from the sales channel or injected via a middleware rule.</li>
        <li>Verify the ops audit log around the quote timestamp for manual factor overrides.</li>
        <li>If no override is found, investigate the engine version (<code className="bg-white px-1 rounded text-[11px]">{m.model_version}</code>) for a known bug — run a batch re-price and flag any other transactions with the same signature.</li>
        <li>Re-quote the customer at the replayed price of £{replay ? fmtInt(replay.replay_premium) : '~20k'} with the underwriter's sign-off.</li>
      </ol>
    </>
  ) : (
    <>
      <p>
        The quote prices within normal bounds for its peer group ({m.region} × {m.construction_type}).
        No anomaly detected.
      </p>
      <p className="mt-2 font-medium">What Claude would check in production:</p>
      <ul className="list-disc list-inside space-y-1 mt-1 text-[13px]">
        <li>Factor-by-factor peer comparison: is any single loading unusually high?</li>
        <li>Model version history: did this quote run through a known-problematic engine version?</li>
        <li>Customer-specific patterns: any recent claims, reinstated cover, or underwriter notes?</li>
        <li>Competitor pricing position: are we materially above or below the market for this risk?</li>
      </ul>
    </>
  );

  return (
    <div className="bg-white rounded-lg border border-dashed border-purple-300 overflow-hidden">
      <div className="px-5 py-3 border-b border-dashed border-purple-200 bg-purple-50 flex items-center justify-between">
        <div>
          <h3 className="font-semibold text-purple-800 text-sm flex items-center gap-2">
            <Sparkles className="w-4 h-4 text-purple-600" />
            AI Analyst <span className="text-[10px] font-normal uppercase tracking-wide bg-purple-200 text-purple-800 px-1.5 py-0.5 rounded">placeholder</span>
          </h3>
          <p className="text-xs text-purple-700 mt-0.5">
            In production: a Claude-backed agent reads the three JSON payloads plus peer-group context,
            explains why the price landed where it did, and recommends a remediation path.
          </p>
        </div>
        <button onClick={runMock} disabled={running}
          className="px-3 py-1.5 bg-purple-600 text-white rounded text-sm hover:bg-purple-700 disabled:opacity-50 flex items-center gap-1.5">
          <Sparkles className="w-4 h-4" />
          {running ? 'Analysing…' : 'Run analysis'}
        </button>
      </div>
      <div className="p-5 text-sm text-gray-800 space-y-2">
        {!shown && !running && (
          <p className="text-gray-500 italic">
            Click <strong>Run analysis</strong> to see a mock Claude-powered root-cause analysis
            over the transaction. In production this would call Mosaic AI Model Serving
            (<code className="bg-gray-100 px-1 rounded text-[11px]">databricks-claude-sonnet-4</code>)
            with the payloads, peer-group stats, and audit log as context.
          </p>
        )}
        {running && <p className="text-gray-500">Running mock analysis — in production this would take ~3–8 seconds on Claude Sonnet.</p>}
        {shown && mockNarrative}
      </div>
    </div>
  );
}

function FlatSummary({ detail }: { detail: TxDetail }) {
  const m = detail.meta;
  const rows: [string, any][] = [
    ['Company',       m.company_name],
    ['SIC',           `${m.sic_code} — ${m.sic_description}`],
    ['Channel',       m.channel],
    ['Agent',         m.agent_user],
    ['Region',        m.region],
    ['Construction',  m.construction_type],
    ['Year built',    m.year_built],
    ['Floor area',    m.floor_area_sqm ? `${fmtInt(m.floor_area_sqm)} m²` : '—'],
    ['Flood zone',    m.flood_zone],
    ['Claims (5y)',   m.claims_last_5y],
    ['Buildings SI',  `£${fmtInt(m.buildings_si)}`],
    ['Contents SI',   `£${fmtInt(m.contents_si)}`],
    ['Liability',     `£${fmtInt(m.liability_si)}`],
  ];
  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="px-5 py-3 border-b bg-gray-50 flex items-center gap-2">
        <FileText className="w-4 h-4 text-gray-600" />
        <h3 className="font-semibold text-gray-800 text-sm">Flattened risk summary</h3>
      </div>
      <div className="grid grid-cols-2 gap-x-6 gap-y-1 p-5 text-sm">
        {rows.map(([k, v]) => (
          <div key={k} className="flex justify-between border-b border-dashed border-gray-100 py-1">
            <span className="text-gray-500">{k}</span>
            <span className="font-mono text-gray-800">{String(v ?? '—')}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function PayloadTabs({ detail }: { detail: TxDetail }) {
  const [sub, setSub] = useState<'sales' | 'engine_request' | 'engine_response'>('sales');
  const labels: Record<string, string> = {
    sales:           'Sales request',
    engine_request:  'Rating engine request',
    engine_response: 'Rating engine response',
  };
  const data = detail.payloads[sub];
  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="border-b flex">
        {(['sales', 'engine_request', 'engine_response'] as const).map(k => (
          <button key={k} onClick={() => setSub(k)}
            className={`px-4 py-2.5 text-xs font-medium border-r last:border-r-0 ${
              sub === k ? 'bg-blue-50 text-blue-700 border-b-2 border-b-blue-600' : 'text-gray-600 hover:text-gray-900'}`}>
            {labels[k]}
          </button>
        ))}
      </div>
      <PayloadView txId={detail.meta.transaction_id} kind={sub} data={data} label={labels[sub]} />
    </div>
  );
}

function PayloadView({ txId, kind, data, label }: { txId: string; kind: string; data: any; label: string }) {
  const [saved, setSaved] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  if (!data) {
    return (
      <div className="p-5 text-sm text-gray-500">
        No {label.toLowerCase()} recorded — the journey was abandoned before this step.
      </div>
    );
  }

  const pretty = useMemo(() => JSON.stringify(data, null, 2), [data]);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(pretty);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch (e) { /* clipboard can fail inside iframes — ignore */ }
  };

  const download = () => {
    const blob = new Blob([pretty], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `${txId}_${kind}.json`;
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
  };

  const save = async () => {
    try {
      const r = await api.saveQuotePayload(txId, kind, data);
      setSaved(r.saved_to);
      setTimeout(() => setSaved(null), 4000);
    } catch (e: any) {
      setSaved('ERROR: ' + (e?.message || String(e)));
    }
  };

  return (
    <div>
      <div className="flex items-center justify-end gap-2 px-3 py-2 bg-gray-50 border-b text-xs">
        <button onClick={copy} className="flex items-center gap-1 px-2 py-1 bg-white border border-gray-300 rounded hover:bg-gray-50">
          {copied ? <CheckCircle2 className="w-3 h-3 text-green-600" /> : <Copy className="w-3 h-3" />}
          {copied ? 'Copied' : 'Copy'}
        </button>
        <button onClick={download} className="flex items-center gap-1 px-2 py-1 bg-white border border-gray-300 rounded hover:bg-gray-50">
          <Download className="w-3 h-3" /> Download
        </button>
        <button onClick={save} className="flex items-center gap-1 px-2 py-1 bg-white border border-gray-300 rounded hover:bg-gray-50">
          <FileText className="w-3 h-3" /> Save to UC volume
        </button>
      </div>
      <pre className="p-4 text-xs font-mono bg-gray-900 text-green-300 overflow-x-auto max-h-[480px] leading-relaxed">
        {pretty}
      </pre>
      {saved && (
        <div className={`px-4 py-2 text-xs border-t ${saved.startsWith('ERROR') ? 'bg-red-50 text-red-700' : 'bg-green-50 text-green-700'}`}>
          {saved.startsWith('ERROR') ? saved : `Saved: ${saved}`}
        </div>
      )}
    </div>
  );
}

// --------------------------------------------------------------------------
// Tab 2 — Analytics
// --------------------------------------------------------------------------

function AnalyticsTab() {
  const [summary, setSummary] = useState<any | null>(null);
  const [outliers, setOutliers] = useState<any[]>([]);
  const [funnel, setFunnel] = useState<any[]>([]);
  const [dist, setDist] = useState<any[]>([]);

  useEffect(() => {
    api.getQuoteStreamSummary().then(setSummary).catch(() => setSummary({}));
    api.getQuoteStreamOutliers().then(setOutliers).catch(() => setOutliers([]));
    api.getQuoteStreamFunnel().then(setFunnel).catch(() => setFunnel([]));
    api.getQuoteStreamDistribution().then(setDist).catch(() => setDist([]));
  }, []);

  const total = Number(summary?.total_transactions) || 0;
  const bound = Number(summary?.bound) || 0;
  const abandoned = Number(summary?.abandoned) || 0;

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-5 gap-4">
        <MetricCard label="Transactions" value={fmtInt(total)} />
        <MetricCard label="Bound"        value={fmtInt(bound)}
          sub={total ? `${((bound / total) * 100).toFixed(0)}% conversion` : ''} />
        <MetricCard label="Abandoned"    value={fmtInt(abandoned)}
          sub={total ? `${((abandoned / total) * 100).toFixed(0)}% drop-out` : ''} tone="amber" />
        <MetricCard label="Avg premium (ex-outliers)" value={summary?.avg_premium ? `£${fmtInt(Number(summary.avg_premium))}` : '—'} />
        <MetricCard label="Outliers"     value={fmtInt(Number(summary?.outliers) || 0)} tone="red" />
      </div>

      <div className="grid grid-cols-2 gap-6">
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="px-5 py-3 border-b bg-gray-50">
            <h3 className="font-semibold text-gray-800 text-sm">Outliers by gross premium</h3>
            <p className="text-[11px] text-gray-500">Any quote above p99 × 3 of its peer group (region × construction type).</p>
          </div>
          <div className="overflow-x-auto max-h-[380px]">
            <table className="w-full text-xs">
              <thead className="bg-gray-50 sticky top-0 text-gray-600">
                <tr>
                  <th className="px-3 py-1.5 text-left font-medium">Transaction</th>
                  <th className="px-3 py-1.5 text-left font-medium">Company</th>
                  <th className="px-3 py-1.5 text-right font-medium">Gross £</th>
                  <th className="px-3 py-1.5 text-right font-medium">vs p99</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {outliers.length === 0 && (
                  <tr><td colSpan={4} className="px-3 py-3 text-center text-gray-400">Stream clean — no outliers beyond p99 × 3.</td></tr>
                )}
                {outliers.map(o => (
                  <tr key={o.transaction_id} className="hover:bg-red-50">
                    <td className="px-3 py-1.5 font-mono text-gray-800 truncate">{o.transaction_id}</td>
                    <td className="px-3 py-1.5 text-gray-700 truncate">{o.company_name}</td>
                    <td className="px-3 py-1.5 text-right font-mono text-gray-800">{fmtInt(Number(o.gross_premium))}</td>
                    <td className="px-3 py-1.5 text-right font-mono text-red-600">{Number(o.vs_peer_p99).toFixed(1)}×</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="px-5 py-3 border-b bg-gray-50">
            <h3 className="font-semibold text-gray-800 text-sm">Quote journey funnel</h3>
            <p className="text-[11px] text-gray-500">By channel — where do customers drop out?</p>
          </div>
          <div className="overflow-x-auto max-h-[380px]">
            <table className="w-full text-xs">
              <thead className="bg-gray-50 sticky top-0 text-gray-600">
                <tr>
                  <th className="px-3 py-1.5 text-left font-medium">Channel</th>
                  <th className="px-3 py-1.5 text-right font-medium">Started</th>
                  <th className="px-3 py-1.5 text-right font-medium">Priced</th>
                  <th className="px-3 py-1.5 text-right font-medium">Bound</th>
                  <th className="px-3 py-1.5 text-right font-medium">Drop %</th>
                  <th className="px-3 py-1.5 text-right font-medium">Bind %</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {funnel.map(f => (
                  <tr key={f.channel}>
                    <td className="px-3 py-1.5 font-medium text-gray-800">{f.channel}</td>
                    <td className="px-3 py-1.5 text-right font-mono">{fmtInt(Number(f.started))}</td>
                    <td className="px-3 py-1.5 text-right font-mono">{fmtInt(Number(f.priced))}</td>
                    <td className="px-3 py-1.5 text-right font-mono">{fmtInt(Number(f.bound))}</td>
                    <td className="px-3 py-1.5 text-right font-mono text-amber-600">{(Number(f.dropout_rate) * 100).toFixed(1)}%</td>
                    <td className="px-3 py-1.5 text-right font-mono text-green-600">{(Number(f.bind_rate) * 100).toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-5 py-3 border-b bg-gray-50">
          <h3 className="font-semibold text-gray-800 text-sm">Premium distribution by region</h3>
          <p className="text-[11px] text-gray-500">Quartiles + p95 (excludes outliers and abandoned).</p>
        </div>
        <DistributionChart data={dist} />
      </div>
    </div>
  );
}

function DistributionChart({ data }: { data: any[] }) {
  if (!data || data.length === 0) return <div className="p-5 text-xs text-gray-400">No data.</div>;
  const globalMax = Math.max(...data.map(d => Number(d.p95) || 0));
  return (
    <div className="p-5 space-y-3">
      {data.map(d => {
        const p25 = Number(d.p25), p50 = Number(d.p50), p75 = Number(d.p75), p95 = Number(d.p95);
        const leftPct  = (p25 / globalMax) * 100;
        const widthPct = ((p75 - p25) / globalMax) * 100;
        const medianPct = (p50 / globalMax) * 100;
        const p95Pct = (p95 / globalMax) * 100;
        return (
          <div key={d.region} className="flex items-center gap-3">
            <div className="w-28 text-xs text-gray-700 truncate">{d.region}</div>
            <div className="flex-1 relative h-6 bg-gray-50 rounded">
              <div className="absolute top-0 bottom-0 bg-blue-200" style={{ left: `${leftPct}%`, width: `${widthPct}%` }} />
              <div className="absolute top-0 bottom-0 w-0.5 bg-blue-700" style={{ left: `${medianPct}%` }} />
              <div className="absolute top-0 bottom-0 w-px bg-gray-500" style={{ left: `${p95Pct}%` }} />
            </div>
            <div className="w-20 text-right text-xs font-mono text-gray-700">£{fmtInt(p50)}</div>
            <div className="w-10 text-right text-[11px] text-gray-400">n={fmtInt(Number(d.n))}</div>
          </div>
        );
      })}
      <div className="flex items-center gap-4 pt-2 border-t text-[11px] text-gray-500">
        <span className="inline-flex items-center gap-1"><span className="w-3 h-3 bg-blue-200 rounded-sm"/> IQR (p25–p75)</span>
        <span className="inline-flex items-center gap-1"><span className="w-0.5 h-3 bg-blue-700"/> median</span>
        <span className="inline-flex items-center gap-1"><span className="w-px h-3 bg-gray-500"/> p95</span>
      </div>
    </div>
  );
}

// --------------------------------------------------------------------------
// Tab 3 — Genie
// --------------------------------------------------------------------------

function GenieTab({ config }: { config: any }) {
  const embedUrl = config?.genie_quote_embed_url;
  const openUrl  = config?.genie_quote_url;

  if (!embedUrl) {
    return (
      <div className="bg-amber-50 border border-amber-200 rounded-lg p-6">
        <h3 className="font-semibold text-amber-800 mb-2">Ask Genie — setup required</h3>
        <p className="text-sm text-amber-700 mb-3">
          Wire a Databricks Genie space over <code className="bg-white px-1 rounded">quotes</code>,
          then set <code className="bg-white px-1 rounded">GENIE_QUOTE_SPACE_ID</code> in <code className="bg-white px-1 rounded">resources/app.yml</code> and redeploy.
        </p>
        <ol className="text-sm text-amber-700 list-decimal list-inside space-y-1">
          <li>Open Genie in the workspace and create a space over <code className="bg-white px-1 rounded">quotes</code>.</li>
          <li>Short description: <em>Commercial quote stream — transactions, prices, drop-outs, outliers.</em></li>
          <li>Copy the space ID from the URL and set it in <code className="bg-white px-1 rounded">app.yaml</code>.</li>
          <li>Redeploy the app.</li>
        </ol>
      </div>
    );
  }

  const suggestions = [
    'How many quotes were abandoned last week by channel?',
    "What's the average gross premium by region for bound policies?",
    'Show the 10 most expensive quotes and the model version that priced them.',
    'Which construction type has the highest drop-out rate?',
  ];

  return (
    <div className="space-y-3">
      <div className="bg-purple-50 border border-purple-200 rounded-t-lg px-5 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <MessageCircle className="w-5 h-5 text-purple-600" />
          <div>
            <h3 className="font-semibold text-purple-800">Ask Genie about the quote stream</h3>
            <p className="text-xs text-purple-600">Natural-language Q&amp;A over <code className="bg-white px-1 rounded">quotes</code></p>
          </div>
        </div>
        {openUrl && (
          <a href={openUrl} target="_blank" rel="noopener noreferrer"
            className="text-xs text-purple-600 hover:text-purple-800 flex items-center gap-1">
            Open full screen <ExternalLink className="w-3 h-3" />
          </a>
        )}
      </div>
      <div className="bg-white border border-t-0 border-purple-200 rounded-b-lg overflow-hidden">
        <iframe src={embedUrl} className="w-full border-0" style={{ height: '560px' }}
          title="Genie — Quote Stream" allow="clipboard-write" />
      </div>
      <div className="flex flex-wrap gap-2">
        <span className="text-xs text-gray-400">Try:</span>
        {suggestions.map((s, i) => (
          <span key={i} className="text-xs text-purple-600 bg-purple-50 border border-purple-200 rounded px-2 py-0.5">"{s}"</span>
        ))}
      </div>
    </div>
  );
}

// --------------------------------------------------------------------------
// Small shared helpers
// --------------------------------------------------------------------------

function Stat({ label, value, sub, subClass, mono }: {
  label: string; value: any; sub?: string; subClass?: string; mono?: boolean;
}) {
  return (
    <div>
      <div className="text-[11px] text-gray-500 uppercase tracking-wide">{label}</div>
      <div className={`text-base font-semibold text-gray-900 ${mono ? 'font-mono' : ''}`}>{value}</div>
      {sub && <div className={`text-[11px] mt-0.5 ${subClass || 'text-gray-500'}`}>{sub}</div>}
    </div>
  );
}

function MetricCard({ label, value, sub, tone }: {
  label: string; value: string; sub?: string; tone?: 'amber' | 'red';
}) {
  const toneMap = {
    amber: 'border-amber-200 bg-amber-50 text-amber-700',
    red:   'border-red-200 bg-red-50 text-red-700',
  };
  const cls = tone ? toneMap[tone] : 'border-gray-200 bg-white text-gray-800';
  return (
    <div className={`rounded-lg border p-4 ${cls}`}>
      <div className="text-xs text-gray-500">{label}</div>
      <div className="text-2xl font-bold mt-1">{value}</div>
      {sub && <div className="text-[11px] mt-0.5 opacity-80">{sub}</div>}
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const map: Record<string, string> = {
    BOUND:     'bg-green-100 text-green-700',
    QUOTED:    'bg-blue-100 text-blue-700',
    ABANDONED: 'bg-gray-100 text-gray-600',
  };
  return <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${map[status] || 'bg-gray-100 text-gray-600'}`}>{status}</span>;
}

function fmtInt(n: number | null | undefined) {
  if (n == null || isNaN(Number(n))) return '—';
  return Math.round(Number(n)).toLocaleString();
}

// SQL Statement Execution API returns booleans as the strings "true"/"false".
// Coerce robustly here.
function asBool(x: any): boolean {
  if (typeof x === 'boolean') return x;
  if (typeof x === 'string') return x.toLowerCase() === 'true';
  return !!x;
}
