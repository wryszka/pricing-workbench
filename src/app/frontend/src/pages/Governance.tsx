import { useEffect, useMemo, useState, FormEvent } from 'react';
import {
  Shield, Layers, Calendar, Search, Loader2, MessageSquare,
  Bot, Send, ChevronDown, Sparkles, ExternalLink, Eye, EyeOff,
  AlertTriangle, UserCircle2, BookOpen,
} from 'lucide-react';
import { api } from '../lib/api';

type Pack = {
  pack_id: string;
  model_family: string;
  model_version: string;
  story?: string;
  simulated?: boolean | null;
  primary_metric?: string;
  primary_value?: number | null;
  pdf_path?: string;
  size_bytes?: number;
  generated_by?: string;
  generated_at?: string;
};
type FamilyPacks = { key: string; label: string; packs: Pack[] };
type Mode = 'by-model' | 'by-date' | 'by-policy';

export default function Governance() {
  const [mode, setMode]         = useState<Mode>('by-model');
  const [familyPacks, setFam]   = useState<FamilyPacks[]>([]);
  const [selectedPack, setPack] = useState<Pack | null>(null);
  const [policyContext, setPolicyContext] = useState<{ policy_id: string } | null>(null);

  useEffect(() => {
    api.listAllPacks().then(d => setFam(d.families || []));
  }, []);

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <div className="mb-4">
        <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
          <Shield className="w-6 h-6 text-amber-600" /> Model Governance
        </h2>
        <p className="text-gray-500 mt-1">
          Defend production models to regulators and auditors. Browse by model, by date, or trace the decision on a single policy.
        </p>
      </div>

      <div className="bg-amber-50 border border-amber-200 rounded-lg p-4 mb-6">
        <div className="flex flex-wrap gap-1.5">
          {[
            'Unity Catalog lineage',
            'MLflow model registry',
            'Governance pack generation',
            'Fairness & risk register',
            'Agent-assisted review · Claude Sonnet 4.6 (Foundation Model API)',
            'Immutable audit trail',
          ].map(f => (
            <span key={f} className="px-2 py-0.5 rounded text-[11px] font-medium bg-amber-100 text-amber-800">{f}</span>
          ))}
        </div>
      </div>

      <div className="bg-white rounded-lg border border-gray-200 p-1 mb-5 inline-flex gap-1">
        <SegButton active={mode === 'by-model'}  onClick={() => { setMode('by-model');  setPack(null); setPolicyContext(null); }}
                   icon={<Layers className="w-3.5 h-3.5" />} label="By model" />
        <SegButton active={mode === 'by-date'}   onClick={() => { setMode('by-date');   setPack(null); setPolicyContext(null); }}
                   icon={<Calendar className="w-3.5 h-3.5" />} label="By date" />
        <SegButton active={mode === 'by-policy'} onClick={() => { setMode('by-policy'); setPack(null); setPolicyContext(null); }}
                   icon={<Search className="w-3.5 h-3.5" />} label="By policy" />
      </div>

      <div className="grid gap-5">
        {!selectedPack && mode === 'by-model' &&
          <ByModel familyPacks={familyPacks} onPick={setPack} />}
        {!selectedPack && mode === 'by-date' &&
          <ByDate onPick={setPack} />}
        {!selectedPack && mode === 'by-policy' &&
          <ByPolicy onPick={(p, ctx) => { setPack(p); setPolicyContext(ctx); }} />}

        {selectedPack && (
          <PackViewer
            pack={selectedPack}
            policyContext={policyContext}
            onBack={() => { setPack(null); setPolicyContext(null); }}
          />
        )}
      </div>
    </div>
  );
}

function SegButton({ active, onClick, icon, label }:
  { active: boolean; onClick: () => void; icon: React.ReactNode; label: string }) {
  return (
    <button onClick={onClick}
            className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded text-sm font-medium transition ${
              active ? 'bg-blue-600 text-white' : 'text-gray-700 hover:bg-gray-100'
            }`}>
      {icon} {label}
    </button>
  );
}

// ---------------------------------------------------------------------------
// By model
// ---------------------------------------------------------------------------

function ByModel({ familyPacks, onPick }: { familyPacks: FamilyPacks[]; onPick: (p: Pack) => void }) {
  const [expanded, setExpanded] = useState<string | null>(familyPacks[0]?.key || null);
  useEffect(() => {
    if (!expanded && familyPacks.length > 0) setExpanded(familyPacks[0].key);
  }, [familyPacks.length]);

  return (
    <section className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="px-4 py-2.5 bg-gray-50 border-b flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-800">Production champions</h3>
        <span className="text-xs text-gray-500">{familyPacks.length} model families</span>
      </div>
      <div>
        {familyPacks.map(fam => {
          const isOpen = expanded === fam.key;
          const champion = fam.packs[0];
          return (
            <div key={fam.key} className="border-b last:border-b-0">
              <button onClick={() => setExpanded(isOpen ? null : fam.key)}
                      className="w-full flex items-center justify-between px-4 py-3 hover:bg-gray-50">
                <div className="flex items-center gap-3">
                  <ChevronDown className={`w-4 h-4 text-gray-500 transition-transform ${isOpen ? '' : '-rotate-90'}`} />
                  <div className="text-left">
                    <div className="font-medium text-gray-900">{fam.label}</div>
                    <div className="text-xs text-gray-500 font-mono">{fam.key}</div>
                  </div>
                </div>
                <div className="text-xs text-gray-600 text-right">
                  {champion ? (
                    <>
                      <div>Current champion: <span className="font-mono text-gray-900">v{champion.model_version}</span></div>
                      <div className="text-[11px] text-gray-500">Pack {formatDate(champion.generated_at)} · {fam.packs.length} total</div>
                    </>
                  ) : <span className="italic text-gray-400">no packs yet</span>}
                </div>
              </button>
              {isOpen && <PackTimeline packs={fam.packs} onPick={onPick} />}
            </div>
          );
        })}
      </div>
    </section>
  );
}

function PackTimeline({ packs, onPick }: { packs: Pack[]; onPick: (p: Pack) => void }) {
  if (packs.length === 0) {
    return <div className="px-4 pb-4 text-xs text-gray-500 italic">No packs generated for this family yet.</div>;
  }
  return (
    <div className="px-4 pb-4">
      <div className="relative pl-6">
        <div className="absolute left-2 top-1 bottom-1 w-px bg-gray-200" />
        {packs.map((p, i) => {
          const isChampion = i === 0;
          return (
            <button key={p.pack_id} onClick={() => onPick(p)}
                    className="relative block w-full text-left py-2 px-3 hover:bg-gray-50 rounded group">
              <div className={`absolute -left-5 top-3 w-3 h-3 rounded-full border-2 ${
                isChampion ? 'bg-emerald-500 border-emerald-600' : 'bg-white border-gray-400'
              }`} />
              <div className="flex items-center justify-between gap-4">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-sm text-gray-900">v{p.model_version}</span>
                    {isChampion && (
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-700 font-medium">current</span>
                    )}
                    {p.simulated && (
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-gray-100 text-gray-600">simulated</span>
                    )}
                    <span className="text-xs text-gray-600">{p.story}</span>
                  </div>
                  <div className="text-[11px] text-gray-500 mt-0.5">
                    Pack {formatDate(p.generated_at)} · generated by {(p.generated_by || '').split('@')[0]}
                    {p.primary_metric && p.primary_value != null && (
                      <span> · <span className="font-mono">{p.primary_metric}={Number(p.primary_value).toFixed(4)}</span></span>
                    )}
                  </div>
                </div>
                <span className="text-xs text-blue-600 group-hover:underline">Open pack →</span>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// By date
// ---------------------------------------------------------------------------

function ByDate({ onPick }: { onPick: (p: Pack) => void }) {
  const today = new Date().toISOString().substring(0, 10);
  const [date, setDate] = useState(today);
  const [packs, setPacks] = useState<Pack[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setLoading(true);
    api.getPacksOnDate(date).then(d => setPacks(d.packs || []))
      .catch(() => setPacks([])).finally(() => setLoading(false));
  }, [date]);

  return (
    <section className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="px-4 py-2.5 bg-gray-50 border-b flex items-center justify-between gap-3">
        <h3 className="text-sm font-semibold text-gray-800">Models in production on</h3>
        <input type="date" value={date} onChange={e => setDate(e.target.value)}
               className="border border-gray-300 rounded px-2 py-1 text-sm" />
      </div>
      {loading ? (
        <div className="py-8 text-center text-sm text-gray-500">
          <Loader2 className="w-4 h-4 inline animate-spin mr-1" /> Looking up packs…
        </div>
      ) : packs.length === 0 ? (
        <div className="py-8 text-center text-sm text-gray-500 italic">
          No packs had been generated on or before this date.
        </div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-gray-500 border-b bg-gray-50">
              <th className="text-left px-3 py-2 font-medium">Family</th>
              <th className="text-left px-3 py-2 font-medium">Version</th>
              <th className="text-left px-3 py-2 font-medium">Story</th>
              <th className="text-right px-3 py-2 font-medium">Primary metric</th>
              <th className="text-left px-3 py-2 font-medium">Pack generated</th>
              <th className="text-right px-3 py-2 font-medium">&nbsp;</th>
            </tr>
          </thead>
          <tbody>
            {packs.map(p => (
              <tr key={p.pack_id} className="border-b last:border-0 hover:bg-gray-50">
                <td className="px-3 py-2 font-medium text-gray-900">{p.model_family}</td>
                <td className="px-3 py-2 font-mono text-xs">v{p.model_version}</td>
                <td className="px-3 py-2 text-xs text-gray-600">{p.story || '—'}</td>
                <td className="px-3 py-2 text-right text-xs font-mono">
                  {p.primary_metric}={p.primary_value != null ? Number(p.primary_value).toFixed(4) : '—'}
                </td>
                <td className="px-3 py-2 text-xs text-gray-600">{formatDate(p.generated_at)}</td>
                <td className="px-3 py-2 text-right">
                  <button onClick={() => onPick(p)}
                          className="text-blue-600 hover:text-blue-800 text-xs font-medium">
                    Open pack →
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

// ---------------------------------------------------------------------------
// By policy — flagship flow
// ---------------------------------------------------------------------------

function ByPolicy({ onPick }: { onPick: (p: Pack, ctx: { policy_id: string }) => void }) {
  const [policyId, setPolicyId] = useState('');
  const [scoring, setScoring]   = useState<any>(null);
  const [loading, setLoading]   = useState(false);
  const [err, setErr]           = useState<string | null>(null);

  const run = async (id: string) => {
    if (!id.trim()) return;
    setLoading(true); setErr(null); setScoring(null);
    try {
      const d = await api.getPolicyScoring(id.trim().toUpperCase());
      setScoring(d);
    } catch (e: any) {
      setErr(e.message);
    } finally {
      setLoading(false);
    }
  };

  const submit = (e?: FormEvent) => { e?.preventDefault(); run(policyId); };

  const openPackFor = async (fam: any) => {
    if (!fam.pack_id) return;
    const pack = await api.getPackDetail(fam.pack_id);
    onPick(pack, { policy_id: scoring.policy_id });
  };

  return (
    <>
      <section className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-4 py-2.5 bg-gray-50 border-b">
          <h3 className="text-sm font-semibold text-gray-800 flex items-center gap-2">
            <UserCircle2 className="w-4 h-4 text-gray-500" /> Why was this customer priced £X?
          </h3>
        </div>
        <form onSubmit={submit} className="p-4 flex items-center gap-2">
          <input value={policyId}
                 onChange={e => setPolicyId(e.target.value.toUpperCase())}
                 placeholder="Policy ID (e.g. POL-100042)"
                 className="flex-1 border border-gray-300 rounded px-3 py-1.5 text-sm font-mono" />
          <button type="submit" disabled={loading || !policyId.trim()}
                  className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 text-white rounded text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
            {loading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Search className="w-3.5 h-3.5" />}
            Look up
          </button>
          <button type="button"
                  onClick={() => { setPolicyId('POL-100042'); run('POL-100042'); }}
                  className="text-xs text-gray-500 hover:text-gray-700 ml-2">
            try POL-100042
          </button>
        </form>
        {err && <div className="px-4 pb-4 text-xs text-red-700"><AlertTriangle className="w-3 h-3 inline mr-1" /> {err}</div>}
      </section>

      {scoring && <ScoringStory scoring={scoring} openPackFor={openPackFor} />}
    </>
  );
}

function ScoringStory({ scoring, openPackFor }: { scoring: any; openPackFor: (fam: any) => void }) {
  return (
    <section className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="px-4 py-2.5 bg-gray-50 border-b flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-800">
          Scoring story — {scoring.policy_id}
        </h3>
        <span className="text-[10px] px-1.5 py-0.5 rounded bg-amber-100 text-amber-800 font-medium">
          Simulated — no real inference log
        </span>
      </div>
      <div className="p-4 grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div>
          <h4 className="text-xs font-semibold text-gray-700 uppercase tracking-wide mb-2">Policy features at quote time</h4>
          <div className="bg-gray-50 border border-gray-200 rounded p-3 space-y-0.5 text-xs">
            {Object.entries(scoring.policy).map(([k, v]) => (
              <div key={k} className="flex justify-between gap-2">
                <span className="text-gray-600">{k}</span>
                <span className="font-mono text-gray-900 text-right truncate">{String(v ?? '—')}</span>
              </div>
            ))}
          </div>
        </div>

        <div>
          <h4 className="text-xs font-semibold text-gray-700 uppercase tracking-wide mb-2">Each model's prediction</h4>
          <div className="space-y-2">
            {scoring.models.map((m: any) => (
              <button key={m.family}
                      onClick={() => openPackFor(m)}
                      disabled={!m.pack_id}
                      className="w-full bg-white border border-gray-200 rounded p-2.5 hover:border-blue-300 hover:shadow-sm disabled:opacity-50 text-left group">
                <div className="flex items-center justify-between">
                  <div className="text-xs font-semibold text-gray-900">{m.label}</div>
                  <div className="text-xs font-mono text-gray-900">
                    {m.unit === 'GBP' ? `£${Number(m.prediction).toLocaleString()}` : Number(m.prediction).toFixed(3)}
                  </div>
                </div>
                <div className="text-[10px] text-gray-500 mt-0.5 flex items-center justify-between">
                  <span>v{m.model_version || '—'} · {m.unit}</span>
                  {m.pack_id && <span className="text-blue-600 group-hover:underline">open pack →</span>}
                </div>
              </button>
            ))}
          </div>
        </div>

        <div>
          <h4 className="text-xs font-semibold text-gray-700 uppercase tracking-wide mb-2">Price build-up</h4>
          <div className="bg-gray-50 border border-gray-200 rounded p-3 text-xs space-y-1">
            {scoring.price_build_up.map((step: any, i: number) => (
              <div key={i}
                   className={`flex justify-between ${step.emphasis ? 'border-t border-gray-300 pt-2 mt-1 font-semibold text-gray-900' : ''}`}>
                <span className="text-gray-700">{step.label}</span>
                <span className="font-mono">£{Number(step.amount).toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>
              </div>
            ))}
          </div>
          <div className="text-[11px] text-gray-500 italic mt-2">
            {scoring.note}
          </div>
        </div>
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Pack Viewer
// ---------------------------------------------------------------------------

function PackViewer({ pack, policyContext, onBack }:
  { pack: Pack; policyContext: { policy_id: string } | null; onBack: () => void }) {
  const [showPdf, setShowPdf]   = useState(true);
  const [showChat, setShowChat] = useState(true);
  const pdfUrl = api.packPdfUrl(pack.pack_id);

  return (
    <section className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="px-4 py-2.5 bg-gray-50 border-b flex items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-gray-800">
            Pack · {pack.model_family} v{pack.model_version}
          </h3>
          <div className="text-[11px] text-gray-500 font-mono">{pack.pack_id}</div>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={() => setShowPdf(!showPdf)}
                  className={`text-xs px-2 py-1 rounded inline-flex items-center gap-1 border ${
                    showPdf ? 'bg-blue-50 text-blue-700 border-blue-200'
                            : 'bg-gray-50 text-gray-600 border-gray-200'
                  }`}>
            {showPdf ? <Eye className="w-3 h-3" /> : <EyeOff className="w-3 h-3" />} PDF
          </button>
          <button onClick={() => setShowChat(!showChat)}
                  className={`text-xs px-2 py-1 rounded inline-flex items-center gap-1 border ${
                    showChat ? 'bg-blue-50 text-blue-700 border-blue-200'
                             : 'bg-gray-50 text-gray-600 border-gray-200'
                  }`}>
            <Bot className="w-3 h-3" /> Agent
          </button>
          <a href={pdfUrl} target="_blank" rel="noopener noreferrer"
             className="text-xs text-gray-600 hover:text-gray-900 inline-flex items-center gap-1">
            <ExternalLink className="w-3 h-3" /> Download
          </a>
          <button onClick={onBack}
                  className="text-xs text-gray-500 hover:text-gray-800">← Back</button>
        </div>
      </div>

      <div className={`grid gap-4 p-4 ${showPdf && showChat ? 'lg:grid-cols-2' : 'grid-cols-1'}`}>
        {showPdf  && <PdfPane pack={pack} pdfUrl={pdfUrl} />}
        {showChat && <ChatPane pack={pack} policyContext={policyContext} />}
      </div>
    </section>
  );
}

function PdfPane({ pack, pdfUrl }: { pack: Pack; pdfUrl: string }) {
  const sections = [
    { id: 1,  label: 'Executive summary' },
    { id: 2,  label: 'Business context & intended use' },
    { id: 3,  label: 'Data lineage & sources' },
    { id: 4,  label: 'Model specification' },
    { id: 5,  label: 'Performance evidence' },
    { id: 6,  label: 'Feature behaviour' },
    { id: 7,  label: 'Stability & version history' },
    { id: 8,  label: 'Fairness & ethical considerations' },
    { id: 9,  label: 'Risks & controls' },
    { id: 10, label: 'Regulatory coverage' },
    { id: 11, label: 'Audit trail' },
    { id: 12, label: 'Committee sign-off' },
  ];

  return (
    <div>
      <div className="flex items-center gap-2 text-xs text-gray-600 mb-2">
        <BookOpen className="w-3.5 h-3.5" />
        Section bookmarks
      </div>
      <div className="flex flex-wrap gap-1 mb-2">
        {sections.map(s => (
          <a key={s.id}
             href={`${pdfUrl}#page=${s.id + 1}`} target="_blank" rel="noopener noreferrer"
             className="text-[10px] px-1.5 py-0.5 rounded bg-gray-100 text-gray-700 hover:bg-blue-100 hover:text-blue-700"
             title={s.label}>
            {s.id}. {s.label}
          </a>
        ))}
      </div>
      <div className="border border-gray-200 rounded overflow-hidden" style={{ height: '70vh' }}>
        <iframe src={pdfUrl} title={`Governance pack ${pack.pack_id}`}
                className="w-full h-full" />
      </div>
      <div className="text-[11px] text-gray-500 mt-2">
        Generated {formatDate(pack.generated_at)} by {(pack.generated_by || '').split('@')[0]} · {formatBytes(pack.size_bytes)}
      </div>
    </div>
  );
}

type ToolStep = { hop: number; tool: string; arguments: any; result_summary: string };

type ChatTurn = {
  question: string;
  answer?: string;
  loading?: boolean;
  model?: string;
  endpoint?: string;
  source?: string;
  cited_sections?: string[];
  tool_trace?: ToolStep[];
  usage?: { prompt_tokens?: number; completion_tokens?: number; total_tokens?: number };
  error?: string;
  fallback_reason?: string;
};

const SUGGESTED_QUESTIONS_GENERAL = [
  "Why was this model promoted over the previous version?",
  "Is this model fair across protected characteristics?",
  "What are the top 5 drivers of predictions?",
  "Has this model drifted since the last version?",
  "Draft a regulator response: customer complains the pricing is unfair",
];
const SUGGESTED_QUESTIONS_POLICY = [
  "Draft a formal response to the customer explaining this price",
  "Are there any fairness concerns specific to this policy?",
  "Which single factor contributed most to this price?",
];

function ChatPane({ pack, policyContext }:
  { pack: Pack; policyContext: { policy_id: string } | null }) {
  const [input, setInput]   = useState('');
  const [turns, setTurns]   = useState<ChatTurn[]>([]);
  const [busy, setBusy]     = useState(false);
  const [showTrace, setShowTrace] = useState(false);

  const suggestions = useMemo(() => [
    ...(policyContext ? SUGGESTED_QUESTIONS_POLICY : []),
    ...SUGGESTED_QUESTIONS_GENERAL,
  ], [policyContext]);

  const ask = async (q: string) => {
    const question = q.trim();
    if (!question || busy) return;
    setBusy(true);
    setInput('');
    setTurns(t => [...t, { question, loading: true }]);
    try {
      const r = await api.chatWithPack(pack.pack_id, question, policyContext?.policy_id);
      setTurns(t => t.map((x, i) =>
        i === t.length - 1
          ? { ...x, loading: false, answer: r.answer, model: r.model, endpoint: r.endpoint,
              source: r.source, fallback_reason: r.fallback_reason,
              cited_sections: r.cited_sections, tool_trace: r.tool_trace,
              usage: r.usage, error: r.error }
          : x));
    } catch (e: any) {
      setTurns(t => t.map((x, i) =>
        i === t.length - 1 ? { ...x, loading: false, error: e.message } : x));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <div className="flex items-center gap-2 text-xs text-gray-600 mb-2 flex-wrap">
        <Sparkles className="w-3.5 h-3.5 text-violet-500" />
        <span>Ask a question — grounded in this pack's content</span>
        <span className="text-[10px] px-1.5 py-0.5 rounded bg-violet-100 text-violet-700 font-medium">
          Agent Framework · pricing_governance_agent
        </span>
      </div>

      <div className="border border-gray-200 rounded flex flex-col" style={{ height: '70vh' }}>
        <div className="flex-1 overflow-y-auto p-3 space-y-3 bg-gray-50">
          {turns.length === 0 && (
            <div className="py-4">
              <div className="text-xs text-gray-500 mb-3 text-center">
                Pre-populated questions{policyContext && <> · <code className="bg-white px-1 rounded">{policyContext.policy_id}</code></>}
              </div>
              <div className="flex flex-col gap-1.5">
                {suggestions.map(s => (
                  <button key={s} onClick={() => ask(s)}
                          className="text-xs text-left px-3 py-1.5 rounded bg-white border border-gray-200 hover:border-blue-300 hover:bg-blue-50 text-gray-800">
                    {s}
                  </button>
                ))}
              </div>
            </div>
          )}

          {turns.map((t, i) => (
            <div key={i} className="space-y-1">
              <div className="flex gap-2">
                <UserCircle2 className="w-4 h-4 text-gray-500 shrink-0 mt-0.5" />
                <div className="text-sm text-gray-800 flex-1">{t.question}</div>
              </div>
              <div className="flex gap-2 pl-1">
                <Bot className="w-4 h-4 text-violet-600 shrink-0 mt-0.5" />
                <div className="flex-1">
                  {t.loading ? (
                    <div className="text-xs text-gray-500 italic inline-flex items-center gap-1">
                      <Loader2 className="w-3 h-3 animate-spin" /> Grounding in pack…
                    </div>
                  ) : t.error ? (
                    <div className="text-xs text-red-700">
                      <AlertTriangle className="w-3 h-3 inline mr-1" /> {t.error}
                    </div>
                  ) : (
                    <>
                      {t.tool_trace && t.tool_trace.length > 0 && (
                        <div className="mb-2 space-y-0.5">
                          {t.tool_trace.map((s, idx) => (
                            <div key={idx}
                                 className="text-[11px] text-violet-900 bg-violet-50 border border-violet-200 rounded px-2 py-1 inline-flex items-center gap-1.5 mr-1">
                              <Bot className="w-3 h-3" />
                              <span className="font-mono">{s.tool}</span>
                              <span className="text-violet-700 font-mono">({summariseArgs(s.arguments)})</span>
                              <span className="text-violet-600">→ {s.result_summary}</span>
                            </div>
                          ))}
                        </div>
                      )}
                      <div className="text-sm text-gray-900 whitespace-pre-wrap">{t.answer}</div>
                      <div className="mt-1 flex items-center gap-3 flex-wrap text-[10px] text-gray-500">
                        {t.endpoint && <span>endpoint: {t.endpoint}</span>}
                        {t.source === 'fm_api_fallback' && (
                          <span className="text-amber-700" title={t.fallback_reason || ''}>
                            fallback: FM API (agent unavailable)
                          </span>
                        )}
                        {t.cited_sections && t.cited_sections.length > 0 && (
                          <span>cited: {t.cited_sections.map(s => `§${s}`).join(', ')}</span>
                        )}
                        {t.usage?.total_tokens != null && <span>tokens: {t.usage.total_tokens}</span>}
                      </div>
                    </>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>

        <form onSubmit={(e) => { e.preventDefault(); ask(input); }}
              className="border-t bg-white p-2 flex gap-2">
          <input value={input} onChange={e => setInput(e.target.value)}
                 placeholder="Ask about this pack…"
                 disabled={busy}
                 className="flex-1 border border-gray-300 rounded px-3 py-1.5 text-sm" />
          <button type="submit" disabled={busy || !input.trim()}
                  className="inline-flex items-center gap-1 px-3 py-1.5 bg-blue-600 text-white rounded text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
            {busy ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Send className="w-3.5 h-3.5" />} Ask
          </button>
        </form>
      </div>

      <button onClick={() => setShowTrace(!showTrace)}
              className="text-[11px] text-gray-500 hover:text-gray-700 mt-2 inline-flex items-center gap-1">
        <MessageSquare className="w-3 h-3" /> {showTrace ? 'Hide' : 'Show'} full LLM interaction
      </button>
      {showTrace && turns.length > 0 && (
        <div className="bg-gray-900 text-gray-100 text-[11px] font-mono rounded p-3 mt-2 whitespace-pre-wrap max-h-96 overflow-auto">
          {turns.map((t, i) => (
            <div key={i} className="mb-3">
              <div className="text-violet-300">user: {t.question}</div>
              {(t.tool_trace || []).map((s, idx) => (
                <div key={idx} className="text-amber-300 mt-1">
                  tool[{s.hop}]: {s.tool}({summariseArgs(s.arguments)}) → {s.result_summary}
                </div>
              ))}
              <div className="text-emerald-300 mt-1">assistant: {t.answer || t.error || '(loading…)'}</div>
              {t.usage && <div className="text-gray-400 mt-1 text-[10px]">tokens: {JSON.stringify(t.usage)}</div>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatDate(iso?: string | null) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso).substring(0, 10);
  return d.toISOString().substring(0, 10);
}

function formatBytes(n?: number) {
  if (!n) return '—';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

function summariseArgs(args: any): string {
  if (!args || typeof args !== 'object') return '';
  const parts: string[] = [];
  for (const [k, v] of Object.entries(args)) {
    if (v == null || v === '') continue;
    let s = typeof v === 'string' ? v : JSON.stringify(v);
    if (s.length > 40) s = s.slice(0, 37) + '…';
    parts.push(`${k}=${s}`);
  }
  return parts.join(' ');
}
