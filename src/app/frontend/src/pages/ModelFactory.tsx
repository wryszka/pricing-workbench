import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { FlaskConical, ChevronRight, CheckCircle2, Clock, AlertTriangle, Bot, Loader2, ChevronDown, ChevronUp, Shield, Sparkles, Send, Check, FileText } from 'lucide-react';
import { api } from '../lib/api';

export default function ModelFactory() {
  const [runs, setRuns] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  // AI Agent state (optional)
  const [agentEnabled, setAgentEnabled] = useState(false);
  const [agentStatus, setAgentStatus] = useState<any>(null);
  const [agentResult, setAgentResult] = useState<any>(null);
  const [agentLoading, setAgentLoading] = useState(false);
  const [showTransparency, setShowTransparency] = useState(false);

  useEffect(() => {
    api.getFactoryRuns().then(setRuns).finally(() => setLoading(false));
    api.getAgentStatus().then(setAgentStatus).catch(() => {});
  }, []);

  const runAgent = async () => {
    setAgentLoading(true);
    try {
      const result = await api.runAgentAnalysis();
      setAgentResult(result);
    } catch (err: any) {
      setAgentResult({ success: false, error: err.message });
    } finally {
      setAgentLoading(false);
    }
  };

  if (loading) return <div className="p-8 text-center text-gray-500">Loading factory runs...</div>;

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Model Factory</h2>
          <p className="text-gray-500 mt-1">Review model factory runs and approve models for production</p>
        </div>
      </div>

      {/* Context panels */}
      <div className="grid grid-cols-2 gap-4 mb-6">
        <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
          <h4 className="text-xs font-semibold text-purple-800 uppercase tracking-wide mb-1">Databricks features demonstrated</h4>
          <div className="flex flex-wrap gap-1.5">
            {["MLflow experiment tracking", "UC model registry", "Automated model evaluation", "Regulatory suitability scoring", "PDF model reports", "Foundation Model API (optional)"].map(f => (
              <span key={f} className="px-2 py-0.5 rounded text-[10px] font-medium bg-purple-100 text-purple-700">{f}</span>
            ))}
          </div>
        </div>
        <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
          <h4 className="text-xs font-semibold text-amber-800 uppercase tracking-wide mb-1">Why it matters for actuaries</h4>
          <p className="text-xs text-amber-700">
            Replaces the manual model comparison process. The factory trains 20+ model configurations,
            ranks them on insurance-specific metrics (Gini, PSI, regulatory suitability), and presents
            a leaderboard for actuarial sign-off — with a one-click PDF report for regulators.
          </p>
        </div>
      </div>

      {/* ── Agentic Planner (plan a new factory run) ── */}
      <AgenticPlanner onSubmitted={() => api.getFactoryRuns().then(setRuns)} />

      {/* ── AI Assistant Toggle (OPTIONAL) ── */}
      <div className="mb-6 bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Bot className={`w-5 h-5 ${agentEnabled ? 'text-purple-600' : 'text-gray-400'}`} />
            <div>
              <span className="text-sm font-medium text-gray-800">AI Model Selection Assistant</span>
              <span className="ml-2 px-1.5 py-0.5 rounded text-[10px] font-medium bg-purple-50 text-purple-600 border border-purple-200">
                OPTIONAL
              </span>
            </div>
          </div>
          <label className="relative inline-flex items-center cursor-pointer">
            <input type="checkbox" checked={agentEnabled} onChange={(e) => setAgentEnabled(e.target.checked)}
              className="sr-only peer" />
            <div className="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-purple-600"></div>
          </label>
        </div>
        {agentEnabled && (
          <div className="mt-3 text-xs text-gray-500 border-t pt-3">
            Uses Foundation Model API ({agentStatus?.endpoint || 'loading...'}) to analyse the Unified Pricing
            Table and recommend which models to train. The AI recommends — you decide.
            {agentStatus?.available === false && (
              <span className="block mt-1 text-amber-600">Agent unavailable: {agentStatus?.message}</span>
            )}
          </div>
        )}
      </div>

      {/* ── AI Analysis Panel ── */}
      {agentEnabled && (
        <div className="mb-6 space-y-4">
          {!agentResult && !agentLoading && (
            <div className="bg-purple-50 border border-purple-200 rounded-lg p-6 text-center">
              <Bot className="w-10 h-10 text-purple-400 mx-auto mb-3" />
              <p className="text-purple-700 font-medium mb-3">
                Run the AI assistant to analyse your feature table and recommend models
              </p>
              <button onClick={runAgent} disabled={!agentStatus?.available}
                className="px-5 py-2 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50 transition-colors inline-flex items-center gap-2">
                <Bot className="w-4 h-4" /> Run Analysis
              </button>
            </div>
          )}

          {agentLoading && <AgentProgress />}

          {agentResult && agentResult.success && (
            <>
              {/* Disclaimer */}
              <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-2 flex items-center gap-2">
                <Shield className="w-4 h-4 text-amber-600 shrink-0" />
                <span className="text-xs text-amber-700">
                  AI-assisted recommendation — review before approving. All LLM interactions are logged and auditable.
                </span>
              </div>

              {/* Overall strategy */}
              <div className="bg-white rounded-lg border border-gray-200 p-5">
                <h3 className="font-semibold text-gray-900 mb-2 flex items-center gap-2">
                  <Bot className="w-4 h-4 text-purple-600" /> Recommended Strategy
                </h3>
                <p className="text-sm text-gray-700">{agentResult.recommendations?.overall_strategy}</p>

                {/* Data quality observations */}
                {agentResult.recommendations?.data_quality_observations?.length > 0 && (
                  <div className="mt-3 border-t pt-3">
                    <h4 className="text-xs font-semibold text-gray-500 uppercase mb-1">Data Quality Observations</h4>
                    <ul className="text-sm text-gray-600 space-y-1">
                      {agentResult.recommendations.data_quality_observations.map((obs: string, i: number) => (
                        <li key={i} className="flex items-start gap-1.5">
                          <span className="text-amber-500 mt-1">-</span> {obs}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>

              {/* Individual recommendations */}
              <div className="grid gap-3">
                {agentResult.recommendations?.recommendations?.map((rec: any, i: number) => (
                  <div key={i} className="bg-white rounded-lg border border-gray-200 p-4">
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                          rec.priority === 'high' ? 'bg-red-50 text-red-700 border border-red-200' :
                          rec.priority === 'medium' ? 'bg-amber-50 text-amber-700 border border-amber-200' :
                          'bg-blue-50 text-blue-700 border border-blue-200'
                        }`}>{rec.priority}</span>
                        <h4 className="font-semibold text-gray-900">{rec.model_name}</h4>
                      </div>
                      <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                        rec.model_type.startsWith('GLM') ? 'bg-blue-100 text-blue-700' : 'bg-green-100 text-green-700'
                      }`}>{rec.model_type}</span>
                    </div>
                    <p className="text-sm text-gray-600 mb-2">{rec.purpose}</p>
                    <div className="grid grid-cols-2 gap-3 text-xs">
                      <div>
                        <span className="text-gray-500 font-medium">Target:</span>{' '}
                        <code className="bg-gray-100 px-1 py-0.5 rounded">{rec.target_variable}</code>
                      </div>
                      <div>
                        <span className="text-gray-500 font-medium">Features:</span>{' '}
                        {rec.recommended_features?.length || 0} selected
                      </div>
                    </div>
                    <p className="text-xs text-gray-500 mt-2">{rec.feature_rationale}</p>
                    {rec.regulatory_notes && (
                      <p className="text-xs text-amber-600 mt-1 flex items-center gap-1">
                        <Shield className="w-3 h-3" /> {rec.regulatory_notes}
                      </p>
                    )}
                  </div>
                ))}
              </div>

              {/* Transparency panel */}
              <div className="bg-gray-50 rounded-lg border border-gray-200">
                <button onClick={() => setShowTransparency(!showTransparency)}
                  className="w-full px-4 py-3 text-left flex items-center justify-between text-sm font-medium text-gray-700 hover:bg-gray-100 transition-colors">
                  <span className="flex items-center gap-2">
                    <Shield className="w-4 h-4 text-gray-500" />
                    Transparency — Full LLM Interaction
                    {agentResult.token_usage?.total_tokens && (
                      <span className="text-xs text-gray-400">({agentResult.token_usage.total_tokens} tokens)</span>
                    )}
                  </span>
                  {showTransparency ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
                </button>
                {showTransparency && (
                  <div className="px-4 pb-4 space-y-3 border-t">
                    <div className="mt-3">
                      <h5 className="text-xs font-semibold text-gray-500 uppercase mb-1">Model Endpoint</h5>
                      <code className="text-xs bg-white border rounded px-2 py-1">{agentResult.endpoint}</code>
                    </div>
                    <div>
                      <h5 className="text-xs font-semibold text-gray-500 uppercase mb-1">System Prompt</h5>
                      <pre className="text-xs bg-white border rounded p-2 overflow-x-auto max-h-32 whitespace-pre-wrap">
                        {agentResult.transparency?.system_prompt}
                      </pre>
                    </div>
                    <div>
                      <h5 className="text-xs font-semibold text-gray-500 uppercase mb-1">User Prompt (Data Profile)</h5>
                      <pre className="text-xs bg-white border rounded p-2 overflow-x-auto max-h-40 whitespace-pre-wrap">
                        {agentResult.transparency?.user_prompt}
                      </pre>
                    </div>
                    <div>
                      <h5 className="text-xs font-semibold text-gray-500 uppercase mb-1">Raw LLM Response</h5>
                      <pre className="text-xs bg-white border rounded p-2 overflow-x-auto max-h-60 whitespace-pre-wrap">
                        {agentResult.transparency?.raw_response}
                      </pre>
                    </div>
                    <div className="bg-blue-50 border border-blue-200 rounded p-3 text-xs text-blue-700">
                      <strong>Governance note:</strong> This interaction is logged in the audit trail (event_type: agent_recommendation).
                      A regulatory auditor can reconstruct exactly what the AI recommended, what data it saw,
                      and which human approved or rejected the recommendation.
                      Unlike black-box SaaS tools, every LLM call in Databricks is logged via AI Gateway
                      with full prompt/response capture, token usage, and user identity.
                    </div>
                  </div>
                )}
              </div>

              {/* Re-run button */}
              <div className="text-center">
                <button onClick={runAgent} disabled={agentLoading}
                  className="text-sm text-purple-600 hover:text-purple-800 font-medium">
                  Re-run analysis
                </button>
              </div>
            </>
          )}

          {agentResult && !agentResult.success && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-sm">
              <p className="text-red-700 font-medium">AI analysis could not parse recommendations.</p>
              <p className="text-red-600 mt-1">{agentResult.error || 'The LLM responded but the output was not valid JSON.'}</p>
              {agentResult.raw_response_preview && (
                <pre className="mt-2 text-xs bg-white border rounded p-2 max-h-32 overflow-auto whitespace-pre-wrap text-gray-600">
                  {agentResult.raw_response_preview}
                </pre>
              )}
              <p className="text-gray-500 mt-2 text-xs">The demo works normally without AI assistance. Try running again.</p>
            </div>
          )}
        </div>
      )}

      {/* ── Factory Runs List ── */}
      {runs.length === 0 ? (
        <div className="bg-white rounded-lg border border-gray-200 p-12 text-center">
          <FlaskConical className="w-12 h-12 text-gray-300 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-700 mb-2">No factory runs yet</h3>
          <p className="text-gray-500 max-w-md mx-auto">
            Run the Model Factory pipeline from the Databricks workflow to generate model candidates.
          </p>
        </div>
      ) : (
        <div className="grid gap-4">
          {runs.map((run) => {
            const allDecided = Number(run.models_decided || 0) >= Number(run.models_succeeded || 0) && Number(run.models_succeeded || 0) > 0;
            const hasApprovals = Number(run.models_approved || 0) > 0;

            return (
              <Link
                key={run.factory_run_id}
                to={`/models/${encodeURIComponent(run.factory_run_id)}`}
                className="bg-white rounded-lg border border-gray-200 p-5 hover:border-blue-300 hover:shadow-md transition-all group"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <div className="w-10 h-10 bg-purple-50 rounded-lg flex items-center justify-center">
                      <FlaskConical className="w-5 h-5 text-purple-600" />
                    </div>
                    <div>
                      <h3 className="font-semibold text-gray-900 group-hover:text-blue-600 transition-colors font-mono">
                        {run.factory_run_id}
                      </h3>
                      <p className="text-sm text-gray-500">
                        Started {run.started_at ? new Date(run.started_at).toLocaleString() : 'Unknown'}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-6">
                    <div className="text-right">
                      <div className="text-sm text-gray-500">Planned</div>
                      <div className="text-sm font-medium">{run.models_planned}</div>
                    </div>
                    <div className="text-right">
                      <div className="text-sm text-gray-500">Trained</div>
                      <div className="text-sm font-medium text-green-600">
                        {run.models_succeeded}
                        {Number(run.models_failed) > 0 && (
                          <span className="text-red-500 ml-1">({run.models_failed} failed)</span>
                        )}
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-sm text-gray-500">Approved</div>
                      <div className="text-sm font-medium">{run.models_approved || 0}</div>
                    </div>
                    {allDecided ? (
                      hasApprovals ? (
                        <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-green-50 text-green-700 border border-green-200">
                          <CheckCircle2 className="w-3.5 h-3.5" /> Complete
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-red-50 text-red-700 border border-red-200">
                          <AlertTriangle className="w-3.5 h-3.5" /> All Rejected
                        </span>
                      )
                    ) : (
                      <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-amber-50 text-amber-700 border border-amber-200">
                        <Clock className="w-3.5 h-3.5" /> Review Needed
                      </span>
                    )}
                    <a href={api.downloadRunLogReport(run.factory_run_id)} target="_blank" rel="noopener noreferrer"
                      onClick={e => e.stopPropagation()}
                      title="Export full run log (PDF)"
                      className="flex items-center gap-1 px-2 py-1 text-xs text-gray-600 border border-gray-300 rounded hover:bg-gray-100">
                      <FileText className="w-3.5 h-3.5" /> Run log
                    </a>
                    <ChevronRight className="w-5 h-5 text-gray-400 group-hover:text-blue-500" />
                  </div>
                </div>
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}


function AgentProgress() {
  const [step, setStep] = useState(0);
  const steps = [
    { label: 'Connecting to Foundation Model API', detail: 'Authenticating with Claude via Databricks FMAPI' },
    { label: 'Profiling Unified Pricing Table', detail: 'Reading column statistics, types, and distributions' },
    { label: 'Building analysis prompt', detail: 'Composing the feature profile for the LLM' },
    { label: 'Waiting for AI response', detail: 'Claude is analysing 90+ features across 50K policies...' },
    { label: 'Parsing recommendations', detail: 'Extracting model configurations from the response' },
    { label: 'Logging to audit trail', detail: 'Recording the full LLM interaction for governance' },
  ];

  useEffect(() => {
    const timers = [
      setTimeout(() => setStep(1), 1500),
      setTimeout(() => setStep(2), 3000),
      setTimeout(() => setStep(3), 5000),
      setTimeout(() => setStep(4), 8000),
      setTimeout(() => setStep(5), 20000),
    ];
    return () => timers.forEach(clearTimeout);
  }, []);

  return (
    <div className="bg-white border border-purple-200 rounded-lg p-6">
      <div className="flex items-center gap-3 mb-4">
        <Loader2 className="w-5 h-5 text-purple-600 animate-spin" />
        <h3 className="font-semibold text-gray-900">AI Analysis in Progress</h3>
      </div>
      <div className="space-y-2">
        {steps.map((s, i) => (
          <div key={i} className={`flex items-center gap-3 py-1.5 transition-all duration-500 ${
            i < step ? 'opacity-100' : i === step ? 'opacity-100' : 'opacity-30'
          }`}>
            <div className={`w-5 h-5 rounded-full flex items-center justify-center shrink-0 transition-colors duration-500 ${
              i < step ? 'bg-green-500' : i === step ? 'bg-purple-500 animate-pulse' : 'bg-gray-200'
            }`}>
              {i < step ? (
                <CheckCircle2 className="w-3.5 h-3.5 text-white" />
              ) : i === step ? (
                <Loader2 className="w-3 h-3 text-white animate-spin" />
              ) : (
                <span className="w-1.5 h-1.5 bg-gray-400 rounded-full" />
              )}
            </div>
            <div>
              <span className={`text-sm font-medium ${i <= step ? 'text-gray-800' : 'text-gray-400'}`}>{s.label}</span>
              {i === step && (
                <p className="text-xs text-purple-600 mt-0.5">{s.detail}</p>
              )}
            </div>
          </div>
        ))}
      </div>
      <p className="text-xs text-gray-400 mt-4">This typically takes 15-30 seconds. The full prompt and response will be visible in the Transparency panel.</p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Agentic Planner — plan a new factory run via dropdowns + Claude
// ---------------------------------------------------------------------------

type FeatureAnalysis = {
  headline?: string;
  strengths?: string[];
  gaps?: string[];
  sensitive?: string[];
  recommended_next?: { target: string; why: string }[];
} | null;

function AgenticPlanner({ onSubmitted }: { onSubmitted: () => void }) {
  const [analysis, setAnalysis] = useState<FeatureAnalysis>(null);
  const [analysisRaw, setAnalysisRaw] = useState<string | null>(null);
  const [analysisLoading, setAnalysisLoading] = useState(true);

  // Intent dropdowns
  const [target, setTarget] = useState('claim_count_5y');
  const [modelFamily, setModelFamily] = useState('GLM_Poisson');
  const [featureScope, setFeatureScope] = useState('plus_real_uk');
  const [sweepSize, setSweepSize] = useState(10);
  const [focus, setFocus] = useState('exploration');
  const [note, setNote] = useState('');

  // Proposal state
  const [plan, setPlan] = useState<any>(null);
  const [proposing, setProposing] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [submitResult, setSubmitResult] = useState<any>(null);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  useEffect(() => {
    api.analyseFeatures()
      .then(r => { setAnalysis(r?.analysis || null); setAnalysisRaw(r?.raw || null); })
      .catch(() => {})
      .finally(() => setAnalysisLoading(false));
  }, []);

  const propose = async () => {
    setProposing(true); setErrMsg(null); setPlan(null); setSubmitResult(null);
    try {
      const r = await api.proposePlan({
        target, model_family: modelFamily, feature_scope: featureScope,
        sweep_size: sweepSize, focus, note: note || undefined,
      });
      if (!r.success || !r.plan) {
        setErrMsg(r.error || 'Agent did not return a valid plan. Raw response saved for debug.');
      } else {
        setPlan(r.plan);
      }
    } catch (e: any) {
      setErrMsg(String(e?.message || e));
    } finally {
      setProposing(false);
    }
  };

  const submit = async () => {
    if (!plan) return;
    setSubmitting(true); setSubmitResult(null); setErrMsg(null);
    try {
      const r = await api.submitPlan({
        intent: { target, model_family: modelFamily, feature_scope: featureScope, sweep_size: sweepSize, focus, note },
        plan_summary: plan.plan_summary,
        configs: plan.configs || [],
        feature_analysis_text: analysis ? JSON.stringify(analysis) : (analysisRaw || undefined),
      });
      setSubmitResult(r);
      onSubmitted();
    } catch (e: any) {
      setErrMsg(String(e?.message || e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="mb-6 bg-white border border-emerald-200 rounded-lg overflow-hidden">
      <div className="px-5 py-3 bg-emerald-50 border-b border-emerald-200 flex items-center gap-2">
        <Sparkles className="w-4 h-4 text-emerald-700" />
        <h3 className="font-semibold text-emerald-800 text-sm">Plan a factory run</h3>
        <span className="text-xs text-emerald-600">Claude reads your feature catalog; you pick the shape of the sweep.</span>
      </div>

      {/* Feature analysis (Claude-backed, auto-runs) */}
      <div className="px-5 py-4 border-b bg-gradient-to-b from-emerald-50/40 to-transparent">
        <div className="text-[11px] font-medium text-emerald-700 uppercase tracking-wide mb-1.5">
          Feature-store analysis
        </div>
        {analysisLoading && (
          <div className="flex items-center gap-2 text-xs text-gray-500"><Loader2 className="w-3 h-3 animate-spin" /> Claude is analysing your features…</div>
        )}
        {!analysisLoading && analysis && (
          <div className="space-y-1.5 text-sm text-gray-800">
            {analysis.headline && <div className="font-medium text-gray-900">{analysis.headline}</div>}
            <div className="grid grid-cols-2 gap-4 text-xs">
              {!!(analysis.strengths || []).length && (
                <div>
                  <div className="text-[10px] font-medium text-green-700 uppercase tracking-wide mb-0.5">Strengths</div>
                  <ul className="list-disc list-inside text-gray-700 space-y-0.5">
                    {analysis.strengths!.map((s, i) => <li key={i}>{s}</li>)}
                  </ul>
                </div>
              )}
              {!!(analysis.gaps || []).length && (
                <div>
                  <div className="text-[10px] font-medium text-amber-700 uppercase tracking-wide mb-0.5">Gaps</div>
                  <ul className="list-disc list-inside text-gray-700 space-y-0.5">
                    {analysis.gaps!.map((s, i) => <li key={i}>{s}</li>)}
                  </ul>
                </div>
              )}
            </div>
            {!!(analysis.sensitive || []).length && (
              <div className="text-[11px] text-red-700 bg-red-50 border border-red-200 rounded px-2 py-1 mt-1 inline-flex items-center gap-1">
                <Shield className="w-3 h-3" /> {analysis.sensitive!.join(' · ')}
              </div>
            )}
            {!!(analysis.recommended_next || []).length && (
              <div className="text-xs text-gray-600 mt-1">
                <span className="font-medium">Suggested next targets:</span>{' '}
                {analysis.recommended_next!.map((r, i) => (
                  <span key={r.target}>
                    <code className="bg-gray-100 px-1 rounded">{r.target}</code>
                    <span className="text-gray-500"> — {r.why}</span>
                    {i < analysis.recommended_next!.length - 1 ? '; ' : ''}
                  </span>
                ))}
              </div>
            )}
          </div>
        )}
        {!analysisLoading && !analysis && (
          <div className="text-xs text-gray-500">Feature analysis not available. (Populate <code className="bg-gray-100 px-1 rounded">feature_catalog</code> via the build_upt job.)</div>
        )}
      </div>

      {/* Intent dropdowns */}
      <div className="px-5 py-4 grid grid-cols-5 gap-3">
        <Dropdown label="Target" value={target} onChange={setTarget} options={[
          ['claim_count_5y',   'Frequency — claim_count_5y'],
          ['total_incurred_5y','Severity — total_incurred_5y'],
          ['loss_ratio_5y',    'Loss ratio — loss_ratio_5y'],
        ]} />
        <Dropdown label="Model family" value={modelFamily} onChange={setModelFamily} options={[
          ['GLM_Poisson',     'GLM · Poisson'],
          ['GLM_Gamma',       'GLM · Gamma'],
          ['GBM_Classifier',  'GBM · Classifier'],
          ['GBM_Regressor',   'GBM · Regressor'],
        ]} />
        <Dropdown label="Feature scope" value={featureScope} onChange={setFeatureScope} options={[
          ['all',                 'All features'],
          ['baseline_only',       'Baseline only'],
          ['plus_real_uk',        'Baseline + real UK enrichment'],
          ['exclude_regulatory',  'Exclude regulatory-sensitive'],
        ]} />
        <Dropdown label="Sweep size" value={String(sweepSize)} onChange={v => setSweepSize(parseInt(v, 10))} options={[
          ['5', '5 configs'], ['10', '10 configs'], ['20', '20 configs'], ['50', '50 configs'],
        ]} />
        <Dropdown label="Focus" value={focus} onChange={setFocus} options={[
          ['exploration',       'Fresh exploration'],
          ['interaction_terms', 'Interaction terms'],
          ['hyperparam_sweep',  'Hyperparameter sweep'],
          ['feature_ablation',  'Feature ablation'],
        ]} />
      </div>

      <div className="px-5 pb-4">
        <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wide mb-1">
          Optional note for the agent
        </label>
        <textarea value={note} onChange={e => setNote(e.target.value)}
          rows={2} placeholder="e.g. focus on interaction between urban_score and claim_count_5y; avoid regulatory-sensitive features on this run"
          className="w-full px-3 py-2 border border-gray-300 rounded text-sm focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500 outline-none" />

        <div className="mt-3 flex items-center justify-between">
          <button onClick={propose} disabled={proposing}
            className="flex items-center gap-2 px-4 py-2 bg-emerald-600 text-white rounded text-sm hover:bg-emerald-700 disabled:opacity-50">
            {proposing ? <Loader2 className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
            {proposing ? 'Claude is proposing…' : 'Propose plan'}
          </button>
          {errMsg && <span className="text-xs text-red-600">{errMsg}</span>}
        </div>
      </div>

      {/* Proposal preview */}
      {plan && (
        <div className="border-t border-gray-200 px-5 py-4 bg-gray-50/50">
          <div className="flex items-center justify-between mb-2">
            <div>
              <div className="text-[11px] font-medium text-emerald-700 uppercase tracking-wide">Proposed plan</div>
              <div className="text-sm text-gray-800 mt-0.5">{plan.plan_summary}</div>
              <div className="text-[11px] text-gray-500 mt-0.5">{(plan.configs || []).length} configs proposed</div>
            </div>
            <button onClick={submit} disabled={submitting || submitResult}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50">
              {submitting ? <Loader2 className="w-4 h-4 animate-spin" /> : submitResult ? <Check className="w-4 h-4" /> : <Send className="w-4 h-4" />}
              {submitting ? 'Submitting…' : submitResult ? 'Submitted' : 'Submit to factory'}
            </button>
          </div>

          <div className="max-h-64 overflow-y-auto border border-gray-200 rounded bg-white text-xs">
            <table className="w-full">
              <thead className="bg-gray-50 text-gray-600 sticky top-0">
                <tr>
                  <th className="px-2 py-1 text-left font-medium">Config</th>
                  <th className="px-2 py-1 text-left font-medium">Features</th>
                  <th className="px-2 py-1 text-left font-medium">Hyperparams</th>
                  <th className="px-2 py-1 text-left font-medium">Rationale</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {(plan.configs || []).map((cfg: any) => (
                  <tr key={cfg.config_id} className="hover:bg-emerald-50">
                    <td className="px-2 py-1 font-mono font-medium">{cfg.config_id}</td>
                    <td className="px-2 py-1 text-gray-700">
                      <div className="truncate max-w-xs">{(cfg.features || []).join(', ')}</div>
                    </td>
                    <td className="px-2 py-1 font-mono text-gray-600">
                      {Object.entries(cfg.hyperparams || {}).map(([k, v]) => `${k}=${v}`).join(', ') || '—'}
                    </td>
                    <td className="px-2 py-1 text-gray-700">{cfg.rationale}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {submitResult && (
            <div className="mt-3 bg-green-50 border border-green-200 rounded px-3 py-2 text-xs text-green-800">
              <div className="font-medium">Submitted as factory run <code className="bg-white px-1 rounded">{submitResult.factory_run_id}</code></div>
              <div className="mt-0.5 text-green-700">{submitResult.next_step}</div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function Dropdown({ label, value, onChange, options }: {
  label: string; value: string; onChange: (v: string) => void; options: [string, string][];
}) {
  return (
    <div>
      <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wide mb-1">{label}</label>
      <select value={value} onChange={e => onChange(e.target.value)}
        className="w-full px-2 py-1.5 border border-gray-300 rounded text-sm bg-white focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500 outline-none">
        {options.map(([v, lbl]) => <option key={v} value={v}>{lbl}</option>)}
      </select>
    </div>
  );
}
