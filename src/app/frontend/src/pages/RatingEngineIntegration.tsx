import { Link } from 'react-router-dom';
import {
  Radar, ArrowLeft, ArrowRight, Database, Table2, Shield,
  CheckCircle2, Zap, Plug, Settings2,
} from 'lucide-react';

/**
 * Rating Engine Integration — placeholder.
 *
 * Framing: the workbench sits UPSTREAM of established rating engines
 * (Radar / Earnix) and can act as an enrichment layer — delivering scored
 * factors, features and ML-loading signals into the engine, without
 * disrupting the actuary's rating table workflow.
 */
export default function RatingEngineIntegration() {
  return (
    <div className="max-w-5xl mx-auto px-6 py-8">
      <Link to="/add-ons"
            className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-gray-800 mb-2">
        <ArrowLeft className="w-3.5 h-3.5" /> Back to Add-ons
      </Link>

      <div className="mb-6">
        <div className="flex items-center gap-2">
          <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Radar className="w-6 h-6 text-emerald-600" /> Rating Engine Integration
          </h2>
          <span className="text-[10px] uppercase tracking-wider font-bold px-2 py-0.5 rounded-full bg-amber-100 text-amber-800 border border-amber-200">
            Placeholder preview
          </span>
        </div>
        <p className="text-gray-600 mt-2 text-sm max-w-3xl">
          <strong>This system can act as an enrichment layer for rating engines.</strong> For
          insurers that already use Willis Towers Watson Radar, Earnix, or a home-grown engine to
          manage rating tables and price calculation — the Pricing Workbench doesn't replace it.
          It augments it. Enriched factors, ML-driven loadings, fraud signals and demand scores
          flow from here into the engine, while the actuary keeps their rating-table workflow.
        </p>
      </div>

      {/* Architecture — simple SVG */}
      <section className="bg-white border border-gray-200 rounded-lg p-5 mb-5">
        <h3 className="text-sm font-semibold text-gray-800 mb-4">Where the workbench fits</h3>
        <div className="flex justify-center overflow-x-auto">
          <ArchitectureDiagram />
        </div>
      </section>

      {/* Two-column narrative */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-5">
        <Card
          icon={<Settings2 className="w-5 h-5 text-emerald-600" />}
          title="What the rating engine keeps"
          bullets={[
            'Rating-table management workflow — actuaries' + " keep the UX they know",
            'Version control and promotion of rating rules',
            'Sign-off and release cycle',
            'Regulatory reporting hooks already wired to the engine',
            'Existing downstream connectors (broker portals, aggregators, policy admin)',
          ]}
        />
        <Card
          icon={<Plug className="w-5 h-5 text-emerald-600" />}
          title="What the workbench contributes"
          bullets={[
            'ML-driven factor scores (frequency, severity, demand, fraud)',
            'Real external-data enrichment (ONSPD, IMD, market benchmarks)',
            'Governance packs and audit trail for each factor',
            'Challenger comparisons: factor X vs factor Y on live book',
            'Retraining cadence + drift monitoring',
          ]}
          accent="emerald"
        />
      </div>

      {/* Integration options */}
      <section className="bg-white border border-gray-200 rounded-lg p-5 mb-5">
        <h3 className="text-sm font-semibold text-gray-800 mb-3">Integration options</h3>
        <div className="space-y-3">
          <IntegrationRow
            icon={<Zap className="w-4 h-4 text-emerald-600" />}
            title="Real-time feature API"
            body="Rating engine calls a Databricks Model Serving endpoint at quote time; workbench returns enriched features + ML scores under 500ms. Features include: IMD deciles, flood risk, credit signals, industry-tier derivations, ML-predicted claim frequency × severity."
          />
          <IntegrationRow
            icon={<Table2 className="w-4 h-4 text-emerald-600" />}
            title="Batch feature export"
            body="Scheduled export of a wide feature table to the rating engine's reference store. Suits engines that expect a nightly table refresh rather than real-time lookup. Output is a Delta table with policy-level enriched features."
          />
          <IntegrationRow
            icon={<Database className="w-4 h-4 text-emerald-600" />}
            title="Rating-factor table generator"
            body="For engines that work on discrete rating tables (Radar-style), the workbench generates the relativity tables directly from trained GLMs — industry tier × region, flood zone × construction type, etc. — and hands them to the actuary for review and paste-in."
          />
          <IntegrationRow
            icon={<Shield className="w-4 h-4 text-emerald-600" />}
            title="Governance bridge"
            body="Every factor / model exported is linked to a governance pack via its pack_id. The rating engine's audit log references the pack; regulators can trace any rate back through the engine to the workbench to the source data."
          />
        </div>
      </section>

      {/* Why it matters */}
      <section className="bg-emerald-50 border border-emerald-200 rounded-lg p-4 mb-5">
        <h3 className="text-sm font-semibold text-emerald-900 mb-2 flex items-center gap-1.5">
          <CheckCircle2 className="w-4 h-4" /> Why it lands with pricing teams
        </h3>
        <ul className="space-y-1 text-sm text-emerald-900">
          <li className="flex items-start gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-600 mt-2 shrink-0" />
            <span><strong>No workflow disruption.</strong> Actuaries keep using Radar / Earnix exactly as before.</span>
          </li>
          <li className="flex items-start gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-600 mt-2 shrink-0" />
            <span><strong>ML lift without rebuild.</strong> You get the Gini uplift from the workbench's models without migrating the rating engine.</span>
          </li>
          <li className="flex items-start gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-600 mt-2 shrink-0" />
            <span><strong>Governance by default.</strong> Every enriched factor has a traceable pack — easier regulatory defence than bolt-on ML fed directly into the engine.</span>
          </li>
          <li className="flex items-start gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-600 mt-2 shrink-0" />
            <span><strong>Shadow pricing friendly.</strong> Existing Quote Review flow compares engine-only vs enriched prices before launch.</span>
          </li>
        </ul>
      </section>

      <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 text-xs text-gray-600">
        This tab is a placeholder preview. When wired, it'll expose the feature API signature,
        example request/response payloads for Radar / Earnix calls, per-factor refresh cadence,
        and a live feed of which engines are currently consuming which workbench features.
      </div>
    </div>
  );
}

function Card({ icon, title, bullets, accent }: {
  icon: React.ReactNode; title: string; bullets: string[]; accent?: 'emerald'
}) {
  const bg = accent === 'emerald' ? 'bg-emerald-50 border-emerald-200' : 'bg-white border-gray-200';
  return (
    <section className={`rounded-lg border p-4 ${bg}`}>
      <div className="flex items-center gap-2 mb-2">
        {icon}
        <h4 className="text-sm font-semibold text-gray-900">{title}</h4>
      </div>
      <ul className="space-y-1 text-sm text-gray-800">
        {bullets.map((b, i) => (
          <li key={i} className="flex items-start gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-gray-400 mt-2 shrink-0" />
            <span>{b}</span>
          </li>
        ))}
      </ul>
    </section>
  );
}

function IntegrationRow({ icon, title, body }: { icon: React.ReactNode; title: string; body: string }) {
  return (
    <div className="flex items-start gap-3 border-l-2 border-emerald-200 pl-3 py-1">
      <div className="mt-0.5 shrink-0">{icon}</div>
      <div>
        <div className="text-sm font-semibold text-gray-900">{title}</div>
        <div className="text-xs text-gray-700 mt-0.5 leading-relaxed">{body}</div>
      </div>
    </div>
  );
}

function ArchitectureDiagram() {
  return (
    <svg viewBox="0 0 820 260" className="w-full max-w-4xl" aria-label="Rating engine integration architecture">
      <defs>
        <marker id="arr" viewBox="0 0 10 10" refX="9" refY="5"
                markerWidth="6" markerHeight="6" orient="auto-start-reverse">
          <path d="M0,0 L10,5 L0,10 z" fill="#64748b" />
        </marker>
      </defs>

      {/* Sources / workbench bubble (upstream, enrichment side) */}
      <rect x="20" y="30" width="200" height="200" rx="10" fill="#ecfdf5" stroke="#10b981" />
      <text x="120" y="55" textAnchor="middle" fontSize="13" fontWeight="600" fill="#064e3b">
        Pricing Workbench
      </text>
      <text x="120" y="72" textAnchor="middle" fontSize="10" fill="#047857">enrichment layer</text>

      {/* Workbench components */}
      {[
        { y: 90,  label: 'Modelling Mart (50 features)' },
        { y: 115, label: 'Freq / Sev / Demand / Fraud' },
        { y: 140, label: 'External enrichment (ONSPD, IMD)' },
        { y: 165, label: 'Governance packs + audit' },
        { y: 190, label: 'Drift + monitoring' },
      ].map(c => (
        <text key={c.y} x="120" y={c.y} textAnchor="middle" fontSize="10" fill="#065f46">{c.label}</text>
      ))}

      {/* API arrow */}
      <line x1="220" y1="130" x2="340" y2="130" stroke="#64748b" strokeWidth="2" markerEnd="url(#arr)" />
      <text x="280" y="122" textAnchor="middle" fontSize="10" fill="#475569" fontWeight="600">
        Feature API / batch export
      </text>
      <text x="280" y="145" textAnchor="middle" fontSize="9" fill="#64748b">
        scored factors · loadings · governance ids
      </text>

      {/* Rating engine bubble */}
      <rect x="340" y="30" width="220" height="200" rx="10" fill="#eff6ff" stroke="#3b82f6" />
      <text x="450" y="55" textAnchor="middle" fontSize="13" fontWeight="600" fill="#1e3a8a">
        Rating Engine
      </text>
      <text x="450" y="72" textAnchor="middle" fontSize="10" fill="#2563eb">Radar · Earnix · in-house</text>
      {[
        { y: 100, label: 'Rating tables + relativities' },
        { y: 123, label: 'Actuary workflow preserved' },
        { y: 146, label: 'Version control + release cycle' },
        { y: 169, label: 'Calculates final price' },
        { y: 192, label: 'Hands off to admin / broker' },
      ].map(c => (
        <text key={c.y} x="450" y={c.y} textAnchor="middle" fontSize="10" fill="#1e40af">{c.label}</text>
      ))}

      {/* Arrow to broker */}
      <line x1="560" y1="130" x2="660" y2="130" stroke="#64748b" strokeWidth="2" markerEnd="url(#arr)" />
      <text x="610" y="122" textAnchor="middle" fontSize="10" fill="#475569" fontWeight="600">
        final premium
      </text>

      {/* Broker / admin */}
      <rect x="660" y="75" width="140" height="110" rx="10" fill="#fdf4ff" stroke="#a855f7" />
      <text x="730" y="100" textAnchor="middle" fontSize="12" fontWeight="600" fill="#6b21a8">
        Broker / portal
      </text>
      <text x="730" y="120" textAnchor="middle" fontSize="10" fill="#7e22ce">quote delivered</text>
      <text x="730" y="145" textAnchor="middle" fontSize="10" fill="#7e22ce">policy admin</text>
      <text x="730" y="170" textAnchor="middle" fontSize="10" fill="#7e22ce">aggregator response</text>

      {/* Back-channel: engine feedback to workbench */}
      <path d="M 450 230 Q 300 260 120 230" fill="none" stroke="#64748b"
            strokeWidth="1.5" strokeDasharray="4 3" markerEnd="url(#arr)" />
      <text x="285" y="252" textAnchor="middle" fontSize="9" fill="#475569">
        bound-policy outcomes · claims feedback (retraining)
      </text>
    </svg>
  );
}
