import { Link } from 'react-router-dom';
import {
  Database, FlaskConical, Table2, Shield, ArrowRight, Code, Rocket, Package, Sparkles,
} from 'lucide-react';

export default function Home() {
  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      {/* Hero */}
      <div className="text-center mb-10">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">Pricing Workbench</h1>
        <p className="text-lg text-blue-600 font-medium">Databricks Accelerator</p>
        <p className="text-gray-500 mt-3 max-w-3xl mx-auto">
          End-to-end commercial pricing on a single platform. Every step of the real data flow is
          traceable, auditable, and governed — from ingestion through promotion, deployment, and
          regulator-facing defence.
        </p>
      </div>

      {/* Main pricing flow — linear spine */}
      <FlowSpine />

      {/* Section cards — mirror the sidebar order */}
      <div className="mb-3 mt-10 flex items-end justify-between">
        <h2 className="text-sm font-semibold text-gray-500 uppercase tracking-wide">The pricing spine</h2>
        <span className="text-[11px] text-gray-500">left to right, every stage is a tab</span>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
        <SectionCard
          to="/datasets"
          icon={Database}
          color="blue"
          title="Ingestion"
          description="Internal book + vendor feeds + public reference data. Vendor data passes through an actuary approval gate with DQ checks."
          features={['Internal + vendor + public', 'DQ expectations', 'Actuary approval gate']}
        />
        <SectionCard
          to="/pricing-table"
          icon={Table2}
          color="green"
          title="Modelling Mart"
          description="Engineered feature table — every approved source joined on the active book. Factor catalog with per-factor provenance and an embedded AI/BI Genie."
          features={['Contributing sources', 'Factor catalog + lineage', 'AI/BI Genie']}
        />
        <SectionCard
          to="/development"
          icon={Code}
          color="purple"
          title="Model Development"
          description="Train, compare, promote. Three tabs: reference notebooks + model library for actuaries; candidate vs champion comparison; pack generation on promotion."
          features={['Train', 'Compare & test', 'Promote → governance pack']}
        />
        <SectionCard
          to="/deployment"
          icon={Rocket}
          color="red"
          title="Model Deployment"
          description="Production champions across all 4 model families, with rollback. Second tab: roadmap for the live pricing system (sub-500ms, 10+ models, online feature store)."
          features={['UC alias-based versioning', 'One-click rollback', 'Live endpoint metrics']}
        />
        <SectionCard
          to="/governance"
          icon={Shield}
          color="amber"
          title="Model Governance"
          description="Post-promotion defence for regulators. Browse by model, by date, or by policy — with an LLM assistant grounded in the governance pack (Claude Sonnet 4.6)."
          features={['By model / date / policy', 'Immutable audit trail', 'Agent-assisted review']}
        />
        <SectionCard
          to="/regulatory-ai"
          icon={Sparkles}
          color="purple"
          title="Regulatory AI"
          description="Placeholder preview: a regulator-facing assistant that pairs a grounded chatbot (Foundation Model API) with AI/BI Genie over the Modelling Mart — for FCA letters, board briefings, and ad-hoc data questions."
          features={['Claude · Foundation Model API', 'AI/BI Genie', 'Placeholder preview']}
        />
        <SectionCard
          to="/add-ons"
          icon={Package}
          color="gray"
          title="Add-ons"
          description="Tools that sit alongside the pricing spine: transaction-level Quote Review, and the New Data Impact module for data scientists measuring lift from external feeds."
          features={['Quote Review', 'New Data Impact']}
        />
      </div>

      {/* Model factory — own row so the 4-step flow gets focus */}
      <div className="mb-8">
        <h2 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3">Bulk model exploration</h2>
        <SectionCard
          to="/models"
          icon={FlaskConical}
          color="indigo"
          title="Model Factory"
          description="Systematic generation of 50+ candidate models in four steps — agent-analysed plan, virtual training, three-tier review (leaderboard → shortlist → portfolio what-if), and selective packaging that hands off to the Promote tab."
          features={['4-step actuary wizard', 'Claude-narrated plan', 'Grounded review agent', 'Hands off to Promote']}
          full
        />
      </div>

      {/* About */}
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <h3 className="font-semibold text-gray-800 mb-2">About this demo</h3>
        <p className="text-sm text-gray-600 mb-3">
          <strong>This is not a Databricks product.</strong> It's an example of what can be built on the
          Databricks platform using standard capabilities (Unity Catalog, Delta Lake, MLflow, Mosaic AI,
          Databricks Apps, Feature Engineering, Foundation Model API). The full source code is public —
          fork it, adapt it, use it as a starting point.
        </p>
        <p className="text-sm text-gray-600">
          All company names (Bricksurance SE), policy data and financial figures are fictional. No real
          customer data. The optional postcode enrichment in Add-ons uses genuine UK public data (ONSPD +
          IMD 2019 + ONS RUC) under the Open Government Licence.
        </p>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Flow spine — simple, left-to-right, reflects the new sidebar structure
// ---------------------------------------------------------------------------

function FlowSpine() {
  const steps = [
    { to: '/datasets',      icon: Database,     label: 'Ingestion',         sub: 'approved sources' },
    { to: '/pricing-table', icon: Table2,       label: 'Modelling Mart',    sub: 'feature table' },
    { to: '/development',   icon: Code,         label: 'Model Development', sub: 'train · compare · promote' },
    { to: '/deployment',    icon: Rocket,       label: 'Deployment',        sub: 'UC champions · rollback' },
    { to: '/governance',    icon: Shield,       label: 'Governance',        sub: 'defend to regulators' },
  ];
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-5 overflow-x-auto">
      <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-4">
        The pricing flow, end to end
      </h3>
      <div className="flex items-stretch gap-2 min-w-[720px]">
        {steps.map((s, i) => (
          <>
            <Link key={s.to} to={s.to}
                  className="flex-1 rounded-lg border border-gray-200 bg-gray-50 hover:bg-blue-50 hover:border-blue-300 p-3 transition">
              <s.icon className="w-4 h-4 text-gray-600 mb-1.5" />
              <div className="text-sm font-semibold text-gray-900 leading-tight">{s.label}</div>
              <div className="text-[11px] text-gray-500 mt-0.5 leading-snug">{s.sub}</div>
            </Link>
            {i < steps.length - 1 && (
              <div key={`arrow-${i}`} className="flex items-center shrink-0 px-1">
                <ArrowRight className="w-4 h-4 text-gray-400" />
              </div>
            )}
          </>
        ))}
      </div>
      <p className="text-xs text-gray-500 mt-4 leading-relaxed">
        Every stage has its own tab — click a card to jump in. Add-ons and the legacy Model Factory live
        off to the side; they aren't part of the main spine.
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Section card
// ---------------------------------------------------------------------------

function SectionCard({ to, icon: Icon, color, title, description, features, full }: {
  to: string; icon: any; color: string; title: string; description: string;
  features: string[]; full?: boolean;
}) {
  const colorMap: Record<string, { bg: string; border: string; icon: string; badge: string }> = {
    blue:   { bg: 'bg-blue-50',   border: 'border-blue-200',   icon: 'text-blue-600',   badge: 'bg-blue-100 text-blue-700' },
    purple: { bg: 'bg-purple-50', border: 'border-purple-200', icon: 'text-purple-600', badge: 'bg-purple-100 text-purple-700' },
    green:  { bg: 'bg-green-50',  border: 'border-green-200',  icon: 'text-green-600',  badge: 'bg-green-100 text-green-700' },
    amber:  { bg: 'bg-amber-50',  border: 'border-amber-200',  icon: 'text-amber-600',  badge: 'bg-amber-100 text-amber-700' },
    red:    { bg: 'bg-red-50',    border: 'border-red-200',    icon: 'text-red-600',    badge: 'bg-red-100 text-red-700' },
    indigo: { bg: 'bg-indigo-50', border: 'border-indigo-200', icon: 'text-indigo-600', badge: 'bg-indigo-100 text-indigo-700' },
    gray:   { bg: 'bg-gray-50',   border: 'border-gray-200',   icon: 'text-gray-600',   badge: 'bg-gray-100 text-gray-700' },
  };
  const c = colorMap[color] || colorMap.blue;
  return (
    <Link to={to}
          className={`group block ${c.bg} border ${c.border} rounded-lg p-5 hover:shadow-md transition-all ${full ? 'col-span-full' : ''}`}>
      <div className="flex items-center gap-3 mb-2">
        <Icon className={`w-5 h-5 ${c.icon}`} />
        <h3 className="font-semibold text-gray-900 group-hover:text-blue-600 transition-colors">{title}</h3>
        <ArrowRight className="w-4 h-4 text-gray-400 ml-auto group-hover:translate-x-1 transition-transform" />
      </div>
      <p className="text-sm text-gray-600 mb-3">{description}</p>
      <div className="flex flex-wrap gap-1.5">
        {features.map((f, i) => (
          <span key={i} className={`px-2 py-0.5 rounded text-[10px] font-medium ${c.badge}`}>{f}</span>
        ))}
      </div>
    </Link>
  );
}
