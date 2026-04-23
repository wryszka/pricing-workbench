import { useState } from 'react';
import { Bot, Database, Send, MessageCircle } from 'lucide-react';

/**
 * Regulatory AI — placeholder page showcasing how Foundation Model API +
 * AI/BI Genie sit together for pricing regulators and the senior committee.
 *
 * Visual-only for now. Send buttons surface a "coming soon" toast; clicking
 * a suggested question populates the input.
 */
export default function RegulatoryAI() {
  const [toast, setToast] = useState<string | null>(null);
  return (
    <div className="max-w-5xl mx-auto px-6 py-8">
      {/* Header */}
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-gray-900">Regulatory AI</h2>
        <div className="flex flex-wrap items-center gap-3 mt-1.5">
          <span className="text-sm text-gray-500">Pricing regulator chatbot</span>
          <span className="text-[10px] uppercase tracking-wider font-bold px-2 py-0.5 rounded-full bg-violet-100 text-violet-800 border border-violet-200">
            Powered by Databricks Foundation Model API + AI/BI Genie
          </span>
        </div>
        <p className="text-gray-500 text-sm mt-3">
          A placeholder for the regulator-facing assistant: two grounded LLM surfaces that answer the
          questions pricing teams get asked every day — from the FCA, internal audit, and the board.
        </p>
      </div>

      {/* Regulatory chatbot card */}
      <ChatbotCard
        icon={<Bot className="w-5 h-5 text-violet-600" />}
        accent="violet"
        title="Regulatory Chatbot"
        subtitle="Writes analysis, letters, briefings"
        suggestions={[
          "The regulator is asking why our property losses went up — draft a response",
          "Prepare a letter to the FCA explaining our Consumer Duty price-walking controls",
          "Why did our loss ratio deteriorate this quarter?",
          "Summarise our pricing performance for the board in plain English",
          "What are the top 3 things the CRO should worry about this quarter?",
          "Compare this quarter to last — what got better and what got worse?",
          "Are our pricing models performing as expected across the book?",
          "Is our fraud-model precision above the SIU referral threshold?",
          "Explain the relationship between our rating factors and the loss ratio",
        ]}
        placeholder="Ask about pricing performance, draft a regulator response…"
        onSend={() => setToast("Regulatory chatbot coming soon — this is a placeholder tab.")}
      />

      {/* AI/BI Genie card */}
      <GenieCard
        onSend={() => setToast("AI/BI Genie connection coming soon — placeholder.")}
        suggestions={[
          "What is the loss ratio by industry tier for Q4 2025?",
          "Show combined ratio by region across all lines",
          "Which postcode sectors have the highest severity?",
          "Show gross written premium by broker channel",
          "Compare promotion cadence across our 4 pricing models this year",
          "How many quotes converted for SME commercial in Q4?",
          "Which policies were repriced after the flood-risk scenario was applied?",
        ]}
      />

      {/* Footer disclaimer */}
      <div className="mt-6 bg-gray-50 border border-gray-200 rounded-lg p-4 text-xs text-gray-500">
        This tab is a preview of the regulator-facing assistant pattern. When wired, the chatbot will
        run as a grounded agent over governance packs, audit log, and the Modelling Mart; the AI/BI
        Genie side will query the live Modelling Mart Delta tables through a scoped Genie space.
      </div>

      {toast && (
        <div onClick={() => setToast(null)}
             className="fixed bottom-4 right-4 bg-gray-900 text-white text-sm px-4 py-2 rounded-lg shadow-lg z-50 cursor-pointer">
          {toast}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Chatbot card — horizontally-wrapping suggestion chips + input
// ---------------------------------------------------------------------------

function ChatbotCard({ icon, accent, title, subtitle, suggestions, placeholder, onSend }: {
  icon: React.ReactNode;
  accent: 'violet' | 'blue';
  title: string; subtitle: string;
  suggestions: string[];
  placeholder: string;
  onSend: () => void;
}) {
  const [value, setValue] = useState('');
  const border = accent === 'violet' ? 'border-violet-200' : 'border-blue-200';
  const headerBg = accent === 'violet' ? 'bg-violet-50' : 'bg-blue-50';
  const titleCls = accent === 'violet' ? 'text-violet-700' : 'text-blue-700';
  const btnBg   = accent === 'violet' ? 'bg-violet-600 hover:bg-violet-700' : 'bg-blue-600 hover:bg-blue-700';

  return (
    <section className={`bg-white rounded-lg border ${border} overflow-hidden mb-4`}>
      <div className={`${headerBg} border-b ${border} px-4 py-2.5 flex items-baseline gap-2`}>
        <span className="inline-flex items-center gap-1.5">{icon}<span className={`font-semibold ${titleCls}`}>{title}</span></span>
        <span className={`text-xs ${titleCls} opacity-80`}>{subtitle}</span>
      </div>
      <div className="p-4">
        <div className="flex flex-wrap gap-2 mb-4">
          {suggestions.map(s => (
            <button key={s} onClick={() => setValue(s)}
                    className="text-xs text-left px-3 py-1.5 rounded-full bg-white border border-gray-300 hover:border-violet-400 hover:bg-violet-50 text-gray-800">
              {s}
            </button>
          ))}
        </div>
        <div className="flex gap-2">
          <input value={value} onChange={e => setValue(e.target.value)}
                 placeholder={placeholder}
                 className="flex-1 border border-gray-300 rounded-lg px-3 py-2 text-sm" />
          <button onClick={() => onSend()}
                  className={`inline-flex items-center justify-center w-10 h-10 rounded-lg text-white ${btnBg}`}
                  aria-label="Send">
            <Send className="w-4 h-4" />
          </button>
        </div>
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Genie card — stacked suggestion rows in a data-panel visual style
// ---------------------------------------------------------------------------

function GenieCard({ suggestions, onSend }: { suggestions: string[]; onSend: () => void }) {
  const [value, setValue] = useState('');
  return (
    <section className="bg-white rounded-lg border border-blue-200 overflow-hidden">
      <div className="bg-blue-50 border-b border-blue-200 px-4 py-2.5 flex items-baseline gap-2">
        <Database className="w-5 h-5 text-blue-600" />
        <span className="font-semibold text-blue-700">AI/BI Genie</span>
        <span className="text-xs text-blue-700/80">Returns tables, charts, SQL queries</span>
      </div>
      <div className="p-4">
        <div className="space-y-1.5 mb-4">
          {suggestions.map(s => (
            <button key={s} onClick={() => setValue(s)}
                    className="w-full text-left text-sm px-3 py-2 rounded-lg bg-gray-50 border border-gray-200 hover:border-blue-300 hover:bg-blue-50 text-gray-800 flex items-center gap-2">
              <MessageCircle className="w-4 h-4 text-gray-400 shrink-0" />
              <span>{s}</span>
            </button>
          ))}
        </div>
        <div className="flex gap-2">
          <input value={value} onChange={e => setValue(e.target.value)}
                 placeholder="Ask about your data…"
                 className="flex-1 border border-gray-300 rounded-lg px-3 py-2 text-sm" />
          <button onClick={() => onSend()}
                  className="inline-flex items-center justify-center w-10 h-10 rounded-lg bg-blue-500/80 hover:bg-blue-600 text-white"
                  aria-label="Send">
            <Send className="w-4 h-4" />
          </button>
        </div>
      </div>
    </section>
  );
}
