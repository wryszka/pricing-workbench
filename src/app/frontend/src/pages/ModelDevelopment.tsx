import { useEffect, useState } from 'react';
import { Code, ExternalLink, FlaskConical, TrendingUp, GitCompare, Shield } from 'lucide-react';
import { api } from '../lib/api';

export default function ModelDevelopment() {
  const [config, setConfig] = useState<any>(null);
  useEffect(() => { api.getConfig().then(setConfig).catch(() => {}); }, []);

  const host = config?.workspace_host || '';

  const notebooks = [
    {
      id: 'glm_frequency',
      title: 'GLM Frequency (Poisson)',
      path: 'src/04_models/model_01_glm_frequency.py',
      icon: TrendingUp,
      color: 'blue',
      description: 'Trains a Poisson GLM for claim frequency prediction. Produces transparent relativities for regulatory submission. Logged with FeatureLookup for automatic serving.',
      topics: ['statsmodels Poisson GLM', 'Exponentiated coefficients', 'fe.log_model() with FeatureLookup', 'MLflow experiment tracking'],
    },
    {
      id: 'glm_severity',
      title: 'GLM Severity (Gamma)',
      path: 'src/04_models/model_02_glm_severity.py',
      icon: TrendingUp,
      color: 'blue',
      description: 'Trains a Gamma GLM for average claim severity. Combined with frequency gives the burning cost: Technical Price = Frequency × Severity.',
      topics: ['statsmodels Gamma GLM', 'Severity relativities', 'Burning cost formula', 'UC model registry'],
    },
    {
      id: 'gbm_demand',
      title: 'GBM Demand (LightGBM)',
      path: 'src/04_models/model_03_gbm_demand.py',
      icon: FlaskConical,
      color: 'green',
      description: 'LightGBM classifier for conversion propensity. Predicts whether a quote converts to a bound policy — drives the commercial pricing overlay.',
      topics: ['LightGBM classifier', 'ROC AUC / precision / recall', 'Demand curve analysis', 'Feature importance'],
    },
    {
      id: 'gbm_uplift',
      title: 'GBM Risk Uplift (Residuals)',
      path: 'src/04_models/model_04_gbm_risk_uplift.py',
      icon: GitCompare,
      color: 'purple',
      description: 'Trains a LightGBM on GLM residuals to capture non-linear interactions the GLM missed. Shows RMSE improvement % over GLM alone.',
      topics: ['GLM + GBM hybrid approach', 'Residual learning', 'Feature expansion (40 features)', 'Model comparison'],
    },
    {
      id: 'fraud',
      title: 'Fraud Propensity',
      path: 'src/04_models/model_05_fraud_propensity.py',
      icon: Shield,
      color: 'red',
      description: 'Binary classifier for claims fraud detection (~3% prevalence). Demonstrates class-imbalanced modelling with scale_pos_weight.',
      topics: ['LightGBM with class imbalance', 'F1 / precision / recall tradeoff', 'Fraud load factor for pricing', 'Synthetic label generation'],
    },
    {
      id: 'retention',
      title: 'Retention / Churn',
      path: 'src/04_models/model_06_retention.py',
      icon: TrendingUp,
      color: 'amber',
      description: 'Binary classifier for policy non-renewal prediction (~15% churn). Retention scores drive discount/pricing flexibility for at-risk renewals.',
      topics: ['Churn indicators (market position, competitor activity)', 'Retention discount in pricing waterfall', 'Portfolio value analysis'],
    },
  ];

  const colorMap: Record<string, { bg: string; border: string; icon: string; badge: string }> = {
    blue:   { bg: 'bg-blue-50',   border: 'border-blue-200',   icon: 'text-blue-600',   badge: 'bg-blue-100 text-blue-700' },
    green:  { bg: 'bg-green-50',  border: 'border-green-200',  icon: 'text-green-600',  badge: 'bg-green-100 text-green-700' },
    purple: { bg: 'bg-purple-50', border: 'border-purple-200', icon: 'text-purple-600', badge: 'bg-purple-100 text-purple-700' },
    red:    { bg: 'bg-red-50',    border: 'border-red-200',    icon: 'text-red-600',    badge: 'bg-red-100 text-red-700' },
    amber:  { bg: 'bg-amber-50',  border: 'border-amber-200',  icon: 'text-amber-600',  badge: 'bg-amber-100 text-amber-700' },
  };

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-gray-900">Model Development</h2>
        <p className="text-gray-500 mt-1">
          From feature store to trained models — open any notebook in Databricks to see the code
        </p>
      </div>

      <div className="bg-blue-50 border border-blue-200 rounded-lg p-5 mb-6">
        <h3 className="font-semibold text-blue-800 mb-2">What this section shows</h3>
        <p className="text-sm text-blue-600 mb-3">
          The model development pipeline reads directly from the Unified Pricing Table (feature store),
          trains models using standard Python libraries (statsmodels, LightGBM), and logs everything to
          MLflow with FeatureLookup specs. This means models automatically know which features they need
          at serving time — zero additional integration.
        </p>
        <div className="flex flex-wrap gap-1.5">
          {['Unified Pricing Table as feature store', 'statsmodels GLMs', 'LightGBM GBMs', 'MLflow tracking', 'fe.log_model() with FeatureLookup', 'Unity Catalog model registry'].map(f => (
            <span key={f} className="px-2 py-0.5 rounded text-[10px] font-medium bg-blue-100 text-blue-700">{f}</span>
          ))}
        </div>
      </div>

      <div className="grid gap-4">
        {notebooks.map((nb) => {
          const c = colorMap[nb.color] || colorMap.blue;
          return (
            <div key={nb.id} className={`${c.bg} border ${c.border} rounded-lg p-5`}>
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-3">
                  <nb.icon className={`w-5 h-5 ${c.icon}`} />
                  <h3 className="font-semibold text-gray-900">{nb.title}</h3>
                </div>
                {host && (
                  <a href={`${host}/editor/notebooks`} target="_blank" rel="noopener noreferrer"
                    className="flex items-center gap-1 px-3 py-1 bg-white border border-gray-300 rounded text-xs font-medium text-gray-700 hover:bg-gray-50 transition-colors">
                    <Code className="w-3 h-3" /> Open in Databricks
                    <ExternalLink className="w-3 h-3" />
                  </a>
                )}
              </div>
              <p className="text-sm text-gray-600 mb-3">{nb.description}</p>
              <div className="flex items-center justify-between">
                <div className="flex flex-wrap gap-1.5">
                  {nb.topics.map((t, i) => (
                    <span key={i} className={`px-2 py-0.5 rounded text-[10px] font-medium ${c.badge}`}>{t}</span>
                  ))}
                </div>
                <code className="text-[10px] text-gray-400 font-mono">{nb.path}</code>
              </div>
            </div>
          );
        })}
      </div>

      <div className="mt-6 bg-gray-50 border border-gray-200 rounded-lg p-5">
        <h3 className="font-semibold text-gray-800 mb-2">Pipeline flow</h3>
        <div className="text-sm text-gray-600 font-mono">
          <span className="text-blue-600">Unified Pricing Table</span>
          {' → '}
          <span className="text-purple-600">FeatureLookup + train/test split</span>
          {' → '}
          <span className="text-green-600">Train (GLM / GBM)</span>
          {' → '}
          <span className="text-amber-600">MLflow log metrics + fe.log_model()</span>
          {' → '}
          <span className="text-red-600">UC Model Registry</span>
          {' → '}
          <span className="text-gray-800">Model Serving (auto feature lookup)</span>
        </div>
      </div>
    </div>
  );
}
