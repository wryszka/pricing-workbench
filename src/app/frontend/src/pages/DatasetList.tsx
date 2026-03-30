import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Database, ChevronRight, CheckCircle2, XCircle, Clock } from 'lucide-react';
import { api } from '../lib/api';

export default function DatasetList() {
  const [datasets, setDatasets] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getDatasets().then(setDatasets).finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="p-8 text-center text-gray-500">Loading datasets...</div>;

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-gray-900">External Data Sources</h2>
        <p className="text-gray-500 mt-1">Review, validate and approve external datasets before they merge into the Unified Pricing Table</p>
      </div>

      <div className="grid gap-4">
        {datasets.map((ds) => {
          const approval = ds.approval;
          const status = approval?.decision || 'pending';

          return (
            <Link
              key={ds.id}
              to={`/dataset/${ds.id}`}
              className="bg-white rounded-lg border border-gray-200 p-5 hover:border-blue-300 hover:shadow-md transition-all group"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <div className="w-10 h-10 bg-blue-50 rounded-lg flex items-center justify-center">
                    <Database className="w-5 h-5 text-blue-600" />
                  </div>
                  <div>
                    <h3 className="font-semibold text-gray-900 group-hover:text-blue-600 transition-colors">
                      {ds.display_name}
                    </h3>
                    <p className="text-sm text-gray-500">{ds.description}</p>
                  </div>
                </div>
                <div className="flex items-center gap-6">
                  <div className="text-right">
                    <div className="text-sm text-gray-500">Source</div>
                    <div className="text-sm font-medium">{ds.source}</div>
                  </div>
                  <div className="text-right">
                    <div className="text-sm text-gray-500">Raw / Silver</div>
                    <div className="text-sm font-medium">
                      {Number(ds.raw_row_count).toLocaleString()} / {Number(ds.silver_row_count).toLocaleString()}
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-sm text-gray-500">DQ Dropped</div>
                    <div className={`text-sm font-medium ${ds.rows_dropped_by_dq > 0 ? 'text-amber-600' : 'text-green-600'}`}>
                      {ds.rows_dropped_by_dq}
                    </div>
                  </div>
                  <StatusBadge status={status} />
                  <ChevronRight className="w-5 h-5 text-gray-400 group-hover:text-blue-500" />
                </div>
              </div>
            </Link>
          );
        })}
      </div>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  if (status === 'approved') {
    return (
      <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-green-50 text-green-700 border border-green-200">
        <CheckCircle2 className="w-3.5 h-3.5" /> Approved
      </span>
    );
  }
  if (status === 'rejected') {
    return (
      <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-red-50 text-red-700 border border-red-200">
        <XCircle className="w-3.5 h-3.5" /> Rejected
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-amber-50 text-amber-700 border border-amber-200">
      <Clock className="w-3.5 h-3.5" /> Pending
    </span>
  );
}
