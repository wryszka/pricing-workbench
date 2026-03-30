const BASE = '/api';

async function fetchJson<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) throw new Error(`API error: ${res.status} ${res.statusText}`);
  return res.json();
}

export const api = {
  getDatasets: () => fetchJson<any[]>('/datasets'),
  getDatasetDiff: (id: string) => fetchJson<any>(`/datasets/${id}/diff`),
  getDatasetImpact: (id: string) => fetchJson<any>(`/datasets/${id}/impact`),
  getDatasetQuality: (id: string) => fetchJson<any>(`/datasets/${id}/quality`),
  approveDataset: (id: string, decision: string, notes: string) =>
    fetchJson<any>(`/datasets/${id}/approve`, {
      method: 'POST',
      body: JSON.stringify({ decision, reviewer_notes: notes }),
    }),
  getApprovalHistory: (id: string) => fetchJson<any[]>(`/datasets/${id}/approvals`),
};
