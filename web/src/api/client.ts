import type {
  BatchEventsRequest,
  BatchIngestResponse,
  DashboardOverview,
  OutboxMessage,
  QuotaCheckResult,
  ReconcileRequest,
  ReconciliationRecord,
  TenantOverview,
  WindowOverview,
} from '../types.generated';

const BASE_URL = '/v1';

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${url}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...init?.headers,
    },
  });
  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(`API Error ${res.status}: ${errorText || res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  getOverview: (): Promise<DashboardOverview> =>
    fetchJson<DashboardOverview>('/dashboard/overview'),

  getTenants: (): Promise<TenantOverview[]> =>
    fetchJson<TenantOverview[]>('/tenants'),

  getWindows: (): Promise<WindowOverview[]> =>
    fetchJson<WindowOverview[]>('/windows'),

  getOutbox: (): Promise<OutboxMessage[]> =>
    fetchJson<OutboxMessage[]>('/outbox'),

  getReconciliationHistory: (): Promise<ReconciliationRecord[]> =>
    fetchJson<ReconciliationRecord[]>('/reconcile/history'),

  checkQuota: (
    tenantId: string,
    metric: string,
    quantity: number = 0,
  ): Promise<QuotaCheckResult> =>
    fetchJson<QuotaCheckResult>(
      `/quota/check?tenant_id=${encodeURIComponent(tenantId)}&metric=${encodeURIComponent(metric)}&quantity=${quantity}`,
    ),

  reconcile: (req: ReconcileRequest): Promise<ReconciliationRecord> =>
    fetchJson<ReconciliationRecord>('/reconcile', {
      method: 'POST',
      body: JSON.stringify(req),
    }),

  ingestBatch: (req: BatchEventsRequest): Promise<BatchIngestResponse> =>
    fetchJson<BatchIngestResponse>('/events', {
      method: 'POST',
      body: JSON.stringify(req),
    }),
};
