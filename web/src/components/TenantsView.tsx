import React, { useState } from 'react';
import type { QuotaCheckResult, TenantOverview } from '../types.generated';
import { api } from '../api/client';

interface TenantsViewProps {
  tenants: TenantOverview[];
  onRefresh: () => void;
}

export const TenantsView: React.FC<TenantsViewProps> = ({ tenants, onRefresh }) => {
  const [selectedTenant, setSelectedTenant] = useState<string>(
    tenants[0]?.tenant_id || 'acme-corp'
  );
  const [selectedMetric, setSelectedMetric] = useState<string>('api_calls');
  const [checkQuantity, setCheckQuantity] = useState<number>(100);
  const [checking, setChecking] = useState<boolean>(false);
  const [checkResult, setCheckResult] = useState<QuotaCheckResult | null>(null);
  const [checkLatency, setCheckLatency] = useState<number | null>(null);

  const handleQuotaCheck = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setChecking(true);
      const start = performance.now();
      const res = await api.checkQuota(selectedTenant, selectedMetric, checkQuantity);
      const elapsed = Math.round((performance.now() - start) * 100) / 100;
      setCheckResult(res);
      setCheckLatency(elapsed);
    } catch (err: any) {
      alert(`Quota Check Failed: ${err.message}`);
    } finally {
      setChecking(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Concept Explainer Banner */}
      <div className="bg-gradient-to-r from-indigo-900/40 to-slate-900/60 rounded-xl p-5 border border-indigo-500/20 shadow-sm">
        <div className="flex items-start space-x-3">
          <div className="p-2 rounded-lg bg-indigo-500/10 text-indigo-400 font-mono text-sm">
            ADR-0002
          </div>
          <div>
            <h4 className="text-sm font-semibold text-slate-900 dark:text-white">
              Enforcement is Approximate, Billing is Exact
            </h4>
            <p className="text-xs text-slate-600 dark:text-slate-400 mt-1 leading-relaxed">
              Enforcement runs on the hot path in Redis (sub-millisecond evaluation, tolerant of bounded drift).
              Billing ledgers run asynchronously in Postgres with append-only revisions and exact <code className="font-mono text-indigo-400">Decimal</code> arithmetic.
            </p>
          </div>
        </div>
      </div>

      {/* Tenants Table */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden">
        <div className="p-5 border-b border-slate-200 dark:border-slate-800 flex items-center justify-between">
          <div>
            <h3 className="text-base font-semibold text-slate-900 dark:text-white">
              Configured Tenants & Quotas
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Live comparison of fast approximate counter vs exact served aggregate
            </p>
          </div>
          <button
            onClick={onRefresh}
            className="text-xs px-2.5 py-1.5 rounded-md font-medium border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800 text-slate-700 dark:text-slate-300 hover:bg-slate-100"
          >
            Refetch
          </button>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="bg-slate-50 dark:bg-slate-800/60 text-slate-500 dark:text-slate-400 border-b border-slate-200 dark:border-slate-800">
                <th className="py-3 px-4 font-semibold">Tenant</th>
                <th className="py-3 px-4 font-semibold">Plan</th>
                <th className="py-3 px-4 font-semibold">Metric</th>
                <th className="py-3 px-4 font-semibold">Approx Usage (Redis)</th>
                <th className="py-3 px-4 font-semibold">Exact Usage (Postgres)</th>
                <th className="py-3 px-4 font-semibold">Drift</th>
                <th className="py-3 px-4 font-semibold">Quota Limit</th>
                <th className="py-3 px-4 font-semibold">Consumption</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
              {tenants.map((t) => {
                const metrics = Object.keys(t.quotas);
                return metrics.map((m, idx) => {
                  const limit = Number(t.quotas[m] || 0);
                  const approx = Number(t.approximate_usage[m] || 0);
                  const exact = Number(t.exact_usage[m] || 0);
                  const drift = t.drift_percent[m] ?? 0;
                  const pct = limit > 0 ? Math.min(100, Math.round((approx / limit) * 100)) : 0;

                  return (
                    <tr
                      key={`${t.tenant_id}-${m}`}
                      className="hover:bg-slate-50/50 dark:hover:bg-slate-800/40 transition"
                    >
                      {idx === 0 && (
                        <td
                          rowSpan={metrics.length}
                          className="py-3 px-4 font-mono font-bold text-slate-900 dark:text-white border-r border-slate-100 dark:border-slate-800"
                        >
                          {t.tenant_id}
                        </td>
                      )}
                      {idx === 0 && (
                        <td
                          rowSpan={metrics.length}
                          className="py-3 px-4 border-r border-slate-100 dark:border-slate-800"
                        >
                          <span
                            className={`inline-block px-2 py-0.5 rounded text-[11px] font-semibold uppercase tracking-wider ${
                              t.plan === 'enterprise'
                                ? 'bg-purple-100 text-purple-800 dark:bg-purple-950 dark:text-purple-300'
                                : t.plan === 'pro'
                                ? 'bg-blue-100 text-blue-800 dark:bg-blue-950 dark:text-blue-300'
                                : 'bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300'
                            }`}
                          >
                            {t.plan}
                          </span>
                        </td>
                      )}
                      <td className="py-3 px-4 font-mono text-slate-700 dark:text-slate-300">
                        {m}
                      </td>
                      <td className="py-3 px-4 font-mono font-medium text-amber-600 dark:text-amber-400">
                        {approx.toLocaleString()}
                      </td>
                      <td className="py-3 px-4 font-mono font-semibold text-emerald-600 dark:text-emerald-400">
                        {exact.toLocaleString()}
                      </td>
                      <td className="py-3 px-4 font-mono">
                        <span
                          className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold ${
                            drift === 0
                              ? 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400'
                              : drift < 1
                              ? 'bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300'
                              : 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300'
                          }`}
                        >
                          {drift > 0 ? `+${drift}%` : `${drift}%`}
                        </span>
                      </td>
                      <td className="py-3 px-4 font-mono text-slate-500">
                        {limit.toLocaleString()}
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center space-x-2">
                          <div className="w-24 bg-slate-100 dark:bg-slate-800 rounded-full h-2 overflow-hidden">
                            <div
                              className={`h-2 rounded-full ${
                                pct >= 100
                                  ? 'bg-rose-500'
                                  : pct > 80
                                  ? 'bg-amber-500'
                                  : 'bg-indigo-500'
                              }`}
                              style={{ width: `${pct}%` }}
                            />
                          </div>
                          <span className="font-mono text-[11px] text-slate-400">{pct}%</span>
                        </div>
                      </td>
                    </tr>
                  );
                });
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Interactive Fast-Path Quota Check Tester */}
      <div className="bg-white dark:bg-slate-900 rounded-xl p-6 border border-slate-200 dark:border-slate-800 shadow-sm">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="text-base font-semibold text-slate-900 dark:text-white">
              Fast-Path Quota Check Simulator
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Exercises GET /v1/quota/check — sub-millisecond approximate hot path
            </p>
          </div>
          {checkLatency !== null && (
            <div className="flex items-center space-x-1.5 px-2.5 py-1 rounded bg-indigo-50 dark:bg-indigo-950/60 border border-indigo-200 dark:border-indigo-800 text-xs font-mono text-indigo-600 dark:text-indigo-400">
              <span className="w-1.5 h-1.5 rounded-full bg-indigo-500"></span>
              <span>Round-trip: {checkLatency} ms</span>
            </div>
          )}
        </div>

        <form onSubmit={handleQuotaCheck} className="grid grid-cols-1 sm:grid-cols-4 gap-4 items-end">
          <div>
            <label className="block text-xs font-semibold text-slate-600 dark:text-slate-400 mb-1">
              Tenant ID
            </label>
            <select
              value={selectedTenant}
              onChange={(e) => setSelectedTenant(e.target.value)}
              className="w-full text-xs rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800 p-2 font-mono"
            >
              {tenants.map((t) => (
                <option key={t.tenant_id} value={t.tenant_id}>
                  {t.tenant_id} ({t.plan})
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-xs font-semibold text-slate-600 dark:text-slate-400 mb-1">
              Metric
            </label>
            <input
              type="text"
              value={selectedMetric}
              onChange={(e) => setSelectedMetric(e.target.value)}
              className="w-full text-xs rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800 p-2 font-mono"
            />
          </div>

          <div>
            <label className="block text-xs font-semibold text-slate-600 dark:text-slate-400 mb-1">
              Proposed Quantity
            </label>
            <input
              type="number"
              value={checkQuantity}
              onChange={(e) => setCheckQuantity(Number(e.target.value))}
              className="w-full text-xs rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800 p-2 font-mono"
            />
          </div>

          <div>
            <button
              type="submit"
              disabled={checking}
              className="w-full py-2 px-4 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white font-medium text-xs shadow-sm transition disabled:opacity-50"
            >
              {checking ? 'Checking...' : 'Check Quota'}
            </button>
          </div>
        </form>

        {checkResult && (
          <div className="mt-5 p-4 rounded-lg bg-slate-50 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                Evaluation Verdict
              </span>
              <span
                className={`px-2.5 py-1 rounded text-xs font-bold ${
                  checkResult.allowed
                    ? 'bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300'
                    : 'bg-rose-100 text-rose-800 dark:bg-rose-950 dark:text-rose-300'
                }`}
              >
                {checkResult.allowed ? '✓ ALLOWED' : '✗ QUOTA EXCEEDED'}
              </span>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs font-mono">
              <div>
                <span className="text-slate-400 block text-[10px]">CURRENT USAGE</span>
                <span className="font-bold text-slate-800 dark:text-slate-200">
                  {checkResult.current_usage}
                </span>
              </div>
              <div>
                <span className="text-slate-400 block text-[10px]">QUOTA LIMIT</span>
                <span className="font-bold text-slate-800 dark:text-slate-200">
                  {checkResult.limit}
                </span>
              </div>
              <div>
                <span className="text-slate-400 block text-[10px]">APPROXIMATE</span>
                <span className="text-amber-500 font-bold">
                  {checkResult.approximate ? 'TRUE (REDIS)' : 'FALSE (EXACT)'}
                </span>
              </div>
              <div>
                <span className="text-slate-400 block text-[10px]">REASON CODE</span>
                <span className="font-bold text-slate-700 dark:text-slate-300">
                  {checkResult.reason_code}
                </span>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
