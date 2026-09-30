import React, { useState } from 'react';
import type { DivergenceClass, ReconciliationRecord, TenantOverview } from '../types.generated';
import { api } from '../api/client';

/**
 * Colour is keyed off the class alone, never off the magnitude of the
 * difference. The two must not be allowed to disagree: if a record is ever
 * rendered green while carrying a non-zero difference, that is a backend
 * classification bug we want visible, not hidden by the UI second-guessing it.
 */
const DIVERGENCE_BADGE: Record<DivergenceClass, string> = {
  MATCH: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300',
  REAL: 'bg-rose-100 text-rose-800 dark:bg-rose-950 dark:text-rose-300',
  UNSETTLED: 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300',
  LATE_ARRIVAL: 'bg-purple-100 text-purple-800 dark:bg-purple-950 dark:text-purple-300',
};

interface ReconciliationViewProps {
  tenants: TenantOverview[];
  history: ReconciliationRecord[];
  onRefresh: () => void;
}

export const ReconciliationView: React.FC<ReconciliationViewProps> = ({
  tenants,
  history,
  onRefresh,
}) => {
  const [tenantId, setTenantId] = useState<string>(tenants[0]?.tenant_id || 'acme-corp');
  const [metric, setMetric] = useState<string>('api_calls');
  const [enforceGate, setEnforceGate] = useState<boolean>(true);
  const [running, setRunning] = useState<boolean>(false);
  const [activeRecord, setActiveRecord] = useState<ReconciliationRecord | null>(
    history[0] || null
  );

  const handleRunAudit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setRunning(true);
      const now = new Date();
      const wEnd = new Date(now.getTime() - 2 * 60 * 1000);
      const wStart = new Date(wEnd.getTime() - 60 * 1000);

      const record = await api.reconcile({
        tenant_id: tenantId,
        metric: metric,
        window_start: wStart.toISOString(),
        window_end: wEnd.toISOString(),
        enforce_gate: enforceGate,
      });
      setActiveRecord(record);
      onRefresh();
    } catch (err: any) {
      alert(`Audit failed: ${err.message}`);
    } finally {
      setRunning(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Thesis Header */}
      <div className="bg-gradient-to-r from-slate-900 to-indigo-950 text-white rounded-xl p-6 shadow-sm border border-slate-800">
        <div className="flex items-start space-x-3">
          <span className="px-2.5 py-1 rounded bg-indigo-500/20 text-indigo-300 font-mono text-xs font-bold border border-indigo-500/30">
            ADR-0007
          </span>
          <div>
            <h3 className="text-base font-bold text-white">
              Completion-Gated Reconciliation Audit
            </h3>
            <p className="text-xs text-slate-300 mt-1 leading-relaxed max-w-3xl">
              Naïve reconciliation compares live streams with served stores and generates false alarms whenever
              events are in-flight. Tally's Completion Gate asserts that the partition watermark has advanced past
              the window, consumer partition lag is 0, and outbox depth is 0 before running the audit.
            </p>
          </div>
        </div>
      </div>

      {/* Audit Trigger Form */}
      <div className="bg-white dark:bg-slate-900 rounded-xl p-6 border border-slate-200 dark:border-slate-800 shadow-sm">
        <h4 className="text-sm font-semibold text-slate-900 dark:text-white mb-4">
          Execute Reconciliation Run
        </h4>

        <form onSubmit={handleRunAudit} className="grid grid-cols-1 sm:grid-cols-4 gap-4 items-end">
          <div>
            <label className="block text-xs font-semibold text-slate-600 dark:text-slate-400 mb-1">
              Tenant ID
            </label>
            <select
              value={tenantId}
              onChange={(e) => setTenantId(e.target.value)}
              className="w-full text-xs rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800 p-2 font-mono"
            >
              {tenants.map((t) => (
                <option key={t.tenant_id} value={t.tenant_id}>
                  {t.tenant_id}
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
              value={metric}
              onChange={(e) => setMetric(e.target.value)}
              className="w-full text-xs rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800 p-2 font-mono"
            />
          </div>

          <div className="flex items-center space-x-2 pt-2">
            <input
              id="enforceGate"
              type="checkbox"
              checked={enforceGate}
              onChange={(e) => setEnforceGate(e.target.checked)}
              className="rounded border-slate-300 text-indigo-600 focus:ring-indigo-500 w-4 h-4"
            />
            <label htmlFor="enforceGate" className="text-xs text-slate-700 dark:text-slate-300 font-medium cursor-pointer">
              Enforce Completion Gate
            </label>
          </div>

          <div>
            <button
              type="submit"
              disabled={running}
              className="w-full py-2 px-4 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white font-medium text-xs shadow-sm transition disabled:opacity-50"
            >
              {running ? 'Replaying & Comparing...' : 'Run Audit'}
            </button>
          </div>
        </form>
      </div>

      {/* Active Audit Drill-down Card */}
      {activeRecord && (
        <div className="bg-white dark:bg-slate-900 rounded-xl p-6 border border-slate-200 dark:border-slate-800 shadow-sm">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 border-b border-slate-100 dark:border-slate-800 gap-2">
            <div>
              <div className="flex items-center space-x-2">
                <span className="font-mono text-sm font-bold text-slate-900 dark:text-white">
                  {activeRecord.tenant_id}
                </span>
                <span className="text-xs text-slate-400">·</span>
                <span className="font-mono text-xs text-slate-600 dark:text-slate-400">
                  {activeRecord.metric}
                </span>
              </div>
              <p className="text-xs text-slate-400 mt-0.5 font-mono">
                Window: {new Date(activeRecord.window_start).toLocaleTimeString()} → {new Date(activeRecord.window_end).toLocaleTimeString()}
              </p>
            </div>

            <div>
              <span
                className={`inline-flex items-center px-3 py-1 rounded-full text-xs font-bold font-mono tracking-wider ${
                  DIVERGENCE_BADGE[activeRecord.divergence_class]
                }`}
              >
                CLASS: {activeRecord.divergence_class}
              </span>
            </div>
          </div>

          {/* Numbers comparison */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 my-5 p-4 rounded-lg bg-slate-50 dark:bg-slate-800/50 border border-slate-200/80 dark:border-slate-700/60 font-mono text-xs">
            <div>
              <span className="text-slate-500 block text-[11px] mb-1">RECOMPUTED (RAW LOG)</span>
              <span className="text-xl font-bold text-slate-900 dark:text-white">
                {Number(activeRecord.recomputed_quantity).toLocaleString()}
              </span>
            </div>
            <div>
              <span className="text-slate-500 block text-[11px] mb-1">SERVED (POSTGRES AGGREGATE)</span>
              <span className="text-xl font-bold text-slate-900 dark:text-white">
                {Number(activeRecord.served_quantity).toLocaleString()}
              </span>
            </div>
            <div>
              <span className="text-slate-500 block text-[11px] mb-1">DIFFERENCE</span>
              <span
                className={`text-xl font-bold ${
                  Number(activeRecord.difference) === 0
                    ? 'text-emerald-600 dark:text-emerald-400'
                    : 'text-rose-600 dark:text-rose-400'
                }`}
              >
                {activeRecord.difference}
              </span>
            </div>
          </div>

          {/* Evidence Trail */}
          <div>
            <h5 className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400 mb-2">
              Divergence Evidence Trail
            </h5>
            {activeRecord.evidence && activeRecord.evidence.length > 0 ? (
              <ul className="space-y-1.5 text-xs font-mono text-slate-700 dark:text-slate-300 bg-slate-100/70 dark:bg-slate-950/60 p-3.5 rounded-lg border border-slate-200 dark:border-slate-800">
                {activeRecord.evidence.map((ev, i) => (
                  <li key={i} className="flex items-start space-x-2">
                    <span className="text-indigo-500">•</span>
                    <span>{ev}</span>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-xs text-slate-400">No specific evidence attached.</p>
            )}
          </div>
        </div>
      )}

      {/* Audit History */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden">
        <div className="p-5 border-b border-slate-200 dark:border-slate-800">
          <h4 className="text-base font-semibold text-slate-900 dark:text-white">
            Reconciliation History
          </h4>
          <p className="text-xs text-slate-500 dark:text-slate-400">
            Recorded replay-and-compare audit runs
          </p>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="bg-slate-50 dark:bg-slate-800/60 text-slate-500 dark:text-slate-400 border-b border-slate-200 dark:border-slate-800">
                <th className="py-3 px-4 font-semibold">Tenant</th>
                <th className="py-3 px-4 font-semibold">Metric</th>
                <th className="py-3 px-4 font-semibold">Classification</th>
                <th className="py-3 px-4 font-semibold">Recomputed</th>
                <th className="py-3 px-4 font-semibold">Served</th>
                <th className="py-3 px-4 font-semibold">Diff</th>
                <th className="py-3 px-4 font-semibold">Window Range</th>
                <th className="py-3 px-4 font-semibold">Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
              {history.length === 0 ? (
                <tr>
                  <td colSpan={8} className="py-8 text-center text-slate-400 font-mono">
                    No audits executed yet.
                  </td>
                </tr>
              ) : (
                history.map((rec, i) => (
                  <tr
                    key={i}
                    className="hover:bg-slate-50/50 dark:hover:bg-slate-800/40 transition cursor-pointer"
                    onClick={() => setActiveRecord(rec)}
                  >
                    <td className="py-3 px-4 font-mono font-bold text-slate-900 dark:text-white">
                      {rec.tenant_id}
                    </td>
                    <td className="py-3 px-4 font-mono text-slate-600 dark:text-slate-400">
                      {rec.metric}
                    </td>
                    <td className="py-3 px-4">
                      <span
                        className={`inline-block px-2 py-0.5 rounded text-[10px] font-bold ${
                          DIVERGENCE_BADGE[rec.divergence_class]
                        }`}
                      >
                        {rec.divergence_class}
                      </span>
                    </td>
                    <td className="py-3 px-4 font-mono text-slate-700 dark:text-slate-300">
                      {rec.recomputed_quantity}
                    </td>
                    <td className="py-3 px-4 font-mono text-slate-700 dark:text-slate-300">
                      {rec.served_quantity}
                    </td>
                    <td className="py-3 px-4 font-mono font-bold">
                      <span className={Number(rec.difference) === 0 ? 'text-emerald-600' : 'text-rose-600'}>
                        {rec.difference}
                      </span>
                    </td>
                    <td className="py-3 px-4 font-mono text-slate-500 text-[11px]">
                      {new Date(rec.window_start).toLocaleTimeString()} → {new Date(rec.window_end).toLocaleTimeString()}
                    </td>
                    <td className="py-3 px-4">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setActiveRecord(rec);
                        }}
                        className="text-indigo-600 dark:text-indigo-400 hover:underline font-mono text-[11px]"
                      >
                        Drill Down
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
