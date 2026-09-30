import React from 'react';
import type { WindowOverview } from '../types.generated';

interface WindowsViewProps {
  windows: WindowOverview[];
  onRefresh: () => void;
}

export const WindowsView: React.FC<WindowsViewProps> = ({ windows, onRefresh }) => {
  return (
    <div className="space-y-6">
      {/* Lifecycle Flow Header */}
      <div className="bg-white dark:bg-slate-900 rounded-xl p-5 border border-slate-200 dark:border-slate-800 shadow-sm">
        <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-3">
          Tumbling Window Lifecycle (ADR-0004 & ADR-0009)
        </h4>
        <div className="grid grid-cols-1 sm:grid-cols-4 gap-3 text-xs">
          <div className="p-3 rounded-lg border border-emerald-200 dark:border-emerald-900/60 bg-emerald-50/50 dark:bg-emerald-950/20">
            <span className="font-bold text-emerald-700 dark:text-emerald-400 block mb-1">
              1. OPEN
            </span>
            <p className="text-[11px] text-slate-600 dark:text-slate-400">
              Current window receiving in-order and early out-of-order events.
            </p>
          </div>

          <div className="p-3 rounded-lg border border-amber-200 dark:border-amber-900/60 bg-amber-50/50 dark:bg-amber-950/20">
            <span className="font-bold text-amber-700 dark:text-amber-400 block mb-1">
              2. SEALING
            </span>
            <p className="text-[11px] text-slate-600 dark:text-slate-400">
              Window duration ended; waiting for partition watermark to advance.
            </p>
          </div>

          <div className="p-3 rounded-lg border border-blue-200 dark:border-blue-900/60 bg-blue-50/50 dark:bg-blue-950/20">
            <span className="font-bold text-blue-700 dark:text-blue-400 block mb-1">
              3. SEALED
            </span>
            <p className="text-[11px] text-slate-600 dark:text-slate-400">
              Watermark passed; row frozen and emitted to transactional outbox.
            </p>
          </div>

          <div className="p-3 rounded-lg border border-purple-200 dark:border-purple-900/60 bg-purple-50/50 dark:bg-purple-950/20">
            <span className="font-bold text-purple-700 dark:text-purple-400 block mb-1">
              4. CORRECTED
            </span>
            <p className="text-[11px] text-slate-600 dark:text-slate-400">
              Late arrival audit emitted new append-only revision (rev &gt; 0).
            </p>
          </div>
        </div>
      </div>

      {/* Aggregates Table */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden">
        <div className="p-5 border-b border-slate-200 dark:border-slate-800 flex items-center justify-between">
          <div>
            <h3 className="text-base font-semibold text-slate-900 dark:text-white">
              Window Aggregates Ledger
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Append-only storage in Postgres: no UPDATE of a sealed row, ever
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
                <th className="py-3 px-4 font-semibold">Metric</th>
                <th className="py-3 px-4 font-semibold">Status</th>
                <th className="py-3 px-4 font-semibold">Revision</th>
                <th className="py-3 px-4 font-semibold">Window Range</th>
                <th className="py-3 px-4 font-semibold">Billed Quantity</th>
                <th className="py-3 px-4 font-semibold">Events</th>
                <th className="py-3 px-4 font-semibold">Sealed At</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
              {windows.length === 0 ? (
                <tr>
                  <td colSpan={8} className="py-8 text-center text-slate-400 font-mono">
                    No window aggregates yet recorded.
                  </td>
                </tr>
              ) : (
                windows.map((w, i) => {
                  const isCorrected = w.status === 'CORRECTED' || w.revision > 0;
                  const isSealed = w.status === 'SEALED';
                  const isSealing = w.status === 'SEALING';

                  return (
                    <tr
                      key={`${w.tenant_id}-${w.window_start}-${w.revision}-${i}`}
                      className="hover:bg-slate-50/50 dark:hover:bg-slate-800/40 transition"
                    >
                      <td className="py-3 px-4 font-mono font-bold text-slate-900 dark:text-white">
                        {w.tenant_id}
                      </td>
                      <td className="py-3 px-4 font-mono text-slate-600 dark:text-slate-400">
                        {w.metric}
                      </td>
                      <td className="py-3 px-4">
                        <span
                          className={`inline-block px-2 py-0.5 rounded text-[10px] font-bold ${
                            isCorrected
                              ? 'bg-purple-100 text-purple-800 dark:bg-purple-950 dark:text-purple-300'
                              : isSealed
                              ? 'bg-blue-100 text-blue-800 dark:bg-blue-950 dark:text-blue-300'
                              : isSealing
                              ? 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300'
                              : 'bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300'
                          }`}
                        >
                          {w.status}
                        </span>
                      </td>
                      <td className="py-3 px-4 font-mono">
                        <span
                          className={`px-1.5 py-0.5 rounded text-[10px] font-bold ${
                            w.revision > 0
                              ? 'bg-purple-200 text-purple-900 dark:bg-purple-900 dark:text-purple-200'
                              : 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400'
                          }`}
                        >
                          rev {w.revision}
                        </span>
                      </td>
                      <td className="py-3 px-4 font-mono text-slate-500 text-[11px]">
                        <div>{new Date(w.window_start).toLocaleTimeString()}</div>
                        <div className="text-slate-400">→ {new Date(w.window_end).toLocaleTimeString()}</div>
                      </td>
                      <td className="py-3 px-4 font-mono font-bold text-emerald-600 dark:text-emerald-400">
                        {Number(w.quantity).toLocaleString()}
                      </td>
                      <td className="py-3 px-4 font-mono text-slate-600 dark:text-slate-300">
                        {w.event_count.toLocaleString()}
                      </td>
                      <td className="py-3 px-4 font-mono text-[11px] text-slate-400">
                        {w.sealed_at ? new Date(w.sealed_at).toLocaleTimeString() : '—'}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
