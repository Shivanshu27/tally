import React, { useState } from 'react';
import type { OutboxMessage } from '../types.generated';

interface OutboxViewProps {
  outbox: OutboxMessage[];
  onRefresh: () => void;
}

export const OutboxView: React.FC<OutboxViewProps> = ({ outbox, onRefresh }) => {
  const [expandedId, setExpandedId] = useState<number | null>(null);

  const pendingCount = outbox.filter((m) => !m.published_at && (m.attempts || 0) < 5).length;
  const publishedCount = outbox.filter((m) => !!m.published_at).length;
  const deadLetterCount = outbox.filter((m) => !m.published_at && (m.attempts || 0) >= 5).length;

  return (
    <div className="space-y-6">
      {/* Outbox Metrics Banner */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="bg-white dark:bg-slate-900 rounded-xl p-5 border border-slate-200 dark:border-slate-800 shadow-sm">
          <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
            Pending Delivery
          </div>
          <div className="mt-2 text-2xl font-bold font-mono text-amber-600 dark:text-amber-400">
            {pendingCount}
          </div>
          <p className="mt-1 text-xs text-slate-400">
            Awaiting dispatch by OutboxRelayWorker (ADR-0006)
          </p>
        </div>

        <div className="bg-white dark:bg-slate-900 rounded-xl p-5 border border-slate-200 dark:border-slate-800 shadow-sm">
          <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
            Published to Billing Sink
          </div>
          <div className="mt-2 text-2xl font-bold font-mono text-emerald-600 dark:text-emerald-400">
            {publishedCount}
          </div>
          <p className="mt-1 text-xs text-slate-400">
            Delivered idempotently with unique aggregate_key
          </p>
        </div>

        <div className="bg-white dark:bg-slate-900 rounded-xl p-5 border border-slate-200 dark:border-slate-800 shadow-sm">
          <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
            Dead-Letter Count
          </div>
          <div className="mt-2 text-2xl font-bold font-mono text-rose-600 dark:text-rose-400">
            {deadLetterCount}
          </div>
          <p className="mt-1 text-xs text-slate-400">
            Retries exhausted (&gt;= 5 attempts) requiring admin audit
          </p>
        </div>
      </div>

      {/* Outbox Ledger Table */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden">
        <div className="p-5 border-b border-slate-200 dark:border-slate-800 flex items-center justify-between">
          <div>
            <h3 className="text-base font-semibold text-slate-900 dark:text-white">
              Transactional Outbox Messages
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Dual-write prevention: aggregates and outbox commit in the same Postgres transaction
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
                <th className="py-3 px-4 font-semibold">ID</th>
                <th className="py-3 px-4 font-semibold">Aggregate Key</th>
                <th className="py-3 px-4 font-semibold">Status</th>
                <th className="py-3 px-4 font-semibold">Attempts</th>
                <th className="py-3 px-4 font-semibold">Created At</th>
                <th className="py-3 px-4 font-semibold">Published At</th>
                <th className="py-3 px-4 font-semibold">Payload</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
              {outbox.length === 0 ? (
                <tr>
                  <td colSpan={7} className="py-8 text-center text-slate-400 font-mono">
                    No outbox messages currently stored.
                  </td>
                </tr>
              ) : (
                outbox.map((msg) => {
                  const isPublished = !!msg.published_at;
                  const isDeadLetter = !isPublished && (msg.attempts || 0) >= 5;
                  const isExpanded = expandedId === msg.id;

                  return (
                    <React.Fragment key={msg.id ?? msg.aggregate_key}>
                      <tr className="hover:bg-slate-50/50 dark:hover:bg-slate-800/40 transition">
                        <td className="py-3 px-4 font-mono font-bold text-slate-900 dark:text-white">
                          #{msg.id ?? '—'}
                        </td>
                        <td className="py-3 px-4 font-mono text-slate-700 dark:text-slate-300">
                          {msg.aggregate_key}
                        </td>
                        <td className="py-3 px-4">
                          <span
                            className={`inline-block px-2 py-0.5 rounded text-[10px] font-bold ${
                              isPublished
                                ? 'bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300'
                                : isDeadLetter
                                ? 'bg-rose-100 text-rose-800 dark:bg-rose-950 dark:text-rose-300'
                                : 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300'
                            }`}
                          >
                            {isPublished ? 'PUBLISHED' : isDeadLetter ? 'DEAD LETTER' : 'PENDING'}
                          </span>
                        </td>
                        <td className="py-3 px-4 font-mono text-slate-600 dark:text-slate-400">
                          {msg.attempts || 0}
                        </td>
                        <td className="py-3 px-4 font-mono text-slate-500 text-[11px]">
                          {msg.created_at ? new Date(msg.created_at).toLocaleTimeString() : '—'}
                        </td>
                        <td className="py-3 px-4 font-mono text-slate-500 text-[11px]">
                          {msg.published_at ? new Date(msg.published_at).toLocaleTimeString() : '—'}
                        </td>
                        <td className="py-3 px-4">
                          <button
                            onClick={() => setExpandedId(isExpanded ? null : (msg.id ?? null))}
                            className="text-[11px] font-mono text-indigo-600 dark:text-indigo-400 hover:underline"
                          >
                            {isExpanded ? 'Hide' : 'Inspect JSON'}
                          </button>
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr className="bg-slate-900 text-slate-100">
                          <td colSpan={7} className="p-4 font-mono text-[11px]">
                            <pre className="overflow-x-auto whitespace-pre-wrap max-h-48 text-emerald-400">
                              {JSON.stringify(msg.payload, null, 2)}
                            </pre>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
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
