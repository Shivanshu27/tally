import React, { useState } from 'react';
import type { DashboardOverview } from '../types.generated';
import { api } from '../api/client';

interface OverviewViewProps {
  overview: DashboardOverview | null;
  onRefresh: () => void;
}

export const OverviewView: React.FC<OverviewViewProps> = ({ overview, onRefresh }) => {
  const [ingesting, setIngesting] = useState(false);
  const [ingestStatus, setIngestStatus] = useState<string | null>(null);

  if (!overview) {
    return (
      <div className="flex items-center justify-center p-12 text-slate-500">
        Loading pipeline telemetry...
      </div>
    );
  }

  const bufferPct = Math.min(
    100,
    Math.round((overview.buffer_depth / (overview.shed_watermark || 50000)) * 100)
  );

  const handleSendTestBatch = async (priority: 'high' | 'low') => {
    try {
      setIngesting(true);
      setIngestStatus(null);
      const nowIso = new Date().toISOString();
      const events = Array.from({ length: 10 }).map((_, i) => ({
        event_id: `evt-${Date.now()}-${i}-${Math.random().toString(36).substring(2, 7)}`,
        tenant_id: 'acme-corp',
        metric: 'api_calls',
        quantity: 1,
        occurred_at: nowIso,
        received_at: nowIso,
        priority: priority,
      }));

      const res = await api.ingestBatch({ events });
      setIngestStatus(
        `Batch ingested: ${res.accepted_count} accepted, ${res.rejected_count} rejected.`
      );
      onRefresh();
    } catch (err: any) {
      setIngestStatus(`Error: ${err.message}`);
    } finally {
      setIngesting(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Top Banner: Ingest Health & Throttle status */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {/* Card 1: Produce Queue Depth */}
        <div className="bg-white dark:bg-slate-900 rounded-xl p-5 border border-slate-200 dark:border-slate-800 shadow-sm">
          <div className="flex items-center justify-between text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">
            <span>Produce Queue Depth</span>
            <span className="font-mono text-slate-400">High: {overview.high_watermark}</span>
          </div>
          <div className="mt-3 flex items-baseline space-x-2">
            <span className="text-3xl font-bold font-mono text-slate-900 dark:text-white">
              {overview.buffer_depth}
            </span>
            <span className="text-xs text-slate-500">events</span>
          </div>
          {/* Gauge bar */}
          <div className="mt-3">
            <div className="w-full bg-slate-100 dark:bg-slate-800 rounded-full h-2 overflow-hidden">
              <div
                className={`h-2 rounded-full transition-all duration-500 ${
                  bufferPct > 80
                    ? 'bg-rose-500'
                    : bufferPct > 40
                    ? 'bg-amber-500'
                    : 'bg-emerald-500'
                }`}
                style={{ width: `${Math.max(4, bufferPct)}%` }}
              />
            </div>
            <div className="flex justify-between text-[11px] text-slate-400 mt-1 font-mono">
              <span>0</span>
              <span>Shed Limit: {overview.shed_watermark}</span>
            </div>
          </div>
        </div>

        {/* Card 2: Tier 1 Bloom Filter */}
        <div className="bg-white dark:bg-slate-900 rounded-xl p-5 border border-slate-200 dark:border-slate-800 shadow-sm">
          <div className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">
            Tier 1 Bloom Filter
          </div>
          <div className="mt-3 flex items-baseline space-x-2">
            <span className="text-3xl font-bold font-mono text-indigo-600 dark:text-indigo-400">
              {overview.bloom_count.toLocaleString()}
            </span>
            <span className="text-xs text-slate-500">indexed</span>
          </div>
          <p className="mt-3 text-xs text-slate-500 dark:text-slate-400">
            FPR Target: 0.1% · In-memory fast reject filter (ADR-0005)
          </p>
        </div>

        {/* Card 3: Prioritized Load Shedding */}
        <div className="bg-white dark:bg-slate-900 rounded-xl p-5 border border-slate-200 dark:border-slate-800 shadow-sm">
          <div className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">
            Low-Priority Shed Count
          </div>
          <div className="mt-3 flex items-baseline space-x-2">
            <span className="text-3xl font-bold font-mono text-amber-600 dark:text-amber-400">
              {overview.shed_count.toLocaleString()}
            </span>
            <span className="text-xs text-slate-500">shed</span>
          </div>
          <p className="mt-3 text-xs text-slate-500 dark:text-slate-400">
            Telemetry shed when buffer exceeds high watermark. Billable events preserved.
          </p>
        </div>

        {/* Card 4: Outbox Pending */}
        <div className="bg-white dark:bg-slate-900 rounded-xl p-5 border border-slate-200 dark:border-slate-800 shadow-sm">
          <div className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">
            Outbox Backlog
          </div>
          <div className="mt-3 flex items-baseline space-x-2">
            <span className="text-3xl font-bold font-mono text-slate-900 dark:text-white">
              {overview.outbox_pending}
            </span>
            <span className="text-xs text-slate-500">pending</span>
          </div>
          <p className="mt-3 text-xs text-slate-500 dark:text-slate-400">
            Transactional outbox relay delivering to billing sinks (ADR-0006).
          </p>
        </div>
      </div>

      {/* Partition Lag Breakdown */}
      <div className="bg-white dark:bg-slate-900 rounded-xl p-6 border border-slate-200 dark:border-slate-800 shadow-sm">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="text-base font-semibold text-slate-900 dark:text-white">
              Consumer Lag per Partition
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Partitioned by tenant_id hash · Aggregator worker consumer lag (ADR-0003)
            </p>
          </div>
          <div className="text-xs font-mono text-slate-500">
            Total Partitions: {overview.partitions.length}
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {overview.partitions.map((p) => {
            const isZeroLag = p.lag === 0;
            return (
              <div
                key={p.partition}
                className="p-4 rounded-lg bg-slate-50 dark:bg-slate-800/50 border border-slate-200/80 dark:border-slate-700/60"
              >
                <div className="flex items-center justify-between text-xs">
                  <span className="font-semibold text-slate-700 dark:text-slate-300">
                    Partition {p.partition}
                  </span>
                  <span
                    className={`px-2 py-0.5 rounded-full text-[10px] font-bold ${
                      isZeroLag
                        ? 'bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300'
                        : 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300'
                    }`}
                  >
                    {isZeroLag ? 'UP TO DATE' : 'LAGGING'}
                  </span>
                </div>

                <div className="mt-3 flex items-baseline space-x-1">
                  <span className="text-2xl font-bold font-mono text-slate-900 dark:text-white">
                    {p.lag}
                  </span>
                  <span className="text-xs text-slate-400">msgs</span>
                </div>

                <div className="mt-2 text-[11px] text-slate-500 dark:text-slate-400 flex justify-between">
                  <span>Watermark Lag:</span>
                  <span className="font-mono text-slate-700 dark:text-slate-300">
                    {p.watermark_lag_ms ?? 0} ms
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Interactive Batch Ingest Test */}
      <div className="bg-slate-900 text-slate-100 rounded-xl p-6 shadow-sm border border-slate-800">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div>
            <h3 className="text-base font-semibold text-white">Live Ingest Simulation</h3>
            <p className="text-xs text-slate-400 mt-0.5">
              Fire an authenticated 10-event batch into the async POST /v1/events pipeline.
            </p>
          </div>
          <div className="flex items-center space-x-3">
            <button
              onClick={() => handleSendTestBatch('high')}
              disabled={ingesting}
              className="px-3.5 py-2 text-xs font-semibold rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition disabled:opacity-50 flex items-center space-x-2"
            >
              <span>{ingesting ? 'Sending...' : 'Send Billable Batch (High)'}</span>
            </button>
            <button
              onClick={() => handleSendTestBatch('low')}
              disabled={ingesting}
              className="px-3.5 py-2 text-xs font-semibold rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 transition disabled:opacity-50"
            >
              <span>Send Telemetry Batch (Low)</span>
            </button>
          </div>
        </div>

        {ingestStatus && (
          <div className="mt-4 p-3 rounded-lg bg-slate-800/80 border border-slate-700 text-xs font-mono text-emerald-400">
            {ingestStatus}
          </div>
        )}
      </div>
    </div>
  );
};
