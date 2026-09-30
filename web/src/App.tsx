import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from './api/client';
import { Header } from './components/Header';
import { OverviewView } from './components/OverviewView';
import { TenantsView } from './components/TenantsView';
import { WindowsView } from './components/WindowsView';
import { OutboxView } from './components/OutboxView';
import { ReconciliationView } from './components/ReconciliationView';

export const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState<string>('overview');
  const [autoRefresh, setAutoRefresh] = useState<boolean>(true);
  const [darkMode, setDarkMode] = useState<boolean>(() => {
    return document.documentElement.classList.contains('dark');
  });

  const toggleDarkMode = () => {
    const next = !darkMode;
    setDarkMode(next);
    document.documentElement.classList.toggle('dark', next);
    try {
      localStorage.setItem('tally.theme', next ? 'dark' : 'light');
    } catch (_) {}
  };

  // Queries
  const {
    data: overview,
    refetch: refetchOverview,
    isFetching: isFetchingOverview,
  } = useQuery({
    queryKey: ['overview'],
    queryFn: api.getOverview,
    refetchInterval: autoRefresh ? 3000 : false,
  });

  const {
    data: tenants = [],
    refetch: refetchTenants,
    isFetching: isFetchingTenants,
  } = useQuery({
    queryKey: ['tenants'],
    queryFn: api.getTenants,
    refetchInterval: autoRefresh ? 3000 : false,
  });

  const {
    data: windows = [],
    refetch: refetchWindows,
    isFetching: isFetchingWindows,
  } = useQuery({
    queryKey: ['windows'],
    queryFn: api.getWindows,
    refetchInterval: autoRefresh ? 3000 : false,
  });

  const {
    data: outbox = [],
    refetch: refetchOutbox,
    isFetching: isFetchingOutbox,
  } = useQuery({
    queryKey: ['outbox'],
    queryFn: api.getOutbox,
    refetchInterval: autoRefresh ? 3000 : false,
  });

  const {
    data: history = [],
    refetch: refetchHistory,
    isFetching: isFetchingHistory,
  } = useQuery({
    queryKey: ['reconciliationHistory'],
    queryFn: api.getReconciliationHistory,
    refetchInterval: autoRefresh ? 3000 : false,
  });

  const isFetching =
    isFetchingOverview ||
    isFetchingTenants ||
    isFetchingWindows ||
    isFetchingOutbox ||
    isFetchingHistory;

  const handleRefreshAll = () => {
    refetchOverview();
    refetchTenants();
    refetchWindows();
    refetchOutbox();
    refetchHistory();
  };

  return (
    <div className="min-h-screen flex flex-col">
      <Header
        status={overview?.status || 'connecting'}
        env={overview?.env || 'local'}
        isFetching={isFetching}
        autoRefresh={autoRefresh}
        onToggleAutoRefresh={() => setAutoRefresh(!autoRefresh)}
        onRefresh={handleRefreshAll}
        darkMode={darkMode}
        onToggleDarkMode={toggleDarkMode}
        activeTab={activeTab}
        onSelectTab={setActiveTab}
      />

      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {activeTab === 'overview' && (
          <OverviewView overview={overview || null} onRefresh={handleRefreshAll} />
        )}
        {activeTab === 'tenants' && (
          <TenantsView tenants={tenants} onRefresh={handleRefreshAll} />
        )}
        {activeTab === 'windows' && (
          <WindowsView windows={windows} onRefresh={handleRefreshAll} />
        )}
        {activeTab === 'outbox' && (
          <OutboxView outbox={outbox} onRefresh={handleRefreshAll} />
        )}
        {activeTab === 'reconcile' && (
          <ReconciliationView
            tenants={tenants}
            history={history}
            onRefresh={handleRefreshAll}
          />
        )}
      </main>

      <footer className="border-t border-slate-200 dark:border-slate-800 py-6 text-center text-xs text-slate-500 font-mono">
        Tally Usage Metering & Quotas Platform · ADR-0001 through ADR-0012 · Zero Float Billable Invariant
      </footer>
    </div>
  );
};
