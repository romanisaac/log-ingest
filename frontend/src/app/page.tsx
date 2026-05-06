'use client';

import dynamic from 'next/dynamic';
import { loader } from '@monaco-editor/react';
import { useState, useCallback } from 'react';

// Load Monaco from CDN so the large worker bundles are not embedded in the binary.
loader.config({
  paths: { vs: 'https://cdn.jsdelivr.net/npm/monaco-editor@0.52.0/min/vs' },
});

const MonacoEditor = dynamic(() => import('@monaco-editor/react'), {
  ssr: false,
  loading: () => <div className="h-full bg-gray-900 animate-pulse rounded" />,
});

interface Stats {
  rows_scanned: number;
  files_pruned: number;
  bytes_scanned: number;
  duration_ms: number;
}

interface QueryError {
  code?: string;
  message: string;
}

function toNs(datetimeLocal: string): number {
  return new Date(datetimeLocal).getTime() * 1_000_000;
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1048576) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1048576).toFixed(1)} MB`;
}

function CellValue({ value }: { value: unknown }) {
  if (value === null || value === undefined)
    return <span className="text-gray-600 italic">null</span>;
  return <>{String(value)}</>;
}

export default function Home() {
  const [sql, setSql] = useState('SELECT * FROM logs LIMIT 100');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<QueryError | null>(null);
  const [rows, setRows] = useState<Record<string, unknown>[] | null>(null);
  const [stats, setStats] = useState<Stats | null>(null);

  const run = useCallback(async () => {
    if (!from || !to) {
      setError({ message: 'Set both From and To before running.' });
      return;
    }
    setLoading(true);
    setError(null);
    setRows(null);
    setStats(null);

    try {
      const res = await fetch('/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: sql.trim(),
          time_from: toNs(from),
          time_to: toNs(to),
          limit: 1000,
        }),
      });

      const text = await res.text();

      if (!res.ok) {
        const json = JSON.parse(text);
        setError({ code: json.code, message: json.error ?? `HTTP ${res.status}` });
        return;
      }

      const lines = text
        .split('\n')
        .filter(Boolean)
        .map((l) => JSON.parse(l));

      const statsLine = lines.pop() as Stats;
      setRows(lines as Record<string, unknown>[]);
      setStats(statsLine);
    } catch (e) {
      setError({ message: String(e) });
    } finally {
      setLoading(false);
    }
  }, [sql, from, to]);

  const columns = rows?.length ? Object.keys(rows[0]) : [];

  return (
    <main className="h-screen flex flex-col">
      {/* Header */}
      <header className="shrink-0 border-b border-gray-800 px-5 py-3 flex items-center gap-3">
        <span className="font-semibold text-blue-400 tracking-tight">log-ingest</span>
        <span className="text-gray-600 text-sm">query console</span>
      </header>

      <div className="flex flex-col flex-1 min-h-0 p-4 gap-3">
        {/* Editor card */}
        <div className="shrink-0 rounded-lg border border-gray-800 overflow-hidden">
          <div className="h-44">
            <MonacoEditor
              defaultLanguage="sql"
              value={sql}
              onChange={(v) => setSql(v ?? '')}
              theme="vs-dark"
              options={{
                minimap: { enabled: false },
                fontSize: 13,
                lineNumbers: 'off',
                scrollBeyondLastLine: false,
                wordWrap: 'on',
                padding: { top: 10 },
                renderLineHighlight: 'none',
              }}
            />
          </div>

          {/* Controls bar */}
          <div className="flex flex-wrap items-center gap-3 px-3 py-2 bg-gray-900 border-t border-gray-800">
            <label className="text-xs text-gray-400 flex items-center gap-1.5">
              From
              <input
                type="datetime-local"
                value={from}
                onChange={(e) => setFrom(e.target.value)}
                className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 focus:outline-none focus:border-blue-500"
              />
            </label>
            <label className="text-xs text-gray-400 flex items-center gap-1.5">
              To
              <input
                type="datetime-local"
                value={to}
                onChange={(e) => setTo(e.target.value)}
                className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 focus:outline-none focus:border-blue-500"
              />
            </label>
            <button
              onClick={run}
              disabled={loading}
              className="ml-auto px-4 py-1.5 rounded bg-blue-600 hover:bg-blue-500 active:bg-blue-700 disabled:opacity-40 text-sm font-medium transition-colors"
            >
              {loading ? 'Running…' : 'Run Query'}
            </button>
          </div>
        </div>

        {/* Error banner */}
        {error && (
          <div className="shrink-0 flex items-start gap-2 rounded-lg border border-red-800 bg-red-950/40 px-4 py-3 text-sm text-red-300">
            {error.code && (
              <span className="mt-0.5 font-mono text-xs bg-red-900 rounded px-1.5 py-0.5 shrink-0">
                {error.code}
              </span>
            )}
            <span>{error.message}</span>
          </div>
        )}

        {/* Stats bar */}
        {stats && (
          <div className="shrink-0 flex items-center gap-4 text-xs text-gray-500">
            <span className="text-gray-300 font-medium">{rows?.length ?? 0} rows</span>
            <span>{stats.rows_scanned.toLocaleString()} scanned</span>
            <span>{stats.files_pruned} files pruned</span>
            <span>{fmtBytes(stats.bytes_scanned)}</span>
            <span>{stats.duration_ms} ms</span>
          </div>
        )}

        {/* Results table */}
        {rows !== null && (
          <div className="flex-1 min-h-0 overflow-auto rounded-lg border border-gray-800">
            {rows.length === 0 ? (
              <div className="flex items-center justify-center h-full text-gray-600 text-sm">
                Query returned no rows
              </div>
            ) : (
              <table className="w-full text-xs border-collapse">
                <thead className="sticky top-0 z-10 bg-gray-900">
                  <tr>
                    {columns.map((col) => (
                      <th
                        key={col}
                        className="text-left px-3 py-2 border-b border-gray-700 font-medium text-gray-400 whitespace-nowrap"
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, i) => (
                    <tr
                      key={i}
                      className="border-b border-gray-800/60 hover:bg-gray-900/40 transition-colors"
                    >
                      {columns.map((col) => (
                        <td
                          key={col}
                          className="px-3 py-1.5 font-mono text-gray-300 whitespace-nowrap max-w-xs truncate"
                          title={row[col] === null ? 'null' : String(row[col])}
                        >
                          <CellValue value={row[col]} />
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}

        {/* Empty state */}
        {!rows && !loading && !error && (
          <div className="flex-1 flex items-center justify-center text-gray-600 text-sm">
            Enter SQL + time range and click Run Query
          </div>
        )}
      </div>
    </main>
  );
}
