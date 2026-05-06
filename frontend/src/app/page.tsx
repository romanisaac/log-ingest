'use client';

import dynamic from 'next/dynamic';
import { loader } from '@monaco-editor/react';
import { useState, useCallback, useRef, useEffect, useMemo } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';

loader.config({
  paths: { vs: 'https://cdn.jsdelivr.net/npm/monaco-editor@0.52.0/min/vs' },
});

const MonacoEditor = dynamic(() => import('@monaco-editor/react'), {
  ssr: false,
  loading: () => <div className="h-full bg-gray-900 animate-pulse rounded" />,
});

// ── Types ─────────────────────────────────────────────────────────────────────

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

interface HistoryEntry {
  sql: string;
  from: string;
  to: string;
  ts: number;
}

interface SavedView {
  name: string;
  sql: string;
  from: string;
  to: string;
}

// ── localStorage helpers ──────────────────────────────────────────────────────

const HISTORY_KEY = 'log-ingest-query-history';
const VIEWS_KEY = 'log-ingest-saved-views';

function loadHistory(): HistoryEntry[] {
  try { return JSON.parse(localStorage.getItem(HISTORY_KEY) ?? '[]'); }
  catch { return []; }
}

function appendHistory(entry: HistoryEntry): HistoryEntry[] {
  const prev = loadHistory();
  // deduplicate by sql+from+to, most-recent first, cap at 50
  const next = [
    entry,
    ...prev.filter(e => !(e.sql === entry.sql && e.from === entry.from && e.to === entry.to)),
  ].slice(0, 50);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(next));
  return next;
}

function loadViews(): SavedView[] {
  try { return JSON.parse(localStorage.getItem(VIEWS_KEY) ?? '[]'); }
  catch { return []; }
}

function upsertView(view: SavedView): SavedView[] {
  const next = [view, ...loadViews().filter(v => v.name !== view.name)];
  localStorage.setItem(VIEWS_KEY, JSON.stringify(next));
  return next;
}

function removeView(name: string): SavedView[] {
  const next = loadViews().filter(v => v.name !== name);
  localStorage.setItem(VIEWS_KEY, JSON.stringify(next));
  return next;
}

// ── Misc helpers ──────────────────────────────────────────────────────────────

function toNs(datetimeLocal: string): number {
  return new Date(datetimeLocal).getTime() * 1_000_000;
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1048576) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1048576).toFixed(1)} MB`;
}

function fmtDateShort(ts: number): string {
  return new Date(ts).toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'short' });
}

// ── Virtualized results table ─────────────────────────────────────────────────

function ResultsTable({
  rows,
  columns,
}: {
  rows: Record<string, unknown>[];
  columns: string[];
}) {
  const parentRef = useRef<HTMLDivElement>(null);
  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 28,
    overscan: 20,
  });

  const virtualItems = rowVirtualizer.getVirtualItems();
  const totalSize = rowVirtualizer.getTotalSize();
  const paddingTop = virtualItems.length > 0 ? virtualItems[0].start : 0;
  const paddingBottom =
    virtualItems.length > 0 ? totalSize - virtualItems[virtualItems.length - 1].end : 0;

  return (
    <div ref={parentRef} className="overflow-auto h-full">
      <table className="w-full text-xs border-collapse">
        <thead className="sticky top-0 z-10 bg-gray-900">
          <tr>
            {columns.map(col => (
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
          {paddingTop > 0 && (
            <tr>
              <td colSpan={columns.length} style={{ height: paddingTop }} />
            </tr>
          )}
          {virtualItems.map(vRow => {
            const row = rows[vRow.index];
            return (
              <tr
                key={vRow.index}
                className="border-b border-gray-800/60 hover:bg-gray-900/40"
              >
                {columns.map(col => (
                  <td
                    key={col}
                    className="px-3 py-1.5 font-mono text-gray-300 whitespace-nowrap max-w-xs truncate"
                    title={row[col] == null ? 'null' : String(row[col])}
                  >
                    {row[col] == null ? (
                      <span className="text-gray-600 italic">null</span>
                    ) : (
                      String(row[col])
                    )}
                  </td>
                ))}
              </tr>
            );
          })}
          {paddingBottom > 0 && (
            <tr>
              <td colSpan={columns.length} style={{ height: paddingBottom }} />
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

// ── Dropdown shell ────────────────────────────────────────────────────────────

function Dropdown({
  label,
  open,
  onToggle,
  children,
  width = 'w-80',
}: {
  label: string;
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
  width?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function onClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) onToggle();
    }
    document.addEventListener('mousedown', onClickOutside);
    return () => document.removeEventListener('mousedown', onClickOutside);
  }, [open, onToggle]);

  return (
    <div ref={ref} className="relative">
      <button
        onClick={onToggle}
        className="px-3 py-1.5 text-xs rounded border border-gray-700 hover:border-gray-500 transition-colors text-gray-400"
      >
        {label}
      </button>
      {open && (
        <div
          className={`absolute right-0 top-full mt-1 ${width} bg-gray-900 border border-gray-700 rounded-lg shadow-xl z-50`}
        >
          {children}
        </div>
      )}
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function Home() {
  const [sql, setSql] = useState('SELECT * FROM logs LIMIT 100');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<QueryError | null>(null);
  const [rows, setRows] = useState<Record<string, unknown>[] | null>(null);
  const [stats, setStats] = useState<Stats | null>(null);

  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [savedViews, setSavedViews] = useState<SavedView[]>([]);
  const [showHistory, setShowHistory] = useState(false);
  const [showSaved, setShowSaved] = useState(false);
  const [saveViewName, setSaveViewName] = useState('');
  const [showSaveInput, setShowSaveInput] = useState(false);

  const abortRef = useRef<AbortController | null>(null);
  const rowBufferRef = useRef<Record<string, unknown>[]>([]);
  const rafRef = useRef<number>(0);

  useEffect(() => {
    setHistory(loadHistory());
    setSavedViews(loadViews());
  }, []);

  const scheduleFlush = useCallback(() => {
    if (rafRef.current) return;
    rafRef.current = requestAnimationFrame(() => {
      rafRef.current = 0;
      setRows([...rowBufferRef.current]);
    });
  }, []);

  const cancel = useCallback(() => {
    abortRef.current?.abort();
  }, []);

  const run = useCallback(async () => {
    if (!from || !to) {
      setError({ message: 'Set both From and To before running.' });
      return;
    }

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setLoading(true);
    setError(null);
    setRows(null);
    setStats(null);
    rowBufferRef.current = [];
    if (rafRef.current) {
      cancelAnimationFrame(rafRef.current);
      rafRef.current = 0;
    }

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
        signal: controller.signal,
      });

      if (!res.ok) {
        const text = await res.text();
        const json = JSON.parse(text);
        setError({ code: json.code, message: json.error ?? `HTTP ${res.status}` });
        return;
      }

      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let remainder = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        remainder += decoder.decode(value, { stream: true });
        const lines = remainder.split('\n');
        remainder = lines.pop() ?? '';

        for (const line of lines) {
          if (!line.trim()) continue;
          rowBufferRef.current.push(JSON.parse(line));
          scheduleFlush();
        }
      }

      if (remainder.trim()) {
        rowBufferRef.current.push(JSON.parse(remainder));
      }

      // Last line is always the stats object.
      const buffer = rowBufferRef.current;
      const statsLine = buffer[buffer.length - 1] as unknown as Stats;
      const dataRows = buffer.slice(0, -1);

      rowBufferRef.current = dataRows;
      if (rafRef.current) {
        cancelAnimationFrame(rafRef.current);
        rafRef.current = 0;
      }
      setRows(dataRows);
      setStats(statsLine);

      const newHistory = appendHistory({ sql: sql.trim(), from, to, ts: Date.now() });
      setHistory(newHistory);
    } catch (e) {
      if ((e as Error).name === 'AbortError') return;
      setError({ message: String(e) });
    } finally {
      setLoading(false);
    }
  }, [sql, from, to, scheduleFlush]);

  const applyHistory = useCallback((entry: HistoryEntry) => {
    setSql(entry.sql);
    setFrom(entry.from);
    setTo(entry.to);
    setShowHistory(false);
  }, []);

  const applySavedView = useCallback((view: SavedView) => {
    setSql(view.sql);
    setFrom(view.from);
    setTo(view.to);
    setShowSaved(false);
  }, []);

  const handleSaveView = useCallback(() => {
    if (!saveViewName.trim()) return;
    setSavedViews(upsertView({ name: saveViewName.trim(), sql: sql.trim(), from, to }));
    setSaveViewName('');
    setShowSaveInput(false);
  }, [saveViewName, sql, from, to]);

  const handleDeleteView = useCallback((name: string) => {
    setSavedViews(removeView(name));
  }, []);

  const firstRow = rows?.length ? rows[0] : null;
  const columns = useMemo(
    () => (firstRow ? Object.keys(firstRow) : []),
    [firstRow],
  );

  const toggleHistory = useCallback(() => {
    setShowHistory(v => !v);
    setShowSaved(false);
  }, []);

  const toggleSaved = useCallback(() => {
    setShowSaved(v => !v);
    setShowHistory(false);
  }, []);

  return (
    <main className="h-screen flex flex-col bg-gray-950 text-gray-100">
      {/* Header */}
      <header className="shrink-0 border-b border-gray-800 px-5 py-3 flex items-center gap-3">
        <span className="font-semibold text-blue-400 tracking-tight">log-ingest</span>
        <span className="text-gray-600 text-sm">query console</span>

        <div className="ml-auto flex items-center gap-2">
          {/* Saved Views */}
          <Dropdown
            label={`Saved Views${savedViews.length ? ` (${savedViews.length})` : ''}`}
            open={showSaved}
            onToggle={toggleSaved}
            width="w-80"
          >
            <div className="max-h-64 overflow-y-auto">
              {savedViews.length === 0 ? (
                <p className="text-xs text-gray-500 p-3">No saved views yet.</p>
              ) : (
                <ul>
                  {savedViews.map(view => (
                    <li
                      key={view.name}
                      className="flex items-center gap-2 px-3 py-2 hover:bg-gray-800 border-b border-gray-800"
                    >
                      <button
                        className="flex-1 text-left text-xs text-gray-200 truncate"
                        onClick={() => applySavedView(view)}
                      >
                        {view.name}
                      </button>
                      <button
                        className="text-gray-600 hover:text-red-400 text-xs shrink-0 px-1"
                        onClick={() => handleDeleteView(view.name)}
                        title="Delete view"
                      >
                        ✕
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
            <div className="p-2 border-t border-gray-800">
              {showSaveInput ? (
                <div className="flex gap-2">
                  <input
                    autoFocus
                    value={saveViewName}
                    onChange={e => setSaveViewName(e.target.value)}
                    onKeyDown={e => e.key === 'Enter' && handleSaveView()}
                    placeholder="View name…"
                    className="flex-1 bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 focus:outline-none focus:border-blue-500"
                  />
                  <button
                    onClick={handleSaveView}
                    className="px-3 py-1 rounded bg-blue-600 hover:bg-blue-500 text-xs font-medium"
                  >
                    Save
                  </button>
                </div>
              ) : (
                <button
                  onClick={() => setShowSaveInput(true)}
                  className="text-xs text-blue-400 hover:text-blue-300"
                >
                  + Save current query
                </button>
              )}
            </div>
          </Dropdown>

          {/* Query History */}
          <Dropdown
            label={`History${history.length ? ` (${history.length})` : ''}`}
            open={showHistory}
            onToggle={toggleHistory}
            width="w-96"
          >
            <div className="max-h-80 overflow-y-auto">
              {history.length === 0 ? (
                <p className="text-xs text-gray-500 p-3">No history yet.</p>
              ) : (
                <ul>
                  {history.map((entry, i) => (
                    <li
                      key={i}
                      className="px-3 py-2 hover:bg-gray-800 cursor-pointer border-b border-gray-800"
                      onClick={() => applyHistory(entry)}
                    >
                      <p className="text-xs text-gray-200 font-mono truncate">{entry.sql}</p>
                      <p className="text-xs text-gray-500 mt-0.5">{fmtDateShort(entry.ts)}</p>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </Dropdown>
        </div>
      </header>

      <div className="flex flex-col flex-1 min-h-0 p-4 gap-3">
        {/* Editor card */}
        <div className="shrink-0 rounded-lg border border-gray-800 overflow-hidden">
          <div className="h-44">
            <MonacoEditor
              language="sql"
              value={sql}
              onChange={v => setSql(v ?? '')}
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
                onChange={e => setFrom(e.target.value)}
                className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 focus:outline-none focus:border-blue-500"
              />
            </label>
            <label className="text-xs text-gray-400 flex items-center gap-1.5">
              To
              <input
                type="datetime-local"
                value={to}
                onChange={e => setTo(e.target.value)}
                className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 focus:outline-none focus:border-blue-500"
              />
            </label>
            <div className="ml-auto flex items-center gap-2">
              {loading && (
                <button
                  onClick={cancel}
                  className="px-4 py-1.5 rounded border border-gray-600 hover:border-red-500 hover:text-red-400 text-sm font-medium transition-colors"
                >
                  Cancel
                </button>
              )}
              <button
                onClick={run}
                disabled={loading}
                className="px-4 py-1.5 rounded bg-blue-600 hover:bg-blue-500 active:bg-blue-700 disabled:opacity-40 text-sm font-medium transition-colors"
              >
                {loading ? 'Running…' : 'Run Query'}
              </button>
            </div>
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

        {/* Stats bar — shows row count live while streaming */}
        {(stats || (loading && rows !== null && rows.length > 0)) && (
          <div className="shrink-0 flex items-center gap-4 text-xs text-gray-500">
            <span className="text-gray-300 font-medium">
              {rows?.length ?? 0} rows{loading ? '…' : ''}
            </span>
            {stats && (
              <>
                <span>{stats.rows_scanned.toLocaleString()} scanned</span>
                <span>{stats.files_pruned} files pruned</span>
                <span>{fmtBytes(stats.bytes_scanned)}</span>
                <span>{stats.duration_ms} ms</span>
              </>
            )}
          </div>
        )}

        {/* Results table */}
        {rows !== null && (
          <div className="flex-1 min-h-0 rounded-lg border border-gray-800 overflow-hidden">
            {rows.length === 0 ? (
              <div className="flex items-center justify-center h-full text-gray-600 text-sm">
                {loading ? 'Streaming…' : 'Query returned no rows'}
              </div>
            ) : (
              <ResultsTable rows={rows} columns={columns} />
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
