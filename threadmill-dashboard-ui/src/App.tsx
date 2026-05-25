import {
  Activity,
  CalendarClock,
  Pause,
  Play,
  RefreshCw,
  Replace,
  ShieldAlert,
  Trash2
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import {
  ColumnDef,
  flexRender,
  getCoreRowModel,
  useReactTable
} from "@tanstack/react-table";
import {
  api,
  ActionResponse,
  JobDetail,
  JobList,
  JobState,
  JobSummary,
  Overview,
  Permission,
  QueueView,
  Session
} from "./api";
import { Badge } from "./components/ui/badge";
import { Button } from "./components/ui/button";
import { Input } from "./components/ui/input";
import { Table, Td, Th } from "./components/ui/table";
import "./styles.css";

const states: JobState[] = [
  "AWAITING",
  "SCHEDULED",
  "ENQUEUED",
  "PROCESSING",
  "SUCCEEDED",
  "FAILED",
  "DELETED",
  "QUARANTINED"
];

function has(session: Session | null, permission: Permission) {
  return !!session && (session.permissions.includes("ADMIN") || session.permissions.includes(permission));
}

function canRequeue(job: JobSummary) {
  return job.state === "FAILED" || job.state === "SUCCEEDED" || job.state === "DELETED";
}

function canReplace(job: JobSummary) {
  return job.state === "ENQUEUED" || job.state === "SCHEDULED" || job.state === "AWAITING";
}

export default function App() {
  const [session, setSession] = useState<Session | null>(null);
  const [overview, setOverview] = useState<Overview | null>(null);
  const [jobs, setJobs] = useState<JobList>({ jobs: [], limit: 50, offset: 0 });
  const [queues, setQueues] = useState<QueueView[]>([]);
  const [selected, setSelected] = useState<JobDetail | null>(null);
  const [state, setState] = useState<string>("ENQUEUED");
  const [filter, setFilter] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    try {
      const nextSession = await api<Session>("/session");
      setSession(nextSession);
      const query = new URLSearchParams();
      if (state) query.set("state", state);
      if (overview?.capabilities.supportsRichSearch && filter) query.set("handlerType", filter);
      const [nextOverview, nextJobs, nextQueues] = await Promise.all([
        api<Overview>("/overview", {}, nextSession),
        api<JobList>(`/jobs?${query}`, {}, nextSession),
        api<QueueView[]>("/queues", {}, nextSession)
      ]);
      setOverview(nextOverview);
      setJobs(nextJobs);
      setQueues(nextQueues);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Dashboard request failed");
    }
  }

  async function mutate(path: string, init: RequestInit, success: string) {
    if (!session) return;
    try {
      const response = await api<ActionResponse>(path, init, session);
      setMessage(`${success}: ${response.target}`);
      await load();
      if (selected) {
        setSelected(await api<JobDetail>(`/jobs/${selected.summary.id}`, {}, session));
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Mutation failed");
    }
  }

  async function openJob(job: JobSummary) {
    if (!session) return;
    setSelected(await api<JobDetail>(`/jobs/${job.id}`, {}, session));
  }

  async function requeue(job: JobSummary) {
    await mutate(
      `/jobs/${job.id}/requeue`,
      { method: "POST", body: JSON.stringify({ expectedVersion: job.version }) },
      "requeued"
    );
  }

  async function retry(job: JobSummary) {
    await mutate(
      `/jobs/${job.id}/schedule-retry`,
      {
        method: "POST",
        body: JSON.stringify({ expectedVersion: job.version, delay: "PT5M" })
      },
      "retry scheduled"
    );
  }

  async function deleteJob(job: JobSummary) {
    await mutate(`/jobs/${job.id}?expectedVersion=${job.version}`, { method: "DELETE" }, "deleted");
  }

  async function replaceJob(job: JobSummary) {
    const handlerType = window.prompt("Handler type", job.handlerType);
    if (!handlerType || handlerType === job.handlerType) return;
    await mutate(
      `/jobs/${job.id}`,
      {
        method: "PATCH",
        body: JSON.stringify({ expectedVersion: job.version, handlerType, arguments: [] })
      },
      "replaced"
    );
  }

  async function pauseQueue(queue: QueueView) {
    await mutate(
      `/queues/${encodeURIComponent(queue.queue)}/pause`,
      { method: "POST", body: JSON.stringify({ reason: "dashboard" }) },
      "paused"
    );
  }

  async function resumeQueue(queue: QueueView) {
    await mutate(`/queues/${encodeURIComponent(queue.queue)}/resume`, { method: "POST" }, "resumed");
  }

  async function triggerRecurring(name: string) {
    await mutate(`/recurring/${encodeURIComponent(name)}/trigger`, { method: "POST" }, "triggered");
  }

  async function updateRecurring(name: string) {
    const triggerValue = window.prompt("Cron expression or ISO-8601 interval");
    if (!triggerValue) return;
    const triggerKind = triggerValue.startsWith("P") ? "INTERVAL" : "CRON";
    await mutate(
      `/recurring/${encodeURIComponent(name)}`,
      { method: "PUT", body: JSON.stringify({ triggerKind, triggerValue }) },
      "updated"
    );
  }

  async function deleteRecurring(name: string) {
    await mutate(`/recurring/${encodeURIComponent(name)}`, { method: "DELETE" }, "deleted");
  }

  useEffect(() => {
    void load();
  }, [state]);

  const total = useMemo(
    () => Object.values(overview?.countsByState ?? {}).reduce((sum, value) => sum + value, 0),
    [overview]
  );
  const richSearch = overview?.capabilities.supportsRichSearch ?? false;

  const columns = useMemo<ColumnDef<JobSummary>[]>(
    () => [
      {
        header: "State",
        accessorKey: "state",
        cell: ({ row }) => <Badge className={`state-${row.original.state.toLowerCase()}`}>{row.original.state}</Badge>
      },
      {
        header: "Job",
        accessorKey: "id",
        cell: ({ row }) => (
          <button className="font-mono text-xs text-primary" onClick={() => void openJob(row.original)}>
            {row.original.id}
          </button>
        )
      },
      { header: "Queue", accessorKey: "queue", cell: ({ row }) => <span className="font-mono text-xs">{row.original.queue}</span> },
      {
        header: "Handler",
        accessorKey: "handlerType",
        cell: ({ row }) => <span className="block max-w-[26rem] truncate font-mono text-xs">{row.original.handlerType}</span>
      },
      { header: "Attempts", accessorKey: "attempts", cell: ({ row }) => <span className="font-mono">{row.original.attempts}</span> },
      {
        header: "Owner",
        accessorKey: "ownerNodeId",
        cell: ({ row }) => <span className="font-mono text-xs">{row.original.ownerNodeId ?? "-"}</span>
      },
      {
        header: "Actions",
        cell: ({ row }) => {
          const job = row.original;
          return (
            <div className="flex gap-1">
              <Button
                variant="ghost"
                size="icon"
                disabled={!has(session, "REQUEUE_JOB") || !canRequeue(job)}
                aria-label="Requeue"
                onClick={() => void requeue(job)}
              >
                <Play className="h-4 w-4" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                disabled={!has(session, "REQUEUE_JOB") || job.state !== "FAILED"}
                aria-label="Retry"
                onClick={() => void retry(job)}
              >
                <RefreshCw className="h-4 w-4" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                disabled={!has(session, "REPLACE_JOB") || !canReplace(job)}
                aria-label="Replace"
                onClick={() => void replaceJob(job)}
              >
                <Replace className="h-4 w-4" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                disabled={!has(session, "DELETE_JOB")}
                aria-label="Delete"
                onClick={() => void deleteJob(job)}
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>
          );
        }
      }
    ],
    [session, selected]
  );

  const table = useReactTable({ data: jobs.jobs, columns, getCoreRowModel: getCoreRowModel() });

  return (
    <main className="min-h-screen bg-background text-foreground">
      <header className="flex h-12 items-center justify-between border-b border-border px-4">
        <div className="flex items-center gap-2">
          <Activity className="h-4 w-4 text-primary" />
          <h1 className="text-sm font-semibold">Threadmill</h1>
          <Badge>{session?.redactionMode ?? "redacted"}</Badge>
        </div>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <span>{session?.displayName ?? "loading"}</span>
          <Button variant="secondary" size="icon" aria-label="Refresh" onClick={() => void load()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>
      </header>

      {error ? <div className="border-b border-destructive bg-destructive/10 px-4 py-2 text-sm">{error}</div> : null}
      {message ? <div className="border-b border-border bg-primary/10 px-4 py-2 text-sm">{message}</div> : null}

      <section className="grid grid-cols-[220px_1fr_360px]">
        <aside className="min-h-[calc(100vh-3rem)] border-r border-border bg-panel p-3">
          <div className="mb-4 text-xs font-semibold uppercase text-muted-foreground">States</div>
          <div className="space-y-1">
            <button className="state-filter" disabled={!richSearch} onClick={() => setState("")}>
              <span>All</span>
              <span className="font-mono">{total}</span>
            </button>
            {states.map((s) => (
              <button className="state-filter" key={s} onClick={() => setState(s)}>
                <span>{s}</span>
                <span className="font-mono">{overview?.countsByState?.[s] ?? 0}</span>
              </button>
            ))}
          </div>
          <div className="mt-6 text-xs font-semibold uppercase text-muted-foreground">Queues</div>
          <div className="mt-2 space-y-1">
            {queues.map((queue) => (
              <div className="flex items-center justify-between rounded-sm px-2 py-1 text-xs" key={queue.queue}>
                <span className="font-mono">{queue.queue}</span>
                <Badge>{queue.depth}</Badge>
              </div>
            ))}
          </div>
        </aside>

        <section className="min-w-0 p-4">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-sm font-semibold">Jobs</h2>
            <Input
              className="w-80"
              placeholder="Handler type"
              disabled={!richSearch}
              value={filter}
              onChange={(event) => setFilter(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") void load();
              }}
            />
          </div>

          <div className="overflow-auto border border-border">
            <Table>
              <thead>
                {table.getHeaderGroups().map((headerGroup) => (
                  <tr key={headerGroup.id}>
                    {headerGroup.headers.map((header) => (
                      <Th key={header.id}>
                        {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                      </Th>
                    ))}
                  </tr>
                ))}
              </thead>
              <tbody>
                {table.getRowModel().rows.map((row) => (
                  <tr key={row.id}>
                    {row.getVisibleCells().map((cell) => (
                      <Td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</Td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </Table>
          </div>

          <div className="mt-4 grid grid-cols-2 gap-3">
            {queues.map((queue) => (
              <div className="border border-border bg-panel p-3" key={queue.queue}>
                <div className="flex items-center justify-between">
                  <div className="font-mono text-sm">{queue.queue}</div>
                  <Badge>{queue.paused ? "paused" : "running"}</Badge>
                </div>
                <div className="mt-2 flex gap-2">
                  <Button
                    variant="secondary"
                    disabled={!has(session, "PAUSE_QUEUE") || queue.paused}
                    onClick={() => void pauseQueue(queue)}
                  >
                    <Pause className="h-4 w-4" /> Pause
                  </Button>
                  <Button
                    variant="secondary"
                    disabled={!has(session, "RESUME_QUEUE") || !queue.paused}
                    onClick={() => void resumeQueue(queue)}
                  >
                    <Play className="h-4 w-4" /> Resume
                  </Button>
                </div>
              </div>
            ))}
          </div>

          <div className="mt-4 border border-border bg-panel">
            <div className="flex h-9 items-center border-b border-border px-3 text-sm font-semibold">
              <CalendarClock className="mr-2 h-4 w-4" /> Recurring
            </div>
            <div className="divide-y divide-border">
              {(overview?.cronTasks ?? []).map(({ task, state: taskState }) => (
                <div className="grid grid-cols-[1fr_auto] gap-3 p-3" key={task.name}>
                  <div>
                    <div className="font-mono text-sm">{task.name}</div>
                    <div className="mt-1 text-xs text-muted-foreground">
                      {task.queue} · {task.handlerType} · next {taskState?.nextRunAt ?? "-"}
                    </div>
                  </div>
                  <div className="flex gap-1">
                    <Button
                      variant="ghost"
                      size="icon"
                      disabled={!has(session, "TRIGGER_RECURRING")}
                      aria-label="Trigger recurring"
                      onClick={() => void triggerRecurring(task.name)}
                    >
                      <Play className="h-4 w-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      disabled={!has(session, "UPDATE_RECURRING")}
                      aria-label="Edit recurring"
                      onClick={() => void updateRecurring(task.name)}
                    >
                      <RefreshCw className="h-4 w-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      disabled={!has(session, "DELETE_RECURRING")}
                      aria-label="Delete recurring"
                      onClick={() => void deleteRecurring(task.name)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </section>

        <aside className="min-h-[calc(100vh-3rem)] border-l border-border bg-panel p-3">
          <h2 className="text-sm font-semibold">Detail</h2>
          {selected ? (
            <div className="mt-3 space-y-3 text-sm">
              <div className="break-all font-mono text-xs">{selected.summary.id}</div>
              <Badge className={`state-${selected.summary.state.toLowerCase()}`}>{selected.summary.state}</Badge>
              <div className="grid grid-cols-2 gap-2 text-xs">
                <span className="text-muted-foreground">Version</span>
                <span className="font-mono">{selected.summary.version}</span>
                <span className="text-muted-foreground">Queue</span>
                <span className="font-mono">{selected.summary.queue}</span>
                <span className="text-muted-foreground">Handler</span>
                <span className="truncate font-mono">{selected.summary.handlerType}</span>
              </div>
              {selected.sensitiveDetailsRedacted ? (
                <div className="flex items-center gap-2 border border-border p-2 text-xs text-muted-foreground">
                  <ShieldAlert className="h-4 w-4" />
                  Sensitive details redacted.
                </div>
              ) : (
                <pre className="max-h-56 overflow-auto border border-border bg-background p-2 text-xs">
                  {JSON.stringify(
                    {
                      arguments: selected.arguments,
                      metadata: selected.metadata,
                      log: selected.log,
                      result: selected.result
                    },
                    null,
                    2
                  )}
                </pre>
              )}
              <div>
                <div className="mb-1 text-xs font-semibold uppercase text-muted-foreground">History</div>
                <div className="space-y-1">
                  {selected.stateHistory.map((entry) => (
                    <div className="flex justify-between gap-2 text-xs" key={`${entry.state}-${entry.at}`}>
                      <span>{entry.state}</span>
                      <span className="font-mono text-muted-foreground">{entry.at}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ) : (
            <div className="mt-3 text-sm text-muted-foreground">Select a job.</div>
          )}

          <div className="mt-6">
            <div className="mb-2 text-xs font-semibold uppercase text-muted-foreground">Nodes</div>
            <div className="space-y-2">
              {(overview?.nodeHeartbeats ?? []).map((node) => (
                <div className="border border-border p-2 text-xs" key={node.nodeId}>
                  <div className="break-all font-mono">{node.nodeId}</div>
                  <div className="mt-1 font-mono text-muted-foreground">{node.lastSeenAt}</div>
                </div>
              ))}
            </div>
          </div>
        </aside>
      </section>
    </main>
  );
}
