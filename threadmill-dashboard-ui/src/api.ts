export type Permission =
  | "READ"
  | "VIEW_SENSITIVE_DETAILS"
  | "PAUSE_QUEUE"
  | "RESUME_QUEUE"
  | "REQUEUE_JOB"
  | "DELETE_JOB"
  | "REPLACE_JOB"
  | "TRIGGER_RECURRING"
  | "UPDATE_RECURRING"
  | "DELETE_RECURRING"
  | "ADMIN";

export type JobState =
  | "AWAITING"
  | "SCHEDULED"
  | "ENQUEUED"
  | "PROCESSING"
  | "PROCESSED"
  | "SUCCEEDED"
  | "FAILED"
  | "DELETED"
  | "QUARANTINED";

export interface Session {
  displayName: string;
  permissions: Permission[];
  csrf: { headerName: string; token: string } | null;
  redactionMode: "redacted" | "full";
}

export interface Overview {
  countsByState: Record<JobState, number>;
  queueDepths: Record<string, number>;
  pausedQueues: string[];
  nodeHeartbeats: NodeHeartbeat[];
  cronTasks: RecurringTask[];
  capabilities: JobStoreCapabilities;
}

export interface JobSummary {
  id: string;
  state: JobState;
  queue: string;
  priority: number;
  handlerType: string;
  attempts: number;
  version: number;
  createdAt: string;
  currentStateAt: string;
  scheduledFor: string | null;
  ownerNodeId: string | null;
  ownerHeartbeatAt: string | null;
  detailsRedacted: boolean;
}

export interface JobList {
  jobs: JobSummary[];
  limit: number;
  offset: number;
}

export interface QueueView {
  queue: string;
  depth: number;
  paused: boolean;
  oldestEnqueuedAt: string | null;
}

export interface JobDetail {
  summary: JobSummary;
  stateHistory: Array<{ state: JobState; at: string; reason: string | null; detail: string | null }>;
  arguments: Array<{ typeTag: string; serialized: string }>;
  metadata: Record<string, string>;
  log: Array<{ at: string; level: string; message: string }>;
  progress: { fraction: number; message: string | null } | null;
  result: { typeTag: string; serialized: string } | null;
  sensitiveDetailsRedacted: boolean;
}

export interface RecurringTask {
  task: {
    name: string;
    queue: string;
    handlerType: string;
    enabled: boolean;
    priority: number;
    missedRunPolicy: string;
    trigger: Record<string, unknown>;
  };
  state: {
    taskName: string;
    lastRunAt: string | null;
    lastRunJobId: string | null;
    nextRunAt: string | null;
    inFlightJobId: string | null;
  } | null;
}

export interface NodeHeartbeat {
  nodeId: string;
  lastSeenAt: string;
}

export interface ActionResponse {
  status: string;
  target: string;
}

export interface JobStoreCapabilities {
  supportsRichSearch: boolean;
  supportsExactCounts: boolean;
  supportsConcurrencyGroups: boolean;
  maxSerializedJobBytes: number;
  maxClaimBatch: number;
}

declare global {
  interface Window {
    __THREADMILL_DASHBOARD_CONFIG__?: {
      apiBasePath?: string;
    };
  }
}

function apiBasePath() {
  const configured = window.__THREADMILL_DASHBOARD_CONFIG__?.apiBasePath;
  if (!configured || !configured.trim()) return "/threadmill/api";
  return configured.endsWith("/") ? configured.slice(0, -1) : configured;
}

export async function api<T>(path: string, init: RequestInit = {}, session?: Session): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("Accept", "application/json");
  if (init.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  if (session?.csrf && init.method && init.method !== "GET") {
    headers.set(session.csrf.headerName, session.csrf.token);
  }
  const response = await fetch(`${apiBasePath()}${path}`, { ...init, headers, credentials: "same-origin" });
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json() as Promise<T>;
}
