import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { JobState, JobSummary, Permission, Session } from "./api";

const jobId = "018f0000-0000-7000-8000-000000000096";
const recurringName = "nightly report";

function job(state: JobState = "ENQUEUED"): JobSummary {
  return {
    id: jobId,
    state,
    queue: "priority queue",
    priority: 10,
    handlerType: "com.example.OriginalHandler",
    attempts: state === "FAILED" ? 1 : 0,
    version: 7,
    createdAt: "2026-01-01T00:00:00Z",
    currentStateAt: "2026-01-01T00:01:00Z",
    scheduledFor: null,
    ownerNodeId: null,
    ownerHeartbeatAt: null,
    detailsRedacted: true,
    cronOrigin: null
  };
}

const counts = {
  AWAITING: 0,
  SCHEDULED: 0,
  ENQUEUED: 1,
  PROCESSING: 0,
  PROCESSED: 0,
  SUCCEEDED: 0,
  FAILED: 1,
  DELETED: 0,
  QUARANTINED: 0
};

interface MockOptions {
  currentJob?: JobSummary;
  paused?: boolean;
  permissions?: Permission[];
  csrf?: Session["csrf"];
  failMutation?: boolean;
  failInitialLoad?: boolean;
  failJobDetail?: boolean;
}

interface CapturedRequest {
  url: string;
  init: RequestInit;
}

function installApiMock(options: MockOptions = {}) {
  const currentJob = options.currentJob ?? job();
  const requests: CapturedRequest[] = [];
  vi.stubGlobal("fetch", (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = input.toString();
    requests.push({ url, init });
    const method = init.method ?? "GET";
    const mutation = method !== "GET";

    if (mutation && options.failMutation) {
      return response({}, false, 409, "Conflict");
    }
    if (mutation) {
      return response({ status: "ok", target: url.split("/").at(-1) ?? "target" });
    }
    if (url.endsWith("/session")) {
      if (options.failInitialLoad) return response({}, false, 503, "Service Unavailable");
      return response({
        displayName: "Ada",
        permissions: options.permissions ?? ["ADMIN"],
        csrf:
          options.csrf === undefined
            ? { headerName: "X-THREADMILL-CSRF", token: "csrf-token" }
            : options.csrf,
        redactionMode: "redacted"
      });
    }
    if (url.endsWith("/overview")) {
      return response({
        countsByState: counts,
        queueDepths: { "priority queue": 1 },
        pausedQueues: options.paused ? ["priority queue"] : [],
        nodeHeartbeats: [],
        cronTasks: [
          {
            task: {
              name: recurringName,
              queue: "priority queue",
              handlerType: "com.example.ReportHandler",
              enabled: true,
              priority: 0,
              exclusive: false,
              missedRunPolicy: "DROP",
              triggerKind: "INTERVAL",
              triggerValue: "PT1H",
              payloadRedacted: true
            },
            state: null
          }
        ],
        capabilities: {
          supportsRichSearch: true,
          supportsExactCounts: true,
          supportsConcurrencyGroups: true,
          maxSerializedJobBytes: 262144,
          maxClaimBatch: 1000
        }
      });
    }
    if (url.includes("/jobs?")) {
      return response({ jobs: [currentJob], limit: 50, offset: 0 });
    }
    if (url.endsWith("/queues")) {
      return response([
        {
          queue: "priority queue",
          depth: 1,
          paused: options.paused ?? false,
          oldestEnqueuedAt: "2026-01-01T00:00:00Z"
        }
      ]);
    }
    if (url.endsWith(`/jobs/${jobId}`)) {
      if (options.failJobDetail) return response({}, false, 404, "Not Found");
      return response({
        summary: currentJob,
        stateHistory: [
          { state: currentJob.state, at: currentJob.currentStateAt, reason: null, detail: null }
        ],
        arguments: [],
        metadata: {},
        log: [],
        progress: null,
        result: null,
        sensitiveDetailsRedacted: true
      });
    }
    throw new Error(`Unexpected request: ${method} ${url}`);
  });
  return requests;
}

function response(body: unknown, ok = true, status = 200, statusText = "OK") {
  return Promise.resolve({
    ok,
    status,
    statusText,
    json: () => Promise.resolve(body)
  });
}

interface OperatorAction {
  name: string;
  label: string;
  method: string;
  path: string;
  state?: JobState;
  paused?: boolean;
  prompt?: string;
  body?: unknown;
}

const actions: OperatorAction[] = [
  {
    name: "pause queue",
    label: "Pause",
    method: "POST",
    path: "/queues/priority%20queue/pause",
    body: { reason: "dashboard" }
  },
  {
    name: "resume queue",
    label: "Resume",
    method: "POST",
    path: "/queues/priority%20queue/resume",
    paused: true
  },
  {
    name: "requeue job",
    label: "Requeue",
    method: "POST",
    path: `/jobs/${jobId}/requeue`,
    state: "FAILED",
    body: { expectedVersion: 7 }
  },
  {
    name: "schedule retry",
    label: "Retry",
    method: "POST",
    path: `/jobs/${jobId}/schedule-retry`,
    state: "FAILED",
    body: { expectedVersion: 7, delay: "PT5M" }
  },
  {
    name: "delete job",
    label: "Delete",
    method: "DELETE",
    path: `/jobs/${jobId}?expectedVersion=7`
  },
  {
    name: "replace job",
    label: "Replace",
    method: "PATCH",
    path: `/jobs/${jobId}`,
    prompt: "com.example.ReplacementHandler",
    body: {
      expectedVersion: 7,
      handlerType: "com.example.ReplacementHandler",
      arguments: []
    }
  },
  {
    name: "trigger recurring task",
    label: "Trigger recurring",
    method: "POST",
    path: "/recurring/nightly%20report/trigger"
  },
  {
    name: "update recurring task",
    label: "Edit recurring",
    method: "PUT",
    path: "/recurring/nightly%20report",
    prompt: "PT2H",
    body: { triggerKind: "INTERVAL", triggerValue: "PT2H" }
  },
  {
    name: "delete recurring task",
    label: "Delete recurring",
    method: "DELETE",
    path: "/recurring/nightly%20report"
  }
];

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  window.__THREADMILL_DASHBOARD_CONFIG__ = undefined;
});

describe.each(actions)("$name", (action) => {
  it("sends the expected request and CSRF header", async () => {
    const requests = installApiMock({
      currentJob: job(action.state),
      paused: action.paused
    });
    if (action.prompt) vi.spyOn(window, "prompt").mockReturnValue(action.prompt);
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: action.label }));

    await waitFor(() =>
      expect(
        requests.some(
          ({ url, init }) => url.endsWith(action.path) && init.method === action.method
        )
      ).toBe(true)
    );
    const request = requests.find(
      ({ url, init }) => url.endsWith(action.path) && init.method === action.method
    );
    expect(request).toBeDefined();
    const headers = new Headers(request?.init.headers);
    expect(headers.get("X-THREADMILL-CSRF")).toBe("csrf-token");
    expect(request?.init.credentials).toBe("same-origin");
    if (action.body) expect(JSON.parse(request?.init.body as string)).toEqual(action.body);
    else expect(request?.init.body).toBeUndefined();
  });

  it("surfaces a non-2xx response without reloading", async () => {
    const requests = installApiMock({
      currentJob: job(action.state),
      paused: action.paused,
      failMutation: true
    });
    if (action.prompt) vi.spyOn(window, "prompt").mockReturnValue(action.prompt);
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: action.label }));

    expect(await screen.findByText("409 Conflict")).toBeInTheDocument();
    expect(
      requests.filter(({ url, init }) => url.endsWith(action.path) && init.method === action.method)
    ).toHaveLength(1);
    expect(requests.filter(({ url }) => url.endsWith("/session"))).toHaveLength(1);
  });
});

it("disables every operator control without its permission", async () => {
  installApiMock({ currentJob: job("FAILED"), permissions: ["READ"] });
  render(<App />);

  expect(await screen.findByRole("button", { name: "Requeue" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Retry" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Delete" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Pause" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Resume" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Trigger recurring" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Edit recurring" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Delete recurring" })).toBeDisabled();
});

it("disables replacement without its permission when the job is replaceable", async () => {
  installApiMock({ currentJob: job("ENQUEUED"), permissions: ["READ"] });
  render(<App />);

  expect(await screen.findByRole("button", { name: "Replace" })).toBeDisabled();
});

it("does not invent a CSRF header when the session does not provide one", async () => {
  const requests = installApiMock({ csrf: null });
  render(<App />);

  fireEvent.click(await screen.findByRole("button", { name: "Pause" }));

  await waitFor(() => expect(requests.some(({ init }) => init.method === "POST")).toBe(true));
  const request = requests.find(({ init }) => init.method === "POST");
  expect(new Headers(request?.init.headers).has("X-THREADMILL-CSRF")).toBe(false);
});

it("does not send the session CSRF header on read requests", async () => {
  const requests = installApiMock();
  render(<App />);

  await screen.findByRole("button", { name: "Pause" });

  const reads = requests.filter(({ init }) => (init.method ?? "GET") === "GET");
  expect(reads.length).toBeGreaterThan(0);
  for (const request of reads) {
    expect(new Headers(request.init.headers).has("X-THREADMILL-CSRF")).toBe(false);
  }
});

it("surfaces a non-2xx initial dashboard response", async () => {
  installApiMock({ failInitialLoad: true });
  render(<App />);

  expect(await screen.findByText("503 Service Unavailable")).toBeInTheDocument();
});

it("surfaces a non-2xx job-detail response", async () => {
  installApiMock({ failJobDetail: true });
  render(<App />);

  fireEvent.click(
    await screen.findByRole("button", {
      name: `Open job details for ${jobId}`
    })
  );

  expect(await screen.findByText("404 Not Found")).toBeInTheDocument();
});
