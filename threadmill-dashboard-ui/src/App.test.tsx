import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import App from "./App";

const responses: Record<string, unknown> = {
  "/threadmill/api/session": {
    displayName: "Ada",
    permissions: ["READ", "PAUSE_QUEUE"],
    csrf: { headerName: "X-CSRF-TOKEN", token: "token" },
    redactionMode: "redacted"
  },
  "/threadmill/api/overview": {
    countsByState: {
      AWAITING: 0,
      SCHEDULED: 0,
      ENQUEUED: 1,
      PROCESSING: 1,
      PROCESSED: 0,
      SUCCEEDED: 2,
      FAILED: 1,
      DELETED: 0,
      QUARANTINED: 0
    },
    queueDepths: { default: 1 },
    pausedQueues: [],
    nodeHeartbeats: [],
    cronTasks: [],
    capabilities: {
      supportsRichSearch: false,
      supportsExactCounts: true,
      supportsConcurrencyGroups: true,
      maxSerializedJobBytes: 262144,
      maxClaimBatch: 1000
    }
  },
  "/threadmill/api/jobs?state=ENQUEUED": {
    jobs: [
      {
        id: "018f0000-0000-7000-8000-000000000001",
        state: "ENQUEUED",
        queue: "default",
        priority: 0,
        handlerType: "com.example.ImportHandler",
        attempts: 0,
        version: 1,
        createdAt: "2026-01-01T00:00:00Z",
        currentStateAt: "2026-01-01T00:00:00Z",
        scheduledFor: null,
        ownerNodeId: null,
        ownerHeartbeatAt: null,
        detailsRedacted: true
      }
    ],
    limit: 50,
    offset: 0
  },
  "/threadmill/api/queues": [
    {
      queue: "default",
      depth: 1,
      paused: false,
      oldestEnqueuedAt: "2026-01-01T00:00:00Z"
    }
  ],
  "/threadmill/api/jobs/018f0000-0000-7000-8000-000000000001": {
    summary: {
      id: "018f0000-0000-7000-8000-000000000001",
      state: "ENQUEUED",
      queue: "default",
      priority: 0,
      handlerType: "com.example.ImportHandler",
      attempts: 0,
      version: 1,
      createdAt: "2026-01-01T00:00:00Z",
      currentStateAt: "2026-01-01T00:00:00Z",
      scheduledFor: null,
      ownerNodeId: null,
      ownerHeartbeatAt: null,
      detailsRedacted: true
    },
    stateHistory: [{ state: "ENQUEUED", at: "2026-01-01T00:00:00Z", reason: null, detail: null }],
    arguments: [],
    metadata: {},
    log: [],
    progress: null,
    result: null,
    sensitiveDetailsRedacted: true
  }
};

beforeEach(() => {
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    const value = responses[url] ?? responses[url.replace(/state=[^&]+/, "").replace(/handlerType=[^&]+/, "")];
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve(value)
    });
  });
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  window.__THREADMILL_DASHBOARD_CONFIG__ = undefined;
});

it("renders dashboard data and redaction state", async () => {
  render(<App />);

  await waitFor(() => expect(screen.getByText("com.example.ImportHandler")).toBeInTheDocument());
  expect(screen.getByText("Ada")).toBeInTheDocument();
  expect(screen.getByText("redacted")).toBeInTheDocument();
  expect(screen.getByLabelText("Requeue")).toBeDisabled();
  expect(screen.getByText("All").closest("button")).toBeDisabled();
});

it("uses the runtime API base path override", async () => {
  window.__THREADMILL_DASHBOARD_CONFIG__ = { apiBasePath: "/admin/threadmill/api" };
  const calls: string[] = [];
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    calls.push(url);
    const value = responses[url.replace("/admin/threadmill/api", "/threadmill/api")];
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve(value)
    });
  });

  render(<App />);

  await waitFor(() => expect(screen.getByText("com.example.ImportHandler")).toBeInTheDocument());
  expect(calls).toContain("/admin/threadmill/api/session");
});

it("opens job details when the job row is clicked", async () => {
  render(<App />);

  const row = await screen.findByRole("button", {
    name: "Open job details for 018f0000-0000-7000-8000-000000000001"
  });
  fireEvent.click(row);

  await waitFor(() => expect(screen.getByText("Sensitive details redacted.")).toBeInTheDocument());
  expect(row).toHaveAttribute("aria-selected", "true");
});
