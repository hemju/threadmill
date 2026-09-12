import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
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
  "/threadmill/api/jobs": {
    jobs: [
      {
        id: "018f0000-0000-7000-8000-000000000001",
        state: "ENQUEUED",
        queue: "default",
        priority: 5,
        handlerType: "com.example.ImportHandler",
        attempts: 0,
        version: 1,
        createdAt: "2026-01-01T00:00:00Z",
        currentStateAt: "2026-01-01T00:00:00Z",
        scheduledFor: null,
        ownerNodeId: null,
        ownerHeartbeatAt: null,
        detailsRedacted: true,
        cronOrigin: "nudge"
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
      priority: 5,
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
    stateHistory: [{ state: "ENQUEUED", at: "2026-01-01T00:00:00Z", reason: null, message: null }],
    arguments: [],
    metadata: {},
    log: [],
    progress: null,
    result: null,
    sensitiveDetailsRedacted: true
  }
};

function fixtureFor(url: string) {
  const normalized = url.replace("/admin/threadmill/api", "/threadmill/api");
  return responses[new URL(normalized, "http://localhost").pathname];
}

function fullJobPage(handlerType: string, offset: number) {
  const first = responses["/threadmill/api/jobs"] as {
    jobs: Array<Record<string, unknown>>;
  };
  return {
    jobs: Array.from({ length: 50 }, (_, index) => ({
      ...first.jobs[0],
      id:
        index === 0
          ? `018f0000-0000-7000-8000-${offset === 0 ? "000000000001" : "000000000002"}`
          : `page-${offset}-job-${index}`,
      handlerType: index === 0 ? handlerType : `com.example.Filler${offset}_${index}`
    })),
    limit: 50,
    offset
  };
}

beforeEach(() => {
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    const value = fixtureFor(url);
    if (value === undefined) throw new Error(`Unexpected request: GET ${url}`);
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
  // Issue #108 requirement 8: a nudged run must be distinguishable from a
  // scheduled one in the console, including on redacted read-level views.
  expect(screen.getByText("nudge")).toBeInTheDocument();
  expect(screen.getByLabelText("Requeue")).toBeDisabled();
  expect(screen.getByText("All").closest("button")).toBeDisabled();
});

it("keeps operational replacement available to a REPLACE_JOB-only session", async () => {
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    const value =
      url === "/threadmill/api/session"
        ? { ...(responses[url] as object), permissions: ["READ", "REPLACE_JOB"] }
        : fixtureFor(url);
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve(value)
    });
  });

  render(<App />);

  await waitFor(() => expect(screen.getByText("com.example.ImportHandler")).toBeInTheDocument());
  expect(screen.getByLabelText("Replace")).toBeEnabled();
});

it("lets a REPLACE_JOB-only session change queue and priority without selecting a handler", async () => {
  const requests: Array<{ url: string; init?: RequestInit }> = [];
  vi.spyOn(window, "prompt")
    .mockReturnValueOnce("critical")
    .mockReturnValueOnce("7")
    .mockReturnValueOnce("");
  vi.stubGlobal("fetch", (input: RequestInfo | URL, init?: RequestInit) => {
    const url = input.toString();
    requests.push({ url, init });
    const value =
      url === "/threadmill/api/session"
        ? { ...(responses[url] as object), permissions: ["READ", "REPLACE_JOB"] }
        : init?.method === "PATCH"
          ? { status: "replaced", target: "018f0000-0000-7000-8000-000000000001" }
          : fixtureFor(url);
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve(value)
    });
  });

  render(<App />);
  await waitFor(() => expect(screen.getByText("com.example.ImportHandler")).toBeInTheDocument());
  fireEvent.click(screen.getByLabelText("Replace"));

  await waitFor(() => expect(requests.some(({ init }) => init?.method === "PATCH")).toBe(true));
  const request = requests.find(({ init }) => init?.method === "PATCH");
  expect(JSON.parse(request?.init?.body as string)).toEqual({
    expectedVersion: 1,
    queue: "critical",
    priority: 7
  });
  expect(window.prompt).toHaveBeenCalledTimes(3);
});

it("treats a blank priority as no change", async () => {
  const requests: Array<{ url: string; init?: RequestInit }> = [];
  vi.spyOn(window, "prompt")
    .mockReturnValueOnce("critical")
    .mockReturnValueOnce("")
    .mockReturnValueOnce("");
  vi.stubGlobal("fetch", (input: RequestInfo | URL, init?: RequestInit) => {
    const url = input.toString();
    requests.push({ url, init });
    const value =
      url === "/threadmill/api/session"
        ? { ...(responses[url] as object), permissions: ["READ", "REPLACE_JOB"] }
        : init?.method === "PATCH"
          ? { status: "replaced", target: "018f0000-0000-7000-8000-000000000001" }
          : fixtureFor(url);
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve(value)
    });
  });

  render(<App />);
  await waitFor(() => expect(screen.getByText("com.example.ImportHandler")).toBeInTheDocument());
  fireEvent.click(screen.getByLabelText("Replace"));

  await waitFor(() => expect(requests.some(({ init }) => init?.method === "PATCH")).toBe(true));
  const request = requests.find(({ init }) => init?.method === "PATCH");
  expect(JSON.parse(request?.init?.body as string)).toEqual({
    expectedVersion: 1,
    queue: "critical"
  });
});

it("uses the runtime API base path override", async () => {
  window.__THREADMILL_DASHBOARD_CONFIG__ = { apiBasePath: "/admin/threadmill/api" };
  const calls: string[] = [];
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    calls.push(url);
    const value = fixtureFor(url);
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

it("requests the next and previous job pages", async () => {
  const calls: string[] = [];
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    calls.push(url);
    if (url.includes("/jobs?")) {
      const secondPage = url.includes("offset=50");
      return Promise.resolve({
        ok: true,
        json: () =>
          Promise.resolve(
            fullJobPage(
              secondPage ? "com.example.SecondHandler" : "com.example.ImportHandler",
              secondPage ? 50 : 0
            )
          )
      });
    }
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve(responses[url])
    });
  });

  render(<App />);

  await screen.findByText("com.example.ImportHandler");
  fireEvent.click(screen.getByLabelText("Next page"));
  await screen.findByText("com.example.SecondHandler");
  expect(calls).toContain("/threadmill/api/jobs?state=ENQUEUED&limit=50&offset=50");

  fireEvent.click(screen.getByLabelText("Previous page"));
  await screen.findByText("com.example.ImportHandler");
  expect(calls.filter((url) => url.endsWith("offset=0"))).toHaveLength(2);
});

it("retries a failed next-page request without advancing the displayed page", async () => {
  let failedNextPage = false;
  const calls: string[] = [];
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    calls.push(url);
    if (url.includes("/jobs?")) {
      const nextPage = url.includes("offset=50");
      if (nextPage && !failedNextPage) {
        failedNextPage = true;
        return Promise.resolve({
          ok: false,
          status: 503,
          statusText: "Service Unavailable",
          json: () => Promise.resolve({})
        });
      }
      return Promise.resolve({
        ok: true,
        json: () =>
          Promise.resolve(
            fullJobPage(
              nextPage ? "com.example.SecondHandler" : "com.example.ImportHandler",
              nextPage ? 50 : 0
            )
          )
      });
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve(fixtureFor(url)) });
  });

  render(<App />);

  await screen.findByText("com.example.ImportHandler");
  fireEvent.click(screen.getByLabelText("Next page"));
  expect(await screen.findByText("503 Service Unavailable")).toBeInTheDocument();
  expect(screen.getByLabelText("Previous page")).toBeDisabled();

  fireEvent.click(screen.getByLabelText("Next page"));
  await screen.findByText("com.example.SecondHandler");
  expect(calls.filter((url) => url.endsWith("offset=50"))).toHaveLength(2);
});

it("paginates with the last submitted handler filter", async () => {
  const calls: string[] = [];
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    calls.push(url);
    if (url.endsWith("/overview")) {
      const overview = responses["/threadmill/api/overview"] as Record<string, unknown>;
      return Promise.resolve({
        ok: true,
        json: () =>
          Promise.resolve({
            ...overview,
            capabilities: {
              ...(overview.capabilities as Record<string, unknown>),
              supportsRichSearch: true
            }
          })
      });
    }
    if (url.includes("/jobs?")) {
      const offset = url.includes("offset=50") ? 50 : 0;
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve(fullJobPage("com.example.ImportHandler", offset))
      });
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve(fixtureFor(url)) });
  });

  render(<App />);

  const search = await screen.findByPlaceholderText("Handler type");
  fireEvent.change(search, { target: { value: "com.example.Pending" } });
  fireEvent.click(screen.getByLabelText("Next page"));
  await waitFor(() => expect(calls.some((url) => url.endsWith("offset=50"))).toBe(true));
  expect(calls.filter((url) => url.includes("/jobs?")).at(-1)).not.toContain("handlerType=");

  fireEvent.keyDown(search, { key: "Enter" });
  await waitFor(() =>
    expect(calls.some((url) => url.includes("handlerType=com.example.Pending"))).toBe(true)
  );
  expect(calls.filter((url) => url.includes("/jobs?")).at(-1)).toContain("offset=0");
});

function delayedResponse() {
  let resolve!: (body: unknown) => void;
  const promise = new Promise<{ ok: boolean; json: () => Promise<unknown> }>((complete) => {
    resolve = (body) => complete({ ok: true, json: () => Promise.resolve(body) });
  });
  return { promise, resolve };
}

it("ignores an older refresh after a newer state selection even if abort is ignored", async () => {
  let holdRefresh = false;
  let held = false;
  const old = delayedResponse();
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    if (holdRefresh && url.includes("/jobs?state=ENQUEUED")) {
      held = true;
      return old.promise;
    }
    const value = url.includes("/jobs?state=FAILED")
      ? fullJobPage("com.example.LatestFailedHandler", 0)
      : fixtureFor(url);
    return Promise.resolve({ ok: true, json: () => Promise.resolve(value) });
  });
  render(<App />);
  await screen.findByText("com.example.ImportHandler");
  holdRefresh = true;
  fireEvent.click(screen.getByLabelText("Refresh"));
  await waitFor(() => expect(held).toBe(true));
  fireEvent.click(screen.getByRole("button", { name: /^FAILED/ }));
  await screen.findByText("com.example.LatestFailedHandler");
  await act(async () => old.resolve(responses["/threadmill/api/jobs"]));
  expect(screen.getByText("com.example.LatestFailedHandler")).toBeInTheDocument();
  expect(screen.queryByText("com.example.ImportHandler")).not.toBeInTheDocument();
});

it("keeps the most recently selected detail and renders the server's message as text", async () => {
  const firstId = "018f0000-0000-7000-8000-000000000001";
  const secondId = "018f0000-0000-7000-8000-000000000002";
  const first = delayedResponse();
  const summary = (responses["/threadmill/api/jobs"] as { jobs: Array<Record<string, unknown>> }).jobs[0];
  const detail = fixtureFor(`/threadmill/api/jobs/${firstId}`) as Record<string, unknown>;
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    if (url.endsWith(`/jobs/${firstId}`)) return first.promise;
    const value = url.includes("/jobs?")
      ? { jobs: [summary, { ...summary, id: secondId }], limit: 50, offset: 0 }
      : url.endsWith(`/jobs/${secondId}`)
        ? { ...detail, summary: { ...summary, id: secondId }, stateHistory: [{
          state: "FAILED", at: "2026-01-01T00:00:00Z", reason: "handler.failure", message: "<b>redacted diagnostic</b>"
        }] }
        : fixtureFor(url);
    return Promise.resolve({ ok: true, json: () => Promise.resolve(value) });
  });
  render(<App />);
  fireEvent.click(await screen.findByRole("button", { name: `Open job details for ${firstId}` }));
  fireEvent.click(screen.getByRole("button", { name: `Open job details for ${secondId}` }));
  expect(await screen.findByText("<b>redacted diagnostic</b>")).toBeInTheDocument();
  expect(screen.getByText("handler.failure")).toBeInTheDocument();
  expect(screen.getByText("<b>redacted diagnostic</b>").querySelector("b")).toBeNull();
  await act(async () => first.resolve(detail));
  expect(screen.getByText("<b>redacted diagnostic</b>")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: `Open job details for ${secondId}` })).toHaveAttribute("aria-selected", "true");
});

it("does not reopen a cleared selection when a late detail arrives", async () => {
  const id = "018f0000-0000-7000-8000-000000000001";
  const delayed = delayedResponse();
  vi.stubGlobal("fetch", (input: RequestInfo | URL) => {
    const url = input.toString();
    if (url.endsWith(`/jobs/${id}`)) return delayed.promise;
    return Promise.resolve({ ok: true, json: () => Promise.resolve(fixtureFor(url)) });
  });
  render(<App />);
  fireEvent.click(await screen.findByRole("button", { name: `Open job details for ${id}` }));
  fireEvent.click(screen.getByRole("button", { name: /^FAILED/ }));
  await act(async () => delayed.resolve(fixtureFor(`/threadmill/api/jobs/${id}`)));
  expect(screen.getByText("Select a job.")).toBeInTheDocument();
  expect(screen.queryByText("Sensitive details redacted.")).not.toBeInTheDocument();
});
