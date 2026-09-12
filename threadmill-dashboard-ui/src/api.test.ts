import { afterEach, expect, it, vi } from "vitest";
import { api } from "./api";

afterEach(() => vi.unstubAllGlobals());

it("shows only the public ProblemDetail message for a client error", async () => {
  vi.stubGlobal("fetch", () => Promise.resolve(new Response(JSON.stringify({
    status: 409, detail: "stale job version", stackTrace: "private internals"
  }), { status: 409, statusText: "Conflict", headers: { "Content-Type": "application/problem+json" } })));
  await expect(api("/jobs/x")).rejects.toThrow("409 Conflict: stale job version");
});

it.each([
  ["text/html", 502, "<html>private proxy internals</html>"],
  ["application/problem+json", 500, '{"detail":"private server internals"}'],
  ["application/problem+json", 400, "not json"]
])("keeps unexpected or malformed error bodies out of the UI: %s %s", async (contentType, status, body) => {
  vi.stubGlobal("fetch", () => Promise.resolve(new Response(body, {
    status, statusText: "Request failed", headers: { "Content-Type": contentType }
  })));
  await expect(api("/jobs/x")).rejects.toThrow(new Error(`${status} Request failed`));
});
