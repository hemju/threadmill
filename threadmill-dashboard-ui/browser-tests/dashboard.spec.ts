import { expect, test, type Browser, type BrowserContext, type Page } from "@playwright/test";

const dashboardPath = "/threadmill/";
const apiBasePath = "/ops/threadmill/api";

async function openDashboard(
  browser: Browser,
  username = "admin",
  password = username
): Promise<{ context: BrowserContext; page: Page }> {
  const context = await browser.newContext({ httpCredentials: { username, password } });
  await context.addInitScript((configuredApiBasePath) => {
    (
      window as Window & {
        __THREADMILL_DASHBOARD_CONFIG__?: { apiBasePath?: string };
      }
    ).__THREADMILL_DASHBOARD_CONFIG__ = { apiBasePath: configuredApiBasePath };
  }, apiBasePath);
  const page = await context.newPage();
  await page.goto(dashboardPath);
  await expect(page.getByRole("heading", { name: "Threadmill" })).toBeVisible();
  return { context, page };
}

function jobRow(page: Page, handlerType: string) {
  return page.locator("tr.job-row").filter({ hasText: handlerType });
}

function recurringRow(page: Page, name: string) {
  return page.getByText(name, { exact: true }).locator("..").locator("..");
}

test("requires authentication and honors the configured API base path", async ({
  browser,
  request
}) => {
  const anonymous = await request.get(dashboardPath, { maxRedirects: 0 });
  expect(anonymous.status()).toBe(401);

  const requests: string[] = [];
  const { context, page } = await openDashboard(browser);
  page.on("request", (request) => requests.push(new URL(request.url()).pathname));
  await page.reload();

  await expect(page.getByText("admin", { exact: true })).toBeVisible();
  await expect.poll(() => requests.some((path) => path === `${apiBasePath}/session`)).toBe(true);
  expect(requests.some((path) => path.startsWith("/threadmill/api/"))).toBe(false);
  await context.close();
});

test("pauses and resumes a queue through the mounted Spring API with CSRF", async ({ browser }) => {
  const { context, page } = await openDashboard(browser);

  const pauseRequestPromise = page.waitForRequest(
    (request) => request.method() === "POST" && request.url().endsWith("/queues/default/pause")
  );
  await page.getByRole("button", { name: "Pause" }).click();
  const pauseRequest = await pauseRequestPromise;
  expect(pauseRequest.headers()["x-xsrf-token"]).toBeTruthy();
  await expect(page.getByText("paused: default")).toBeVisible();
  await expect(page.getByRole("button", { name: "Resume" })).toBeEnabled();

  await page.getByRole("button", { name: "Resume" }).click();
  await expect(page.getByText("resumed: default")).toBeVisible();
  await expect(page.getByRole("button", { name: "Pause" })).toBeEnabled();
  await context.close();
});

test("performs requeue, retry, replace, and delete job actions", async ({ browser }) => {
  const { context, page } = await openDashboard(browser);

  const replaceRow = jobRow(page, "com.example.ReplaceMeHandler");
  await expect(replaceRow).toBeVisible();
  page.once("dialog", (dialog) => dialog.accept("com.example.ReplacedHandler"));
  await replaceRow.getByRole("button", { name: "Replace" }).click();
  await expect(page.getByText(/replaced: 018f0000-0000-7000-8000-000000000101/)).toBeVisible();
  await expect(jobRow(page, "com.example.ReplacedHandler")).toBeVisible();

  await page.getByRole("button", { name: /^FAILED/ }).click();
  const requeueRow = jobRow(page, "com.example.RequeueMeHandler");
  await requeueRow.getByRole("button", { name: "Requeue" }).click();
  await expect(page.getByText(/requeued: 018f0000-0000-7000-8000-000000000102/)).toBeVisible();

  const retryRow = jobRow(page, "com.example.RetryMeHandler");
  await retryRow.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByText(/retry scheduled: 018f0000-0000-7000-8000-000000000103/)).toBeVisible();

  await page.getByRole("button", { name: /^SUCCEEDED/ }).click();
  const deleteRow = jobRow(page, "com.example.DeleteMeHandler");
  await deleteRow.getByRole("button", { name: "Delete" }).click();
  await expect(page.getByText(/deleted: 018f0000-0000-7000-8000-000000000104/)).toBeVisible();
  await expect(deleteRow).toHaveCount(0);
  await context.close();
});

test("performs recurring trigger, update, and delete actions", async ({ browser }) => {
  const { context, page } = await openDashboard(browser);

  await recurringRow(page, "nightly-trigger")
    .getByRole("button", { name: "Trigger recurring" })
    .click();
  await expect(page.getByText("triggered: nightly-trigger")).toBeVisible();

  page.once("dialog", (dialog) => dialog.accept("PT2H"));
  await recurringRow(page, "nightly-update")
    .getByRole("button", { name: "Edit recurring" })
    .click();
  await expect(page.getByText("updated: nightly-update")).toBeVisible();

  const deletedRow = recurringRow(page, "nightly-delete");
  await deletedRow.getByRole("button", { name: "Delete recurring" }).click();
  await expect(page.getByText("deleted: nightly-delete")).toBeVisible();
  await expect(deletedRow).toHaveCount(0);
  await context.close();
});

test("redacts sensitive job details for readers and reveals them to admins", async ({ browser }) => {
  const viewer = await openDashboard(browser, "viewer");
  await jobRow(viewer.page, "com.example.SensitiveHandler").click();
  await expect(viewer.page.getByText("Sensitive details redacted.")).toBeVisible();
  await expect(viewer.page.getByText("visible-to-admin")).toHaveCount(0);
  await viewer.context.close();

  const admin = await openDashboard(browser);
  await jobRow(admin.page, "com.example.SensitiveHandler").click();
  await expect(admin.page.getByText("visible-to-admin", { exact: false })).toBeVisible();
  await expect(admin.page.getByText("full", { exact: true })).toBeVisible();
  await admin.context.close();
});
