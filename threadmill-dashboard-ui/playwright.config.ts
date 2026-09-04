import { defineConfig } from "@playwright/test";

const port = process.env.THREADMILL_BROWSER_PORT;
const serverClasspath = process.env.THREADMILL_BROWSER_SERVER_CLASSPATH;

if (!port || !serverClasspath) {
  throw new Error(
    "THREADMILL_BROWSER_PORT and THREADMILL_BROWSER_SERVER_CLASSPATH are required; "
      + "run the Gradle browserTest task"
  );
}

export default defineConfig({
  testDir: "./browser-tests",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  forbidOnly: !!process.env.CI,
  timeout: 30_000,
  expect: { timeout: 10_000 },
  outputDir: "build/playwright-results",
  reporter: [
    ["list"],
    ["html", { outputFolder: "build/playwright-report", open: "never" }]
  ],
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
    video: "retain-on-failure"
  },
  webServer: {
    command: `java -cp ${JSON.stringify(serverClasspath)} com.hemju.threadmill.dashboard.spring.browser.DashboardBrowserTestApplication`,
    url: `http://127.0.0.1:${port}/threadmill/`,
    reuseExistingServer: false,
    stdout: "pipe",
    stderr: "pipe",
    timeout: 120_000
  }
});
