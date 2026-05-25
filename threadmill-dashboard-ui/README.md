# threadmill-dashboard-ui

Static operations console for Threadmill.

The UI is a Vite React/TypeScript app using Tailwind CSS, shadcn-style local
components, lucide icons, and TanStack Table. It is designed to be served at
`/threadmill` with the JSON API available from the same origin at
`/threadmill/api/**`, so Spring Security sessions and CSRF tokens work without
CORS or separate browser tokens.

The API base path can be overridden before the app loads:

```html
<script>
  window.__THREADMILL_DASHBOARD_CONFIG__ = { apiBasePath: "/admin/threadmill/api" };
</script>
```

## Build

```bash
npm install
npm run test
npm run build
```

Gradle also wires the static build into `:threadmill-dashboard-ui:check`.

## Layout

The console is intentionally dense: state filters, job table, queue controls,
recurring task controls, node heartbeats, and a job detail drawer are all visible
without a landing page. Permission-gated actions are hidden or disabled in the UI,
but the API remains authoritative.
