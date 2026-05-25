# threadmill-dashboard-spring

Spring MVC/Security adapter for the Threadmill dashboard.

The portable dashboard service and DTOs live in `threadmill-dashboard-api`.
This module exposes them over Spring MVC at `/threadmill/api/**` by default.
Authentication stays in the host; Threadmill adds a scoped dashboard security
chain, redaction, audit hooks, and state-machine enforcement for operator
actions.

If the `threadmill-dashboard-ui` asset jar is also on the classpath, this
adapter mounts those static assets under `/threadmill`. The UI remains a
portable static app and can be reused by other framework adapters.

## Security

`DashboardAuthorizer` maps the current Spring `Authentication` to dashboard
permissions. The default implementation grants permissions from authorities
named `THREADMILL_<PERMISSION>` or `ROLE_THREADMILL_<PERMISSION>`; `ADMIN` or
`ROLE_THREADMILL_ADMIN` grants all dashboard permissions.

By default, Threadmill registers a `SecurityFilterChain` for
`/threadmill/api/**`. It requires authentication and CSRF tokens for mutating
requests, using `CookieCsrfTokenRepository.withHttpOnlyFalse()` so the static
UI can send the token back in the configured header. Set
`threadmill.dashboard.security.auto-configure=false` to provide a custom chain.
If no `SecurityFilterChain` exists, startup fails unless unsafe read-only local
mode is enabled.

Sensitive fields are redacted by default: payload arguments, metadata, logs,
results, and failure messages. Full detail requires both
`DashboardOptions.exposeSensitiveDetails=true` and
`VIEW_SENSITIVE_DETAILS`.

## Configuration

| Property | Default | Purpose |
|---|---:|---|
| `threadmill.dashboard.api.base-path` | `/threadmill/api` | Base path for every API endpoint. |
| `threadmill.dashboard.security.auto-configure` | `true` | Register Threadmill's scoped dashboard security chain. |
| `threadmill.dashboard.expose-sensitive-details` | `false` | Allow full payload / metadata / log / result exposure when the user also has `VIEW_SENSITIVE_DETAILS`. |
| `threadmill.dashboard.allow-unsafe-read-only-without-authentication` | `false` | Local-only escape hatch for read-only unauthenticated access. |

The default audit sink is a noop and logs a startup warning. Production
applications should provide a `DashboardAuditSink` bean that writes operator
actions to the host audit pipeline.

## Operator Actions

The controller exposes pause/resume queue, requeue, retry scheduling, soft-delete,
pending-job replacement, recurring trigger/update/delete, and node/queue/job
read endpoints. Actions always re-check permissions server-side and reject
illegal state transitions.
