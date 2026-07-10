# Security Policy

## Reporting a Vulnerability

Please do not open a public issue for a suspected security vulnerability.

Use GitHub's enabled
[private vulnerability reporting](https://github.com/hemju/threadmill/security/advisories/new)
to send the maintainers:

- a description of the issue;
- affected modules or versions, if known;
- a minimal reproduction or proof of concept, if available;
- any known impact, workaround, or mitigation.

You should receive an acknowledgement within 7 days. Confirmed vulnerabilities
will be handled with a coordinated fix and release note.

## Supported Versions

Threadmill is currently pre-1.0. Security fixes target the latest published
release and `main`. Older pre-1.0 releases are not supported unless a release
note explicitly says otherwise.

## Operational Notes

Threadmill provides at-least-once delivery. Security-sensitive handlers must be
idempotent and must not rely on a job running exactly once.

For Redis-backed deployments, use Redis persistence appropriate for a durable
job store, such as AOF. For PostgreSQL-backed deployments, use PostgreSQL 18 or
later as documented.
