# Security Policy

## Reporting a Vulnerability

Please do not open a public issue for a suspected security vulnerability.

Until GitHub private vulnerability reporting is configured for this repository,
send a private report to the repository owner with:

- a description of the issue;
- affected modules or versions, if known;
- a minimal reproduction or proof of concept, if available;
- any known impact, workaround, or mitigation.

You should receive an acknowledgement within 7 days. Confirmed vulnerabilities
will be handled with a coordinated fix and release note.

## Supported Versions

Threadmill is currently pre-1.0. Security fixes target the current active
release candidate unless otherwise stated in the release notes.

## Operational Notes

Threadmill provides at-least-once delivery. Security-sensitive handlers must be
idempotent and must not rely on a job running exactly once.

For Redis-backed deployments, use Redis persistence appropriate for a durable
job store, such as AOF. For PostgreSQL-backed deployments, use PostgreSQL 18 or
later as documented.
