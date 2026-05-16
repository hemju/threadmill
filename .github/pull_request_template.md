## Summary

Describe the change and why it is needed.

## Testing

- [ ] `./gradlew check`
- [ ] Real-backend tests, if applicable:
  `./gradlew :threadmill-store-postgres:test :threadmill-store-redis:test --rerun-tasks`
- [ ] Docs/Javadoc updated, if applicable

## Checklist

- [ ] The at-least-once delivery contract remains clear and preserved.
- [ ] Store behavior changes are covered by the shared contract tests or a
      named regression test.
- [ ] Public API changes are intentional and documented.
- [ ] No private local material or generated build output is included.
