# v0.3.0 compatibility fixtures

Generated with the unmodified production model and `JsonJobSerializer` at tag
`v0.3.0`, commit `0af7e0ac22f36f255e00610c93aac566ba0dfe87`, on Java 25.
The generator fixed ids, timestamps, version 7, Unicode payload/diagnostics,
workflow relationships, and eight states. Log timestamps record generation time. These are historical bytes: do not
regenerate them using the current serializer to make an upgrade test pass.

They intentionally omit the later `failureDecision` and `executionRevision`
fields. Legacy failures have unknown retry disposition and require operator
review; zero is the initial execution revision. Type tags remain exact class
names and are not interpreted as Java types by these fixture-read tests.
