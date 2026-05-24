# threadmill-tracing

Optional OpenTelemetry tracing for Threadmill.

The module depends on `opentelemetry-api` only. Applications keep ownership of
the OpenTelemetry SDK, exporter, sampling, and resource configuration.

```java
var tracing = ThreadmillTracing.global();

var store = tracing.wrapStore(new PostgresJobStore(dataSource));
var node = ProcessingNode.builder(store)
    .interceptor(tracing.asInterceptor())
    .build();
```

In Spring Boot, expose `tracing.asInterceptor()` as a `JobInterceptor` bean.
The auto-configuration adds user-provided interceptors to the
`ProcessingNode`.

Processing spans are named `threadmill.job.process` and carry job id, queue,
handler type, attempt, node id, final state, and failure cause when present.
Store spans are named `threadmill.store.*` and carry the store description plus
operation-specific attributes such as queue, job id, and claimed count.
