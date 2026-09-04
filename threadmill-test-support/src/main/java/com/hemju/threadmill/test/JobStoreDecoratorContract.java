package com.hemju.threadmill.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.RemoteWakeChannel;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStore.NudgeOutcome;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.NodeHeartbeat;

/**
 * Reflective contract for {@link JobStore} decorators: every SPI operation —
 * the interface's {@code default} methods included — must reach the wrapped
 * store exactly once with the caller's arguments, and the wrapped store's
 * result must come back unchanged.
 *
 * <p>The shared {@link AbstractJobStoreContractTest} proves a decorator does
 * not <em>break</em> the store it wraps, but it cannot see a {@code default}
 * method that silently fell through to the interface: an in-memory store
 * answers {@code false} to {@link JobStore#supportsExternalTransactions()}
 * whether or not the decorator forwarded the call. This check wraps a
 * recording {@link Proxy} instead, so a forgotten forward — today's or one
 * for an SPI method added later — fails by name.
 *
 * <p>A decorator may make additional calls on its delegate (the tracing
 * decorator reads {@link JobStore#describe()} for every span); only the
 * operation under test is required to arrive exactly once. Sample values for
 * a new parameter or return type must be added to this class deliberately:
 * an unknown type fails the check rather than being skipped.
 */
public final class JobStoreDecoratorContract {

  private JobStoreDecoratorContract() {}

  /**
   * Assert that the decorator produced by {@code decorate} forwards every
   * {@link JobStore} operation to its delegate and returns the delegate's
   * immediate identity from {@link JobStore#delegate()}.
   *
   * @param decorate wraps the recording delegate it is handed; called once
   */
  public static void assertForwardsEveryOperation(UnaryOperator<JobStore> decorate) {
    var recorder = new RecordingHandler();
    var delegate = (JobStore) Proxy.newProxyInstance(
        JobStore.class.getClassLoader(), new Class<?>[] {JobStore.class}, recorder);
    var decorated = decorate.apply(delegate);
    var decoratorName = decorated.getClass().getName();

    assertThat(decorated.delegate())
        .as("%s.delegate() must return its immediate delegate", decoratorName)
        .isSameAs(delegate);

    var operations = Arrays.stream(JobStore.class.getMethods())
        .filter(method -> !method.getName().equals("delegate"))
        .sorted(Comparator.comparing(Method::getName))
        .toList();
    for (var operation : operations) {
      recorder.invocations.clear();
      var args = sampleArguments(operation);
      var returned = invoke(decorated, operation, args);

      var forwarded = recorder.invocations.stream()
          .filter(invocation -> invocation.method().equals(operation))
          .toList();
      assertThat(forwarded)
          .as(
              "%s must forward JobStore.%s to its delegate exactly once (a default method that"
                  + " is not forwarded silently falls back to the interface default)",
              decoratorName, describe(operation))
          .hasSize(1);
      var invocation = forwarded.getFirst();
      var parameterTypes = operation.getParameterTypes();
      for (var i = 0; i < args.length; i++) {
        var argument = assertThat(invocation.args()[i])
            .as(
                "%s must pass argument %d of JobStore.%s through unchanged",
                decoratorName, i, describe(operation));
        // Primitives are re-boxed on every reflective boundary, so only reference
        // arguments can be checked by identity.
        if (parameterTypes[i].isPrimitive()) {
          argument.isEqualTo(args[i]);
        } else {
          argument.isSameAs(args[i]);
        }
      }
      if (operation.getReturnType() != void.class) {
        if (operation.getReturnType().isPrimitive()) {
          assertThat(returned)
              .as(
                  "%s must return the delegate's result from JobStore.%s",
                  decoratorName, describe(operation))
              .isEqualTo(invocation.returned());
        } else {
          assertThat(returned)
              .as(
                  "%s must return the delegate's result from JobStore.%s",
                  decoratorName, describe(operation))
              .isSameAs(invocation.returned());
        }
      }
    }
  }

  private static Object invoke(JobStore decorated, Method operation, Object[] args) {
    try {
      return operation.invoke(decorated, args);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (InvocationTargetException e) {
      throw new AssertionError(
          "JobStore." + describe(operation) + " threw through the decorator", e.getCause());
    }
  }

  private static String describe(Method method) {
    var params =
        Arrays.stream(method.getParameterTypes()).map(Class::getSimpleName).toList();
    return method.getName() + "(" + String.join(", ", params) + ")";
  }

  private static Object[] sampleArguments(Method operation) {
    var types = operation.getGenericParameterTypes();
    var args = new Object[types.length];
    for (var i = 0; i < types.length; i++) {
      args[i] = sample(types[i], operation.getName() + "#" + i);
    }
    return args;
  }

  /**
   * A fresh, type-correct sample for {@code type}. Every reference sample is a
   * new instance so the identity assertions above are meaningful.
   */
  private static Object sample(Type type, String label) {
    if (type instanceof ParameterizedType parameterized) {
      var raw = (Class<?>) parameterized.getRawType();
      var element = parameterized.getActualTypeArguments();
      if (raw == Optional.class) return Optional.of(sample(element[0], label));
      if (raw == List.class) return new ArrayList<>(List.of(sample(element[0], label)));
      if (raw == Set.class) return new HashSet<>(Set.of(sample(element[0], label)));
      if (raw == Map.class) {
        var map = new HashMap<Object, Object>();
        map.put(sample(element[0], label), sample(element[1], label));
        return map;
      }
      return fail("add a sample for parameterized JobStore type " + type);
    }
    var raw = (Class<?>) type;
    if (raw == boolean.class || raw == Boolean.class) return Boolean.TRUE;
    if (raw == int.class || raw == Integer.class) return 7;
    if (raw == long.class || raw == Long.class) return 4242L;
    if (raw == String.class) return "sample-" + label;
    if (raw == Instant.class) return Instant.parse("2026-01-02T03:04:05Z");
    if (raw == Duration.class) return Duration.ofSeconds(90);
    if (raw == Job.class) return Jobs.enqueued("com.example.DecoratedHandler");
    if (raw == JobId.class) return JobId.newId();
    if (raw == NodeId.class) return NodeId.newId();
    if (raw == JobState.class) return JobState.SUCCEEDED;
    if (raw == JobSearch.class) return new JobSearch(JobState.ENQUEUED, "q", null, 10, 0);
    if (raw == JobReplacement.class) {
      return JobReplacement.ofSpec(
          JobSpec.of("com.example.Replacement", new JobArgument("java.lang.String", "\"r\"")));
    }
    if (raw == JobStoreCapabilities.class) {
      return new JobStoreCapabilities(1024, 128, 64, 5, false, false, false, false);
    }
    if (raw == EnqueueResult.class) return new EnqueueResult.Coalesced(JobId.newId());
    if (raw == NudgeOutcome.class) return NudgeOutcome.DISABLED;
    if (raw == NodeHeartbeat.class) {
      return new NodeHeartbeat(NodeId.newId(), Instant.parse("2026-01-02T03:04:05Z"));
    }
    if (raw == RemoteWakeChannel.class) return new NoopWakeChannel();
    if (raw == CronTask.class) {
      return new CronTask(
          "decorated-" + label,
          new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
          "com.example.RecurringHandler",
          new JobArgument("java.lang.String", "\"c\""),
          "system",
          0,
          null,
          null,
          false,
          CronTask.MissedRunPolicy.DROP,
          ZoneOffset.UTC,
          true);
    }
    if (raw == CronTaskScheduleState.class) {
      return CronTaskScheduleState.initial(
          "decorated-" + label, Instant.parse("2026-01-02T03:04:05Z"), "fingerprint");
    }
    return fail("add a sample for JobStore type " + type);
  }

  private record Invocation(Method method, Object[] args, Object returned) {}

  private static final class RecordingHandler implements InvocationHandler {
    private final List<Invocation> invocations = new CopyOnWriteArrayList<>();

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      if (method.getDeclaringClass() == Object.class) {
        return switch (method.getName()) {
          case "toString" -> "RecordingJobStore";
          case "hashCode" -> System.identityHashCode(proxy);
          case "equals" -> proxy == args[0];
          default -> throw new UnsupportedOperationException(method.getName());
        };
      }
      var returned = method.getReturnType() == void.class
          ? null
          : sample(method.getGenericReturnType(), method.getName() + "#return");
      invocations.add(new Invocation(method, args == null ? new Object[0] : args, returned));
      return returned;
    }
  }

  private static final class NoopWakeChannel implements RemoteWakeChannel {
    @Override
    public void publish(String queue) {}

    @Override
    public void start(Consumer<String> wakeSink) {}

    @Override
    public void close() {}
  }
}
