package com.hemju.threadmill.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.function.Consumer;

import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.engine.RemoteWakeChannel;
import com.hemju.threadmill.core.store.ForwardingJobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.test.JobStoreDecoratorContract;

/**
 * Regression for issue #131: the tracing decorator used to implement
 * {@code JobStore} directly and forgot {@code supportsExternalTransactions()}
 * and {@code createRemoteWakeChannel(String)}, so a traced PostgreSQL store
 * reported no external-transaction support and no remote wake channel.
 */
class TracingJobStoreForwardingTest {

  private final ThreadmillTracing tracing = ThreadmillTracing.of(OpenTelemetry.noop());

  @Test
  void forwardsEveryStoreOperationIncludingInterfaceDefaults() {
    JobStoreDecoratorContract.assertForwardsEveryOperation(tracing::wrapStore);
  }

  @Test
  void tracedStoreReportsTheWrappedStoresExternalTransactionSupport() {
    var transactional = new ForwardingJobStore(new InMemoryJobStore()) {
      @Override
      public boolean supportsExternalTransactions() {
        return true;
      }
    };

    assertThat(tracing.wrapStore(transactional).supportsExternalTransactions()).isTrue();
    assertThat(tracing.wrapStore(new InMemoryJobStore()).supportsExternalTransactions())
        .isFalse();
  }

  @Test
  void tracedStoreForwardsRemoteWakeChannelCreation() {
    var channel = new RemoteWakeChannel() {
      @Override
      public void publish(String queue) {}

      @Override
      public void start(Consumer<String> wakeSink) {}

      @Override
      public void close() {}
    };
    var requested = new String[1];
    var wakeCapable = new ForwardingJobStore(new InMemoryJobStore()) {
      @Override
      public Optional<RemoteWakeChannel> createRemoteWakeChannel(String channelName) {
        requested[0] = channelName;
        return Optional.of(channel);
      }
    };

    var created = tracing.wrapStore(wakeCapable).createRemoteWakeChannel("threadmill_wake");

    assertThat(created).containsSame(channel);
    assertThat(requested[0]).isEqualTo("threadmill_wake");
  }
}
