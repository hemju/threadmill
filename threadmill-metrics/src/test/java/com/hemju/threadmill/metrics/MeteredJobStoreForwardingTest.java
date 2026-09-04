package com.hemju.threadmill.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.function.Consumer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.engine.RemoteWakeChannel;
import com.hemju.threadmill.core.store.ForwardingJobStore;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.test.JobStoreDecoratorContract;

/**
 * The metrics decorator instruments claims and writes only; everything else,
 * interface defaults included, must reach the wrapped store unchanged (issue
 * #131).
 */
class MeteredJobStoreForwardingTest {

  private static JobStore metered(JobStore store) {
    return new ThreadmillMetrics(new SimpleMeterRegistry(), store).meteredStore();
  }

  @Test
  void forwardsEveryStoreOperationIncludingInterfaceDefaults() {
    JobStoreDecoratorContract.assertForwardsEveryOperation(MeteredJobStoreForwardingTest::metered);
  }

  @Test
  void meteredStoreReportsTheWrappedStoresExternalTransactionSupport() {
    var transactional = new ForwardingJobStore(new InMemoryJobStore()) {
      @Override
      public boolean supportsExternalTransactions() {
        return true;
      }
    };

    assertThat(metered(transactional).supportsExternalTransactions()).isTrue();
    assertThat(metered(new InMemoryJobStore()).supportsExternalTransactions()).isFalse();
  }

  @Test
  void meteredStoreForwardsRemoteWakeChannelCreation() {
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

    var created = metered(wakeCapable).createRemoteWakeChannel("threadmill_wake");

    assertThat(created).containsSame(channel);
    assertThat(requested[0]).isEqualTo("threadmill_wake");
  }
}
