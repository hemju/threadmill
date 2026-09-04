package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.engine.RemoteWakeChannel;
import com.hemju.threadmill.core.store.ForwardingJobStore;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.JobStoreDecoratorContract;

/**
 * The shared decorator base forwards every {@link JobStore} operation — the
 * interface defaults included — so a decorator built on it cannot advertise
 * different capabilities than the store it wraps (issue #131).
 */
class ForwardingJobStoreTest {

  @Test
  void forwardsEveryStoreOperationIncludingInterfaceDefaults() {
    JobStoreDecoratorContract.assertForwardsEveryOperation(ForwardingJobStore::new);
  }

  @Test
  void decoratorContractNamesADefaultMethodThatFellThroughToTheInterface() {
    // A decorator that "forgets" a default forward looks exactly like this:
    // it compiles, answers the interface default, and hides the wrapped
    // store's capability. The contract must fail on it by method name.
    assertThatThrownBy(() -> JobStoreDecoratorContract.assertForwardsEveryOperation(
            delegate -> new ForwardingJobStore(delegate) {
              @Override
              public boolean supportsExternalTransactions() {
                return false;
              }
            }))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("supportsExternalTransactions");
  }

  @Test
  void forwardsExternalTransactionSupportOfTheWrappedStore() {
    var transactional = new ForwardingJobStore(new InMemoryJobStore()) {
      @Override
      public boolean supportsExternalTransactions() {
        return true;
      }
    };

    assertThat(new ForwardingJobStore(transactional).supportsExternalTransactions())
        .isTrue();
    assertThat(new ForwardingJobStore(new InMemoryJobStore()).supportsExternalTransactions())
        .isFalse();
  }

  @Test
  void forwardsRemoteWakeChannelCreationWithTheRequestedChannelName() {
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

    var created = new ForwardingJobStore(wakeCapable).createRemoteWakeChannel("threadmill_wake");

    assertThat(created).containsSame(channel);
    assertThat(requested[0]).isEqualTo("threadmill_wake");
  }

  @Test
  void delegateIsTheImmediateLayerSoAChainUnwrapsOneStepAtATime() {
    var inner = new InMemoryJobStore();
    var middle = new ForwardingJobStore(inner);
    var outer = new ForwardingJobStore(middle);

    assertThat(outer.delegate()).isSameAs(middle);
    assertThat(middle.delegate()).isSameAs(inner);
    assertThat(inner.delegate()).isSameAs(inner);
  }
}
