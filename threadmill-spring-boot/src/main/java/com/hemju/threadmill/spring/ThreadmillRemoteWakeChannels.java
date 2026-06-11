package com.hemju.threadmill.spring;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.hemju.threadmill.core.engine.RemoteWakeChannel;

final class ThreadmillRemoteWakeChannels implements AutoCloseable {

    private static final ThreadmillRemoteWakeChannels NONE = new ThreadmillRemoteWakeChannels(null, false);

    private final RemoteWakeChannel channel;
    private final boolean owned;
    private final AtomicBoolean closed = new AtomicBoolean();

    private ThreadmillRemoteWakeChannels(RemoteWakeChannel channel, boolean owned) {
        this.channel = channel;
        this.owned = owned;
    }

    static ThreadmillRemoteWakeChannels none() {
        return NONE;
    }

    /** Wrap a user-provided channel bean. Threadmill never closes it — its own bean destruction does. */
    static ThreadmillRemoteWakeChannels of(RemoteWakeChannel channel) {
        return new ThreadmillRemoteWakeChannels(Objects.requireNonNull(channel, "channel"), false);
    }

    /** Wrap a channel Threadmill created itself; Threadmill owns its lifecycle. */
    static ThreadmillRemoteWakeChannels ofManaged(RemoteWakeChannel channel) {
        return new ThreadmillRemoteWakeChannels(Objects.requireNonNull(channel, "channel"), true);
    }

    Optional<RemoteWakeChannel> channel() {
        return Optional.ofNullable(channel);
    }

    void publish(String queue) {
        if (channel != null) channel.publish(queue);
    }

    void start(Consumer<String> wakeSink) {
        if (channel != null) channel.start(wakeSink);
    }

    @Override
    public void close() {
        // Both the SmartLifecycle stop and Spring's inferred destroy method
        // route here; the CAS keeps the managed close single-shot, and a
        // user-provided bean is never closed by Threadmill at all — closing
        // a bean we do not own is a lifecycle-ownership leak.
        if (channel == null || !owned) return;
        if (!closed.compareAndSet(false, true)) return;
        channel.close();
    }
}
