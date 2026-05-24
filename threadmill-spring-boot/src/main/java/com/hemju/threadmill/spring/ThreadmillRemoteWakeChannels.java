package com.hemju.threadmill.spring;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import com.hemju.threadmill.core.engine.RemoteWakeChannel;

final class ThreadmillRemoteWakeChannels implements AutoCloseable {

    private static final ThreadmillRemoteWakeChannels NONE = new ThreadmillRemoteWakeChannels(null);

    private final RemoteWakeChannel channel;

    private ThreadmillRemoteWakeChannels(RemoteWakeChannel channel) {
        this.channel = channel;
    }

    static ThreadmillRemoteWakeChannels none() {
        return NONE;
    }

    static ThreadmillRemoteWakeChannels of(RemoteWakeChannel channel) {
        return new ThreadmillRemoteWakeChannels(Objects.requireNonNull(channel, "channel"));
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
        if (channel != null) channel.close();
    }
}
