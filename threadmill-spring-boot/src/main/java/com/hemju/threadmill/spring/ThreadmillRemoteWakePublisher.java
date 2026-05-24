package com.hemju.threadmill.spring;

import java.util.Objects;

import com.hemju.threadmill.core.engine.LocalWakeBus;

final class ThreadmillRemoteWakePublisher implements AutoCloseable {

    private final Runnable unregister;

    ThreadmillRemoteWakePublisher(LocalWakeBus wakeBus, ThreadmillRemoteWakeChannels channels) {
        Objects.requireNonNull(wakeBus, "wakeBus");
        Objects.requireNonNull(channels, "channels");
        this.unregister = wakeBus.register(channels::publish);
    }

    @Override
    public void close() {
        unregister.run();
    }
}
