package com.hemju.threadmill.spring;

import java.util.Objects;

import org.springframework.context.SmartLifecycle;

import com.hemju.threadmill.core.engine.ProcessingNode;

final class ThreadmillRemoteWakeLifecycle implements SmartLifecycle {

    private final ThreadmillRemoteWakeChannels channels;
    private final ProcessingNode node;
    private volatile boolean running;

    ThreadmillRemoteWakeLifecycle(ThreadmillRemoteWakeChannels channels, ProcessingNode node) {
        this.channels = Objects.requireNonNull(channels, "channels");
        this.node = Objects.requireNonNull(node, "node");
    }

    @Override
    public void start() {
        if (running) return;
        channels.start(node::wake);
        running = true;
    }

    @Override
    public void stop() {
        if (!running) return;
        running = false;
        channels.close();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return ThreadmillLifecycle.PHASE + 1;
    }
}
