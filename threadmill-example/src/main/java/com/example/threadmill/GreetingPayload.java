package com.example.threadmill;

import com.hemju.threadmill.core.handler.JobPayload;

/** Tiny serializable payload used by the demo. */
public final class GreetingPayload implements JobPayload {

    public String name;
    public int sequence;

    public GreetingPayload() {}

    public GreetingPayload(String name, int sequence) {
        this.name = name;
        this.sequence = sequence;
    }
}
