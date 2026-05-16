package com.example.threadmill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;

/**
 * Demo handler. Each invocation logs the node that processed it together
 * with the payload's sequence number — that's how you can see, across
 * several worker terminals, which node picked up which job.
 */
public final class GreetingHandler implements JobHandler<GreetingPayload> {

    private static final Logger LOG = LoggerFactory.getLogger(GreetingHandler.class);

    @Override
    public void run(GreetingPayload p, JobExecutionContext ctx) throws InterruptedException {
        LOG.info("ran #{} ({}) on node={} attempt={}", p.sequence, p.name, shortId(ctx.nodeId()), ctx.attempt());
        // Simulate a little work so multi-node behaviour is visible to the eye.
        Thread.sleep(150);
    }

    private static String shortId(NodeId n) {
        return n.asUuid().toString().substring(0, 8);
    }
}
