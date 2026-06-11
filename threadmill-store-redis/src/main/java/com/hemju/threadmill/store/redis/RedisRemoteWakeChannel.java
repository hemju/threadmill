package com.hemju.threadmill.store.redis;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.engine.RemoteWakeChannel;

/** Redis Pub/Sub implementation of {@link RemoteWakeChannel}. */
public final class RedisRemoteWakeChannel implements RemoteWakeChannel {

    public static final String CHANNEL = RedisKeys.PREFIX + "wake";

    private static final Logger LOG = LoggerFactory.getLogger(RedisRemoteWakeChannel.class);

    private final AbstractRedisClient client;
    private final AutoCloseable commandConnection;
    private final AutoCloseable pubSubConnection;
    private final Publisher publisher;
    private final Subscriber subscriber;
    private final boolean ownsClient;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RedisRemoteWakeChannel(RedisURI uri) {
        this(uri, CHANNEL);
    }

    public RedisRemoteWakeChannel(RedisURI uri, String channel) {
        this(connectOwned(RedisClient.create(uri), channel), true);
    }

    public RedisRemoteWakeChannel(RedisStoreConfig config) {
        this(config, CHANNEL);
    }

    public RedisRemoteWakeChannel(RedisStoreConfig config, String channel) {
        this(connect(Objects.requireNonNull(config, "config"), channel), true);
    }

    public RedisRemoteWakeChannel(RedisClient client) {
        this(client, CHANNEL);
    }

    public RedisRemoteWakeChannel(RedisClient client, String channel) {
        this(connectStandalone(Objects.requireNonNull(client, "client"), channel), false);
    }

    private RedisRemoteWakeChannel(ConnectionHandle handle, boolean ownsClient) {
        this.client = handle.client();
        this.commandConnection = handle.commandConnection();
        this.pubSubConnection = handle.pubSubConnection();
        this.publisher = handle.publisher();
        this.subscriber = handle.subscriber();
        this.ownsClient = ownsClient;
    }

    @Override
    public void publish(String queue) {
        Names.requireName("queue", queue);
        try {
            publisher.publish(queue);
        } catch (RuntimeException e) {
            LOG.debug("Threadmill Redis remote wake publish failed; dispatcher polling remains the fallback", e);
        }
    }

    @Override
    public void start(Consumer<String> wakeSink) {
        Objects.requireNonNull(wakeSink, "wakeSink");
        if (!running.compareAndSet(false, true)) return;
        try {
            subscriber.subscribe(queue -> {
                if (!running.get()) return;
                try {
                    wakeSink.accept(Names.requireName("queue", queue));
                } catch (RuntimeException e) {
                    LOG.debug("Ignoring invalid Redis remote wake payload", e);
                }
            });
        } catch (RuntimeException e) {
            running.set(false);
            LOG.debug("Threadmill Redis remote wake subscribe failed; dispatcher polling remains the fallback", e);
        }
    }

    @Override
    public void close() {
        running.set(false);
        try {
            subscriber.unsubscribe();
        } catch (RuntimeException e) {
            LOG.debug("Failed to unsubscribe Redis remote wake channel", e);
        }
        closeQuietly(pubSubConnection, "pub/sub");
        closeQuietly(commandConnection, "command");
        if (ownsClient) client.shutdown();
    }

    private static void closeQuietly(AutoCloseable closeable, String label) {
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.debug("Failed to close Redis remote wake {} connection", label, e);
        }
    }

    private record ConnectionHandle(
            AbstractRedisClient client,
            AutoCloseable commandConnection,
            AutoCloseable pubSubConnection,
            Publisher publisher,
            Subscriber subscriber) {}

    @FunctionalInterface
    private interface Publisher {
        void publish(String queue);
    }

    private interface Subscriber {
        void subscribe(Consumer<String> wakeSink);

        void unsubscribe();
    }

    private static ConnectionHandle connect(RedisStoreConfig config, String channel) {
        return switch (config) {
            case RedisStoreConfig.Standalone standalone -> connectOwned(RedisClient.create(standalone.uri()), channel);
            case RedisStoreConfig.Sentinel sentinel -> connectSentinel(sentinel, channel);
            case RedisStoreConfig.Cluster cluster -> connectCluster(cluster, channel);
        };
    }

    /** Connect with a client this channel owns: a partial connect shuts it down. */
    private static ConnectionHandle connectOwned(RedisClient client, String channel) {
        try {
            return connectStandalone(client, channel);
        } catch (RuntimeException connectFailure) {
            client.shutdown();
            throw connectFailure;
        }
    }

    private static ConnectionHandle connectStandalone(RedisClient client, String channel) {
        String wakeChannel = normalizeChannel(channel);
        StatefulRedisConnection<String, String> commandConnection = client.connect();
        StatefulRedisPubSubConnection<String, String> pubSubConnection;
        try {
            pubSubConnection = client.connectPubSub();
        } catch (RuntimeException pubSubFailure) {
            // Partial connect must not leak the already-open command
            // connection (and the owned client's event loops).
            try {
                commandConnection.close();
            } catch (RuntimeException closeFailure) {
                pubSubFailure.addSuppressed(closeFailure);
            }
            throw pubSubFailure;
        }
        return new ConnectionHandle(
                client,
                commandConnection,
                pubSubConnection,
                queue -> commandConnection.sync().publish(wakeChannel, queue),
                new Subscriber() {
                    @Override
                    public void subscribe(Consumer<String> wakeSink) {
                        pubSubConnection.addListener(new RedisPubSubAdapter<>() {
                            @Override
                            public void message(String channel, String message) {
                                if (wakeChannel.equals(channel)) wakeSink.accept(message);
                            }
                        });
                        pubSubConnection.sync().subscribe(wakeChannel);
                    }

                    @Override
                    public void unsubscribe() {
                        pubSubConnection.sync().unsubscribe(wakeChannel);
                    }
                });
    }

    private static ConnectionHandle connectSentinel(RedisStoreConfig.Sentinel config, String channel) {
        var first = config.nodes().getFirst();
        var builder = RedisURI.Builder.sentinel(first.host(), first.port(), config.master());
        for (int i = 1; i < config.nodes().size(); i++) {
            var node = config.nodes().get(i);
            builder.withSentinel(node.host(), node.port());
        }
        if (config.password() != null && !config.password().isBlank()) {
            builder.withPassword(config.password().toCharArray());
        }
        return connectOwned(RedisClient.create(builder.build()), channel);
    }

    private static ConnectionHandle connectCluster(RedisStoreConfig.Cluster config, String channel) {
        String wakeChannel = normalizeChannel(channel);
        var uris = config.nodes().stream()
                .map(node -> RedisURI.Builder.redis(node.host(), node.port()).build())
                .toList();
        RedisClusterClient client = RedisClusterClient.create(uris);
        StatefulRedisClusterConnection<String, String> commandConnection = client.connect();
        StatefulRedisClusterPubSubConnection<String, String> pubSubConnection;
        try {
            pubSubConnection = client.connectPubSub();
        } catch (RuntimeException pubSubFailure) {
            try {
                commandConnection.close();
            } catch (RuntimeException closeFailure) {
                pubSubFailure.addSuppressed(closeFailure);
            }
            // The cluster client is always created (owned) here.
            client.shutdown();
            throw pubSubFailure;
        }
        pubSubConnection.setNodeMessagePropagation(true);
        return new ConnectionHandle(
                client,
                commandConnection,
                pubSubConnection,
                queue -> commandConnection.sync().publish(wakeChannel, queue),
                new Subscriber() {
                    @Override
                    public void subscribe(Consumer<String> wakeSink) {
                        pubSubConnection.addListener(new RedisClusterPubSubAdapter<>() {
                            @Override
                            public void message(RedisClusterNode node, String channel, String message) {
                                if (wakeChannel.equals(channel)) wakeSink.accept(message);
                            }
                        });
                        pubSubConnection.sync().subscribe(wakeChannel);
                    }

                    @Override
                    public void unsubscribe() {
                        pubSubConnection.sync().unsubscribe(wakeChannel);
                    }
                });
    }

    private static String normalizeChannel(String channel) {
        if (channel == null || channel.isBlank()) return CHANNEL;
        return Names.requireName("channel", channel);
    }
}
