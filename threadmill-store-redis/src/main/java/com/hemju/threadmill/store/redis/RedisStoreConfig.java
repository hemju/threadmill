package com.hemju.threadmill.store.redis;

import java.util.List;
import java.util.Objects;

import io.lettuce.core.RedisURI;

/** Configuration for creating a Redis-backed job store across supported topologies. */
public sealed interface RedisStoreConfig
        permits RedisStoreConfig.Standalone, RedisStoreConfig.Sentinel, RedisStoreConfig.Cluster {

    record Standalone(RedisURI uri) implements RedisStoreConfig {
        public Standalone {
            Objects.requireNonNull(uri, "uri");
        }
    }

    record Sentinel(String master, List<HostAndPort> nodes, String password) implements RedisStoreConfig {
        public Sentinel {
            Objects.requireNonNull(master, "master");
            if (master.isBlank()) throw new IllegalArgumentException("master must not be blank");
            nodes = List.copyOf(nodes);
            if (nodes.isEmpty()) throw new IllegalArgumentException("sentinel nodes must not be empty");
        }
    }

    record Cluster(List<HostAndPort> nodes, String readFrom) implements RedisStoreConfig {
        public Cluster {
            nodes = List.copyOf(nodes);
            if (nodes.isEmpty()) throw new IllegalArgumentException("cluster nodes must not be empty");
            readFrom = readFrom == null || readFrom.isBlank() ? "master" : readFrom;
            if (!"master".equals(readFrom)) {
                throw new IllegalArgumentException("Threadmill engine Redis reads must use master");
            }
        }
    }

    record HostAndPort(String host, int port) {
        public HostAndPort {
            Objects.requireNonNull(host, "host");
            if (host.isBlank()) throw new IllegalArgumentException("host must not be blank");
            if (port <= 0 || port > 65535) throw new IllegalArgumentException("port out of range");
        }
    }
}
