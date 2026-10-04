package com.practice.grantbatch.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

public record Topology(Settings settings, List<ServiceNode> services) {

    public record Settings(int fetchBatchSize, int gridSize, int chunkSize, Topics topics, Egc egc) {}
    public record Topics(String partitionRequests, String stockExternal, String cashInternal) {}
    public record Egc(String host, int port, int deadlineMs, int maxAttempts) {}
    public record ServiceNode(String name, String profile, int port) {}

    public static Topology load() {
        try (InputStream in = Topology.class.getResourceAsStream("/topology.json")) {
            return new ObjectMapper().readValue(in, Topology.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ServiceNode service(String name) {
        return services.stream().filter(s -> s.name().equals(name)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown service '" + name + "' in topology.json"));
    }
}
