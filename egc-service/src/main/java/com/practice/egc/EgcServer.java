package com.practice.egc;

import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Stand-in for the real "egc" service. Fault-injection knobs (env vars) let you
 * manufacture tail latency on purpose:
 *   BASE_LATENCY_MS (5)  SLOW_PERCENT (0)  SLOW_MS (1500)  ERROR_PERCENT (0)
 */
public class EgcServer {

    public static void main(String[] args) throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);

        Server server = ServerBuilder.forPort(env("GRPC_PORT", 9090))
                .addService(new EgcServiceImpl(registry,
                        env("BASE_LATENCY_MS", 5), env("SLOW_PERCENT", 0),
                        env("SLOW_MS", 1500), env("ERROR_PERCENT", 0)))
                .build().start();

        HttpServer http = HttpServer.create(new InetSocketAddress(env("METRICS_PORT", 8090)), 0);
        http.createContext("/metrics", ex -> {
            byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4");
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(body); }
        });
        http.start();

        System.out.println("egc-service up: grpc=" + server.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        server.awaitTermination();
    }

    private static int env(String key, int def) {
        String v = System.getenv(key);
        return v == null ? def : Integer.parseInt(v);
    }
}
