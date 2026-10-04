package com.practice.grantbatch;

import com.practice.grantbatch.config.Topology;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;

/**
 * One jar, many roles. SERVICE_NAME picks an entry from topology.json, which decides
 * the Spring profile (master | worker) and the HTTP port.
 */
@SpringBootApplication
public class GrantBatchApplication {

    public static void main(String[] args) {
        String serviceName = System.getenv().getOrDefault("SERVICE_NAME", "grant-manager");
        Topology.ServiceNode node = Topology.load().service(serviceName);

        SpringApplication app = new SpringApplication(GrantBatchApplication.class);
        app.setAdditionalProfiles(node.profile());
        app.setDefaultProperties(Map.of(
                "server.port", node.port(),
                "spring.application.name", node.name()));
        app.run(args);
    }
}
