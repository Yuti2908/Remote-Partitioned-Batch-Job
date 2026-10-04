package com.practice.grantbatch.config;

import com.practice.egc.proto.EgcServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;

/** Beans needed by every role (master and worker). */
@Configuration
public class AppConfig {

    @Bean
    Topology topology() {
        return Topology.load();
    }

    /**
     * Spring Batch's built-in timers may publish to Micrometer's global registry; bridge Boot's registry
     * to it. Check /actuator/prometheus for spring_batch_* series to confirm.
     */
    @Bean
    SmartInitializingSingleton globalRegistryBridge(MeterRegistry registry) {
        return () -> Metrics.addRegistry(registry);
    }

    // ---- topics (created by KafkaAdmin on startup) ----
    @Bean NewTopic partitionRequestsTopic(Topology t) {
        return TopicBuilder.name(t.settings().topics().partitionRequests())
                .partitions(t.settings().gridSize()).replicas(1).build();
    }
    @Bean NewTopic stockTopic(Topology t) {
        return TopicBuilder.name(t.settings().topics().stockExternal()).partitions(1).replicas(1).build();
    }
    @Bean NewTopic cashTopic(Topology t) {
        return TopicBuilder.name(t.settings().topics().cashInternal()).partitions(1).replicas(1).build();
    }

    // ---- award publisher (String/String) ----
    @Bean
    ProducerFactory<String, String> awardProducerFactory(KafkaProperties props, MeterRegistry registry) {
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props.buildProducerProperties(null));
        pf.addListener(new MicrometerProducerListener<>(registry));   // kafka_producer_* metrics
        return pf;
    }

    @Bean
    KafkaTemplate<String, String> awardKafkaTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf);
    }

    // ---- egc gRPC client ----
    @Bean(destroyMethod = "shutdownNow")
    ManagedChannel egcChannel(Topology t) {
        return ManagedChannelBuilder.forAddress(t.settings().egc().host(), t.settings().egc().port())
                .usePlaintext().build();
    }

    @Bean
    EgcServiceGrpc.EgcServiceBlockingStub egcStub(ManagedChannel channel) {
        return EgcServiceGrpc.newBlockingStub(channel);
    }
}
