package com.practice.grantbatch.config;

import com.practice.grantbatch.batch.BatchMetricsListener;
import com.practice.grantbatch.batch.GrantGroupingReader;
import com.practice.grantbatch.batch.GrantProcessor;
import com.practice.grantbatch.batch.GrantWriter;
import com.practice.grantbatch.kafka.StepRequestSerde;
import com.practice.grantbatch.model.Models.AwardResult;
import com.practice.grantbatch.model.Models.GrantChange;
import com.practice.grantbatch.model.Models.GrantGroup;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/** Worker role: consume step-execution requests from Kafka, run the chunk-oriented worker step. */
@Configuration
@Profile("worker")
@EnableBatchIntegration
public class WorkerConfig {

    @Bean
    public DirectChannel workerRequests() {
        return new DirectChannel();
    }

    @Bean
    public ConsumerFactory<String, StepExecutionRequest> requestConsumerFactory(KafkaProperties kp, MeterRegistry registry) {
        Map<String, Object> props = new HashMap<>(kp.buildConsumerProperties(null));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "grant-workers");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);   // one partition per poll
        DefaultKafkaConsumerFactory<String, StepExecutionRequest> cf =
                new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StepRequestSerde.De());
        cf.addListener(new MicrometerConsumerListener<>(registry)); // kafka_consumer_* metrics
        return cf;
    }

    @Bean
    public IntegrationFlow workerInboundFlow(ConsumerFactory<String, StepExecutionRequest> cf, Topology t) {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(cf,
                                KafkaMessageDrivenChannelAdapter.ListenerMode.record,
                                t.settings().topics().partitionRequests())
                        .configureListenerContainer(c -> c.ackMode(ContainerProperties.AckMode.RECORD)
                                .id("grant-worker-listener")))
                .channel(workerRequests())
                .get();
    }

    /** Reads this partition's slice of grant_change and folds consecutive rows of one grant into a GrantGroup. */
    @Bean
    @StepScope
    public GrantGroupingReader grantReader(DataSource ds,
            @Value("#{stepExecutionContext['fromGrantId']}") Long from,
            @Value("#{stepExecutionContext['toGrantId']}") Long to,
            @Value("#{stepExecutionContext['maxChangeId']}") Long maxId) {
        JdbcCursorItemReader<GrantChange> cursor = new JdbcCursorItemReaderBuilder<GrantChange>()
                .name("grantChangeCursor")
                .dataSource(ds)
                .sql("SELECT id, grant_id, change_type, new_value FROM grant_change "
                        + "WHERE status = 'NEW' AND grant_id BETWEEN ? AND ? AND id <= ? ORDER BY grant_id, id")
                .preparedStatementSetter(ps -> { ps.setLong(1, from); ps.setLong(2, to); ps.setLong(3, maxId); })
                .rowMapper((rs, i) -> new GrantChange(rs.getLong("id"), rs.getLong("grant_id"),
                        rs.getString("change_type"), rs.getString("new_value")))
                .fetchSize(100)
                .build();
        return new GrantGroupingReader(cursor);
    }

    @Bean
    public Step workerStep(RemotePartitioningWorkerStepBuilderFactory factory, PlatformTransactionManager tx,
                           GrantGroupingReader reader, GrantProcessor processor, GrantWriter writer,
                           BatchMetricsListener listener, Topology t) {
        return factory.get("workerStep")
                .inputChannel(workerRequests())
                .<GrantGroup, AwardResult>chunk(t.settings().chunkSize(), tx)
                .reader(reader).processor(processor).writer(writer)
                .listener((StepExecutionListener) listener)
                .listener((ChunkListener) listener)
                .build();
    }
}
