package com.practice.grantbatch.config;

import com.practice.grantbatch.batch.*;
import com.practice.grantbatch.kafka.StepRequestSerde;
import com.practice.grantbatch.model.Models.AwardResult;
import com.practice.grantbatch.model.Models.GrantGroup;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Master role: stage 1 = partitioned manager step (publishes only step-execution ids to Kafka,
 * then polls the job repository for completion); stage 2 = local step, only if stage_record has rows.
 */
@Configuration
@Profile("master")
@EnableBatchIntegration
public class ManagerConfig {

    @Bean
    public DirectChannel managerRequests() {
        return new DirectChannel();
    }

    @Bean
    ProducerFactory<String, StepExecutionRequest> requestProducerFactory(KafkaProperties kp, MeterRegistry registry) {
        Map<String, Object> props = new HashMap<>(kp.buildProducerProperties(null));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StepRequestSerde.Ser.class);
        DefaultKafkaProducerFactory<String, StepExecutionRequest> pf = new DefaultKafkaProducerFactory<>(props);
        pf.addListener(new MicrometerProducerListener<>(registry));
        return pf;
    }

    @Bean
    KafkaTemplate<String, StepExecutionRequest> requestKafkaTemplate(ProducerFactory<String, StepExecutionRequest> pf) {
        return new KafkaTemplate<>(pf);
    }

    /** stepExecutionId % partitions => requests spread evenly over the topic's partitions (null keys would not). */
    @Bean
    public IntegrationFlow managerOutboundFlow(KafkaTemplate<String, StepExecutionRequest> template, Topology t) {
        return IntegrationFlow.from(managerRequests())
                .handle(Kafka.outboundChannelAdapter(template)
                        .topic(t.settings().topics().partitionRequests())
                        .partitionIdExpression("payload.stepExecutionId % " + t.settings().gridSize())
                        .sync(true))
                .get();
    }

    @Bean
    public Step stage1ManagerStep(RemotePartitioningManagerStepBuilderFactory factory,
                                  GrantPartitioner partitioner, Topology t) {
        return factory.get("stage1ManagerStep")
                .partitioner("workerStep", partitioner)
                .gridSize(t.settings().gridSize())
                .outputChannel(managerRequests())
                .pollInterval(2000)          // no inputChannel => completion detected by polling BATCH_STEP_EXECUTION
                .build();
    }

    @Bean
    @StepScope
    public StageGroupReader stageGroupReader(JdbcTemplate jdbc, StringRedisTemplate redis,
                                             ObjectMapper mapper, MeterRegistry registry) {
        return new StageGroupReader(jdbc, redis, mapper, registry);
    }

    @Bean
    public Step stage2Step(JobRepository repo, PlatformTransactionManager tx, StageGroupReader reader,
                           GrantProcessor processor, GrantWriter writer, BatchMetricsListener listener, Topology t) {
        return new StepBuilder("stage2Step", repo)
                .<GrantGroup, AwardResult>chunk(t.settings().chunkSize(), tx)
                .reader(reader).processor(processor).writer(writer)
                .listener((StepExecutionListener) listener)
                .listener((ChunkListener) listener)
                .build();
    }

    @Bean
    public Job grantJob(JobRepository repo, Step stage1ManagerStep, Step stage2Step,
                        StageDecider decider, JobLoggingListener jobListener) {
        return new JobBuilder("grantJob", repo)
                .listener(jobListener)
                .start(stage1ManagerStep)
                .next(decider).on("RUN_STAGE2").to(stage2Step)
                .from(decider).on("*").end()
                .end()
                .build();
    }
}
