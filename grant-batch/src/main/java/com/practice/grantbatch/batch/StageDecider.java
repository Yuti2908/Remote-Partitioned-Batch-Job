package com.practice.grantbatch.batch;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@Profile("master")
public class StageDecider implements JobExecutionDecider {
    private final JdbcTemplate jdbc;
    private final MeterRegistry registry;

    public StageDecider(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.registry = registry;
    }

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        Integer staged = jdbc.queryForObject("SELECT count(*) FROM stage_record", Integer.class);
        if (staged != null && staged > 0) {
            registry.counter("batch.stage2.triggered").increment();   // leading indicator of Kafka trouble
            return new FlowExecutionStatus("RUN_STAGE2");
        }
        return new FlowExecutionStatus("SKIP");
    }
}
