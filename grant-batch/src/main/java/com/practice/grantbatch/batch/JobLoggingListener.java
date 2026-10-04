package com.practice.grantbatch.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@Profile("master")
public class JobLoggingListener implements JobExecutionListener {
    private static final Logger log = LoggerFactory.getLogger(JobLoggingListener.class);

    @Override
    public void afterJob(JobExecution je) {
        boolean stage2 = je.getStepExecutions().stream().anyMatch(s -> s.getStepName().equals("stage2Step"));
        log.info("Job {} finished status={} duration={}s stage2Ran={}", je.getId(), je.getStatus(),
                Duration.between(je.getStartTime(), je.getEndTime()).toSeconds(), stage2);
    }
}
