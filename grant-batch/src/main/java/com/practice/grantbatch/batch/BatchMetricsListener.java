package com.practice.grantbatch.batch;

import com.practice.grantbatch.util.Meters;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Step + chunk listener. Records:
 *  - batch.partition.queue.wait : step create_time -> step start_time (time spent waiting in Kafka / for a free worker)
 *  - batch.chunk.duration       : whole chunk incl. commit (afterChunk fires after the commit)
 */
@Component
public class BatchMetricsListener implements StepExecutionListener, ChunkListener {
    private static final Logger log = LoggerFactory.getLogger(BatchMetricsListener.class);
    private final MeterRegistry registry;

    public BatchMetricsListener(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void beforeStep(StepExecution se) {
        if (se.getCreateTime() != null && se.getStartTime() != null) {
            Meters.timer(registry, "batch.partition.queue.wait", "step", Meters.baseStep(se.getStepName()))
                    .record(Duration.between(se.getCreateTime(), se.getStartTime()));
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution se) {
        log.info("{} finished: read={} written={} commits={} rollbacks={} status={}", se.getStepName(),
                se.getReadCount(), se.getWriteCount(), se.getCommitCount(), se.getRollbackCount(), se.getExitStatus().getExitCode());
        return se.getExitStatus();
    }

    @Override public void beforeChunk(ChunkContext ctx) { ctx.setAttribute("chunkStart", System.nanoTime()); }
    @Override public void afterChunk(ChunkContext ctx) { record(ctx, "ok"); }
    @Override public void afterChunkError(ChunkContext ctx) { record(ctx, "error"); }

    private void record(ChunkContext ctx, String outcome) {
        Object t0 = ctx.getAttribute("chunkStart");
        if (t0 == null) return;
        Meters.timer(registry, "batch.chunk.duration",
                        "step", Meters.baseStep(ctx.getStepContext().getStepName()), "outcome", outcome)
                .record(System.nanoTime() - (Long) t0, TimeUnit.NANOSECONDS);
    }
}
