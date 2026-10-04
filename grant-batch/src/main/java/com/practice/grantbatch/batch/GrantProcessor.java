package com.practice.grantbatch.batch;

import com.practice.egc.proto.*;
import com.practice.grantbatch.config.Topology;
import com.practice.grantbatch.model.Models.AwardResult;
import com.practice.grantbatch.model.Models.GrantChange;
import com.practice.grantbatch.model.Models.GrantGroup;
import com.practice.grantbatch.util.Meters;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/** One gRPC call per grant group, with retry + deadline. Exhausted retries become an ERROR result, not a failed step. */
@Component
public class GrantProcessor implements ItemProcessor<GrantGroup, AwardResult> {
    private static final Logger log = LoggerFactory.getLogger(GrantProcessor.class);

    private final EgcServiceGrpc.EgcServiceBlockingStub stub;
    private final MeterRegistry registry;
    private final RetryTemplate retry;
    private final int deadlineMs;

    public GrantProcessor(EgcServiceGrpc.EgcServiceBlockingStub stub, MeterRegistry registry, Topology topology) {
        this.stub = stub;
        this.registry = registry;
        Topology.Egc egc = topology.settings().egc();
        this.deadlineMs = egc.deadlineMs();
        this.retry = RetryTemplate.builder()
                .maxAttempts(egc.maxAttempts())
                .exponentialBackoff(100, 2.0, 1000)
                .retryOn(StatusRuntimeException.class)
                .build();
    }

    @Override
    public AwardResult process(GrantGroup group) {
        TransformRequest.Builder rb = TransformRequest.newBuilder().setGrantId(group.grantId());
        for (GrantChange c : group.changes()) {
            rb.addChanges(Change.newBuilder()
                    .setChangeId(c.id())
                    .setType(ChangeType.valueOf(c.changeType()))
                    .setNewValue(c.newValue()));
        }
        TransformRequest req = rb.build();
        try {
            TransformResponse resp = retry.execute(ctx -> attempt(req, ctx.getRetryCount()));
            return new AwardResult(group.grantId(), resp.getAwardType().name(), resp.getShares(),
                    resp.getAmountCents(), resp.getCurrency(), null, group);
        } catch (StatusRuntimeException e) {
            registry.counter("egc.grpc.exhausted").increment();
            log.warn("egc call failed for grant {} after retries: {}", group.grantId(), e.getStatus());
            return AwardResult.failed(group, "egc " + e.getStatus().getCode());
        }
    }

    /** One physical attempt: timed individually so retries show up as extra samples, not one long one. */
    private TransformResponse attempt(TransformRequest req, int retryCount) {
        if (retryCount > 0) registry.counter("egc.grpc.retries").increment();
        Timer.Sample sample = Timer.start(registry);
        String outcome = "ok";
        try {
            return stub.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS).transform(req);
        } catch (StatusRuntimeException e) {
            outcome = e.getStatus().getCode().name().toLowerCase();
            throw e;
        } finally {
            sample.stop(Meters.timer(registry, "egc.grpc.attempt", "outcome", outcome));
        }
    }
}
