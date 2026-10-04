package com.practice.egc;

import com.practice.egc.proto.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.ThreadLocalRandom;

class EgcServiceImpl extends EgcServiceGrpc.EgcServiceImplBase {

    private final MeterRegistry registry;
    private final int baseLatencyMs, slowPercent, slowMs, errorPercent;

    EgcServiceImpl(MeterRegistry registry, int baseLatencyMs, int slowPercent, int slowMs, int errorPercent) {
        this.registry = registry;
        this.baseLatencyMs = baseLatencyMs;
        this.slowPercent = slowPercent;
        this.slowMs = slowMs;
        this.errorPercent = errorPercent;
    }

    @Override
    public void transform(TransformRequest req, StreamObserver<TransformResponse> out) {
        Timer.Sample sample = Timer.start(registry);
        String outcome = "ok";
        try {
            simulateLatency();
            if (ThreadLocalRandom.current().nextInt(100) < errorPercent) {
                outcome = "error";
                out.onError(Status.UNAVAILABLE.withDescription("injected failure").asRuntimeException());
                return;
            }

            long shares = 0, cents = 0;
            boolean cash = false;
            TransformResponse.Builder resp = TransformResponse.newBuilder().setGrantId(req.getGrantId());
            for (Change c : req.getChangesList()) {
                switch (c.getType()) {
                    case QUANTITY -> shares = Long.parseLong(c.getNewValue());
                    case CASH_PAYOUT -> { cash = true; cents = Long.parseLong(c.getNewValue()); }
                    default -> { /* PRICE / VESTING_SCHEDULE: no effect on the amount in this toy version */ }
                }
                resp.addAppliedChangeIds(c.getChangeId());
            }
            resp.setAwardType(cash ? AwardType.CASH : AwardType.STOCK)
                .setShares(shares).setAmountCents(cents).setCurrency("USD");
            out.onNext(resp.build());
            out.onCompleted();
        } finally {
            sample.stop(Timer.builder("egc.server.transform").tag("outcome", outcome)
                    .publishPercentileHistogram().register(registry));
        }
    }

    private void simulateLatency() {
        long ms = baseLatencyMs;
        if (ThreadLocalRandom.current().nextInt(100) < slowPercent) ms += slowMs;
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
