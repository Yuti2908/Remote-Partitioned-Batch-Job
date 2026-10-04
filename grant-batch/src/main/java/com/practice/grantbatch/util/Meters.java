package com.practice.grantbatch.util;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

public final class Meters {
    private Meters() {}

    /** Histogram-enabled timer so histogram_quantile() works across pods. Keep tags low-cardinality. */
    public static Timer timer(MeterRegistry registry, String name, String... tags) {
        return Timer.builder(name).tags(tags)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMinutes(5))
                .register(registry);
    }

    /** "workerStep:partition3" -> "workerStep" (never tag by partition number). */
    public static String baseStep(String stepName) {
        int i = stepName.indexOf(':');
        return i < 0 ? stepName : stepName.substring(0, i);
    }
}
