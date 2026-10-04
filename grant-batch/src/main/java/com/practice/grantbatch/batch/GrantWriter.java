package com.practice.grantbatch.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.grantbatch.config.Topology;
import com.practice.grantbatch.model.Models.AwardResult;
import com.practice.grantbatch.model.Models.GrantChange;
import com.practice.grantbatch.util.Meters;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Per chunk, strictly in sequence:  Kafka (stock -> external topic, cash -> internal topic)
 *                                -> Redis  ->  DB status (SUCCESS / ERROR).
 * Each sink is timed separately (batch.writer.sink{sink=...}); chunk write time is their SUM.
 * Kafka failures do not fail the chunk: the grant is written to stage_record for stage 2.
 */
@Component
public class GrantWriter implements ItemWriter<AwardResult> {
    private static final Logger log = LoggerFactory.getLogger(GrantWriter.class);

    private final KafkaTemplate<String, String> kafka;
    private final StringRedisTemplate redis;
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final MeterRegistry registry;
    private final Topology.Topics topics;

    public GrantWriter(KafkaTemplate<String, String> kafka, StringRedisTemplate redis, JdbcTemplate jdbc,
                       ObjectMapper mapper, MeterRegistry registry, Topology topology) {
        this.kafka = kafka;
        this.redis = redis;
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.registry = registry;
        this.topics = topology.settings().topics();
    }

    @Override
    public void write(Chunk<? extends AwardResult> chunk) {
        List<AwardResult> items = new ArrayList<>(chunk.getItems());
        List<AwardResult> ok = items.stream().filter(r -> r.error() == null).toList();
        Map<Long, String> sendErrors = new HashMap<>();

        timed("kafka", () -> publish(ok, sendErrors));
        timed("redis", () -> cache(ok));
        timed("db", () -> persist(items, sendErrors));
    }

    private void publish(List<AwardResult> ok, Map<Long, String> sendErrors) {
        record Sent(AwardResult item, CompletableFuture<?> future) {}
        List<Sent> sent = new ArrayList<>();
        for (AwardResult r : ok) {
            String topic = "STOCK".equals(r.awardType()) ? topics.stockExternal() : topics.cashInternal();
            try {
                sent.add(new Sent(r, kafka.send(topic, String.valueOf(r.grantId()), mapper.writeValueAsString(r))));
            } catch (Exception e) {            // send() itself can block up to max.block.ms, then throw
                sendErrors.put(r.grantId(), "kafka send: " + e.getClass().getSimpleName());
            }
        }
        for (Sent s : sent) {
            try {
                s.future().get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                sendErrors.put(s.item().grantId(), "kafka ack: " + e.getClass().getSimpleName());
            }
        }
    }

    /** One round trip per item on purpose: switch to pipelining and compare the redis sink p99 in Grafana. */
    private void cache(List<AwardResult> ok) {
        for (AwardResult r : ok) {
            try {
                redis.opsForValue().set("grant:" + r.grantId(), mapper.writeValueAsString(r), Duration.ofHours(1));
            } catch (Exception e) {
                log.warn("Redis write failed for grant {}: {}", r.grantId(), e.toString());
            }
        }
    }

    private void persist(List<AwardResult> items, Map<Long, String> sendErrors) {
        List<Object[]> statusRows = new ArrayList<>();
        List<Object[]> changeRows = new ArrayList<>();
        List<Object[]> staged = new ArrayList<>();
        List<Object[]> cleared = new ArrayList<>();

        for (AwardResult r : items) {
            boolean procFailed = r.error() != null;
            boolean sendFailed = sendErrors.containsKey(r.grantId());
            String status = (procFailed || sendFailed) ? "ERROR" : "SUCCESS";
            String error = procFailed ? r.error() : sendErrors.get(r.grantId());
            String changeStatus = procFailed ? "ERROR" : sendFailed ? "STAGED" : "PROCESSED";

            statusRows.add(new Object[]{r.grantId(), r.awardType(), status, error});
            for (GrantChange c : r.group().changes()) changeRows.add(new Object[]{changeStatus, c.id()});
            if (sendFailed) staged.add(new Object[]{r.grantId(), json(r.group())});
            else cleared.add(new Object[]{r.grantId()});     // also clears rows re-processed by stage 2
        }

        jdbc.batchUpdate("INSERT INTO grant_result_status (grant_id, award_type, status, error, updated_at) "
                + "VALUES (?, ?, ?, ?, now()) ON CONFLICT (grant_id) DO UPDATE SET award_type = EXCLUDED.award_type, "
                + "status = EXCLUDED.status, error = EXCLUDED.error, updated_at = now()", statusRows);
        jdbc.batchUpdate("UPDATE grant_change SET status = ? WHERE id = ?", changeRows);
        if (!staged.isEmpty()) {
            jdbc.batchUpdate("INSERT INTO stage_record (grant_id, payload) VALUES (?, ?) "
                    + "ON CONFLICT (grant_id) DO UPDATE SET payload = EXCLUDED.payload", staged);
        }
        if (!cleared.isEmpty()) jdbc.batchUpdate("DELETE FROM stage_record WHERE grant_id = ?", cleared);
    }

    private String json(Object o) {
        try { return mapper.writeValueAsString(o); }
        catch (JsonProcessingException e) { throw new IllegalStateException(e); }
    }

    private void timed(String sink, Runnable r) {
        Timer.Sample sample = Timer.start(registry);
        try { r.run(); }
        finally { sample.stop(Meters.timer(registry, "batch.writer.sink", "sink", sink)); }
    }
}
