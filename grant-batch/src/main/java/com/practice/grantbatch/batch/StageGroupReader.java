package com.practice.grantbatch.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.grantbatch.model.Models.AwardResult;
import com.practice.grantbatch.model.Models.GrantGroup;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Stage 2 reader. For every staged grant: prefer the copy in Redis (it may be newer than the staged row),
 * otherwise fall back to the stage table payload.
 */
public class StageGroupReader implements ItemReader<GrantGroup> {
    private static final Logger log = LoggerFactory.getLogger(StageGroupReader.class);

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final MeterRegistry registry;
    private Iterator<GrantGroup> it;

    public StageGroupReader(JdbcTemplate jdbc, StringRedisTemplate redis, ObjectMapper mapper, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.mapper = mapper;
        this.registry = registry;
    }

    @Override
    public GrantGroup read() {
        if (it == null) it = load().iterator();
        return it.hasNext() ? it.next() : null;
    }

    private List<GrantGroup> load() {
        List<GrantGroup> groups = new ArrayList<>();
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT grant_id, payload FROM stage_record ORDER BY grant_id");
        for (Map<String, Object> row : rows) {
            long grantId = ((Number) row.get("grant_id")).longValue();
            GrantGroup group = fromRedis(grantId);
            if (group != null) {
                registry.counter("batch.stage2.source", "source", "redis").increment();
            } else {
                registry.counter("batch.stage2.source", "source", "table").increment();
                try {
                    group = mapper.readValue((String) row.get("payload"), GrantGroup.class);
                } catch (Exception e) {
                    throw new IllegalStateException("Bad stage payload for grant " + grantId, e);
                }
            }
            groups.add(group);
        }
        log.info("Stage 2 loaded {} staged grants", groups.size());
        return groups;
    }

    private GrantGroup fromRedis(long grantId) {
        try {
            String cached = redis.opsForValue().get("grant:" + grantId);
            return cached == null ? null : mapper.readValue(cached, AwardResult.class).group();
        } catch (Exception e) {
            log.warn("Redis lookup failed for grant {}, using stage table", grantId);
            return null;
        }
    }
}
