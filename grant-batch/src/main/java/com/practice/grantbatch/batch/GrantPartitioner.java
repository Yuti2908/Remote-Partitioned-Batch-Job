package com.practice.grantbatch.batch;

import com.practice.grantbatch.config.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Fetches ~fetchBatchSize pending rows ordered by (grant_id, id), completes the last grant group,
 * then greedily cuts partitions on grant boundaries so a grant never spans two partitions.
 * Only a (fromGrantId, toGrantId, maxChangeId) range goes into the ExecutionContext.
 */
@Component
public class GrantPartitioner implements Partitioner {
    private static final Logger log = LoggerFactory.getLogger(GrantPartitioner.class);

    private final JdbcTemplate jdbc;
    private final int fetchBatchSize;

    public GrantPartitioner(JdbcTemplate jdbc, Topology topology) {
        this.jdbc = jdbc;
        this.fetchBatchSize = topology.settings().fetchBatchSize();
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        List<long[]> rows = new ArrayList<>(jdbc.query(
                "SELECT id, grant_id FROM grant_change WHERE status = 'NEW' ORDER BY grant_id, id LIMIT ?",
                (rs, i) -> new long[]{rs.getLong(1), rs.getLong(2)}, fetchBatchSize));
        if (rows.isEmpty()) {
            log.info("No pending grant changes; nothing to partition");
            return Map.of();
        }

        // LIMIT may have cut the last grant in half: pull in the rest of its rows.
        long[] last = rows.get(rows.size() - 1);
        rows.addAll(jdbc.query(
                "SELECT id, grant_id FROM grant_change WHERE status = 'NEW' AND grant_id = ? AND id > ? ORDER BY id",
                (rs, i) -> new long[]{rs.getLong(1), rs.getLong(2)}, last[1], last[0]));

        Map<Long, Integer> perGrant = new LinkedHashMap<>();
        long maxId = 0;
        for (long[] r : rows) {
            perGrant.merge(r[1], 1, Integer::sum);
            maxId = Math.max(maxId, r[0]);
        }

        int target = (int) Math.ceil(rows.size() / (double) gridSize);
        Map<String, ExecutionContext> result = new LinkedHashMap<>();
        long from = -1, to = -1;
        int size = 0, idx = 0;
        for (Map.Entry<Long, Integer> e : perGrant.entrySet()) {
            if (size == 0) from = e.getKey();
            to = e.getKey();
            size += e.getValue();
            if (size >= target) {
                result.put("partition" + idx++, context(from, to, maxId, size));
                size = 0;
            }
        }
        if (size > 0) result.put("partition" + idx, context(from, to, maxId, size));

        log.info("Partitioned {} rows / {} grants into {} partitions (target {} rows each)",
                rows.size(), perGrant.size(), result.size(), target);
        return result;
    }

    private static ExecutionContext context(long from, long to, long maxId, int records) {
        ExecutionContext ec = new ExecutionContext();
        ec.putLong("fromGrantId", from);
        ec.putLong("toGrantId", to);
        ec.putLong("maxChangeId", maxId);
        ec.putInt("expectedRecords", records);
        return ec;
    }
}
