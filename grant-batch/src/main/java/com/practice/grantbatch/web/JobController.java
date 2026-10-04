package com.practice.grantbatch.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Master-only helper endpoints: load test data, start the job, look at table counts. */
@RestController
@Profile("master")
public class JobController {
    private static final Logger log = LoggerFactory.getLogger(JobController.class);

    private final JobLauncher launcher;
    private final Job job;
    private final JdbcTemplate jdbc;
    private final ExecutorService runner = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "job-runner");
        t.setDaemon(true);
        return t;
    });

    public JobController(JobLauncher launcher, Job grantJob, JdbcTemplate jdbc) {
        this.launcher = launcher;
        this.job = grantJob;
        this.jdbc = jdbc;
    }

    /** POST /seed?grants=500 : each grant gets 1-4 pending changes; ~10% are cash payouts. */
    @PostMapping("/seed")
    public Map<String, Object> seed(@RequestParam(defaultValue = "500") int grants) {
        Random rnd = new Random();
        long base = jdbc.queryForObject("SELECT COALESCE(MAX(grant_id), 0) FROM grant_change", Long.class);
        String[] types = {"QUANTITY", "PRICE", "VESTING_SCHEDULE"};
        List<Object[]> rows = new ArrayList<>();
        for (int g = 1; g <= grants; g++) {
            int n = 1 + rnd.nextInt(4);
            boolean cash = rnd.nextInt(10) == 0;
            for (int k = 0; k < n; k++) {
                String type = (cash && k == n - 1) ? "CASH_PAYOUT" : (k == 0 ? "QUANTITY" : types[rnd.nextInt(types.length)]);
                String value = type.equals("CASH_PAYOUT") ? String.valueOf(10_000 + rnd.nextInt(500_000))
                                                          : String.valueOf(1 + rnd.nextInt(1000));
                rows.add(new Object[]{base + g, type, value});
            }
        }
        jdbc.batchUpdate("INSERT INTO grant_change (grant_id, change_type, new_value) VALUES (?, ?, ?)", rows);
        return Map.of("grants", grants, "rows", rows.size());
    }

    /** POST /jobs/run : launches asynchronously; watch logs + Grafana. */
    @PostMapping("/jobs/run")
    public Map<String, String> run() {
        JobParameters params = new JobParametersBuilder().addLong("ts", System.currentTimeMillis()).toJobParameters();
        runner.submit(() -> {
            try { launcher.run(job, params); }
            catch (Exception e) { log.error("Job launch failed", e); }
        });
        return Map.of("status", "submitted");
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("grant_change", jdbc.queryForList("SELECT status, count(*) AS n FROM grant_change GROUP BY status"));
        out.put("grant_result_status", jdbc.queryForList("SELECT status, count(*) AS n FROM grant_result_status GROUP BY status"));
        out.put("stage_record", jdbc.queryForObject("SELECT count(*) FROM stage_record", Long.class));
        return out;
    }
}
