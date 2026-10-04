# grant-batch-practice

A scaled-down rebuild of a two-stage Spring Batch pipeline with remote partitioning over Kafka.

```
POST /jobs/run (master)
  Stage 1  stage1ManagerStep --partition by grant range--> Kafka (only stepExecutionId) --> workers
           worker: reader (group rows per grant) -> processor (gRPC egc, retry) -> writer
                   writer = Kafka (stock|cash topic) -> Redis -> DB status (SUCCESS|ERROR)
                   Kafka failure => stage_record row (grant marked STAGED)
  Decider  stage_record not empty ? RUN_STAGE2 : end
  Stage 2  local step: reader (Redis first, stage table fallback) -> same processor -> same writer
```

## Prerequisites
JDK 17+, Docker, Gradle (only to generate the wrapper once: `gradle wrapper --gradle-version 8.10.2`; IntelliJ can also import the build directly).

## Run
```bash
docker compose up -d                       # postgres, redis, kafka, prometheus (:9091), grafana (:3000, admin/admin)

./gradlew :egc-service:run                 # terminal 1  (gRPC :9090, metrics :8090/metrics)

SERVICE_NAME=grant-manager  ./gradlew :grant-batch:bootRun   # terminal 2 (start first: it creates BATCH_* tables)
SERVICE_NAME=grant-worker-1 ./gradlew :grant-batch:bootRun   # terminal 3
SERVICE_NAME=grant-worker-2 ./gradlew :grant-batch:bootRun   # terminal 4
```
PowerShell: `$env:SERVICE_NAME="grant-worker-1"; ./gradlew :grant-batch:bootRun`

```bash
curl -X POST "localhost:8081/seed?grants=2000"   # load pending changes
curl -X POST localhost:8081/jobs/run             # start the job
curl localhost:8081/stats                        # counts per status
```
Roles, ports and settings come from `grant-batch/src/main/resources/topology.json`.
Add a `grant-worker-3` entry there (and in `docker/prometheus/prometheus.yml`) to scale out.

## Grafana
Prometheus is pre-wired as the Grafana data source. Build the dashboard yourself; that's the practice.
Check `http://localhost:8082/actuator/prometheus` for the exact `spring_batch_*` names/labels in your version.

Custom metrics (all histogram-enabled):
| metric | meaning |
|---|---|
| `batch_writer_sink_seconds{sink=kafka\|redis\|db}` | per-sink time inside one chunk write |
| `batch_chunk_duration_seconds{step,outcome}` | whole chunk incl. commit |
| `batch_partition_queue_wait_seconds{step}` | create_time -> start_time of a worker step |
| `egc_grpc_attempt_seconds{outcome}` | one physical gRPC attempt |
| `egc_grpc_retries_total`, `egc_grpc_exhausted_total` | retry pressure |
| `batch_stage2_triggered_total`, `batch_stage2_source_total{source}` | stage 2 activity |
| `egc_server_transform_seconds` | server-side view (compare with client-side to isolate network/queueing) |

## Experiments: break it on purpose, then find it in Grafana
| Do this | Expect |
|---|---|
| Restart egc with `SLOW_PERCENT=2 SLOW_MS=1500` | gRPC attempt p99 jumps; chunk p99 and partition p99 follow; p50 barely moves |
| `SLOW_PERCENT=2 SLOW_MS=2500` (deadline is 2000ms) | DEADLINE_EXCEEDED attempts, `egc_grpc_retries_total` rises, partition p99 = timeout + backoff + retry |
| `ERROR_PERCENT=10` | retries without slow attempts; `exhausted` appears once 3 attempts fail |
| `docker compose stop kafka` mid-run | kafka sink p99 hits ~max.block.ms (3s) per record, grants go STAGED; restart kafka, next job runs stage 2 |
| Set `hikari.maximum-pool-size: 2` | `hikaricp_connections_pending` > 0, db sink + chunk p99 rise |
| Run with 1 worker but `gridSize: 8` | queue wait p99 grows: partitions wait behind each other |
| Set `max.poll.interval.ms: 10000` with a slow partition | consumer evicted, rebalance, partition redelivered and re-run |
| Replace the per-item Redis `set` with a pipeline | redis sink p99 drops |

Not production code: single broker, one Postgres, stage 2 runs locally on the master, Kafka/Redis/DB writes are not atomic
(duplicate publishes are possible on chunk retry; real code would make consumers idempotent).
