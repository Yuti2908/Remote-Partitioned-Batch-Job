-- Spring Batch's own BATCH_* tables are created by Spring Boot (initialize-schema: always).

CREATE TABLE IF NOT EXISTS grant_change (
    id          BIGSERIAL PRIMARY KEY,
    grant_id    BIGINT       NOT NULL,
    change_type VARCHAR(32)  NOT NULL,
    new_value   VARCHAR(64)  NOT NULL,
    status      VARCHAR(16)  NOT NULL DEFAULT 'NEW',   -- NEW | PROCESSED | STAGED | ERROR
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_grant_change_status_grant ON grant_change (status, grant_id, id);

CREATE TABLE IF NOT EXISTS grant_result_status (
    grant_id   BIGINT PRIMARY KEY,
    award_type VARCHAR(16),
    status     VARCHAR(16) NOT NULL,                   -- SUCCESS | ERROR
    error      VARCHAR(512),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Rows land here when the Kafka publish failed; stage 2 picks them up.
CREATE TABLE IF NOT EXISTS stage_record (
    grant_id   BIGINT PRIMARY KEY,
    payload    TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
