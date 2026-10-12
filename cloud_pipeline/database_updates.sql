-- Minimal operational tables required by the scheduled cloud update.
USE sum26_foodfacts;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id BIGINT NOT NULL AUTO_INCREMENT,
    started_at DATETIME NOT NULL,
    completed_at DATETIME NULL,
    status VARCHAR(20) NOT NULL,
    records_received INT NOT NULL DEFAULT 0,
    records_valid INT NOT NULL DEFAULT 0,
    records_rejected INT NOT NULL DEFAULT 0,
    error_message TEXT NULL,
    PRIMARY KEY (run_id),
    INDEX ix_pipeline_runs_started_at (started_at),
    INDEX ix_pipeline_runs_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS pipeline_state (
    state_key VARCHAR(100) NOT NULL,
    state_value VARCHAR(255) NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (state_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO pipeline_state (state_key, state_value, updated_at)
VALUES ('last_successful_source_timestamp', '0', UTC_TIMESTAMP())
ON DUPLICATE KEY UPDATE state_key=VALUES(state_key);

-- The existing case-study database identifies protein as nutrient_id 8.
INSERT INTO nutrients (nutrient_id, nutrient_code, nutrient_name, unit)
VALUES (8, 'proteins', 'Proteins', 'g')
ON DUPLICATE KEY UPDATE
    nutrient_code=VALUES(nutrient_code),
    nutrient_name=VALUES(nutrient_name),
    unit=VALUES(unit);
