USE atlasfarm;

UPDATE jobs_projection
SET execution_id = 0
WHERE execution_id IS NULL;

ALTER TABLE jobs_projection
    MODIFY COLUMN execution_id BIGINT NOT NULL DEFAULT 0;
