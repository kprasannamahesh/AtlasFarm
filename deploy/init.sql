CREATE DATABASE IF NOT EXISTS atlasfarm;
USE atlasfarm;

CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(128) NOT NULL UNIQUE,
    weight INT NOT NULL,
    max_cpu INT NOT NULL,
    max_ram_mb BIGINT NOT NULL,
    max_gpu INT NOT NULL,
    max_concurrent_jobs INT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

INSERT INTO tenants
    (id, name, weight, max_cpu, max_ram_mb, max_gpu, max_concurrent_jobs)
VALUES
    ('tenant-a', 'Tenant A', 40, 64, 131072, 8, 100),
    ('tenant-b', 'Tenant B', 30, 64, 131072, 8, 100),
    ('tenant-c', 'Tenant C', 30, 64, 131072, 8, 100)
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    weight = VALUES(weight),
    max_cpu = VALUES(max_cpu),
    max_ram_mb = VALUES(max_ram_mb),
    max_gpu = VALUES(max_gpu),
    max_concurrent_jobs = VALUES(max_concurrent_jobs);

CREATE TABLE IF NOT EXISTS job_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL,
    sequence_num INT NOT NULL,
    event_type ENUM(
        'JOB_CREATED',
        'JOB_SCHEDULED',
        'JOB_LEASED',
        'JOB_STARTED',
        'JOB_COMPLETED',
        'JOB_FAILED',
        'JOB_RETRIED',
        'JOB_CANCELLED'
    ) NOT NULL,
    payload JSON NOT NULL,
    trace_id VARCHAR(64),
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_job_seq (job_id, sequence_num),
    KEY idx_job_events_created_at (created_at)
);

CREATE TABLE IF NOT EXISTS jobs_projection (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id VARCHAR(36) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    execution_id VARCHAR(36),
    command TEXT NOT NULL,
    req_cpu INT NOT NULL DEFAULT 1,
    req_memory_mb BIGINT NOT NULL DEFAULT 1024,
    req_gpu INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    worker_id VARCHAR(36),
    lease_expires_at TIMESTAMP(6) NULL,
    trace_id VARCHAR(64),
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_jobs_idempotency_key (idempotency_key),
    KEY idx_jobs_tenant_status (tenant_id, status),
    KEY idx_jobs_lease_expires_at (lease_expires_at),
    CONSTRAINT fk_jobs_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id)
);

CREATE TABLE IF NOT EXISTS workers (
    id VARCHAR(36) PRIMARY KEY,
    total_cpu INT NOT NULL,
    used_cpu INT NOT NULL DEFAULT 0,
    total_memory_mb BIGINT NOT NULL,
    used_memory_mb BIGINT NOT NULL DEFAULT 0,
    total_gpu INT NOT NULL DEFAULT 0,
    used_gpu INT NOT NULL DEFAULT 0,
    runner_version VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    last_heartbeat TIMESTAMP(6) NULL,
    KEY idx_workers_status (status)
);
