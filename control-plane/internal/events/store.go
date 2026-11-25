package events

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/quota"
)

var (
	ErrNotFound          = errors.New("resource not found")
	ErrInvalidTransition = errors.New("invalid job status transition")
	ErrInvalidRequest    = errors.New("invalid request")
)

type Store struct {
	db *sql.DB
}

type Job struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Command        string    `json:"command"`
	CPU            int       `json:"req_cpu"`
	MemoryMB       int64     `json:"req_memory_mb"`
	GPU            int       `json:"req_gpu"`
	Status         string    `json:"status"`
	WorkerID       string    `json:"worker_id,omitempty"`
	Result         string    `json:"result,omitempty"`
	Error          string    `json:"error,omitempty"`
	Claimed        bool      `json:"claimed,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type Worker struct {
	ID            string    `json:"id"`
	CPU           int       `json:"cpu"`
	UsedCPU       int       `json:"used_cpu"`
	MemoryMB      int64     `json:"memory_mb"`
	UsedMemoryMB  int64     `json:"used_memory_mb"`
	GPU           int       `json:"gpu"`
	UsedGPU       int       `json:"used_gpu"`
	RunnerVersion string    `json:"runner_version"`
	Status        string    `json:"status"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

type CreateJobRequest struct {
	ID             string `json:"id,omitempty"`
	TenantID       string `json:"tenant_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Command        string `json:"command"`
	CPU            int    `json:"req_cpu"`
	MemoryMB       int64  `json:"req_memory_mb"`
	GPU            int    `json:"req_gpu"`
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate ID: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func (s *Store) CreateJob(ctx context.Context, req CreateJobRequest) (Job, bool, error) {
	if strings.TrimSpace(req.TenantID) == "" || strings.TrimSpace(req.IdempotencyKey) == "" ||
		len(req.TenantID) > 36 || len(req.IdempotencyKey) > 128 ||
		(req.ID != "" && len(req.ID) > 36) ||
		strings.TrimSpace(req.Command) == "" || len(req.Command) > 65535 ||
		req.CPU < 1 || req.MemoryMB < 1 || req.GPU < 0 {
		return Job{}, false, fmt.Errorf("%w: tenant_id, idempotency_key, command and valid resource requests are required", ErrInvalidRequest)
	}
	if req.ID == "" {
		var err error
		req.ID, err = newID()
		if err != nil {
			return Job{}, false, err
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, fmt.Errorf("begin create job transaction: %w", err)
	}
	defer tx.Rollback()

	existing, err := scanJob(tx.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection WHERE idempotency_key = ? FOR UPDATE`, req.IdempotencyKey))
	if err == nil {
		if err := tx.Commit(); err != nil {
			return Job{}, false, fmt.Errorf("commit duplicate job lookup: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, fmt.Errorf("check existing idempotency key: %w", err)
	}
	if err := quota.CheckTransaction(ctx, tx, req.TenantID, quota.Request{
		CPU: req.CPU, MemoryMB: req.MemoryMB, GPU: req.GPU,
	}); err != nil {
		return Job{}, false, err
	}
	existing, err = scanJob(tx.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection WHERE idempotency_key = ? FOR UPDATE`, req.IdempotencyKey))
	if err == nil {
		if err := tx.Commit(); err != nil {
			return Job{}, false, fmt.Errorf("commit duplicate job lookup: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, fmt.Errorf("recheck existing idempotency key: %w", err)
	}
	result, err := tx.ExecContext(ctx, `INSERT IGNORE INTO jobs_projection
		(id, tenant_id, idempotency_key, command, req_cpu, req_memory_mb, req_gpu, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')`,
		req.ID, req.TenantID, req.IdempotencyKey, req.Command, req.CPU, req.MemoryMB, req.GPU)
	if err != nil {
		return Job{}, false, fmt.Errorf("insert job projection: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Job{}, false, fmt.Errorf("read insert result: %w", err)
	}

	job, err := scanJob(tx.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection WHERE idempotency_key = ? FOR UPDATE`,
		req.IdempotencyKey))
	if err != nil {
		return Job{}, false, fmt.Errorf("read created job: %w", err)
	}
	if rows == 1 {
		if job.ID != req.ID {
			return Job{}, false, errors.New("job ID already exists")
		}
		if err := appendEvent(ctx, tx, job.ID, "JOB_CREATED", map[string]any{
			"status": job.Status, "tenant_id": job.TenantID,
		}); err != nil {
			return Job{}, false, err
		}
	} else {
		job, err = scanJob(tx.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
			req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
			created_at, updated_at FROM jobs_projection WHERE idempotency_key = ?`, req.IdempotencyKey))
		if err != nil {
			return Job{}, false, fmt.Errorf("read duplicate idempotent job: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, fmt.Errorf("commit create job transaction: %w", err)
	}
	return job, rows == 1, nil
}

func (s *Store) ListQueuedJobs(ctx context.Context, limit int) ([]Job, error) {
	if limit <= 0 || limit > 1000 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at
		FROM (
			SELECT id, tenant_id, idempotency_key, command, req_cpu, req_memory_mb,
				req_gpu, status, worker_id, result, error_message, created_at, updated_at,
				ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY created_at, id) AS queue_position
			FROM jobs_projection WHERE status = 'queued'
		) AS queued_jobs
		WHERE queue_position <= ?
		ORDER BY tenant_id, queue_position`, limit)
	if err != nil {
		return nil, fmt.Errorf("list queued jobs: %w", err)
	}
	defer rows.Close()
	jobs := make([]Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan queued job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued jobs: %w", err)
	}
	return jobs, nil
}

func (s *Store) TenantWeights(ctx context.Context) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, weight FROM tenants ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list tenant weights: %w", err)
	}
	defer rows.Close()
	weights := make(map[string]int)
	for rows.Next() {
		var tenantID string
		var weight int
		if err := rows.Scan(&tenantID, &weight); err != nil {
			return nil, fmt.Errorf("scan tenant weight: %w", err)
		}
		weights[tenantID] = weight
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenant weights: %w", err)
	}
	return weights, nil
}

func (s *Store) ScheduleJob(ctx context.Context, id, workerID string) (Job, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, fmt.Errorf("begin job scheduling: %w", err)
	}
	defer tx.Rollback()
	job, err := scanJob(tx.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection WHERE id = ? FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, ErrNotFound
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("lock queued job: %w", err)
	}
	if job.Status != "queued" {
		if err := tx.Commit(); err != nil {
			return Job{}, false, fmt.Errorf("commit duplicate job scheduling: %w", err)
		}
		return job, false, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE workers SET used_cpu = used_cpu + ?,
		used_memory_mb = used_memory_mb + ?, used_gpu = used_gpu + ?
		WHERE id = ? AND status IN ('online', 'active', 'healthy')
		AND total_cpu - used_cpu >= ?
		AND total_memory_mb - used_memory_mb >= ?
		AND total_gpu - used_gpu >= ?`,
		job.CPU, job.MemoryMB, job.GPU, workerID, job.CPU, job.MemoryMB, job.GPU)
	if err != nil {
		return Job{}, false, fmt.Errorf("reserve worker resources: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Job{}, false, fmt.Errorf("read worker resource reservation: %w", err)
	}
	if changed != 1 {
		return Job{}, false, fmt.Errorf("worker %q is unavailable or lacks requested resources", workerID)
	}
	if err := appendEvent(ctx, tx, job.ID, "JOB_SCHEDULED", map[string]any{
		"status": "scheduled", "worker_id": workerID,
	}); err != nil {
		return Job{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs_projection SET status = 'scheduled', worker_id = ? WHERE id = ?`,
		workerID, job.ID); err != nil {
		return Job{}, false, fmt.Errorf("update scheduled job projection: %w", err)
	}
	job, err = scanJob(tx.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection WHERE id = ?`, id))
	if err != nil {
		return Job{}, false, fmt.Errorf("read scheduled job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, fmt.Errorf("commit scheduled job: %w", err)
	}
	return job, true, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (Job, error) {
	job, err := scanJob(s.db.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

func (s *Store) ListJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection ORDER BY created_at DESC LIMIT 500`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	jobs := make([]Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}
	return jobs, nil
}

func (s *Store) TransitionJob(ctx context.Context, id, status, workerID, output, failure string) (Job, error) {
	job, _, err := s.TransitionJobWithChange(ctx, id, status, workerID, output, failure)
	return job, err
}

func (s *Store) TransitionJobWithChange(ctx context.Context, id, status, workerID, output, failure string) (Job, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, fmt.Errorf("begin job transition: %w", err)
	}
	defer tx.Rollback()

	job, err := scanJob(tx.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection WHERE id = ? FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, ErrNotFound
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("lock job: %w", err)
	}
	if status == job.Status {
		if err := tx.Commit(); err != nil {
			return Job{}, false, fmt.Errorf("commit idempotent transition: %w", err)
		}
		return job, false, nil
	}
	if !validTransition(job.Status, status) {
		return Job{}, false, fmt.Errorf("%w: %s to %s", ErrInvalidTransition, job.Status, status)
	}
	if status != "cancelled" && (workerID == "" || workerID != job.WorkerID) {
		return Job{}, false, fmt.Errorf("%w: worker %q does not own job %s", ErrInvalidTransition, workerID, id)
	}
	eventType := map[string]string{
		"running": "JOB_STARTED", "succeeded": "JOB_COMPLETED",
		"failed": "JOB_FAILED", "cancelled": "JOB_CANCELLED",
	}[status]
	payload := map[string]string{"status": status}
	if workerID != "" {
		payload["worker_id"] = workerID
	}
	if output != "" {
		payload["result"] = output
	}
	if failure != "" {
		payload["error"] = failure
	}
	if status == "cancelled" && job.WorkerID != "" {
		workerID = job.WorkerID
		payload["worker_id"] = workerID
	}
	if err := appendEvent(ctx, tx, id, eventType, payload); err != nil {
		return Job{}, false, err
	}
	if (status == "succeeded" || status == "failed" || status == "cancelled") &&
		job.WorkerID != "" && (job.Status == "scheduled" || job.Status == "leased" || job.Status == "running") {
		if _, err := tx.ExecContext(ctx, `UPDATE workers SET
			used_cpu = GREATEST(0, used_cpu - ?),
			used_memory_mb = GREATEST(0, used_memory_mb - ?),
			used_gpu = GREATEST(0, used_gpu - ?) WHERE id = ?`,
			job.CPU, job.MemoryMB, job.GPU, job.WorkerID); err != nil {
			return Job{}, false, fmt.Errorf("release worker resources: %w", err)
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE jobs_projection SET status = ?,
		worker_id = COALESCE(NULLIF(?, ''), worker_id),
		result = COALESCE(NULLIF(?, ''), result),
		error_message = COALESCE(NULLIF(?, ''), error_message) WHERE id = ?`,
		status, workerID, output, failure, id)
	if err != nil {
		return Job{}, false, fmt.Errorf("update job projection: %w", err)
	}
	job, err = scanJob(tx.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection WHERE id = ?`, id))
	if err != nil {
		return Job{}, false, fmt.Errorf("read transitioned job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, fmt.Errorf("commit job transition: %w", err)
	}
	return job, true, nil
}

func (s *Store) ClaimJob(ctx context.Context, id, workerID string) (Job, bool, error) {
	if strings.TrimSpace(workerID) == "" {
		return Job{}, false, fmt.Errorf("%w: worker_id is required", ErrInvalidRequest)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, fmt.Errorf("begin job claim: %w", err)
	}
	defer tx.Rollback()
	job, err := scanJob(tx.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection WHERE id = ? FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, ErrNotFound
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("lock job for claim: %w", err)
	}
	if job.Status != "scheduled" && job.Status != "queued" {
		if err := tx.Commit(); err != nil {
			return Job{}, false, fmt.Errorf("commit duplicate job claim: %w", err)
		}
		return job, false, nil
	}
	if job.Status == "scheduled" && job.WorkerID != workerID {
		if err := tx.Commit(); err != nil {
			return Job{}, false, fmt.Errorf("commit rejected job claim: %w", err)
		}
		return job, false, nil
	}
	if err := appendEvent(ctx, tx, id, "JOB_STARTED", map[string]string{
		"status": "running", "worker_id": workerID,
	}); err != nil {
		return Job{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs_projection SET status = 'running', worker_id = ? WHERE id = ?`,
		workerID, id); err != nil {
		return Job{}, false, fmt.Errorf("claim job: %w", err)
	}
	job, err = scanJob(tx.QueryRowContext(ctx, `SELECT id, tenant_id, idempotency_key, command,
		req_cpu, req_memory_mb, req_gpu, status, worker_id, result, error_message,
		created_at, updated_at FROM jobs_projection WHERE id = ?`, id))
	if err != nil {
		return Job{}, false, fmt.Errorf("read claimed job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, fmt.Errorf("commit job claim: %w", err)
	}
	job.Claimed = true
	return job, true, nil
}

func validTransition(from, to string) bool {
	if to == "cancelled" {
		return from == "queued" || from == "scheduled" || from == "leased" || from == "running"
	}
	return (from == "queued" && to == "running") ||
		(from == "scheduled" && to == "running") ||
		(from == "running" && (to == "succeeded" || to == "failed"))
}

func appendEvent(ctx context.Context, tx *sql.Tx, jobID, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode event payload: %w", err)
	}
	var sequence int
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence_num), 0) + 1
		FROM job_events WHERE job_id = ?`, jobID).Scan(&sequence)
	if err != nil {
		return fmt.Errorf("get next event sequence: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO job_events
		(job_id, sequence_num, event_type, payload) VALUES (?, ?, ?, ?)`,
		jobID, sequence, eventType, string(body)); err != nil {
		return fmt.Errorf("append %s event: %w", eventType, err)
	}
	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row scanner) (Job, error) {
	var job Job
	var workerID, result, failure sql.NullString
	err := row.Scan(&job.ID, &job.TenantID, &job.IdempotencyKey, &job.Command,
		&job.CPU, &job.MemoryMB, &job.GPU, &job.Status, &workerID, &result, &failure,
		&job.CreatedAt, &job.UpdatedAt)
	job.WorkerID, job.Result, job.Error = workerID.String, result.String, failure.String
	return job, err
}

func (s *Store) RegisterWorker(ctx context.Context, worker Worker) error {
	if worker.ID == "" || len(worker.ID) > 36 ||
		worker.CPU < 1 || worker.MemoryMB < 1 || worker.GPU < 0 ||
		worker.RunnerVersion == "" || len(worker.RunnerVersion) > 64 {
		return fmt.Errorf("%w: id, positive cpu/memory, nonnegative gpu, and runner_version are required", ErrInvalidRequest)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO workers
		(id, total_cpu, total_memory_mb, total_gpu, runner_version, status, last_heartbeat)
		VALUES (?, ?, ?, ?, ?, 'online', CURRENT_TIMESTAMP(6))
		ON DUPLICATE KEY UPDATE total_cpu = VALUES(total_cpu),
			total_memory_mb = VALUES(total_memory_mb), total_gpu = VALUES(total_gpu),
			runner_version = VALUES(runner_version), status = 'online',
			last_heartbeat = CURRENT_TIMESTAMP(6)`,
		worker.ID, worker.CPU, worker.MemoryMB, worker.GPU, worker.RunnerVersion)
	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	return nil
}

func (s *Store) Heartbeat(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE workers SET status = 'online',
		last_heartbeat = CURRENT_TIMESTAMP(6) WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("update worker heartbeat: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read heartbeat result: %w", err)
	}
	if changed == 0 {
		var exists int
		if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM workers WHERE id = ?`, id).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		} else if err != nil {
			return fmt.Errorf("verify worker heartbeat: %w", err)
		}
	}
	return nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, total_cpu, used_cpu, total_memory_mb,
		used_memory_mb, total_gpu, used_gpu, runner_version, status,
		COALESCE(last_heartbeat, '1970-01-01 00:00:00')
		FROM workers ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()
	workers := make([]Worker, 0)
	for rows.Next() {
		var worker Worker
		if err := rows.Scan(&worker.ID, &worker.CPU, &worker.UsedCPU, &worker.MemoryMB,
			&worker.UsedMemoryMB, &worker.GPU, &worker.UsedGPU,
			&worker.RunnerVersion, &worker.Status, &worker.LastHeartbeat); err != nil {
			return nil, fmt.Errorf("scan worker: %w", err)
		}
		workers = append(workers, worker)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workers: %w", err)
	}
	return workers, nil
}
