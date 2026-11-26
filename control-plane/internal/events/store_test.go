package events

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestCreateJobAppendsInitialEventTransactionally(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	now := time.Now()
	traceID := "0123456789abcdef0123456789abcdef"
	expectCreateLookup(mock, now, "job-1", "queued", traceID)
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(sequence_num\\), 0\\) \\+ 1").
		WithArgs("job-1").WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(1))
	mock.ExpectExec("INSERT INTO job_events").
		WithArgs("job-1", 1, "JOB_CREATED", `{"status":"queued","tenant_id":"tenant-1"}`, traceID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	job, created, err := store.CreateJob(context.Background(), CreateJobRequest{
		ID: "job-1", TenantID: "tenant-1", IdempotencyKey: "request-1",
		Command: "go test ./...", CPU: 2, MemoryMB: 2048, TraceID: traceID,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if !created || job.ID != "job-1" || job.Status != "queued" {
		t.Fatalf("CreateJob() = (%+v, created=%v), want queued new job", job, created)
	}
	if job.TraceID != traceID {
		t.Fatalf("CreateJob() trace ID = %q, want %q", job.TraceID, traceID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateJobIsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	mock.ExpectBegin()
	mock.ExpectQuery("WHERE idempotency_key = \\?").WithArgs("request-1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "idempotency_key", "command",
			"req_cpu", "req_memory_mb", "req_gpu", "status", "worker_id", "result", "error_message",
			"created_at", "updated_at", "execution_id", "lease_expires_at", "trace_id", "traceparent"}).AddRow(
			"job-original", "tenant-1", "request-1", "original", 1, int64(1024), 0,
			"queued", nil, nil, nil, time.Now(), time.Now(), int64(0), nil, nil, nil))
	mock.ExpectCommit()

	job, created, err := store.CreateJob(context.Background(), CreateJobRequest{
		ID: "ignored-new-id", TenantID: "tenant-1", IdempotencyKey: "request-1",
		Command: "different payload", CPU: 1, MemoryMB: 1024,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if created || job.ID != "job-original" {
		t.Fatalf("CreateJob() = (%+v, created=%v), want existing job", job, created)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMarkUnhealthyWorkersMarksStaleWorkersOfflineAndClearsUsage(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	mock.ExpectExec("(?s)UPDATE workers SET status = 'OFFLINE',.*used_cpu = 0, used_memory_mb = 0, used_gpu = 0.*INTERVAL 15 SECOND").
		WillReturnResult(sqlmock.NewResult(0, 2))
	if err := store.MarkUnhealthyWorkers(context.Background()); err != nil {
		t.Fatalf("MarkUnhealthyWorkers() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetJobWithEventsReturnsChronologicalAuditTrail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	now := time.Now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRows(now, "job-1", "queued"))
	mock.ExpectQuery("SELECT sequence_num, event_type, payload, trace_id, created_at").
		WithArgs("job-1").
		WillReturnRows(sqlmock.NewRows([]string{"sequence_num", "event_type", "payload", "trace_id", "created_at"}).
			AddRow(1, "JOB_CREATED", []byte(`{"status":"queued"}`), nil, now).
			AddRow(2, "JOB_SCHEDULED", []byte(`{"status":"scheduled"}`), nil, now.Add(time.Second)))
	mock.ExpectCommit()

	job, err := store.GetJobWithEvents(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("GetJobWithEvents() error = %v", err)
	}
	if len(job.Events) != 2 || job.Events[0].SequenceNum != 1 ||
		job.Events[1].SequenceNum != 2 || job.Events[1].EventType != "JOB_SCHEDULED" {
		t.Fatalf("GetJobWithEvents() events = %+v, want ordered creation and scheduling events", job.Events)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestListWorkersScansNullableHeartbeat(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	lastHeartbeat := time.Now()
	mock.ExpectQuery("SELECT id, total_cpu").WillReturnRows(
		sqlmock.NewRows([]string{"id", "total_cpu", "used_cpu", "total_memory_mb", "used_memory_mb",
			"total_gpu", "used_gpu", "runner_version", "status", "last_heartbeat"}).
			AddRow("worker-live", 4, 0, int64(8192), int64(0), 0, 0, "test", "online", lastHeartbeat).
			AddRow("worker-no-heartbeat", 2, 0, int64(4096), int64(0), 0, 0, "test", "offline", nil))

	workers, err := store.ListWorkers(context.Background())
	if err != nil {
		t.Fatalf("ListWorkers() error = %v", err)
	}
	if len(workers) != 2 || !workers[0].LastHeartbeat.Equal(lastHeartbeat) ||
		!workers[1].LastHeartbeat.IsZero() {
		t.Fatalf("ListWorkers() = %+v, want nullable heartbeat timestamps handled", workers)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestScheduleJobReservesWorkerAndAppendsEventTransactionally(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	now := time.Now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRows(now, "job-1", "queued"))
	mock.ExpectExec("UPDATE workers SET used_cpu").
		WithArgs(2, int64(2048), 0, "worker-1", 2, int64(2048), 0).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(sequence_num\\), 0\\) \\+ 1").
		WithArgs("job-1").WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(2))
	mock.ExpectExec("INSERT INTO job_events").
		WithArgs("job-1", 2, "JOB_SCHEDULED", `{"execution_id":1,"status":"scheduled","worker_id":"worker-1"}`, "").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectExec("UPDATE jobs_projection SET status = 'scheduled'").
		WithArgs("worker-1", "", "", "job-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithExecution(now, "job-1", "scheduled", "worker-1", 1, now.Add(15*time.Second)))
	mock.ExpectCommit()

	job, scheduled, err := store.ScheduleJob(context.Background(), "job-1", "worker-1")
	if err != nil || !scheduled || job.Status != "scheduled" || job.WorkerID != "worker-1" {
		t.Fatalf("ScheduleJob() = (%+v, scheduled=%v, err=%v)", job, scheduled, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTransitionJobCommitsProjectionAndEventTogether(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	now := time.Now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithExecution(now, "job-1", "running", "worker-1", 2, now.Add(15*time.Second)))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(sequence_num\\), 0\\) \\+ 1").
		WithArgs("job-1").WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(2))
	mock.ExpectExec("INSERT INTO job_events").
		WithArgs("job-1", 2, "JOB_COMPLETED", `{"execution_id":2,"result":"done","status":"succeeded","worker_id":"worker-1"}`, "").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectExec("UPDATE workers SET used_cpu").
		WithArgs(2, int64(2048), 0, "worker-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE jobs_projection SET status").
		WithArgs("succeeded", "worker-1", "done", "", "", "", "job-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithExecution(now, "job-1", "succeeded", "worker-1", 2, time.Time{}))
	mock.ExpectCommit()

	job, _, err := store.TransitionJobWithExecution(context.Background(), "job-1", "succeeded", "worker-1", 2, "done", "")
	if err != nil {
		t.Fatalf("TransitionJob() error = %v", err)
	}
	if job.Status != "succeeded" {
		t.Fatalf("TransitionJob() status = %q, want succeeded", job.Status)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestClaimJobValidatesAndFencesExecutionToken(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	now := time.Now()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithExecution(now, "job-1", "scheduled", "worker-1", 1, now.Add(15*time.Second)))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(sequence_num\\), 0\\) \\+ 1").
		WithArgs("job-1").WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(2))
	mock.ExpectExec("INSERT INTO job_events").
		WithArgs("job-1", 2, "JOB_LEASED", `{"execution_id":2,"status":"leased","worker_id":"worker-1"}`, "").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(sequence_num\\), 0\\) \\+ 1").
		WithArgs("job-1").WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(3))
	mock.ExpectExec("INSERT INTO job_events").
		WithArgs("job-1", 3, "JOB_STARTED", `{"execution_id":2,"status":"running","worker_id":"worker-1"}`, "").
		WillReturnResult(sqlmock.NewResult(3, 1))
	mock.ExpectExec("UPDATE jobs_projection SET status = 'running'").
		WithArgs("worker-1", "", "", "job-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithExecution(now, "job-1", "running", "worker-1", 2, now.Add(15*time.Second)))
	mock.ExpectCommit()

	job, claimed, err := store.ClaimJob(context.Background(), "job-1", "worker-1", 1)
	if err != nil || !claimed || !job.Claimed {
		t.Fatalf("ClaimJob() = (%+v, claimed=%v, err=%v), want newly claimed job", job, claimed, err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithExecution(now, "job-1", "running", "worker-1", 2, now.Add(15*time.Second)))
	mock.ExpectRollback()
	_, claimed, err = store.ClaimJob(context.Background(), "job-1", "worker-2", 1)
	if !errors.Is(err, ErrStaleExecution) || claimed {
		t.Fatalf("stale ClaimJob() = (claimed=%v, err=%v), want ErrStaleExecution", claimed, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStaleExecutionCannotCommitAfterReassignment(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithExecution(time.Now(), "job-1", "running", "worker-new", 4, time.Now().Add(15*time.Second)))
	mock.ExpectRollback()

	_, _, err = store.TransitionJobWithExecution(
		context.Background(), "job-1", "succeeded", "worker-old", 2, "stale output", "")
	if !errors.Is(err, ErrStaleExecution) {
		t.Fatalf("stale completion error = %v, want ErrStaleExecution", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatRenewsOnlySuppliedExecutionLeases(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE workers SET status = 'online'").
		WithArgs("worker-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE jobs_projection SET").
		WithArgs("job-1", "worker-1", int64(4)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := store.HeartbeatWithLeases(context.Background(), "worker-1", []ActiveLease{
		{JobID: "job-1", ExecutionID: 4},
		{JobID: "job-invalid", ExecutionID: 0},
	}); err != nil {
		t.Fatalf("HeartbeatWithLeases() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReapExpiredJobRequeuesAndReleasesResourcesTransactionally(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	now := time.Now()
	mock.ExpectQuery("SELECT id FROM jobs_projection").
		WithArgs(100).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("job-1"))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithExecution(now, "job-1", "running", "worker-1", 1, now.Add(-time.Second)))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(sequence_num\\), 0\\) \\+ 1").
		WithArgs("job-1").WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(2))
	mock.ExpectExec("INSERT INTO job_events").
		WithArgs("job-1", 2, "JOB_RETRIED",
			`{"execution_id":1,"reason":"lease expired","status":"queued","worker_id":"worker-1"}`, "").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectExec("UPDATE workers SET used_cpu").
		WithArgs(2, int64(2048), 0, "worker-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE jobs_projection SET status").
		WithArgs("queued", "", 1, "", "job-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	reaped, err := store.ReapExpiredJobs(context.Background(), 3, 100)
	if err != nil {
		t.Fatalf("ReapExpiredJobs() error = %v", err)
	}
	if len(reaped) != 1 || reaped[0].Status != "queued" || reaped[0].WorkerID != "worker-1" {
		t.Fatalf("ReapExpiredJobs() = %+v, want recovered queued job", reaped)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTransitionJobRejectsInvalidStateWithoutAppendingEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithExecution(time.Now(), "job-1", "running", "worker-1", 1, time.Time{}))
	mock.ExpectRollback()

	_, _, err = store.TransitionJobWithExecution(context.Background(), "job-1", "queued", "worker-1", 1, "", "")
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("TransitionJob() error = %v, want ErrInvalidTransition", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateJobRollsBackWhenEventAppendFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	expectCreateLookup(mock, time.Now(), "job-1", "queued", "")
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(sequence_num\\), 0\\) \\+ 1").
		WithArgs("job-1").WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(1))
	mock.ExpectExec("INSERT INTO job_events").WillReturnError(fmt.Errorf("disk full"))
	mock.ExpectRollback()

	_, _, err = store.CreateJob(context.Background(), CreateJobRequest{
		ID: "job-1", TenantID: "tenant-1", IdempotencyKey: "request-1",
		Command: "go test ./...", CPU: 2, MemoryMB: 2048,
	})
	if err == nil {
		t.Fatal("CreateJob() succeeded despite event append failure")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func expectCreateLookup(mock sqlmock.Sqlmock, now time.Time, id, status, traceID string) {
	mock.ExpectBegin()
	emptyJobRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"id", "tenant_id", "idempotency_key", "command",
			"req_cpu", "req_memory_mb", "req_gpu", "status", "worker_id", "result", "error_message",
			"created_at", "updated_at", "execution_id", "lease_expires_at"})
	}
	mock.ExpectQuery("WHERE idempotency_key = \\?").WithArgs("request-1").WillReturnRows(emptyJobRows())
	mock.ExpectQuery("SELECT max_cpu").WithArgs("tenant-1").
		WillReturnRows(sqlmock.NewRows([]string{"max_cpu", "max_ram_mb", "max_gpu", "max_concurrent_jobs"}).
			AddRow(64, int64(131072), 8, 100))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(req_cpu\\)").WithArgs("tenant-1").
		WillReturnRows(sqlmock.NewRows([]string{"cpu", "memory", "gpu", "jobs"}).AddRow(0, int64(0), 0, 0))
	mock.ExpectQuery("WHERE idempotency_key = \\? FOR UPDATE").WithArgs("request-1").WillReturnRows(emptyJobRows())
	mock.ExpectExec("INSERT IGNORE INTO jobs_projection").
		WithArgs(id, "tenant-1", "request-1", "go test ./...", 2, int64(2048), 0, traceID, "").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("WHERE idempotency_key = \\? FOR UPDATE").WithArgs("request-1").
		WillReturnRows(jobRows(now, id, status))
}

func jobRows(now time.Time, id, status string) *sqlmock.Rows {
	return jobRowsWithWorker(now, id, status, "")
}

func jobRowsWithWorker(now time.Time, id, status, workerID string) *sqlmock.Rows {
	return jobRowsWithExecution(now, id, status, workerID, 0, time.Time{})
}

func jobRowsWithExecution(now time.Time, id, status, workerID string, executionID int64, leaseExpiresAt time.Time) *sqlmock.Rows {
	var worker any
	if workerID != "" {
		worker = workerID
	}
	var lease any
	if !leaseExpiresAt.IsZero() {
		lease = leaseExpiresAt
	}
	return sqlmock.NewRows([]string{"id", "tenant_id", "idempotency_key", "command",
		"req_cpu", "req_memory_mb", "req_gpu", "status", "worker_id", "result", "error_message",
		"created_at", "updated_at", "execution_id", "lease_expires_at", "trace_id", "traceparent"}).AddRow(
		id, "tenant-1", "request-1", "go test ./...", 2, int64(2048), 0, status,
		worker, nil, nil, now, now, executionID, lease, nil, nil)
}
