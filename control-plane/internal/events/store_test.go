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
	expectCreateLookup(mock, now, "job-1", "queued")
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(sequence_num\\), 0\\) \\+ 1").
		WithArgs("job-1").WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(1))
	mock.ExpectExec("INSERT INTO job_events").
		WithArgs("job-1", 1, "JOB_CREATED", `{"status":"queued","tenant_id":"tenant-1"}`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	job, created, err := store.CreateJob(context.Background(), CreateJobRequest{
		ID: "job-1", TenantID: "tenant-1", IdempotencyKey: "request-1",
		Command: "go test ./...", CPU: 2, MemoryMB: 2048,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if !created || job.ID != "job-1" || job.Status != "queued" {
		t.Fatalf("CreateJob() = (%+v, created=%v), want queued new job", job, created)
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
			"created_at", "updated_at"}).AddRow("job-original", "tenant-1", "request-1",
			"original", 1, int64(1024), 0, "queued", nil, nil, nil, time.Now(), time.Now()))
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
		WithArgs("job-1", 2, "JOB_SCHEDULED", `{"status":"scheduled","worker_id":"worker-1"}`).
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectExec("UPDATE jobs_projection SET status = 'scheduled'").
		WithArgs("worker-1", "job-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithWorker(now, "job-1", "scheduled", "worker-1"))
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
		WillReturnRows(jobRowsWithWorker(now, "job-1", "running", "worker-1"))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(sequence_num\\), 0\\) \\+ 1").
		WithArgs("job-1").WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(2))
	mock.ExpectExec("INSERT INTO job_events").
		WithArgs("job-1", 2, "JOB_COMPLETED", `{"result":"done","status":"succeeded","worker_id":"worker-1"}`).
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectExec("UPDATE workers SET used_cpu").
		WithArgs(2, int64(2048), 0, "worker-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE jobs_projection SET status").
		WithArgs("succeeded", "worker-1", "done", "", "job-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRows(now, "job-1", "succeeded"))
	mock.ExpectCommit()

	job, err := store.TransitionJob(context.Background(), "job-1", "succeeded", "worker-1", "done", "")
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

func TestClaimJobIsExclusiveAndIdempotent(t *testing.T) {
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
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(sequence_num\\), 0\\) \\+ 1").
		WithArgs("job-1").WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(2))
	mock.ExpectExec("INSERT INTO job_events").
		WithArgs("job-1", 2, "JOB_STARTED", `{"status":"running","worker_id":"worker-1"}`).
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectExec("UPDATE jobs_projection SET status = 'running'").
		WithArgs("worker-1", "job-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithWorker(now, "job-1", "running", "worker-1"))
	mock.ExpectCommit()

	job, claimed, err := store.ClaimJob(context.Background(), "job-1", "worker-1")
	if err != nil || !claimed || !job.Claimed {
		t.Fatalf("ClaimJob() = (%+v, claimed=%v, err=%v), want newly claimed job", job, claimed, err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
		WillReturnRows(jobRowsWithWorker(now, "job-1", "running", "worker-1"))
	mock.ExpectCommit()
	job, claimed, err = store.ClaimJob(context.Background(), "job-1", "worker-2")
	if err != nil || claimed || job.Claimed {
		t.Fatalf("duplicate ClaimJob() = (%+v, claimed=%v, err=%v), want not claimed", job, claimed, err)
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
		WillReturnRows(jobRows(time.Now(), "job-1", "queued"))
	mock.ExpectRollback()

	_, err = store.TransitionJob(context.Background(), "job-1", "succeeded", "worker-1", "", "")
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
	expectCreateLookup(mock, time.Now(), "job-1", "queued")
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

func expectCreateLookup(mock sqlmock.Sqlmock, now time.Time, id, status string) {
	mock.ExpectBegin()
	emptyJobRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"id", "tenant_id", "idempotency_key", "command",
			"req_cpu", "req_memory_mb", "req_gpu", "status", "worker_id", "result", "error_message",
			"created_at", "updated_at"})
	}
	mock.ExpectQuery("WHERE idempotency_key = \\?").WithArgs("request-1").WillReturnRows(emptyJobRows())
	mock.ExpectQuery("SELECT max_cpu").WithArgs("tenant-1").
		WillReturnRows(sqlmock.NewRows([]string{"max_cpu", "max_ram_mb", "max_gpu", "max_concurrent_jobs"}).
			AddRow(64, int64(131072), 8, 100))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(req_cpu\\)").WithArgs("tenant-1").
		WillReturnRows(sqlmock.NewRows([]string{"cpu", "memory", "gpu", "jobs"}).AddRow(0, int64(0), 0, 0))
	mock.ExpectQuery("WHERE idempotency_key = \\? FOR UPDATE").WithArgs("request-1").WillReturnRows(emptyJobRows())
	mock.ExpectExec("INSERT IGNORE INTO jobs_projection").
		WithArgs(id, "tenant-1", "request-1", "go test ./...", 2, int64(2048), 0).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("WHERE idempotency_key = \\? FOR UPDATE").WithArgs("request-1").
		WillReturnRows(jobRows(now, id, status))
}

func jobRows(now time.Time, id, status string) *sqlmock.Rows {
	return jobRowsWithWorker(now, id, status, "")
}

func jobRowsWithWorker(now time.Time, id, status, workerID string) *sqlmock.Rows {
	var worker any
	if workerID != "" {
		worker = workerID
	}
	return sqlmock.NewRows([]string{"id", "tenant_id", "idempotency_key", "command",
		"req_cpu", "req_memory_mb", "req_gpu", "status", "worker_id", "result", "error_message",
		"created_at", "updated_at"}).AddRow(id, "tenant-1", "request-1", "go test ./...",
		2, int64(2048), 0, status, worker, nil, nil, now, now)
}
