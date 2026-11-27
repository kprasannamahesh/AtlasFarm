package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/events"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/quota"
)

func TestQuotaExceededReturnsTooManyRequests(t *testing.T) {
	handler := &Handler{}
	response := httptest.NewRecorder()
	handler.storeError(response, errors.Join(quota.ErrExceeded, errors.New("CPU quota exceeded")))
	if response.Code != 429 {
		t.Fatalf("quota error returned HTTP %d, want 429", response.Code)
	}
}

func TestUnknownTenantReturnsBadRequest(t *testing.T) {
	handler := &Handler{}
	response := httptest.NewRecorder()
	handler.storeError(response, quota.ErrTenantNotFound)
	if response.Code != 400 {
		t.Fatalf("unknown tenant returned HTTP %d, want 400", response.Code)
	}
}

func TestStaleCommitReturnsConflictAndIncrementsCounter(t *testing.T) {
	handler := &Handler{}
	handler.recordStaleCommit(events.ErrStaleExecution)
	response := httptest.NewRecorder()
	handler.storeError(response, events.ErrStaleExecution)
	if response.Code != 409 {
		t.Fatalf("stale execution returned HTTP %d, want 409", response.Code)
	}
	if got := handler.StaleCommitsRejected(); got != 1 {
		t.Fatalf("stale commit counter = %d, want 1", got)
	}
}

func TestStatusCallbackReturnsConflictForStaleExecution(t *testing.T) {
	tests := []struct {
		name         string
		status       string
		workerID     any
		executionID  int64
		requestToken int64
	}{
		{
			name:         "terminal job with nullable result columns",
			status:       "succeeded",
			executionID:  2,
			requestToken: 1,
		},
		{
			name:         "reassigned running job",
			status:       "running",
			workerID:     "worker-new",
			executionID:  4,
			requestToken: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			now := time.Now()
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT id, tenant_id").WithArgs("job-1").
				WillReturnRows(sqlmock.NewRows([]string{
					"id", "tenant_id", "idempotency_key", "command", "req_cpu",
					"req_memory_mb", "req_gpu", "status", "worker_id", "result",
					"error_message", "created_at", "updated_at", "execution_id",
					"lease_expires_at", "trace_id", "traceparent",
				}).AddRow(
					"job-1", "tenant-a", "request-1", "echo test", 1, int64(1024), 0,
					tt.status, tt.workerID, nil, nil, now, now, tt.executionID,
					nil, nil, nil,
				))
			mock.ExpectRollback()

			handler := NewHandler(events.New(db), nil)
			body := `{"status":"failed","worker_id":"worker-old","execution_id":` +
				strconv.FormatInt(tt.requestToken, 10) + `}`
			request := httptest.NewRequest(http.MethodPost, "/v1/jobs/job-1/status", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.Routes().ServeHTTP(response, request)

			if response.Code != http.StatusConflict {
				t.Fatalf("status callback returned HTTP %d, want 409: %s", response.Code, response.Body.String())
			}
			if got := handler.StaleCommitsRejected(); got != 1 {
				t.Fatalf("stale commit counter = %d, want 1", got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
