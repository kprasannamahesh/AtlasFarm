package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/events"
)

func TestWorkerListingActiveOnlyAndStalePurge(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := NewHandler(events.New(db), nil)

	mock.ExpectQuery("FROM workers WHERE status = 'HEALTHY'").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "total_cpu", "used_cpu", "total_memory_mb", "used_memory_mb",
			"total_gpu", "used_gpu", "runner_version", "status", "last_heartbeat",
		}).AddRow("worker-active", 4, 0, 4096, 0, 1, 0, "v1", "HEALTHY", nil))
	listRequest := httptest.NewRequest(http.MethodGet, "/v1/workers?active_only=true", nil)
	listResponse := httptest.NewRecorder()
	handler.Routes().ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK {
		t.Fatalf("GET /workers?active_only=true status = %d, want 200", listResponse.Code)
	}

	mock.ExpectExec("DELETE FROM workers").
		WillReturnResult(sqlmock.NewResult(0, 3))
	purgeRequest := httptest.NewRequest(http.MethodDelete, "/v1/workers/stale", nil)
	purgeResponse := httptest.NewRecorder()
	handler.Routes().ServeHTTP(purgeResponse, purgeRequest)
	if purgeResponse.Code != http.StatusOK {
		t.Fatalf("DELETE /workers/stale status = %d, want 200: %s", purgeResponse.Code, purgeResponse.Body.String())
	}
	var result struct {
		Purged int64 `json:"purged"`
	}
	if err := json.Unmarshal(purgeResponse.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Purged != 3 {
		t.Fatalf("purged count = %d, want 3", result.Purged)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
