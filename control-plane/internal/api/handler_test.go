package api

import (
	"errors"
	"net/http/httptest"
	"testing"

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
