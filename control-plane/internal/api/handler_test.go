package api

import (
	"errors"
	"net/http/httptest"
	"testing"

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
