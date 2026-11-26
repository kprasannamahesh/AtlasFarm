package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/rollout"
)

type fakeRolloutStore struct {
	current rollout.Rollout
	created rollout.CreateRequest
}

func (s *fakeRolloutStore) GetCurrent(context.Context) (rollout.Rollout, error) {
	if s.current.ID == 0 {
		return rollout.Rollout{}, rollout.ErrNotFound
	}
	return s.current, nil
}

func (s *fakeRolloutStore) Create(_ context.Context, req rollout.CreateRequest) (rollout.Rollout, error) {
	s.created = req
	s.current = rollout.Rollout{
		ID: 1, TargetVersion: req.TargetVersion, CanaryPercentage: req.CanaryPercentage,
		ErrorThresholdPct: req.ErrorThresholdPct, Status: "ACTIVE",
	}
	return s.current, nil
}

func (s *fakeRolloutStore) Rollback(context.Context) (rollout.Rollout, error) {
	if s.current.ID == 0 {
		return rollout.Rollout{}, rollout.ErrNotFound
	}
	s.current.CanaryPercentage = 0
	s.current.Status = "ROLLED_BACK"
	return s.current, nil
}

func TestRolloutEndpointsCreateCurrentAndRollback(t *testing.T) {
	store := &fakeRolloutStore{}
	handler := NewHandlerWithRollouts(nil, nil, store)

	createRequest := httptest.NewRequest(http.MethodPost, "/v1/rollouts",
		strings.NewReader(`{"target_version":"v2.0.0-canary","canary_percentage":25,"error_threshold_pct":10}`))
	createRequest.Header.Set("Content-Type", "application/json")
	createResponse := httptest.NewRecorder()
	handler.Routes().ServeHTTP(createResponse, createRequest)
	if createResponse.Code != http.StatusCreated {
		t.Fatalf("POST /rollouts status = %d, want 201: %s", createResponse.Code, createResponse.Body.String())
	}
	if store.created.TargetVersion != "v2.0.0-canary" || store.created.CanaryPercentage != 25 {
		t.Fatalf("Create request = %+v, want target version and percentage", store.created)
	}

	currentRequest := httptest.NewRequest(http.MethodGet, "/v1/rollouts/current", nil)
	currentResponse := httptest.NewRecorder()
	handler.Routes().ServeHTTP(currentResponse, currentRequest)
	var current rollout.Rollout
	if err := json.Unmarshal(currentResponse.Body.Bytes(), &current); err != nil {
		t.Fatal(err)
	}
	if currentResponse.Code != http.StatusOK || current.Status != "ACTIVE" {
		t.Fatalf("GET /rollouts/current = %d %+v, want active rollout", currentResponse.Code, current)
	}

	rollbackRequest := httptest.NewRequest(http.MethodPost, "/v1/rollouts/rollback", nil)
	rollbackResponse := httptest.NewRecorder()
	handler.Routes().ServeHTTP(rollbackResponse, rollbackRequest)
	var rolledBack rollout.Rollout
	if err := json.Unmarshal(rollbackResponse.Body.Bytes(), &rolledBack); err != nil {
		t.Fatal(err)
	}
	if rollbackResponse.Code != http.StatusOK || rolledBack.Status != "ROLLED_BACK" ||
		rolledBack.CanaryPercentage != 0 {
		t.Fatalf("POST /rollouts/rollback = %d %+v, want zero-percent rollback", rollbackResponse.Code, rolledBack)
	}
}
