package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/events"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/quota"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/rollout"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type Store interface {
	CreateJob(context.Context, events.CreateJobRequest) (events.Job, bool, error)
	GetJob(context.Context, string) (events.Job, error)
	GetJobWithEvents(context.Context, string) (events.Job, error)
	ListJobs(context.Context) ([]events.Job, error)
	TransitionJob(context.Context, string, string, string, string, string) (events.Job, error)
	TransitionJobWithChange(context.Context, string, string, string, string, string) (events.Job, bool, error)
	TransitionJobWithExecution(context.Context, string, string, string, int64, string, string) (events.Job, bool, error)
	ClaimJob(context.Context, string, string, int64) (events.Job, bool, error)
	RegisterWorker(context.Context, events.Worker) error
	HeartbeatWithLeases(context.Context, string, []events.ActiveLease) error
	ListWorkers(context.Context) ([]events.Worker, error)
	ListActiveWorkers(context.Context) ([]events.Worker, error)
	PurgeStaleWorkers(context.Context) (int64, error)
}

type Publisher interface {
	Publish(context.Context, events.Job) error
	PublishToWorker(context.Context, string, events.Job) error
}

type ResourceReleaser interface {
	ReleaseJob(string) bool
	ReleaseWorkerResources(string, int, int64, int) error
}

type Handler struct {
	store                Store
	publisher            Publisher
	resources            ResourceReleaser
	rollouts             RolloutStore
	staleCommitsRejected atomic.Uint64
}

type RolloutStore interface {
	GetCurrent(context.Context) (rollout.Rollout, error)
	Create(context.Context, rollout.CreateRequest) (rollout.Rollout, error)
	Rollback(context.Context) (rollout.Rollout, error)
}

func NewHandler(store Store, publisher Publisher, resources ...ResourceReleaser) *Handler {
	handler := &Handler{store: store, publisher: publisher}
	if len(resources) > 0 {
		handler.resources = resources[0]
	}
	return handler
}

func NewHandlerWithRollouts(store Store, publisher Publisher, rolloutStore RolloutStore, resources ...ResourceReleaser) *Handler {
	handler := NewHandler(store, publisher, resources...)
	handler.rollouts = rolloutStore
	return handler
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/jobs", h.jobs)
	mux.HandleFunc("/v1/jobs/", h.job)
	mux.HandleFunc("/v1/workers", h.workers)
	mux.HandleFunc("/v1/workers/stale", h.staleWorkers)
	mux.HandleFunc("/v1/workers/", h.worker)
	mux.HandleFunc("/v1/rollouts/current", h.currentRollout)
	mux.HandleFunc("/v1/rollouts/rollback", h.rollback)
	mux.HandleFunc("/v1/rollouts", h.rolloutsEndpoint)
	return mux
}

func (h *Handler) jobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		ctx, span := otel.Tracer("atlasfarm/control-plane").Start(r.Context(), "POST /jobs")
		defer span.End()
		traceID := ""
		if span.SpanContext().IsValid() {
			traceID = span.SpanContext().TraceID().String()
		}
		r = r.WithContext(ctx)
		var req events.CreateJobRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		req.TraceID = traceID
		if req.IdempotencyKey == "" {
			req.IdempotencyKey = r.Header.Get("Idempotency-Key")
		}
		if req.CPU == 0 {
			req.CPU = 1
		}
		if req.MemoryMB == 0 {
			req.MemoryMB = 1024
		}
		job, created, err := h.store.CreateJob(r.Context(), req)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "job creation failed")
			h.storeError(w, err)
			return
		}
		span.SetAttributes(attribute.String("job.id", job.ID), attribute.String("tenant.id", job.TenantID))
		status := http.StatusCreated
		if !created {
			status = http.StatusOK
		}
		writeJSON(w, status, job)
	case http.MethodGet:
		jobs, err := h.store.ListJobs(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, jobs)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
	}
}

type statusRequest struct {
	Status      string `json:"status"`
	WorkerID    string `json:"worker_id"`
	ExecutionID int64  `json:"execution_id"`
	Result      string `json:"result"`
	Error       string `json:"error"`
}

type heartbeatRequest struct {
	ActiveJobs []events.ActiveLease `json:"active_jobs"`
}

func (h *Handler) job(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/jobs/"), "/"), "/")
	if len(parts) == 1 && parts[0] != "" && r.Method == http.MethodGet {
		ctx, span := otel.Tracer("atlasfarm/control-plane").Start(r.Context(), "jobs.get")
		defer span.End()
		span.SetAttributes(attribute.String("job.id", parts[0]))
		r = r.WithContext(ctx)
		job, err := h.store.GetJobWithEvents(r.Context(), parts[0])
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "job lookup failed")
			h.storeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, job)
		return
	}
	if len(parts) == 1 && parts[0] != "" && r.Method == http.MethodDelete {
		current, err := h.store.GetJob(r.Context(), parts[0])
		if err != nil {
			h.storeError(w, err)
			return
		}
		job, changed, err := h.store.TransitionJobWithChange(r.Context(), parts[0], "cancelled", "", "", "")
		if err != nil {
			h.storeError(w, err)
			return
		}
		h.releaseResources(job, changed)
		if current.WorkerID != "" && current.Status != "succeeded" && current.Status != "failed" && current.Status != "cancelled" &&
			h.publisher != nil {
			if err := h.publisher.PublishToWorker(r.Context(), current.WorkerID, job); err != nil {
				writeError(w, http.StatusServiceUnavailable, fmt.Errorf("publish cancellation signal: %w", err))
				return
			}
		}
		writeJSON(w, http.StatusOK, job)
		return
	}
	if len(parts) == 2 && parts[0] != "" && parts[1] == "status" && r.Method == http.MethodPost {
		var req statusRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		if req.ExecutionID <= 0 {
			writeError(w, http.StatusBadRequest, errors.New("execution_id must be greater than zero"))
			return
		}
		var job events.Job
		var err error
		if req.Status == "running" {
			var claimed bool
			job, claimed, err = h.store.ClaimJob(
				r.Context(), parts[0], req.WorkerID, req.ExecutionID)
			if err == nil {
				job.Claimed = claimed
			}
		} else {
			var changed bool
			job, changed, err = h.store.TransitionJobWithExecution(
				r.Context(), parts[0], req.Status, req.WorkerID,
				req.ExecutionID, req.Result, req.Error)
			if err == nil {
				h.releaseResources(job, changed)
			}
		}
		if err != nil {
			if errors.Is(err, events.ErrStaleExecution) {
				h.recordStaleCommit(err)
				writeError(w, http.StatusConflict, err)
				return
			}
			h.storeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, job)
		return
	}
	writeError(w, http.StatusNotFound, errors.New("route not found"))
}

func (h *Handler) releaseResources(job events.Job, changed bool) {
	if !changed || h.resources == nil ||
		(job.Status != "succeeded" && job.Status != "failed" && job.Status != "cancelled") {
		return
	}
	if !h.resources.ReleaseJob(job.ID) && job.WorkerID != "" {
		if err := h.resources.ReleaseWorkerResources(job.WorkerID, job.CPU, job.MemoryMB, job.GPU); err != nil {
			log.Printf("release in-memory resources for job %s: %v", job.ID, err)
		}
	}
}

type registerWorkerRequest struct {
	ID            string `json:"id"`
	CPU           int    `json:"cpu"`
	MemoryMB      int64  `json:"memory_mb"`
	GPU           int    `json:"gpu"`
	RunnerVersion string `json:"runner_version"`
}

func (h *Handler) workers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req registerWorkerRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		worker := events.Worker{
			ID: req.ID, CPU: req.CPU, MemoryMB: req.MemoryMB, GPU: req.GPU,
			RunnerVersion: req.RunnerVersion, Status: "online", LastHeartbeat: time.Now().UTC(),
		}
		if err := h.store.RegisterWorker(r.Context(), worker); err != nil {
			if errors.Is(err, events.ErrInvalidRequest) {
				writeError(w, http.StatusBadRequest, err)
			} else {
				writeError(w, http.StatusInternalServerError, err)
			}
			return
		}
		writeJSON(w, http.StatusCreated, worker)
	case http.MethodGet:
		activeOnly := false
		if value := r.URL.Query().Get("active_only"); value != "" {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				writeError(w, http.StatusBadRequest, errors.New("active_only must be a boolean"))
				return
			}
			activeOnly = parsed
		}
		var workers []events.Worker
		var err error
		if activeOnly {
			workers, err = h.store.ListActiveWorkers(r.Context())
		} else {
			workers, err = h.store.ListWorkers(r.Context())
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, workers)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
	}
}

func (h *Handler) staleWorkers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.Header().Set("Allow", "DELETE")
		writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	purged, err := h.store.PurgeStaleWorkers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"purged": purged})
}

func (h *Handler) worker(w http.ResponseWriter, r *http.Request) {
	const suffix = "/heartbeat"
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/workers/"), "/")
	if !strings.HasSuffix(path, suffix) || r.Method != http.MethodPost {
		writeError(w, http.StatusNotFound, errors.New("route not found"))
		return
	}
	id := strings.TrimSuffix(path, suffix)
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, errors.New("route not found"))
		return
	}
	var heartbeat heartbeatRequest
	if !decodeJSON(w, r, &heartbeat) {
		return
	}
	if len(heartbeat.ActiveJobs) > 1024 {
		writeError(w, http.StatusBadRequest, errors.New("active_jobs cannot exceed 1024 entries"))
		return
	}
	if err := h.store.HeartbeatWithLeases(r.Context(), id, heartbeat.ActiveJobs); err != nil {
		h.storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "online"})
}

func (h *Handler) currentRollout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	if h.rollouts == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("rollout service unavailable"))
		return
	}
	current, err := h.rollouts.GetCurrent(r.Context())
	if err != nil {
		h.rolloutError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, current)
}

func (h *Handler) rolloutsEndpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	if h.rollouts == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("rollout service unavailable"))
		return
	}
	var req struct {
		TargetVersion     string  `json:"target_version"`
		CanaryPercentage  int     `json:"canary_percentage"`
		ErrorThresholdPct float64 `json:"error_threshold_pct"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	created, err := h.rollouts.Create(r.Context(), rollout.CreateRequest{
		TargetVersion: req.TargetVersion, CanaryPercentage: req.CanaryPercentage,
		ErrorThresholdPct: req.ErrorThresholdPct,
	})
	if err != nil {
		h.rolloutError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (h *Handler) rollback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	if h.rollouts == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("rollout service unavailable"))
		return
	}
	rolledBack, err := h.rollouts.Rollback(r.Context())
	if err != nil {
		h.rolloutError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rolledBack)
}

func (h *Handler) rolloutError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, rollout.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, rollout.ErrInvalidRequest):
		writeError(w, http.StatusBadRequest, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func (h *Handler) storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, events.ErrStaleExecution):
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, events.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, events.ErrInvalidTransition):
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, events.ErrInvalidRequest):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, quota.ErrExceeded):
		writeError(w, http.StatusTooManyRequests, err)
	case errors.Is(err, quota.ErrTenantNotFound):
		writeError(w, http.StatusBadRequest, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func (h *Handler) recordStaleCommit(err error) {
	if errors.Is(err, events.ErrStaleExecution) {
		count := h.staleCommitsRejected.Add(1)
		log.Printf("rejected stale execution commit (total=%d): %v", count, err)
	}
}

func (h *Handler) StaleCommitsRejected() uint64 {
	return h.staleCommitsRejected.Load()
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, errors.New("request body must contain exactly one JSON value"))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
