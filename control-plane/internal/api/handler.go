package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/events"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/quota"
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
	staleCommitsRejected atomic.Uint64
}

func NewHandler(store Store, publisher Publisher, resources ...ResourceReleaser) *Handler {
	handler := &Handler{store: store, publisher: publisher}
	if len(resources) > 0 {
		handler.resources = resources[0]
	}
	return handler
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/jobs", h.jobs)
	mux.HandleFunc("/v1/jobs/", h.job)
	mux.HandleFunc("/v1/workers", h.workers)
	mux.HandleFunc("/v1/workers/", h.worker)
	return mux
}

func (h *Handler) jobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req events.CreateJobRequest
		if !decodeJSON(w, r, &req) {
			return
		}
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
			h.storeError(w, err)
			return
		}
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
		job, err := h.store.GetJobWithEvents(r.Context(), parts[0])
		if err != nil {
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
			h.recordStaleCommit(err)
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
		workers, err := h.store.ListWorkers(r.Context())
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
