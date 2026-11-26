package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrWorkerNotFound   = errors.New("worker not found")
	ErrInsufficientRoom = errors.New("worker has insufficient available resources")
)

type ResourceRequest struct {
	CPU      int
	MemoryMB int64
	GPU      int
}

type WorkerState struct {
	ID            string
	TotalCPU      int
	UsedCPU       int
	TotalMemoryMB int64
	UsedMemoryMB  int64
	TotalGPU      int
	UsedGPU       int
	RunnerVersion string
	Status        string
}

func (w *WorkerState) AvailableCPU() int {
	return w.TotalCPU - w.UsedCPU
}

func (w *WorkerState) AvailableMemoryMB() int64 {
	return w.TotalMemoryMB - w.UsedMemoryMB
}

func (w *WorkerState) AvailableGPU() int {
	return w.TotalGPU - w.UsedGPU
}

type reservation struct {
	workerID string
	request  ResourceRequest
}

type ClusterState struct {
	mu           sync.RWMutex
	workers      map[string]*WorkerState
	reservations map[string]reservation
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		workers:      make(map[string]*WorkerState),
		reservations: make(map[string]reservation),
	}
}

func (c *ClusterState) Upsert(worker WorkerState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	copy := worker
	c.workers[worker.ID] = &copy
}

func (c *ClusterState) Remove(workerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.workers, workerID)
}

func (c *ClusterState) Snapshot() []*WorkerState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]string, 0, len(c.workers))
	for id := range c.workers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	workers := make([]*WorkerState, 0, len(ids))
	for _, id := range ids {
		copy := *c.workers[id]
		workers = append(workers, &copy)
	}
	return workers
}

func (c *ClusterState) Reserve(workerID string, request ResourceRequest) error {
	if !validRequest(request) {
		return fmt.Errorf("invalid resource request: cpu and memory must be positive, gpu must be nonnegative")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	worker, ok := c.workers[workerID]
	if !ok {
		return ErrWorkerNotFound
	}
	if !active(worker.Status) || !canFit(worker, request) {
		return ErrInsufficientRoom
	}
	worker.UsedCPU += request.CPU
	worker.UsedMemoryMB += request.MemoryMB
	worker.UsedGPU += request.GPU
	return nil
}

func (c *ClusterState) Release(workerID string, request ResourceRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker, ok := c.workers[workerID]
	if !ok {
		return ErrWorkerNotFound
	}
	worker.UsedCPU = maxInt(0, worker.UsedCPU-request.CPU)
	worker.UsedMemoryMB = maxInt64(0, worker.UsedMemoryMB-request.MemoryMB)
	worker.UsedGPU = maxInt(0, worker.UsedGPU-request.GPU)
	return nil
}

func (c *ClusterState) ReleaseWorkerResources(workerID string, cpu int, memoryMB int64, gpu int) error {
	return c.Release(workerID, ResourceRequest{CPU: cpu, MemoryMB: memoryMB, GPU: gpu})
}

func (c *ClusterState) ReserveJob(jobID, workerID string, request ResourceRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !validRequest(request) {
		return errors.New("invalid resource request: cpu and memory must be positive, gpu must be nonnegative")
	}
	if _, exists := c.reservations[jobID]; exists {
		return nil
	}
	worker, ok := c.workers[workerID]
	if !ok {
		return ErrWorkerNotFound
	}
	if !active(worker.Status) || !canFit(worker, request) {
		return ErrInsufficientRoom
	}
	worker.UsedCPU += request.CPU
	worker.UsedMemoryMB += request.MemoryMB
	worker.UsedGPU += request.GPU
	c.reservations[jobID] = reservation{workerID: workerID, request: request}
	return nil
}

func (c *ClusterState) ReleaseJob(jobID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	reservation, exists := c.reservations[jobID]
	if !exists {
		return false
	}
	if worker, ok := c.workers[reservation.workerID]; ok {
		worker.UsedCPU = maxInt(0, worker.UsedCPU-reservation.request.CPU)
		worker.UsedMemoryMB = maxInt64(0, worker.UsedMemoryMB-reservation.request.MemoryMB)
		worker.UsedGPU = maxInt(0, worker.UsedGPU-reservation.request.GPU)
	}
	delete(c.reservations, jobID)
	return true
}

type Utilization struct {
	CPUPercent       float64
	MemoryPercent    float64
	GPUPercent       float64
	GPUFragmentation float64
}

func (c *ClusterState) Utilization() Utilization {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var totalCPU, usedCPU int
	var totalMemory, usedMemory int64
	var totalGPU, usedGPU, freeGPU, largestFreeGPU int
	for _, worker := range c.workers {
		if !active(worker.Status) {
			continue
		}
		totalCPU += worker.TotalCPU
		usedCPU += worker.UsedCPU
		totalMemory += worker.TotalMemoryMB
		usedMemory += worker.UsedMemoryMB
		totalGPU += worker.TotalGPU
		usedGPU += worker.UsedGPU
		available := maxInt(0, worker.AvailableGPU())
		freeGPU += available
		if available > largestFreeGPU {
			largestFreeGPU = available
		}
	}
	return Utilization{
		CPUPercent:       percentage(usedCPU, totalCPU),
		MemoryPercent:    percentage64(usedMemory, totalMemory),
		GPUPercent:       percentage(usedGPU, totalGPU),
		GPUFragmentation: fragmentation(freeGPU, largestFreeGPU),
	}
}

func (u Utilization) Prometheus() string {
	return strings.Join([]string{
		"# HELP atlasfarm_cluster_cpu_utilization_percent Cluster CPU utilization percentage.",
		"# TYPE atlasfarm_cluster_cpu_utilization_percent gauge",
		fmt.Sprintf("atlasfarm_cluster_cpu_utilization_percent %g", u.CPUPercent),
		"# HELP atlasfarm_cluster_memory_utilization_percent Cluster memory utilization percentage.",
		"# TYPE atlasfarm_cluster_memory_utilization_percent gauge",
		fmt.Sprintf("atlasfarm_cluster_memory_utilization_percent %g", u.MemoryPercent),
		"# HELP atlasfarm_cluster_gpu_utilization_percent Cluster GPU utilization percentage.",
		"# TYPE atlasfarm_cluster_gpu_utilization_percent gauge",
		fmt.Sprintf("atlasfarm_cluster_gpu_utilization_percent %g", u.GPUPercent),
		"# HELP atlasfarm_cluster_gpu_fragmentation_ratio Fraction of free GPU capacity fragmented across workers.",
		"# TYPE atlasfarm_cluster_gpu_fragmentation_ratio gauge",
		fmt.Sprintf("atlasfarm_cluster_gpu_fragmentation_ratio %g", u.GPUFragmentation),
		"",
	}, "\n")
}

func active(status string) bool {
	switch strings.ToLower(status) {
	case "online", "active", "healthy":
		return true
	default:
		return false
	}
}

func validRequest(request ResourceRequest) bool {
	return request.CPU > 0 && request.MemoryMB > 0 && request.GPU >= 0
}

func canFit(worker *WorkerState, request ResourceRequest) bool {
	return worker.AvailableCPU() >= request.CPU &&
		worker.AvailableMemoryMB() >= request.MemoryMB &&
		worker.AvailableGPU() >= request.GPU
}

func percentage(used, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(used) * 100 / float64(total)
}

func percentage64(used, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(used) * 100 / float64(total)
}

func fragmentation(freeGPU, largestFreeGPU int) float64 {
	if freeGPU == 0 {
		return 0
	}
	return 1 - float64(largestFreeGPU)/float64(freeGPU)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
