package scheduler

import (
	"sort"
	"sync"
)

type Job struct {
	ID          string
	TenantID    string
	Request     ResourceRequest
	TraceParent string
}

type FairScheduler struct {
	mu       sync.Mutex
	weights  map[string]int
	current  map[string]int
	queues   map[string][]Job
	queuedID map[string]struct{}
}

func NewFairScheduler() *FairScheduler {
	return &FairScheduler{
		weights:  make(map[string]int),
		current:  make(map[string]int),
		queues:   make(map[string][]Job),
		queuedID: make(map[string]struct{}),
	}
}

func (s *FairScheduler) SetWeights(weights map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.weights = make(map[string]int, len(weights))
	for tenant, weight := range weights {
		if tenant != "" && weight > 0 {
			s.weights[tenant] = weight
		}
	}
}

func (s *FairScheduler) Enqueue(job Job) bool {
	if job.ID == "" || job.TenantID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.queuedID[job.ID]; exists {
		return false
	}
	s.queues[job.TenantID] = append(s.queues[job.TenantID], job)
	s.queuedID[job.ID] = struct{}{}
	if s.weights[job.TenantID] <= 0 {
		s.weights[job.TenantID] = 1
	}
	return true
}

func (s *FairScheduler) Next() (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tenants := make([]string, 0, len(s.queues))
	totalWeight := 0
	for tenant, queue := range s.queues {
		if len(queue) == 0 {
			continue
		}
		tenants = append(tenants, tenant)
		totalWeight += s.weights[tenant]
	}
	if len(tenants) == 0 {
		return Job{}, false
	}
	sort.Strings(tenants)
	selected := tenants[0]
	for _, tenant := range tenants {
		s.current[tenant] += s.weights[tenant]
		if s.current[tenant] > s.current[selected] {
			selected = tenant
		}
	}
	s.current[selected] -= totalWeight
	queue := s.queues[selected]
	job := queue[0]
	if len(queue) == 1 {
		delete(s.queues, selected)
		s.current[selected] = 0
	} else {
		s.queues[selected] = queue[1:]
	}
	delete(s.queuedID, job.ID)
	return job, true
}

func (s *FairScheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queuedID)
}
