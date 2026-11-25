package lease

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/events"
	"github.com/kprasannamahesh/AtlasFarm/scheduler"
)

const (
	DefaultMaxRetries = 3
	ReapInterval      = 5 * time.Second
)

type Store interface {
	MarkUnhealthyWorkers(context.Context) error
	ReapExpiredJobs(context.Context, int, int) ([]events.ExpiredJob, error)
}

type ResourceState interface {
	ReleaseJob(string) bool
	ReleaseWorkerResources(string, int, int64, int) error
}

type Reaper struct {
	store      Store
	cluster    ResourceState
	maxRetries int
}

func NewReaper(store Store, cluster ResourceState, maxRetries int) *Reaper {
	if maxRetries <= 0 {
		maxRetries = DefaultMaxRetries
	}
	return &Reaper{store: store, cluster: cluster, maxRetries: maxRetries}
}

func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(ReapInterval)
	defer ticker.Stop()
	for {
		if err := r.ReapOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("run lease recovery: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Reaper) ReapOnce(ctx context.Context) error {
	if err := r.store.MarkUnhealthyWorkers(ctx); err != nil {
		return err
	}
	expired, err := r.store.ReapExpiredJobs(ctx, r.maxRetries, 100)
	for _, job := range expired {
		if r.cluster == nil || job.WorkerID == "" {
			continue
		}
		if !r.cluster.ReleaseJob(job.ID) {
			if releaseErr := r.cluster.ReleaseWorkerResources(
				job.WorkerID, job.CPU, job.MemoryMB, job.GPU); releaseErr != nil &&
				!errors.Is(releaseErr, scheduler.ErrWorkerNotFound) {
				log.Printf("release cluster resources for expired job %s: %v", job.ID, releaseErr)
			}
		}
	}
	return err
}
