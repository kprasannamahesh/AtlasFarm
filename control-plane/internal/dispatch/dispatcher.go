package dispatch

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/events"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/rollout"
	"github.com/kprasannamahesh/AtlasFarm/scheduler"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Store interface {
	ListQueuedJobs(context.Context, int) ([]events.Job, error)
	ListWorkers(context.Context) ([]events.Worker, error)
	TenantWeights(context.Context) (map[string]int, error)
	ScheduleJob(context.Context, string, string) (events.Job, bool, error)
}

type RolloutReader interface {
	GetCurrent(context.Context) (rollout.Rollout, error)
}

type Publisher interface {
	PublishToWorker(context.Context, string, events.Job) error
}

type Dispatcher struct {
	store   Store
	publish Publisher
	cluster *scheduler.ClusterState
	packer  scheduler.BinPacker
	fair    *scheduler.FairScheduler
	rollout RolloutReader
}

func New(store Store, publish Publisher, cluster *scheduler.ClusterState, packer scheduler.BinPacker, rolloutReaders ...RolloutReader) *Dispatcher {
	dispatcher := &Dispatcher{
		store: store, publish: publish, cluster: cluster, packer: packer,
		fair: scheduler.NewFairScheduler(),
	}
	if len(rolloutReaders) > 0 {
		dispatcher.rollout = rolloutReaders[0]
	}
	return dispatcher
}

func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := d.DispatchOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("dispatch queued jobs: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *Dispatcher) DispatchOnce(ctx context.Context) error {
	workers, err := d.store.ListWorkers(ctx)
	if err != nil {
		return err
	}
	activeIDs := make(map[string]struct{}, len(workers))
	for _, worker := range workers {
		activeIDs[worker.ID] = struct{}{}
		d.cluster.Upsert(scheduler.WorkerState{
			ID: worker.ID, TotalCPU: worker.CPU, UsedCPU: worker.UsedCPU,
			TotalMemoryMB: worker.MemoryMB, UsedMemoryMB: worker.UsedMemoryMB,
			TotalGPU: worker.GPU, UsedGPU: worker.UsedGPU, Status: worker.Status,
			RunnerVersion: worker.RunnerVersion,
		})
	}
	for _, worker := range d.cluster.Snapshot() {
		if _, ok := activeIDs[worker.ID]; !ok {
			d.cluster.Remove(worker.ID)
		}
	}

	weights, err := d.store.TenantWeights(ctx)
	if err != nil {
		return err
	}
	d.fair.SetWeights(weights)
	jobs, err := d.store.ListQueuedJobs(ctx, 500)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		d.fair.Enqueue(scheduler.Job{
			ID: job.ID, TenantID: job.TenantID, TraceParent: job.TraceParent,
			Request: scheduler.ResourceRequest{CPU: job.CPU, MemoryMB: job.MemoryMB, GPU: job.GPU},
		})
	}
	rolloutConfig := scheduler.RolloutConfig{}
	if d.rollout != nil {
		current, err := d.rollout.GetCurrent(ctx)
		if err != nil && !errors.Is(err, rollout.ErrNotFound) {
			return err
		}
		if err == nil {
			rolloutConfig = scheduler.RolloutConfig{
				TargetVersion: current.TargetVersion, CanaryPercentage: current.CanaryPercentage, Status: current.Status,
			}
		}
	}

	queued := d.fair.Len()
	for i := 0; i < queued; i++ {
		scheduledJob, ok := d.fair.Next()
		if !ok {
			break
		}
		request := scheduledJob.Request
		workers := scheduler.WorkersForRollout(scheduledJob.ID, d.cluster.Snapshot(), rolloutConfig)
		worker, err := d.packer.SelectWorker(request, workers)
		if err != nil {
			if errors.Is(err, scheduler.ErrNoEligibleWorker) {
				d.fair.Enqueue(scheduledJob)
				continue
			}
			return err
		}
		if err := d.cluster.ReserveJob(scheduledJob.ID, worker.ID, request); err != nil {
			d.fair.Enqueue(scheduledJob)
			continue
		}
		dispatchCtx := ctx
		if scheduledJob.TraceParent != "" {
			dispatchCtx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{
				"traceparent": scheduledJob.TraceParent,
			})
		}
		dispatchCtx, span := otel.Tracer("atlasfarm/scheduler").Start(dispatchCtx, "scheduler.dispatch",
			trace.WithAttributes(attribute.String("job.id", scheduledJob.ID), attribute.String("worker.id", worker.ID)))
		job, claimed, err := d.store.ScheduleJob(dispatchCtx, scheduledJob.ID, worker.ID)
		if err != nil {
			span.RecordError(err)
			span.End()
			d.cluster.ReleaseJob(scheduledJob.ID)
			d.fair.Enqueue(scheduledJob)
			return err
		}
		if !claimed {
			span.End()
			d.cluster.ReleaseJob(scheduledJob.ID)
			continue
		}
		if err := d.publish.PublishToWorker(dispatchCtx, worker.ID, job); err != nil {
			log.Printf("publish scheduled job %s to worker %s: %v", job.ID, worker.ID, err)
			span.RecordError(err)
		}
		span.End()
	}
	return nil
}
