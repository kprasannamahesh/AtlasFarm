package lease

import (
	"context"
	"testing"

	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/events"
	"github.com/kprasannamahesh/AtlasFarm/scheduler"
)

type fakeStore struct {
	jobs []events.ExpiredJob
}

func (s *fakeStore) MarkUnhealthyWorkers(context.Context) error {
	return nil
}

func (s *fakeStore) ReapExpiredJobs(context.Context, int, int) ([]events.ExpiredJob, error) {
	return s.jobs, nil
}

func TestReaperReleasesClusterReservation(t *testing.T) {
	cluster := scheduler.NewClusterState()
	cluster.Upsert(scheduler.WorkerState{
		ID: "worker-1", TotalCPU: 4, TotalMemoryMB: 4096, TotalGPU: 1, Status: "online",
	})
	request := scheduler.ResourceRequest{CPU: 2, MemoryMB: 1024, GPU: 1}
	if err := cluster.ReserveJob("job-1", "worker-1", request); err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{jobs: []events.ExpiredJob{
		{ID: "job-1", WorkerID: "worker-1", CPU: 2, MemoryMB: 1024, GPU: 1, Status: "queued"},
	}}

	if err := NewReaper(store, cluster, 0).ReapOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	worker := cluster.Snapshot()[0]
	if worker.UsedCPU != 0 || worker.UsedMemoryMB != 0 || worker.UsedGPU != 0 {
		t.Fatalf("expired job resources remain reserved: %+v", worker)
	}
}
