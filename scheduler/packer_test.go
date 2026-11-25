package scheduler

import (
	"errors"
	"testing"
)

func TestBinPackerStrategies(t *testing.T) {
	workers := []*WorkerState{
		{ID: "first", TotalCPU: 8, TotalMemoryMB: 8192, TotalGPU: 4, Status: "online"},
		{ID: "best", TotalCPU: 4, TotalMemoryMB: 4096, TotalGPU: 2, Status: "online"},
		{ID: "worst", TotalCPU: 16, TotalMemoryMB: 16384, TotalGPU: 8, Status: "online"},
	}
	request := ResourceRequest{CPU: 2, MemoryMB: 2048, GPU: 1}
	tests := []struct {
		name     string
		strategy string
		want     string
	}{
		{name: "first fit", strategy: "first_fit", want: "first"},
		{name: "best fit", strategy: "best_fit", want: "best"},
		{name: "worst fit", strategy: "worst_fit", want: "worst"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			packer, err := NewBinPacker(test.strategy)
			if err != nil {
				t.Fatal(err)
			}
			worker, err := packer.SelectWorker(request, workers)
			if err != nil {
				t.Fatal(err)
			}
			if worker.ID != test.want {
				t.Fatalf("selected %q, want %q", worker.ID, test.want)
			}
		})
	}
}

func TestBinPackerResourceBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		worker  WorkerState
		request ResourceRequest
		wantFit bool
	}{
		{
			name:    "exact CPU memory and GPU boundary fits",
			worker:  WorkerState{ID: "exact", TotalCPU: 2, TotalMemoryMB: 1024, TotalGPU: 1, Status: "online"},
			request: ResourceRequest{CPU: 2, MemoryMB: 1024, GPU: 1},
			wantFit: true,
		},
		{
			name:    "GPU over capacity does not fit",
			worker:  WorkerState{ID: "gpu", TotalCPU: 4, TotalMemoryMB: 4096, TotalGPU: 1, Status: "online"},
			request: ResourceRequest{CPU: 1, MemoryMB: 1, GPU: 2},
			wantFit: false,
		},
		{
			name:    "RAM over capacity does not fit",
			worker:  WorkerState{ID: "ram", TotalCPU: 4, TotalMemoryMB: 1024, Status: "online"},
			request: ResourceRequest{CPU: 1, MemoryMB: 1025},
			wantFit: false,
		},
		{
			name:    "inactive worker does not fit",
			worker:  WorkerState{ID: "offline", TotalCPU: 4, TotalMemoryMB: 4096, Status: "offline"},
			request: ResourceRequest{CPU: 1, MemoryMB: 1},
			wantFit: false,
		},
	}
	packer, err := NewBinPacker("best_fit")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			worker, err := packer.SelectWorker(test.request, []*WorkerState{&test.worker})
			if test.wantFit {
				if err != nil || worker == nil {
					t.Fatalf("expected fit, got worker=%v error=%v", worker, err)
				}
				return
			}
			if !errors.Is(err, ErrNoEligibleWorker) {
				t.Fatalf("expected ErrNoEligibleWorker, got %v", err)
			}
		})
	}
}

func TestClusterStateReserveReleaseAndMetrics(t *testing.T) {
	state := NewClusterState()
	state.Upsert(WorkerState{
		ID: "worker", TotalCPU: 8, TotalMemoryMB: 8192, TotalGPU: 4, Status: "online",
	})
	request := ResourceRequest{CPU: 2, MemoryMB: 2048, GPU: 1}
	if err := state.ReserveJob("job-1", "worker", request); err != nil {
		t.Fatal(err)
	}
	if err := state.ReserveJob("job-1", "worker", request); err != nil {
		t.Fatal(err)
	}
	usage := state.Utilization()
	if usage.CPUPercent != 25 || usage.MemoryPercent != 25 || usage.GPUPercent != 25 {
		t.Fatalf("unexpected utilization: %+v", usage)
	}
	if !state.ReleaseJob("job-1") || state.ReleaseJob("job-1") {
		t.Fatal("job resource release must be idempotent")
	}
	if got := state.Snapshot()[0].UsedCPU; got != 0 {
		t.Fatalf("CPU reservation remains after release: %d", got)
	}
}
