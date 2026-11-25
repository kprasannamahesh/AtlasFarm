package scheduler

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
)

var ErrNoEligibleWorker = errors.New("no eligible worker has the requested resources")

type BinPacker interface {
	SelectWorker(req ResourceRequest, workers []*WorkerState) (*WorkerState, error)
}

type PackingStrategy string

const (
	FirstFit PackingStrategy = "first_fit"
	BestFit  PackingStrategy = "best_fit"
	WorstFit PackingStrategy = "worst_fit"
)

type binPacker struct {
	strategy PackingStrategy
}

func NewBinPacker(strategy string) (BinPacker, error) {
	selected := PackingStrategy(strings.ToLower(strings.TrimSpace(strategy)))
	switch selected {
	case FirstFit, BestFit, WorstFit:
		return binPacker{strategy: selected}, nil
	default:
		return nil, fmt.Errorf("unknown scheduler strategy %q (expected first_fit, best_fit, or worst_fit)", strategy)
	}
}

func NewBinPackerFromEnv() (BinPacker, error) {
	strategy := os.Getenv("SCHEDULER_STRATEGY")
	if strategy == "" {
		strategy = string(BestFit)
	}
	return NewBinPacker(strategy)
}

func (p binPacker) SelectWorker(req ResourceRequest, workers []*WorkerState) (*WorkerState, error) {
	if !validRequest(req) {
		return nil, errors.New("invalid resource request: cpu and memory must be positive, gpu must be nonnegative")
	}
	var selected *WorkerState
	selectedScore := 0.0
	for _, worker := range workers {
		if worker == nil || !active(worker.Status) || !canFit(worker, req) {
			continue
		}
		if p.strategy == FirstFit {
			return worker, nil
		}
		score := remainingCapacity(worker, req)
		if selected == nil ||
			(p.strategy == BestFit && score < selectedScore) ||
			(p.strategy == WorstFit && score > selectedScore) {
			selected = worker
			selectedScore = score
		}
	}
	if selected == nil {
		return nil, ErrNoEligibleWorker
	}
	return selected, nil
}

func remainingCapacity(worker *WorkerState, req ResourceRequest) float64 {
	remCPU := worker.AvailableCPU() - req.CPU
	remMemory := worker.AvailableMemoryMB() - req.MemoryMB
	remGPU := worker.AvailableGPU() - req.GPU
	score := float64(remCPU) / float64(worker.TotalCPU)
	score += float64(remMemory) / float64(worker.TotalMemoryMB)
	if worker.TotalGPU > 0 {
		score += float64(remGPU) / float64(worker.TotalGPU)
	}
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return math.MaxFloat64
	}
	return score
}
