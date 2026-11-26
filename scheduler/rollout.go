package scheduler

import (
	"crypto/sha256"
	"encoding/binary"
)

type RolloutConfig struct {
	TargetVersion    string
	CanaryPercentage int
	Status           string
}

func WorkersForRollout(jobID string, workers []*WorkerState, rollout RolloutConfig) []*WorkerState {
	if rollout.Status == "" {
		return workers
	}
	targetJob := rollout.Status == "COMPLETED"
	if rollout.Status == "ACTIVE" {
		digest := sha256.Sum256([]byte(jobID))
		canary := int(binary.BigEndian.Uint64(digest[:8]) % 100)
		targetJob = canary < rollout.CanaryPercentage
	}
	selected := make([]*WorkerState, 0, len(workers))
	for _, worker := range workers {
		isTarget := worker.RunnerVersion == rollout.TargetVersion
		if isTarget == targetJob {
			selected = append(selected, worker)
		}
	}
	return selected
}
