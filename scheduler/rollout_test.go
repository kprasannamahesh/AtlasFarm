package scheduler

import (
	"fmt"
	"testing"
)

func TestWorkersForRolloutUsesStableDeterministicPercentage(t *testing.T) {
	workers := []*WorkerState{
		{ID: "stable-1", RunnerVersion: "v1"},
		{ID: "stable-2", RunnerVersion: "v1"},
		{ID: "canary", RunnerVersion: "v2-canary"},
	}
	config := RolloutConfig{TargetVersion: "v2-canary", CanaryPercentage: 25, Status: "ACTIVE"}
	canaryCount := 0
	for i := 0; i < 1000; i++ {
		jobID := fmt.Sprintf("job-%d", i)
		first := WorkersForRollout(jobID, workers, config)
		second := WorkersForRollout(jobID, workers, config)
		if len(first) == 0 || len(second) != len(first) || first[0].ID != second[0].ID {
			t.Fatalf("job %s received inconsistent or empty worker set: %v / %v", jobID, first, second)
		}
		if first[0].RunnerVersion == config.TargetVersion {
			canaryCount++
			if len(first) != 1 {
				t.Fatalf("canary job %s was routed to non-canary workers", jobID)
			}
		} else {
			for _, worker := range first {
				if worker.RunnerVersion == config.TargetVersion {
					t.Fatalf("stable job %s reached canary version", jobID)
				}
			}
		}
	}
	if canaryCount < 200 || canaryCount > 300 {
		t.Fatalf("canary assignments = %d/1000, want approximately 25%%", canaryCount)
	}
}

func TestWorkersForCompletedRolloutUsesTargetVersionForAllJobs(t *testing.T) {
	workers := []*WorkerState{
		{ID: "stable", RunnerVersion: "v1"},
		{ID: "canary", RunnerVersion: "v2"},
	}
	got := WorkersForRollout("job", workers, RolloutConfig{
		TargetVersion: "v2", CanaryPercentage: 10, Status: "COMPLETED",
	})
	if len(got) != 1 || got[0].ID != "canary" {
		t.Fatalf("completed rollout workers = %v, want only target version", got)
	}
}

func TestWorkersForRolledBackRolloutExcludesTargetVersion(t *testing.T) {
	workers := []*WorkerState{
		{ID: "stable", RunnerVersion: "v1"},
		{ID: "canary", RunnerVersion: "v2"},
	}
	got := WorkersForRollout("job", workers, RolloutConfig{
		TargetVersion: "v2", CanaryPercentage: 0, Status: "ROLLED_BACK",
	})
	if len(got) != 1 || got[0].ID != "stable" {
		t.Fatalf("rolled-back rollout workers = %v, want only stable version", got)
	}
}
