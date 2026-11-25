package scheduler

import (
	"strconv"
	"testing"
)

func TestFairSchedulerWeightedDistributionWithBackloggedTenant(t *testing.T) {
	scheduler := NewFairScheduler()
	scheduler.SetWeights(map[string]int{"tenant-a": 40, "tenant-b": 30})
	for i := 0; i < 1000; i++ {
		scheduler.Enqueue(Job{ID: "a-" + strconv.Itoa(i), TenantID: "tenant-a"})
	}
	for i := 0; i < 5; i++ {
		scheduler.Enqueue(Job{ID: "b-" + strconv.Itoa(i), TenantID: "tenant-b"})
	}

	firstB := -1
	countA := 0
	countB := 0
	for i := 0; i < 100; i++ {
		job, ok := scheduler.Next()
		if !ok {
			t.Fatal("scheduler unexpectedly ran out of work")
		}
		switch job.TenantID {
		case "tenant-a":
			countA++
		case "tenant-b":
			countB++
			if firstB == -1 {
				firstB = i
			}
		}
	}
	if firstB < 0 || firstB > 3 {
		t.Fatalf("tenant B starved until dispatch %d", firstB)
	}
	if countA == 0 || countB == 0 {
		t.Fatalf("both tenants should receive service, got A=%d B=%d", countA, countB)
	}
	if countB != 5 {
		t.Fatalf("tenant B should dispatch all 5 queued jobs, got %d", countB)
	}
}
