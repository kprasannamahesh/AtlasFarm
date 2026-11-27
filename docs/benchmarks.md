# Performance Engineering and Benchmarks

The repository includes an HTTP job-submission load test and a PowerShell
runner that captures Go runtime profiles before and after the load.

## What the load test measures

`k6/load-test-10k.js` submits 10,000 lightweight jobs using 32 concurrent
virtual users. The workload is distributed deterministically: four of each ten
requests go to `tenant-a`, three to `tenant-b`, and three to `tenant-c`. Each
request asks for one CPU, 64 MiB of RAM, and no GPU, and uses a unique
idempotency key.

The test checks HTTP 201 responses, tracks accepted/submitted counters and
submission latency, and checks for dropped iterations and failed requests.
The local runner uses a configurable p99 threshold of 1,000 ms by default to
account for Docker Desktop storage latency; the production p99 target remains
below 25 ms. The local runner passes that limit to k6 as well as the Go
fallback. `jobs_submitted` and `jobs_accepted` counter rates provide request
throughput in k6's output; p50 and p95 are recorded without local pass limits.

The runner saves each tenant's current quotas, temporarily raises them for the
benchmark window, and restores the saved values in a `finally` block. It sends
one preflight job before the measured 10,000 submissions and prints its HTTP
status and response body if rejected. Both load generators also print a small
sample of failed responses (including status and body).

This is an API submission benchmark, not an end-to-end execution-latency
benchmark. Jobs continue through the scheduler and workers after the HTTP
request returns. The test persists job records and can leave queued work;
run it against a disposable local database or clean the test data before using
the environment for other work.

## Run and collect profiles

Start the local Compose services and the control plane. The control-plane
pprof endpoints are enabled by default for benchmarking; disable them on an
untrusted network with `--enable-pprof=false`. From the repository root:

```powershell
.\scripts\run-benchmarks.ps1
```

Override the local p99 pass limit when needed:

```powershell
.\scripts\run-benchmarks.ps1 -MaxP99Ms 1000
```

The script captures the baseline and post-load goroutine text and heap profiles
from `/debug/pprof/goroutine?debug=2` and `/debug/pprof/heap`. It invokes k6 when
available; otherwise it runs a 32-worker Go HTTP generator that
submits the same 10,000-job workload. The fallback reports accepted/rejected
counts, total duration, attempted requests per second, accepted jobs per second,
and latency percentiles. On a successful run the script also adds measured
throughput, latency percentiles, and baseline/post-load pprof counts to the
results table below.

Generated artifacts are saved under `docs/profiles/` and are git-ignored:

```text
<run-id>-baseline.goroutine.txt
<run-id>-baseline.heap.pprof
<run-id>-post-load.goroutine.txt
<run-id>-post-load.heap.pprof
<run-id>-load-output.txt
```

Review an interactive heap profile or its top allocations with:

```powershell
go tool pprof .\docs\profiles\<run-id>-post-load.heap.pprof
go tool pprof -top .\docs\profiles\<run-id>-post-load.heap.pprof
```

The saved goroutine text can be counted or compared with PowerShell:

```powershell
(Select-String -Path .\docs\profiles\<run-id>-post-load.goroutine.txt `
  -Pattern '^goroutine \d+ \[').Count
```

## Goroutine-leak verification

Compare baseline and post-load goroutine counts only after the load has
finished and transient HTTP, scheduler, and worker activity has settled. The
runner waits three seconds before taking the post-load snapshot; repeat the
snapshot manually if the count is still changing. A count that returns near
baseline is useful evidence of no persistent goroutine growth, but one sample
does not prove the absence of a leak. For stronger evidence, run several
identical load/settle cycles and compare counts and goroutine stacks.

Heap profiles are snapshots, not leak verdicts. The Go runtime may retain
freed memory for reuse, so compare allocation profiles and repeated snapshots
instead of expecting identical heap sizes.

## Results record

This verified Option A run used local Docker Desktop with 32 concurrent
submissions. The measured p99 is below the local 1,000 ms pass threshold but
does not meet the production target of 25 ms.

| Date / environment | Tool and version | Submitted / accepted | Jobs/sec | p50 (ms) | p95 (ms) | p99 (ms) | Goroutines baseline → post | Heap memory baseline → post | Chaos suite | Outcome |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2026-11-27 / Option A, local Docker Desktop | Go fallback, 32 workers | 10,000 / 10,000 | 129.63 | 44.48 | 629.63 | 684.78 | 26 → 61 (backlog draining) | 1.05 MB → 0.93 MB | 4 / 4 passed | 0 errors; 0 deadlocks after Error 1213 lock-ordering fix |

The post-load profile was captured while queued jobs were still draining, which
accounts for the temporary increase from 26 to 61 goroutines. The observed heap
was lower post-load, and no persistent memory or goroutine leak was observed.

## Bottleneck Analysis & Production Scaling Path

The local run accepted all 10,000 submissions at 129.63 jobs/sec, but latency
rose from 44.48 ms at p50 to 684.78 ms at p99 under 32 concurrent clients:

1. **Tenant-row ordering removed deadlocks.** Each submission locks its parent
   `tenants` row before quota checks and projection/event writes. This
   serializes submissions for the same tenant and avoids competing
   `jobs_projection` next-key/gap locks. After this Error 1213 fix, acceptance
   increased from 0.15% to 100%, with zero deadlocks in the verified run.
2. **Local transaction commits remain the latency floor.** Up to 32 requests
   queue behind each tenant's row lock. On Docker Desktop's WSL2 virtual disk,
   `innodb_flush_log_at_trx_commit=1` makes durable commits pay local fsync
   latency (about 20 ms per commit in this run). The serialized commit queue
   explains why p50 stayed at 44.48 ms while tail latency reached 684.78 ms.
3. **Production scaling options:** shard quota accounting across independent
   quota buckets to reduce per-tenant lock contention; move admission checks
   to an atomic Redis Lua token-bucket operation with durable reconciliation;
   or batch/group-commit transaction writes. These approaches need explicit
   quota consistency and recovery semantics. Production validation should
   target sub-25 ms p99 at 10k+ jobs/sec on production-grade storage; the local
   Docker Desktop result is not evidence that this target is met.

## Chaos recovery result

The verified Sprint 5 Chaos Recovery Suite passed all four scenarios:
worker SIGKILL lease recovery, RabbitMQ restart/reconnect, MySQL restart
recovery, and stale `ExecutionID` fencing with HTTP 409.

## Kind sizing and caveats

The Kind manifests request roughly 1.7 GiB of memory across two control-plane
replicas, three small demo workers, MySQL, RabbitMQ, Prometheus, and Jaeger;
container limits allow higher peaks. Reserve at least several GiB for Docker
and Kind overhead. A 16 GiB development machine is a practical baseline, but
actual load-test capacity depends on host CPU, Docker limits, and other running
applications. MySQL receives a 5 GiB persistent volume claim. RabbitMQ,
Prometheus, and Jaeger use single replicas and are not configured here for
durable production storage or high availability.

The GPU worker pool in Kind is a scheduling simulation: the deployment labels
a Kind node as GPU-capable, but no physical GPU is passed through. HPA CPU
metrics require a metrics-server installation; the deployment script does not
install one. These manifests demonstrate workload wiring and should not be
treated as production sizing or availability evidence.
