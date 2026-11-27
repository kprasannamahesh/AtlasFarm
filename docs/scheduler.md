# Scheduling and Resource Placement

The dispatcher runs once per second. It loads a snapshot of queued jobs, worker
capacities, and tenant weights from MySQL, selects jobs fairly across tenant
queues, chooses an eligible worker, reserves capacity, records
`JOB_SCHEDULED`, and publishes a worker-targeted RabbitMQ message.

## Scheduling stages

```text
queued jobs ──> tenant fair queues ──> rollout eligibility ──> resource fit
                                      │                        │
                                      └── version filter       └── reserve
                                                                    │
                                                    MySQL schedule transaction
                                                                    │
                                                    RabbitMQ worker route
```

### 1. Tenant fairness

`FairScheduler` uses a smooth weighted round-robin score. On each selection,
each non-empty tenant queue receives its configured weight in a running
current-score; the highest score is selected and reduced by the sum of active
weights. The selected tenant's oldest queued job is then removed. The seeded
weights produce a long-run scheduling share of approximately 40:30:30 for
Tenant A, B, and C when all three queues remain backlogged:

```text
tenant_share ≈ tenant_weight / sum(weights of non-empty tenant queues)
```

The order is fair-queue selection, not a guarantee of equal completion time:
job duration and worker fit affect when a job actually runs. Empty queues are
removed from the active weight denominator.

### 2. Eligibility and resource fit

A worker is eligible only when it is active and has enough *available* capacity
in every requested dimension:

```text
available_cpu      = total_cpu      - used_cpu
available_memory   = total_memory   - used_memory
available_gpu      = total_gpu      - used_gpu

eligible(worker, job) =
    active(worker)
    AND available_cpu    >= req_cpu
    AND available_memory >= req_memory_mb
    AND available_gpu    >= req_gpu
```

The dispatcher additionally filters candidates during canary rollouts: a
deterministic hash of `job_id` chooses the target-version or stable cohort, and
only workers in the selected version cohort are considered. If no eligible
worker is available, the job remains queued for a later dispatcher pass.

### 3. Packing strategies

Select `first_fit`, `best_fit`, or `worst_fit` through `SCHEDULER_STRATEGY`.
Workers are considered in their cluster snapshot's lexicographic ID order.
For each fitting worker, the current normalized residual-capacity score is:

```text
cpu_residual = (available_cpu - req_cpu) / total_cpu
ram_residual = (available_memory_mb - req_memory_mb) / total_memory_mb
gpu_residual = (available_gpu - req_gpu) / total_gpu    when total_gpu > 0

score = cpu_residual + ram_residual + gpu_residual
```

* **First Fit** selects the first eligible worker in ID order. It has low
  selection cost and is deterministic, but can leave awkward capacity in later
  workers.
* **Best Fit** selects the lowest score. It tends to pack work tightly and
  preserve larger empty workers, but may concentrate utilization and requires
  scanning the candidates.
* **Worst Fit** selects the highest score. It spreads work toward workers with
  more remaining normalized capacity, but can fragment the cluster and leave
  small residual bins.

The score is a simple sum, not a tunable optimization model. CPU, RAM, and GPU
dimensions are normalized independently; it does not account for different
resource prices, NUMA locality, affinity, job duration, or physical GPU type.
If a worker has no GPU capacity, GPU is omitted from its score; GPU eligibility
is still checked.

### Multi-dimensional fragmentation

A job requiring CPU, RAM, and GPU can be unschedulable even when the cluster
has enough aggregate free resources. Example:

```text
Worker 1: free 8 CPU, 2 GiB RAM, 1 GPU
Worker 2: free 2 CPU, 8 GiB RAM, 1 GPU
Job:      needs 8 CPU, 8 GiB RAM, 1 GPU
```

No single worker satisfies all three dimensions. `GET /metrics` exposes
cluster-wide CPU/RAM/GPU utilization and a GPU fragmentation ratio:

```text
gpu_fragmentation = 1 - largest_free_gpu_count_on_one_worker / total_free_gpu_count
```

The ratio describes GPU fragmentation only; it is not a general CPU/RAM
fragmentation score. Resource requirements are admission checks against a
single worker, not a cluster-wide reservation that can be split among nodes.

## Quota enforcement

At submission, the control plane locks the tenant row and checks its limits in
the same SQL transaction used to create the job. The check sums requested
resources for jobs in `scheduled`, `leased`, or `running` states and compares
them against the tenant's CPU, RAM, GPU, and concurrent-job limits. The seeded
tenant limits are in `deploy/init.sql`; the weights are independent of these
limits.

Queued jobs do not count as active resource usage in this check. Therefore,
resource quotas prevent over-admission into active work but do not cap the
number of queued requests or the total database backlog. Capacity is reserved
again transactionally when the dispatcher schedules a job to a worker.

## Operational tuning

1. Start with `best_fit` for a general-purpose resource-aware pool.
2. Compare scheduling delay and per-dimension utilization before changing the
   strategy; a high GPU fragmentation ratio can indicate that aggregate GPU
   capacity is misleading.
3. Keep advertised worker CPU/RAM/GPU values within the resources the host can
   safely provide. These values are scheduler metadata, not OS enforcement.
4. Track queued-job age, no-eligible-worker errors, quota rejections, retries,
   and `atlasfarm_cluster_*_utilization_percent` metrics together.
5. Canary percentages only control routing to runner versions; they do not
   provision capacity or guarantee the target version has enough eligible
   workers.

The scheduler is intentionally a small in-process component. The dispatcher
uses periodic database reads and an in-memory cluster snapshot; the current
design does not implement distributed leader election or globally shared
resource reservations.
