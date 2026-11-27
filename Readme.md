# AtlasFarm

**AtlasFarm** is a Go-based job execution platform. It accepts jobs through a REST API, stores their state and audit events in MySQL, selects workers using tenant fairness and resource fit, then dispatches work through RabbitMQ to worker agents. The repository includes a React operations console and a Docker Compose stack for local infrastructure.

> **Demo Walkthrough:** `[Add your 3-minute Loom / YouTube demo link here]`

---

## Key Capabilities

* **Transactional job state:** Idempotent submissions and job lifecycle events are persisted with their current projection in MySQL.
* **Resource-aware scheduling:** Jobs are matched against worker CPU, memory, and GPU capacity, with first-fit, best-fit, and worst-fit strategies.
* **Tenant fairness and quotas:** Weighted scheduling uses tenant weights; submission checks per-tenant resource and concurrent-job limits.
* **Lease recovery and fencing:** Worker heartbeats renew active leases; expired executions are retried or failed, and stale execution callbacks are rejected.
* **Canary routing:** Jobs are deterministically split between stable-version and target-version workers. A watcher can automatically roll back when the target version's failure rate exceeds its configured threshold.
* **Tracing and metrics:** OpenTelemetry traces are exported to Jaeger; Prometheus scrapes the control-plane and worker metrics endpoints.

---

## System Architecture

```text
Browser: React + TypeScript console
        │ HTTP (localhost:8080)
        ▼
┌──────────────────────────── Go Control Plane ───────────────────────────┐
│ REST API │ dispatcher (1s loop) │ lease reaper (5s) │ rollout watcher   │
│          │ weighted fair queue + resource packer + in-memory cluster    │
└───────┬──────────────────────┬──────────────────────┬───────────────────┘
        │ SQL transactions     │ topic exchange       │ OTLP gRPC
        ▼                      ▼                      ▼
     MySQL 8                RabbitMQ 3              Jaeger
        ▲                      │                      ▲
        │ worker/job state     │ jobs.worker.<id>     │ OTLP gRPC
        │                      ▼                      │
        └────────────── Go Worker Agent ───────────────┘
                         │
                         ├─ HTTP register, heartbeat, lifecycle callbacks
                         └─ runs the command on the worker host

Prometheus (Docker) ──scrapes──> control-plane :8080/metrics
                         └─────> worker-agent :9091/metrics
Grafana and Loki are also included in the local Compose stack.
```

---

## Tech Stack

* **Control Plane & Worker Agent:** Go 1.22+ (`database/sql`, goroutines, `context`, `os/exec`)
* **Transactional State:** MySQL 8.0
* **Message Broker:** RabbitMQ topic exchange with per-worker routing keys
* **Metrics and Tracing:** Prometheus and OpenTelemetry/Jaeger (OTLP gRPC)
* **Additional local observability services:** Grafana and Loki
* **Frontend Ops Console:** React, TypeScript, Tailwind CSS
* **Local Infrastructure:** Docker Compose

---

## Quickstart (Local 16GB-Friendly Setup)

### Prerequisites
* Docker & Docker Compose
* Go 1.22+
* Node.js 20+ (for the Ops Console)

### 1. Start Infrastructure Services
Spin up MySQL, RabbitMQ, Prometheus, Grafana, Loki, and Jaeger:
```bash
make infra-up
```
* **RabbitMQ UI:** `http://localhost:15672` (`guest` / `guest`)
* **Grafana UI:** `http://localhost:3000` (`admin` / `admin`)
* **Prometheus:** `http://localhost:9090`
* **Jaeger UI:** `http://localhost:16686`
* **Loki API:** `http://localhost:3100`

### 2. Run the Control Plane
```bash
make run-control
```
The control plane uses `DATABASE_DSN` (default
`root:atlasfarm-local@tcp(127.0.0.1:3306)/atlasfarm?parseTime=true`) and
`RABBITMQ_URL` (default `amqp://guest:guest@127.0.0.1:25672/`). Its job and
worker endpoints are documented in `api/openapi.yaml`.

### 3. Start Worker Agents
Launch a worker with its advertised resource capacity:
```bash
go -C worker-agent run ./cmd/worker --cpu=4 --memory=8192 --gpu=1 --version=v1.0.0 --concurrency=2 --job-timeout=15m
```
Use `--control-plane`, `--rabbitmq-url`, `--queue`, `--heartbeat-interval`,
and `--id` to configure worker registration and execution. `RABBITMQ_URL` can
also override the worker's default broker URL; `--rabbitmq-url` takes precedence.
The worker agent runs as a Go process on the host when started this way; it is
not launched in a container by the current Compose file.

### 4. Launch the Ops Console
```powershell
cd web-console
npm install
npm run dev
```
Open `http://localhost:5173` to submit jobs, inspect fleet health, and manage canary rollouts.

---

## Job and Scheduling Lifecycle

1. **Submit:** A client posts a job to `POST /api/v1/jobs` (the equivalent
   `/v1/jobs` route is also available). The request includes a tenant, command,
   idempotency key, and CPU/RAM/GPU requirements.
2. **Validate and persist:** The control plane checks the tenant's quotas and
   uses the idempotency key to avoid duplicate jobs. In one MySQL transaction,
   it creates the `jobs_projection` row and appends the `JOB_CREATED` event.
   The job is then visible as `queued`.
3. **Choose a tenant queue:** The dispatcher runs once per second, loads queued
   jobs, worker capacity, and tenant weights from MySQL, then uses weighted
   round-robin scheduling to select the next tenant/job. Seeded tenant weights
   are A=40, B=30, and C=30.
4. **Find a worker:** The scheduler filters to active workers with enough
   available CPU, RAM, and GPU. `SCHEDULER_STRATEGY` selects `first_fit`,
   `best_fit` (default), or `worst_fit`. During an active rollout, a stable hash
   of the job ID selects whether the job is routed to a target-version worker
   or a non-target worker.
5. **Reserve and dispatch:** The scheduler reserves capacity in its in-memory
   cluster state. MySQL records `JOB_SCHEDULED`, the worker assignment, an
   incremented `execution_id`, and a 15-second lease in the job projection.
   The dispatcher publishes a persistent message to RabbitMQ routing key
   `jobs.worker.<worker-id>`.
6. **Claim and execute:** The worker consumes only its own queue, reports its
   claim to the control plane (which advances the fencing token again), and
   executes the command with a context timeout.
   On Windows it invokes `cmd.exe /C`; on Linux it invokes `sh -c`. Output is
   captured (up to 1 MiB) and returned with the completion/failure callback.
7. **Finish or recover:** Successful/failed completion is committed with its
   event and projection update, and the worker reservation is released. Worker
   heartbeats run every five seconds and renew active leases. The five-second
   reaper marks workers offline after 15 seconds without a heartbeat and
   recovers expired job leases. The reaper uses its default `max_retries=3`
   policy to decide whether an expired job is requeued or failed; execution IDs
   act as fencing tokens so stale workers cannot commit results.

The normal event sequence is `JOB_CREATED` → `JOB_SCHEDULED` → `JOB_LEASED` →
`JOB_STARTED` → `JOB_COMPLETED` or `JOB_FAILED`. Recovery adds `JOB_RETRIED`
before the job returns to the queued state; cancellation records
`JOB_CANCELLED`. The projection is optimized for current-state reads, while
`job_events` provides the chronological audit history.

The rollout watcher checks every ten seconds. It considers target-version jobs
completed or failed since the rollout began, waits for at least five outcomes,
and rolls back when the observed error percentage exceeds the configured
threshold. A healthy rollout at 100% can be marked complete.

The worker's CPU/RAM/GPU values are **scheduler-advertised capacity**, not OS
resource limits. The agent currently runs commands directly on its host. The
GPU field is a scheduling capacity value; the agent does not itself execute
GPU-specific work or enforce GPU isolation.

## What Runs Where

For the local workflow above:

* `docker compose -f deploy/docker-compose.yml up -d` starts infrastructure
  containers, not the Go control plane or worker agents.
* `make run-control` runs the Go control-plane process on the host.
* `go -C worker-agent run ./cmd/worker ...` runs a worker process on the host.
* `npm run dev` in `web-console/` runs the Vite development server on the host.

The control plane and workers connect to the host-published MySQL and RabbitMQ
ports. Running the worker in a container or deploying the Go services to a
cluster requires separate deployment configuration.

## Local Infrastructure Services

| Service | Local endpoint | Role in AtlasFarm |
| --- | --- | --- |
| MySQL 8 | `localhost:3306` | Durable source of truth: tenant quotas/weights, current job projections, append-only job events, worker records, and rollout configuration. `deploy/init.sql` creates the schema and seeds three tenants. |
| RabbitMQ 3 (management) | AMQP `localhost:25672`; UI `localhost:15672` | Durable topic exchange `atlasfarm.jobs`. The dispatcher routes each scheduled job to `jobs.worker.<worker-id>`; each worker binds and consumes its own queue. |
| Prometheus | `localhost:9090` | Scrapes control-plane metrics on port 8080 and a worker metrics endpoint on port 9091 using `deploy/prometheus.yml`. |
| Grafana | `localhost:3000` | Visualization service included in Compose. Dashboard/data-source provisioning is not currently configured by the Compose file; the dashboard JSON is a placeholder. |
| Loki | `localhost:3100` | Log storage service included in Compose. An application log shipper/source is not currently configured. |
| Jaeger | UI `localhost:16686`; OTLP gRPC `localhost:4317` | Receives traces from both Go services. W3C trace context is propagated over HTTP and RabbitMQ so spans can be correlated across submission, dispatch, and execution. |

Compose applies local memory limits to services to keep the stack suitable for
development laptops. Persistent Docker volumes retain MySQL, Prometheus, Grafana,
and Loki data between restarts.

## API and Operator Surfaces

The API is served under both `/v1/...` and `/api/v1/...` prefixes. Useful routes:

* `GET /healthz` and `GET /readyz` — process health and MySQL/RabbitMQ readiness.
* `POST /api/v1/jobs`, `GET /api/v1/jobs`, `GET /api/v1/jobs/{id}` — submit,
  list, and inspect jobs with their event timeline.
* `POST /api/v1/workers`, `POST /api/v1/workers/{id}/heartbeat` — register
  workers and renew their active leases.
* `GET /api/v1/workers?active_only=true` — list workers that are healthy or
  have heartbeated within the last 30 seconds.
* `DELETE /api/v1/workers/stale` — purge offline or over-30-second-stale worker
  records.
* `GET /api/v1/rollouts/current`, `POST /api/v1/rollouts`, and
  `POST /api/v1/rollouts/rollback` — inspect and control worker-version rollouts.

The React console polls jobs, workers, and rollout state every five seconds. It
provides job submission, event/trace inspection, active/offline worker controls,
and rollout configuration. See [web-console/README.md](web-console/README.md)
for local UI instructions. The complete API schema is in [api/openapi.yaml](api/openapi.yaml).

## Sprint 5: Chaos, Kind, and Performance Workflows

These are local development tools. Chaos tests restart the local Compose
RabbitMQ/MySQL services and forcibly terminate temporary worker processes.
Benchmarks create persistent job records and can leave queued work. Do not run
either against a shared or production environment.

### Chaos recovery suite

Start Compose infrastructure and the control plane in separate terminals:

```powershell
docker compose -f deploy/docker-compose.yml up -d
go -C control-plane run ./cmd/server
```

After `http://localhost:8080/readyz` returns HTTP 200, run the suite from the
repository root:

```powershell
.\scripts\chaos\run-chaos-suite.ps1
```

It verifies worker-crash lease recovery and `JOB_RETRIED`, RabbitMQ reconnect,
MySQL reconnect, and HTTP 409 fencing of an outdated execution token. It builds
and starts temporary workers itself. The worker-crash case requires no active
canary rollout so it can reliably target its capacity-matched worker. Job/event
records remain in MySQL after the run.

### Kind deployment

Prerequisites: Docker, Kind, kubectl, and Go 1.22+. From the repository root:

```powershell
.\scripts\deploy-kind.ps1
```

The script creates or reuses the `atlasfarm` Kind cluster, generates a local
credential for its Secret, creates the MySQL initialization ConfigMap, builds
and loads both Go images, applies the manifests, and waits for pods to become
Ready. It labels the local Kind node for the demo CPU and simulated GPU pools;
this does not provide physical GPU access. HPA CPU metrics require
metrics-server, which is not installed by the script. Existing credentials are
reused on subsequent runs. `-Recreate` deletes and rebuilds the local cluster,
including its local database volume.

To access local services:

```powershell
kubectl port-forward -n atlasfarm svc/atlasfarm-control-plane 8080:8080
kubectl port-forward -n atlasfarm svc/atlasfarm-jaeger 16686:16686
```

The manifests are a single-node development baseline, not a production HA
deployment. See [docs/architecture.md](docs/architecture.md) for system
boundaries and [docs/reliability.md](docs/reliability.md) for recovery
assumptions.

### k6 and pprof benchmarks

Start the Compose services and the control plane with its default profiling
configuration, then run:

```powershell
.\scripts\run-benchmarks.ps1
```

The runner captures pre/post goroutine and heap profiles in `docs/profiles/`,
then runs the 10,000-job workload with k6 or a bounded-concurrency Go fallback.
The verified Option A local Docker Desktop run accepted all 10,000 jobs with no
errors or deadlocks; all four chaos recovery scenarios passed.

| Throughput | p50 latency | p95 latency | p99 latency | Post-load heap |
| ---: | ---: | ---: | ---: | ---: |
| 129.63 jobs/sec | 44.48 ms | 629.63 ms | 684.78 ms | 0.93 MB |

Use the local default p99 limit (`-MaxP99Ms 1000`) for Docker Desktop/WSL2
storage variability. The production p99 target remains below 25 ms; this local
run does not meet it. Profiles are git-ignored. The load test persists jobs, so
use a disposable development database. See [docs/benchmarks.md](docs/benchmarks.md)
for full results, pprof comparisons, methodology, and scaling analysis.

---

## Repository Structure

```text
atlasfarm/
├── api/                    # OpenAPI contract
├── control-plane/          # Go control-plane module
├── scheduler/              # Go resource-aware scheduling module
├── worker-agent/           # Go worker module
├── deploy/                 # Compose stack and Kind Kubernetes manifests
├── helm/atlasfarm/         # Kubernetes Helm chart
├── k6/                     # Load tests
├── scripts/                # Chaos, benchmark, and Kind deployment helpers
├── dashboards/             # Grafana dashboard placeholders
├── proto/                  # Worker protocol placeholder
├── web-console/            # React + TypeScript operations console
└── docs/                   # Architecture, scheduler, reliability, benchmarks
    └── profiles/           # Local pprof artifacts (git-ignored)
```

---

## Current Scope Notes

AtlasFarm is an evolving development platform. The local Compose stack provides
MySQL, RabbitMQ, Prometheus, Grafana, Loki, and Jaeger; it does not containerize
the Go services. The Kind manifests are intended for local validation and
demonstration, not high availability or production security. Helm chart
templates and dashboards remain starter artifacts. There is no configured
dead-letter queue, DAG orchestrator, Elasticsearch/Fluent Bit pipeline, or
Prometheus-driven rollout controller in the current implementation. Rollout
decisions use job outcomes recorded in MySQL.

---

## License

Licensed under the [Apache License 2.0](LICENSE).
