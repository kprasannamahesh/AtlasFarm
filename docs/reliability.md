# Reliability, Leases, and Recovery

AtlasFarm assumes that workers, brokers, and database connections can fail
independently. Durable job state lives in MySQL; RabbitMQ delivery is
at-least-once; time-bounded leases and monotonically increasing execution IDs
recover interrupted work and fence stale workers.

## Heartbeats and leases

Workers register their advertised CPU, memory, GPU, and runner version, then
send a heartbeat every five seconds. The heartbeat includes active
`(job_id, execution_id)` leases. The control plane extends each matching lease
to 15 seconds from the current database time. The reaper runs every five
seconds; it marks workers whose heartbeat is older than 15 seconds offline,
clears their reported used resources, and recovers expired job leases.

An expired job is recorded as `JOB_RETRIED` and returned to the queued state
until the retry limit (three retries by default) is reached. The reaper releases
the corresponding resource reservation before the dispatcher tries another
worker. The timer is not an exact deadline: recovery normally occurs on a
reaper tick after lease expiry, subject to database availability.

## Execution ID fencing

`execution_id` is a fencing token, not a worker identity. It increases as the
job is scheduled and claimed. Lifecycle callbacks must present the current
token and the assigned worker ID. If an old worker wakes after a retry or
reassignment, its callback cannot overwrite the newer execution's state; the
API rejects the stale transition with HTTP 409 Conflict.

```text
Attempt 1: scheduled token 1 -> claimed token 2 -> worker disappears
Reaper:    lease expires -> JOB_RETRIED -> job returns to queued state
Attempt 2: scheduled token 3 -> claimed token 4 -> current worker may commit
Late attempt 1 callback with token 2 -> rejected (HTTP 409)
```

RabbitMQ messages are acknowledged only after the control plane accepts the
result. If a delivery is repeated, the database claim prevents an already
claimed job from starting a second time. An ambiguous publisher-confirm result
can also result in a duplicate message; the claim and fencing checks are the
final guards.

## Broker and database recovery behavior

The control-plane publisher reconnects after publish/channel failures using
exponential backoff and retries the persistent publish. A background
reconnect manager also restores the publisher when there is no active publish.
It atomically replaces the active connection/channel reference, which
`GET /readyz` checks directly; readiness recovers once that reference is live.
The worker agent recreates its RabbitMQ consumer session with exponential
backoff capped at 30 seconds and registers again after reconnecting.

`database/sql` can establish new MySQL connections after a transient server
restart. Startup pings retry with bounded exponential backoff within a
10-second deadline; `/readyz` performs a fresh database ping with a 2-second
request timeout. It returns 503 while MySQL or RabbitMQ is unavailable.
`/healthz` remains a process-liveness check and does not depend on MySQL. The
periodic dispatcher, reaper, and rollout watcher report errors and continue
their loops. Startup exits if MySQL or RabbitMQ cannot be reached during
initialization.

## Local chaos runbook

The suite is destructive to the local Compose stack. Run it only against a
disposable development environment, not a shared or production cluster. It
restarts the Compose `rabbitmq` and `mysql` services and forcibly terminates
temporary worker processes. Existing job data remains in MySQL.

### Prepare

1. Start the local dependencies from the repository root:

   ```powershell
   docker compose -f deploy/docker-compose.yml up -d
   ```
2. Start the control plane in a separate terminal:

   ```powershell
   go -C control-plane run ./cmd/server
   ```
3. Ensure `http://localhost:8080/readyz` returns HTTP 200. The chaos suite
   builds and starts its own temporary worker processes.
4. Ensure no active canary rollout is influencing worker selection before the
   worker-crash scenario. The suite deliberately fails that scenario rather
   than changing an operator's rollout configuration. The exact-fit placement
   check assumes the default `best_fit` strategy; a `worst_fit` override can
   select a different eligible worker.
5. From the repository root, run:

   ```powershell
   .\scripts\chaos\run-chaos-suite.ps1
   ```

The suite prints one `[PASS]` or `[FAIL]` result per scenario and exits nonzero
if any check fails. Worker logs are written under the current user's temporary
directory.

### Experiments and expected evidence

1. **Worker crash / lease recovery:** the suite starts a capacity-matched
   temporary worker and a surviving worker, submits a 10-second job, and kills
   the assigned worker after the job enters `running`. Inspect
   `GET /api/v1/jobs/{id}` for `JOB_RETRIED`, a later worker assignment, and
   final `succeeded` status. This depends on the lease timeout plus the reaper
   interval; it is not an instantaneous retry.
2. **RabbitMQ restart:** the suite restarts the local Compose RabbitMQ service
   while submitting jobs. It waits for readiness and verifies all submitted
   jobs reach `succeeded`, exercising the control-plane publisher and worker
   consumer reconnect loops.
3. **MySQL transient outage:** the suite restarts the local Compose MySQL
   service, waits for `/readyz` to recover, then submits a job and checks its
   terminal state. The API process must remain running during this test.
4. **Stale-worker fencing:** after a probe job succeeds, the suite sends a
   deliberately mismatched execution token to the lifecycle endpoint and
   requires HTTP 409. The terminal job state must remain unchanged.

If a scenario fails, check the control-plane terminal and worker logs, then
inspect job events and `GET /readyz`. Do not repeat database or broker restarts
until all prior restart operations have completed.

## Manual checks

```powershell
# Process liveness and dependency readiness
Invoke-WebRequest http://localhost:8080/healthz
Invoke-WebRequest http://localhost:8080/readyz

# Inspect a job's current projection and chronological audit events
Invoke-RestMethod http://localhost:8080/api/v1/jobs/<job-id>

# Verify the API fences a stale execution (the chaos suite automates this)
Invoke-RestMethod http://localhost:8080/metrics
```

The worker's advertised capacity is scheduler metadata. It does not impose
operating-system resource limits or isolate arbitrary commands. Run only
trusted workloads on these agents.
