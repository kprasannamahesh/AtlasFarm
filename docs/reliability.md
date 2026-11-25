# Reliability

Jobs are represented as an append-only event stream with a queryable
`jobs_projection`. Creation and state transitions append their event and update
the projection in the same MySQL transaction. Repeated idempotency keys return
the original job. Workers use a durable RabbitMQ queue, manual acknowledgements,
prefetch, and a fixed-size execution pool. A worker claims a queued job before
execution; duplicate deliveries of already-claimed jobs are acknowledged
without running again.

Worker registration and five-second heartbeats are recorded in MySQL.
Heartbeats renew the 15-second leases for active `(job_id, execution_id)` pairs;
workers with no heartbeat for 20 seconds are marked unhealthy. The lease reaper
checks expired jobs every five seconds, releases worker capacity transactionally,
and either requeues the job or fails it after the retry limit.

Execution IDs are monotonic fencing tokens. Lifecycle callbacks must present the
current token, so a delayed worker cannot commit after a job is reassigned.
Rejected stale callbacks return HTTP 409 and increment a Prometheus counter.
For databases created before fencing-token support, apply
`deploy/migrations/001_execution_id_fencing.sql` before upgrading the Control Plane.
