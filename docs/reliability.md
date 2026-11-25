# Reliability

Jobs are represented as an append-only event stream with a queryable
`jobs_projection`. Creation and state transitions append their event and update
the projection in the same MySQL transaction. Repeated idempotency keys return
the original job. Workers use a durable RabbitMQ queue, manual acknowledgements,
prefetch, and a fixed-size execution pool. A worker claims a queued job before
execution; duplicate deliveries of already-claimed jobs are acknowledged
without running again.

Worker registration and periodic heartbeats are recorded in MySQL. Each command
has a configurable timeout, and success, nonzero exit, timeout, and output are
reported as job lifecycle state. A failed control-plane report causes delivery
requeue rather than a silent acknowledgement. Lease-based recovery of jobs
orphaned by worker termination is not yet implemented.
