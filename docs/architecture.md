# AtlasFarm Architecture

AtlasFarm is a distributed, resource-aware job execution platform. The
control plane exposes the job API and owns durable job state; the scheduler
assigns work using tenant fairness and worker capacity; worker agents consume
RabbitMQ jobs with bounded concurrency and report their status.

The local infrastructure baseline uses MySQL for event and projection data,
RabbitMQ for brokered work, Prometheus and Grafana for metrics, Loki for logs,
and Jaeger for traces. The control plane writes job projections and ordered
events in one MySQL transaction. Idempotency keys protect submission retries,
and a transactional queued-to-running claim prevents duplicate broker messages
from starting the same job twice.

The REST API is described in `api/openapi.yaml`. The control plane defaults to
`:8080`, MySQL at `127.0.0.1:3306`, RabbitMQ at `127.0.0.1:25672`, and a durable
`jobs` queue; each endpoint can be configured through environment variables.
