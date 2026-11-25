# AtlasFarm Architecture

AtlasFarm is a distributed, resource-aware job execution platform. The
control plane exposes the job API and owns durable job state; the scheduler
assigns work using tenant fairness and worker capacity; worker agents execute
leased jobs and report their status.

The local infrastructure baseline uses MySQL for event and projection data,
RabbitMQ for brokered work, Prometheus and Grafana for metrics, Loki for logs,
and Jaeger for traces. Detailed service contracts and operational flows remain
to be defined.
