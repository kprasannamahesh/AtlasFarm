# Scheduler

The scheduler is responsible for matching job resource requests to available
worker capacity while respecting per-tenant weights and quotas. Its baseline
package is in `scheduler/`; packing policy, fairness, and cluster-state
behavior are intentionally left for implementation.
