# Reliability

Jobs are represented as an append-only event stream with a queryable
`jobs_projection`. Worker leases and heartbeats provide the basis for
recovering work after failures. Retry policy, lease renewal, and rollout
recovery behavior remain to be specified.
