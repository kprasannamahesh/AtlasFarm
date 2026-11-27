import http from 'k6/http';
import { check } from 'k6';
import execution from 'k6/execution';
import { Counter, Rate, Trend } from 'k6/metrics';

const baseURL = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/+$/, '');
const jobsSubmitted = new Counter('jobs_submitted');
const jobsAccepted = new Counter('jobs_accepted');
const submissionSuccess = new Rate('job_submission_success');
const submissionLatency = new Trend('job_submission_latency_ms', true);
let printedFailureSample = false;
const maxP99Ms = Number(__ENV.MAX_P99_MS || 25);

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(95)', 'p(99)'],
  scenarios: {
    submit_10k_jobs: {
      executor: 'shared-iterations',
      vus: 32,
      iterations: 10000,
      maxDuration: '5m',
    },
  },
  thresholds: {
    checks: ['rate==1'],
    dropped_iterations: ['count==0'],
    job_submission_success: ['rate==1'],
    job_submission_latency_ms: [`p(99)<${maxP99Ms}`],
    http_req_failed: ['rate==0'],
  },
};

function tenantFor(index) {
  const bucket = index % 10;
  if (bucket < 4) return 'tenant-a';
  if (bucket < 7) return 'tenant-b';
  return 'tenant-c';
}

export default function () {
  const iteration = execution.scenario.iterationInTest;
  const payload = JSON.stringify({
    tenant_id: tenantFor(iteration),
    idempotency_key: `k6-${__VU}-${iteration}`,
    command: 'echo ok',
    req_cpu: 1,
    req_memory_mb: 64,
    req_gpu: 0,
  });
  const response = http.post(`${baseURL}/api/v1/jobs`, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '10s',
  });
  const accepted = response.status === 201;
  if (!accepted && execution.vu.idInTest <= 5 && !printedFailureSample) {
    const body = String(response.body || '').slice(0, 400).replace(/\s+/g, ' ');
    console.log(`job failure sample: HTTP ${response.status} ${body}`);
    printedFailureSample = true;
  }
  jobsSubmitted.add(1);
  if (accepted) jobsAccepted.add(1);
  submissionSuccess.add(accepted);
  submissionLatency.add(response.timings.duration);
  check(response, {
    'job accepted with HTTP 201': (res) => res.status === 201,
    'response includes a job ID': (res) => {
      try {
        return Boolean(res.json('id'));
      } catch (_) {
        return false;
      }
    },
  });
}
