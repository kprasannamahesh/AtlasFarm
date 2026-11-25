import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    health_check: {
      executor: 'constant-vus',
      vus: 10000,
      duration: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
  },
};

const baseURL = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  const response = http.get(`${baseURL}/healthz`);
  check(response, { 'health endpoint returns 200': (res) => res.status === 200 });
}
