import http from 'k6/http';
import { check, sleep } from 'k6';
export const options = {
    stages: [
        { duration: '30s', target: 20 },
        { duration: '1m', target: 50 },
        { duration: '30s', target: 0 },
    ],
};
const searchTerms = ['Wireless', 'Pro', 'Ergonomic', 'Smart', 'Leather', 'Portable', 'Books'];
export default function () {
    const term = searchTerms[Math.floor(Math.random() * searchTerms.length)];
    const res = http.get(`http://localhost:8080/api/products/search?q=${term}`);
    check(res, {
        'search status is 200': (r) => r.status === 200,
          'search latency < 200ms': (r) => r.timings.duration < 200,
    });
    sleep(0.1);
}
