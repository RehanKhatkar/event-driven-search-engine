import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    stages: [
        { duration: '30s', target: 50 },
        { duration: '2m', target: 100 },
        { duration: '30s', target: 0 },
    ],
};
function getRandomProductId() {
    if (Math.random() < 0.8) {
        return Math.floor(Math.random() * 1000) + 1;
    }
    return Math.floor(Math.random() * 49000) + 1001;
}
export default function () {
    const id = getRandomProductId();
    const res = http.get(`http://localhost:8080/api/products/${id}`);
    check(res, {
        'status is 200': (r) => r.status === 200,
          'response time < 50ms': (r) => r.timings.duration < 50,
    });
    sleep(0.05);
}
