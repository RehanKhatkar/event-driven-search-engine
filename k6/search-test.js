import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';

export const options = {
    stages: [
        { duration: '30s', target: 200 },
        { duration: '2m', target: 500 },
        { duration: '30s', target: 0 },
    ],
};
const searchTerms = [
    'wireless', 'bluetooth', 'monitor', 'laptop', 'keyboard',
'mouse', 'cable', 'gaming', 'desk', 'chair', 'usb', 'smart'
];
function getRandomTerm() {
    return searchTerms[Math.floor(Math.random() * searchTerms.length)];
}
export default function () {
    const term = getRandomTerm();
    const res = http.get(`http://localhost/api/products/search?q=${term}`);
    check(res, {
        'status is 200': (r) => r.status === 200,
        'response time < 100ms': (r) => r.timings.duration < 100,
    });
}
