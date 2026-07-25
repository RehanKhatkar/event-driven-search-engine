import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
const productIds = new SharedArray('product ids', function () {
    return JSON.parse(open('./ids.json'));
});
export const options = {
    stages: [
        { duration: '30s', target: 200 },
        { duration: '2m', target: 500 },
        { duration: '30s', target: 0 },
    ],
};
function getRandomProductId() {
    const totalIds = productIds.length;
    const hotPoolSize = Math.floor(totalIds * 0.2);
    let selectedIndex;
    if (Math.random() < 0.8) {
        selectedIndex = Math.floor(Math.random() * hotPoolSize);
    }
    else {
        selectedIndex = Math.floor(Math.random() * (totalIds - hotPoolSize)) + hotPoolSize;
    }
    return productIds[selectedIndex];
}
export default function () {
    const id = getRandomProductId();
    const res = http.get(`http://localhost/api/products/${id}`);
    check(res, {
        'status is 200': (r) => r.status === 200,
          'response time < 50ms': (r) => r.timings.duration < 50,
    });
    //sleep(0.05);
}
