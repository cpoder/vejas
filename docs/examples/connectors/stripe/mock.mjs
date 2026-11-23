// Admission mock for stripe connector (http-poll driver)
import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9200);
let count = 0;

const EVENTS_RESPONSE = {
  "object": "list",
  "data": [
    {
      "id": "evt_1QRmK2LkdIwHu7ixZvPGmuGU",
      "object": "event",
      "api_version": "2023-10-16",
      "created": 1732368730,
      "data": {
        "object": {
          "id": "pi_3QRmK1LkdIwHu7ix0B8tBfzF",
          "object": "payment_intent",
          "amount": 2000,
          "amount_received": 2000,
          "currency": "usd",
          "customer": "cus_RQvWzKV4cOOYYY",
          "description": "Payment for order #12345",
          "status": "succeeded",
          "metadata": {
            "order_id": "12345",
            "customer_email": "customer@example.com"
          }
        }
      },
      "livemode": false,
      "type": "payment_intent.succeeded"
    }
  ],
  "has_more": false,
  "url": "/v1/events"
};

createServer((req, res) => {
  if (req.url === '/__count') {
    res.writeHead(200);
    res.end(String(count));
    return;
  }
  
  let body = '';
  req.on('data', c => body += c);
  req.on('end', () => {
    count++;
    res.writeHead(200, {'content-type': 'application/json'});
    res.end(JSON.stringify(EVENTS_RESPONSE));
  });
}).listen(port, '127.0.0.1', () => console.log('READY', port));
