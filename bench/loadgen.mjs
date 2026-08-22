// The load generator: POSTs realistic order events to the webhook entry
// (http-in) over keep-alive connections for a fixed duration, stamping each
// event with t=now for the sink's end-to-end latency measure.
//   node loadgen.mjs <seconds> <concurrency> [url]
import { request, Agent } from 'node:http';

const [seconds = '30', conc = '32', url = 'http://127.0.0.1:8790/ingest/bench.orders'] = process.argv.slice(2);
const u = new URL(url);
const agent = new Agent({ keepAlive: true, maxSockets: Number(conc) });
const deadline = Date.now() + Number(seconds) * 1000;
let sent = 0, accepted = 0, errors = 0;

function event() {
  return JSON.stringify({
    id: `SO#${1000 + (sent % 100000)}`,
    email: 'Jane.Doe@ACME.com',
    total_price: '347.00',
    t: Date.now(),
    shipping_address: { country: ['France', 'Germany', 'Italy', 'Spain'][sent % 4] },
    line_items: [
      { sku: 'A-12', quantity: '2', unit_price_cents: 9900 },
      { sku: 'B-07', quantity: '1', unit_price_cents: 14900 },
      { sku: 'C-33', quantity: '4', unit_price_cents: 450 },
    ],
  });
}

function worker() {
  return new Promise(resolve => {
    (function loop() {
      if (Date.now() >= deadline) return resolve();
      const body = event();
      sent++;
      const req = request({
        hostname: u.hostname, port: u.port, path: u.pathname, method: 'POST',
        agent, headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) },
      }, res => { res.resume(); if (res.statusCode === 202 || res.statusCode === 200) accepted++; else errors++; loop(); });
      req.on('error', () => { errors++; setTimeout(loop, 50); });
      req.end(body);
    })();
  });
}

const t0 = Date.now();
await Promise.all(Array.from({ length: Number(conc) }, worker));
const dt = (Date.now() - t0) / 1000;
console.log(JSON.stringify({ sent, accepted, errors, seconds: dt, ingest_rate: Math.round(accepted / dt) }));
