// The counting sink: receives the flow's output (via the http-out driver),
// counts deliveries and measures end-to-end latency from the `t` stamp the
// load generator put in each event. GET /stats returns the tally.
import { createServer } from 'node:http';

let n = 0;
let lat = [];        // ms, end-to-end (loadgen stamp -> sink arrival)
const t0 = Date.now();

const srv = createServer((req, res) => {
  if (req.method === 'POST') {
    let body = '';
    req.on('data', c => (body += c));
    req.on('end', () => {
      n++;
      try {
        const t = JSON.parse(body).t;
        if (typeof t === 'number' && t > 0) lat.push(Date.now() - t);
      } catch {}
      res.writeHead(200); res.end('ok');
    });
    return;
  }
  if (req.url === '/stats') {
    lat.sort((a, b) => a - b);
    const q = p => (lat.length ? lat[Math.min(lat.length - 1, Math.floor(p * lat.length))] : null);
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      delivered: n,
      elapsed_s: (Date.now() - t0) / 1000,
      latency_ms: { p50: q(0.50), p90: q(0.90), p99: q(0.99), max: lat[lat.length - 1] ?? null, samples: lat.length },
    }));
    return;
  }
  if (req.url === '/reset') { n = 0; lat = []; res.writeHead(200); res.end('ok'); return; }
  res.writeHead(404); res.end();
});
srv.listen(9099, '127.0.0.1', () => console.log('sink on :9099'));
