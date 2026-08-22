// Admission mock for servicenow_incidents_poll — a minimal local stand-in for the remote API.
// Usage: node mock.mjs <port>. GET /__count returns how many real calls landed.
import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9200);
let count = 0;
const GET_BODY = {"result": [{"number": "INC0010023", "short_description": "Email gateway unreachable", "priority": "1", "state": "2", "sys_created_on": "2026-08-22 10:00:00"}]};
const POST_BODY = null;
createServer((req, res) => {
  if (req.url === '/__count') { res.writeHead(200); res.end(String(count)); return; }
  let b = ''; req.on('data', c => b += c);
  req.on('end', () => {
    count++;
    const body = req.method === 'POST' ? POST_BODY : GET_BODY;
    if (body === null) { res.writeHead(405); res.end(); return; }
    res.writeHead(req.method === 'POST' ? 201 : 200, {'content-type':'application/json'});
    res.end(JSON.stringify(body));
  });
}).listen(port, '127.0.0.1', () => console.log('READY', port));
