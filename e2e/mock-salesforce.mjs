// Mock Salesforce — OAuth2 token + Bulk API 2.0 query job, zero deps.
// Validates the vejas-salesforce connector (OAuth -> create job -> poll ->
// paginated CSV results via Sforce-Locator) without a real org
// (contrainte Cyril : dev mock-first ; la forme se fige au premier vrai org).
//
//   POST /services/oauth2/token                         -> {access_token, instance_url}
//   POST /services/data/v60.0/jobs/query                -> {id, state}
//   GET  /services/data/v60.0/jobs/query/:id            -> {state: "JobComplete"}
//   GET  /services/data/v60.0/jobs/query/:id/results    -> CSV page + Sforce-Locator
//
// node e2e/mock-salesforce.mjs   (PORT=9099, HOST=127.0.0.1 by default)

import { createServer } from 'node:http';

const PORT = Number(process.env.PORT || 9099);
const HOST = process.env.HOST || '127.0.0.1';
const TOTAL = Number(process.env.TOTAL || 250);

// Rows to export. One name has a comma + quote to exercise CSV quoting.
const rows = Array.from({ length: TOTAL }, (_, i) => ({
  Id: `003${String(i).padStart(15, '0')}`,
  Name: i === 3 ? 'Doe, "Jack" Jr' : `Contact ${i}`,
  Email: `c${i}@example.com`,
}));

const csvField = (v) => (/[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v);
const toCsv = (slice) => {
  const head = 'Id,Name,Email';
  const body = slice.map((r) => [r.Id, r.Name, r.Email].map(csvField).join(',')).join('\n');
  return head + '\n' + body + '\n';
};

const json = (res, code, obj) => {
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
};
const readBody = (req) => new Promise((r) => { let d = ''; req.on('data', (c) => (d += c)); req.on('end', () => r(d)); });

createServer(async (req, res) => {
  const url = new URL(req.url, `http://${HOST}:${PORT}`);
  const p = url.pathname;

  if (req.method === 'POST' && p === '/services/oauth2/token') {
    await readBody(req);
    return json(res, 200, { access_token: 'mock-token-123', instance_url: `http://${HOST}:${PORT}`, token_type: 'Bearer' });
  }
  const authed = req.headers.authorization === 'Bearer mock-token-123';
  if (req.method === 'POST' && p === '/services/data/v60.0/jobs/query') {
    if (!authed) return json(res, 401, { error: 'unauthorized' });
    await readBody(req);
    return json(res, 200, { id: 'job1', state: 'UploadComplete', operation: 'query' });
  }
  if (req.method === 'GET' && p === '/services/data/v60.0/jobs/query/job1') {
    if (!authed) return json(res, 401, { error: 'unauthorized' });
    return json(res, 200, { id: 'job1', state: 'JobComplete', numberRecordsProcessed: TOTAL });
  }
  if (req.method === 'GET' && p === '/services/data/v60.0/jobs/query/job1/results') {
    if (!authed) return json(res, 401, { error: 'unauthorized' });
    const max = Number(url.searchParams.get('maxRecords') || 10000);
    const offset = Number(url.searchParams.get('locator') || 0);
    const slice = rows.slice(offset, offset + max);
    const next = offset + slice.length;
    const locator = next < rows.length ? String(next) : 'null';
    res.writeHead(200, { 'content-type': 'text/csv', 'Sforce-Locator': locator, 'Sforce-NumberOfRecords': String(slice.length) });
    return res.end(toCsv(slice));
  }
  json(res, 404, { error: 'not found', path: p });
}).listen(PORT, HOST, () => console.log(`mock Salesforce Bulk 2.0 on http://${HOST}:${PORT} (${TOTAL} rows)`));
