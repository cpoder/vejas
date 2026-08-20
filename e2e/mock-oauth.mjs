// Mock OAuth2 + REST + ingestion server — validates the oauth-poll driver and
// the authenticated http-out sink without real credentials. Zero dependencies.
//
//   POST /token      client-credentials → rotating bearer (only latest valid)
//   GET  /v1/users   paginated (@odata.nextLink), Bearer required
//   GET  /v1/secureScores  single page, Bearer required
//   POST /rotate     invalidates the current bearer (next GET → 401)
//   POST /evidence   ingestion API stand-in — requires Bearer rgz_test,
//                    expects {raw, facts[]}, answers {accepted, deduplicated}
//   GET  /received   what /evidence has accepted so far (assertions)
//
// node e2e/mock-oauth.mjs   (PORT=9099 by default)

import { createServer } from 'node:http';

const PORT = Number(process.env.PORT || 9099);
let tokenCounter = 0;
let currentToken = null;
const received = [];

const json = (res, code, obj) => {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(body);
};
const readBody = req => new Promise(resolve => {
  let data = '';
  req.on('data', c => (data += c));
  req.on('end', () => resolve(data));
});
const bearerOk = req => currentToken !== null &&
  req.headers.authorization === `Bearer ${currentToken}`;

createServer(async (req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${PORT}`);
  const route = `${req.method} ${url.pathname}`;

  if (route === 'POST /token') {
    const p = new URLSearchParams(await readBody(req));
    if (p.get('grant_type') !== 'client_credentials' ||
        p.get('client_id') !== 'mock-client' ||
        p.get('client_secret') !== 'mock-secret' ||
        !p.get('scope')) {
      console.log('token: REJECTED', Object.fromEntries(p));
      return json(res, 400, { error: 'invalid_client' });
    }
    currentToken = `tok-${++tokenCounter}`;
    console.log(`token: issued ${currentToken}`);
    return json(res, 200, { access_token: currentToken, token_type: 'Bearer', expires_in: 3600 });
  }

  if (route === 'POST /rotate') {
    console.log(`rotate: ${currentToken} invalidated`);
    currentToken = null;
    return json(res, 200, { rotated: true });
  }

  if (url.pathname.startsWith('/v1/')) {
    if (!bearerOk(req)) return json(res, 401, { error: 'invalid_token' });
    if (url.pathname === '/v1/users') {
      const page = url.searchParams.get('page') || '1';
      if (page === '1') {
        return json(res, 200, {
          value: [
            { id: 'u1', displayName: 'Ana', mfa: true },
            { id: 'u2', displayName: 'Bob', mfa: false },
          ],
          '@odata.nextLink': `http://127.0.0.1:${PORT}/v1/users?page=2`,
        });
      }
      return json(res, 200, { value: [{ id: 'u3', displayName: 'Carla', mfa: true }] });
    }
    if (url.pathname === '/v1/secureScores') {
      return json(res, 200, { value: [{ currentScore: 51, maxScore: 100 }] });
    }
    return json(res, 404, { error: 'unknown endpoint' });
  }

  if (route === 'POST /evidence') {
    if (req.headers.authorization !== 'Bearer rgz_test') {
      console.log('evidence: 401 (bad ingestion token)');
      return json(res, 401, { error: 'unauthorized' });
    }
    let batch;
    try { batch = JSON.parse(await readBody(req)); }
    catch { return json(res, 400, { error: 'invalid JSON' }); }
    const facts = Array.isArray(batch.facts) ? batch.facts : [];
    received.push({ endpoint: batch.raw?.endpoint ?? null, facts: facts.length,
      keys: facts.map(f => f.idempotency_key) });
    console.log(`evidence: accepted ${facts.length} fact(s) from ${batch.raw?.endpoint}`);
    return json(res, 200, { accepted: facts.length, deduplicated: 0 });
  }

  if (route === 'GET /received') return json(res, 200, { batches: received });
  json(res, 404, { error: 'not found' });
}).listen(PORT, process.env.HOST || '127.0.0.1', () =>
  console.log(`mock oauth+rest+ingestion on http://${process.env.HOST || '127.0.0.1'}:${PORT}`));
