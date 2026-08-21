// Mock EDR — CrowdStrike Falcon + SentinelOne, zéro dépendance.
// Valide les templates edr-crowdstrike / edr-sentinelone sans compte réel
// (contrainte Cyril : dev mock-first ; la forme se fige au premier vrai compte).
//
//   CrowdStrike (oauth2 client-credentials SANS scope) :
//     POST /oauth2/token                    → bearer (client cs-mock-*)
//     GET  /devices/queries/devices/v1      → {resources:[ids]}  (Bearer)
//     GET  /devices/entities/devices/v2?ids= → {resources:[{status}]} (Bearer)
//   SentinelOne (token statique ApiToken) :
//     GET  /web/api/v2.1/agents             → {data:[agents]} (ApiToken)
//
// node e2e/mock-edr.mjs   (PORT=9098, HOST=127.0.0.1 par défaut)

import { createServer } from 'node:http';

const PORT = Number(process.env.PORT || 9098);
const HOST = process.env.HOST || '127.0.0.1';

// 4 hôtes CrowdStrike, 2 dégradés → edr_agents_healthy 2/4
const CS_HOSTS = {
  h1: { device_id: 'h1', hostname: 'PC-1', status: 'normal' },
  h2: { device_id: 'h2', hostname: 'PC-2', status: 'normal' },
  h3: { device_id: 'h3', hostname: 'PC-3', status: 'reduced_functionality_mode' },
  h4: { device_id: 'h4', hostname: 'PC-4', status: 'normal' },
};
// 4 agents SentinelOne, 1 inactif + 1 infecté → 2/4 sains
const S1_AGENTS = [
  { id: 's1', computerName: 'LAP-1', isActive: true, infected: false },
  { id: 's2', computerName: 'LAP-2', isActive: false, infected: false },
  { id: 's3', computerName: 'LAP-3', isActive: true, infected: true },
  { id: 's4', computerName: 'LAP-4', isActive: true, infected: false },
];

let csToken = null;
const json = (res, code, obj) => {
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
};
const readBody = req => new Promise(r => { let d = ''; req.on('data', c => (d += c)); req.on('end', () => r(d)); });

createServer(async (req, res) => {
  const url = new URL(req.url, `http://${HOST}:${PORT}`);
  const route = `${req.method} ${url.pathname}`;

  // ── CrowdStrike ──
  if (route === 'POST /oauth2/token') {
    const p = new URLSearchParams(await readBody(req));
    if (p.get('grant_type') !== 'client_credentials'
        || p.get('client_id') !== 'cs-mock-client'
        || p.get('client_secret') !== 'cs-mock-secret') {
      return json(res, 400, { error: 'access_denied' });
    }
    if (p.get('scope')) { console.log('CS: scope présent (le driver ne devrait PAS en envoyer)'); }
    csToken = `cs-tok-${Date.now()}`;
    return json(res, 201, { access_token: csToken, expires_in: 1799, token_type: 'bearer' });
  }
  const csAuthed = () => csToken && req.headers.authorization === `Bearer ${csToken}`;
  if (url.pathname === '/devices/queries/devices/v1') {
    if (!csAuthed()) return json(res, 401, { errors: [{ message: 'invalid token' }] });
    return json(res, 200, { resources: Object.keys(CS_HOSTS), meta: { pagination: { total: 4 } } });
  }
  if (url.pathname === '/devices/entities/devices/v2') {
    if (!csAuthed()) return json(res, 401, { errors: [{ message: 'invalid token' }] });
    const ids = url.searchParams.getAll('ids');
    return json(res, 200, { resources: ids.map(id => CS_HOSTS[id]).filter(Boolean) });
  }

  // ── SentinelOne ──
  if (url.pathname === '/web/api/v2.1/agents') {
    if (req.headers.authorization !== 'ApiToken s1-mock-token') {
      return json(res, 401, { errors: [{ detail: 'Authentication required' }] });
    }
    return json(res, 200, { data: S1_AGENTS, pagination: { totalItems: S1_AGENTS.length, nextCursor: null } });
  }

  json(res, 404, { error: 'not found' });
}).listen(PORT, HOST, () => console.log(`mock EDR (CrowdStrike+SentinelOne) on http://${HOST}:${PORT}`));
