// Vejas — démo E2E enregistrée, côté expert métier.
//
// Ce script est à la fois un test de bout en bout du panel et le générateur
// de la vidéo destinée à un public métier : il prépare une intégration réelle
// (flow + trafic) par MCP, puis pilote le navigateur — corrections dans le
// panel, shadow-replay, promotion, effet immédiat — en enregistrant la vidéo.
//
// Prérequis : la stack tourne (docker compose up), state NATS neuf de
// préférence. Lancer :  node e2e/demo.mjs        (vidéo dans e2e/out/)
// Variables : VEJAS_URL (défaut http://localhost:8686),
//             INGEST_URL (défaut http://localhost:8787)

import { chromium } from 'playwright';
import { mkdirSync, renameSync, readdirSync, rmSync } from 'node:fs';
import assert from 'node:assert';

const BASE = process.env.VEJAS_URL || 'http://localhost:8686';
const INGEST = process.env.INGEST_URL || 'http://localhost:8787';
const OUT = new URL('./out/', import.meta.url).pathname;

// ─────────────────────────── le scénario métier ───────────────────────────
// Une intégration "commandes boutique -> ERP + alerte" écrite comme un agent
// l'écrit : règles métier en littéraux UPPERCASE, algorithmique en dessous.

const FLOW = `# flow: order_dispatch
source "vx.shop.orders"

# Règles métier — l'expert les corrige dans le panel.
WAREHOUSE_CODES = {"FR": "WH-PAR", "DE": "WH-MUC", "IT": "WH-MIL"}
DEFAULT_WAREHOUSE = "WH-INT"
ALERT_THRESHOLD_EUR = 500

total_eur = round(total_cents / 100, 2)
warehouse = WAREHOUSE_CODES[country] ?? DEFAULT_WAREHOUSE
positions = []
for l in lines:
  positions = positions + [{sku: l.sku, qty: num(l.qty)}]
end
emit "vx.erp.dispatch", {order: order_id, warehouse: warehouse, total_eur: total_eur, positions: positions}
if total_eur > ALERT_THRESHOLD_EUR:
  emit "vx.slack.out", {text: f"Grosse commande {order_id}: {total_eur} EUR -> {warehouse}"}
end
`;

const ORDERS = [
  { order_id: 'SO-1001', country: 'FR', total_cents: 34700, lines: [{ sku: 'A-12', qty: '2' }, { sku: 'B-07', qty: '1' }] },
  { order_id: 'SO-1002', country: 'ES', total_cents: 12000, lines: [{ sku: 'C-33', qty: '4' }] },
  { order_id: 'SO-1003', country: 'DE', total_cents: 89000, lines: [{ sku: 'A-12', qty: '10' }] },
];
const LATE_ORDER = { order_id: 'SO-1004', country: 'FR', total_cents: 26000, lines: [{ sku: 'D-02', qty: '3' }] };

const CAPTIONS = {
  intro: "Un agent a construit cette intégration. Voici l'écran de l'expert métier.",
  pipeline: 'Le pipeline et les derniers événements traités — dérivés du code réel.',
  rules: 'Les règles métier, extraites du code : tables de transcodage, seuils.',
  fix1: "L'entrepôt par défaut est faux. L'expert le corrige — sans une ligne de code.",
  replay1: 'Avant d’appliquer : la correction est rejouée sur les dernières commandes réelles.',
  promote1: 'Une commande changerait — exactement l’effet voulu. On promeut.',
  fix2: 'Deuxième correction : le seuil d’alerte passe de 500 à 200 €.',
  promote2: 'Rejouée aussi : la commande de 347 € aurait alerté. On promeut.',
  live: 'Une nouvelle commande arrive… la nouvelle règle s’applique immédiatement.',
  outro: 'L’agent écrit le code. Vous gardez ce que ça veut dire.  —  Vejas',
};

// ─────────────────────────── plateforme (MCP) ───────────────────────────

async function mcp(name, args) {
  const res = await fetch(`${BASE}/mcp`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }),
  });
  const j = await res.json();
  if (j.error) throw new Error(j.error.message);
  if (j.result?.isError) throw new Error(j.result.content?.[0]?.text || 'MCP error');
  return j.result?.content?.[0]?.text;
}

async function ingest(order) {
  const res = await fetch(`${INGEST}/ingest/shop.orders`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(order),
  });
  assert.equal(res.status, 202, `ingest ${order.order_id}`);
}

async function eventCount() {
  const res = await fetch(`${BASE}/events?flow=flow:order_dispatch`);
  return (await res.json()).events.length;
}

async function setup() {
  console.log('· setup: flow + fixture + trafic réel');
  await mcp('vejas_write_flow', { path: 'flows/order_dispatch.vjs', content: FLOW });
  await mcp('vejas_write_flow', { path: 'flows/fixtures/order_dispatch.json', content: JSON.stringify(ORDERS[2], null, 2) });
  await new Promise(r => setTimeout(r, 2500)); // le consumer durable démarre
  for (const o of ORDERS) await ingest(o);
  for (let i = 0; i < 30 && (await eventCount()) < ORDERS.length; i++) {
    await new Promise(r => setTimeout(r, 500));
  }
  const n = await eventCount();
  assert.ok(n >= ORDERS.length, `trace ring holds ${n}/${ORDERS.length} events`);
  console.log(`· setup ok (${n} events traités)`);
}

// ─────────────────────────── mise en scène ───────────────────────────

async function stage(page) {
  await page.addStyleTag({
    content: `
      #vjs-caption { position: fixed; left: 50%; bottom: 34px; transform: translateX(-50%);
        max-width: 78%; padding: 14px 26px; border-radius: 14px; z-index: 9999;
        background: rgba(12, 24, 28, .92); color: #fff; font: 600 22px/1.45 system-ui, sans-serif;
        text-align: center; opacity: 0; transition: opacity .45s; pointer-events: none;
        box-shadow: 0 10px 40px rgba(0,0,0,.35); }
      #vjs-cursor { position: fixed; width: 18px; height: 18px; border-radius: 50%;
        background: rgba(11, 114, 133, .9); border: 2.5px solid #fff; z-index: 9998;
        pointer-events: none; transition: left .55s ease, top .55s ease;
        box-shadow: 0 2px 10px rgba(0,0,0,.4); left: -40px; top: -40px; }`,
  });
  await page.evaluate(() => {
    const c = document.createElement('div'); c.id = 'vjs-caption'; document.body.appendChild(c);
    const k = document.createElement('div'); k.id = 'vjs-cursor'; document.body.appendChild(k);
  });
}

async function caption(page, text, holdMs = 3200) {
  await page.evaluate(t => {
    const c = document.getElementById('vjs-caption');
    c.textContent = t; c.style.opacity = '1';
  }, text);
  await page.waitForTimeout(holdMs);
}
async function captionOff(page) {
  await page.evaluate(() => { document.getElementById('vjs-caption').style.opacity = '0'; });
  await page.waitForTimeout(500);
}

async function glide(page, locator) {
  await locator.scrollIntoViewIfNeeded();
  await page.waitForTimeout(400);
  const box = await locator.boundingBox();
  if (box) {
    await page.evaluate(([x, y]) => {
      const k = document.getElementById('vjs-cursor');
      k.style.left = `${x - 9}px`; k.style.top = `${y - 9}px`;
    }, [box.x + box.width / 2, box.y + box.height / 2]);
    await page.waitForTimeout(650);
  }
}
async function click(page, locator) {
  await glide(page, locator);
  await locator.click();
  await page.waitForTimeout(350);
}

async function correctConstant(page, card, label, newValue, capFix, capReplay, capPromote) {
  const box = card.locator('.const', { hasText: label });
  const input = box.locator('input.rule');
  await caption(page, capFix, 2600);
  await glide(page, input);
  await input.click();
  await input.fill('');
  await input.pressSequentially(String(newValue), { delay: 90 });
  await page.waitForTimeout(500);
  await click(page, box.locator('button.apply'));

  const strip = card.locator('.replay');
  await strip.waitFor({ state: 'visible', timeout: 15000 });
  const stripText = await strip.textContent();
  assert.ok(/would change/.test(stripText), `replay strip shows a diff for ${label}`);
  assert.ok(!/(^|[^0-9])0(<\/b>)? would change/.test(stripText), `at least one event changes for ${label}`);
  await caption(page, capReplay, 3600);
  await captionOff(page);
  await caption(page, capPromote, 2400);
  await click(page, strip.locator('button', { hasText: 'Promote' }));
  await page.waitForSelector('#toast.show', { timeout: 10000 });
  await page.waitForTimeout(1200);
  await captionOff(page);
}

// ─────────────────────────── le film ───────────────────────────

async function main() {
  await setup();
  rmSync(OUT, { recursive: true, force: true });
  mkdirSync(OUT, { recursive: true });
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    recordVideo: { dir: OUT, size: { width: 1440, height: 900 } },
    deviceScaleFactor: 2,
  });
  const page = await context.newPage();

  console.log('· scène 1 : panel + pipeline + événements');
  await page.goto(BASE, { waitUntil: 'networkidle' });
  await stage(page);
  await caption(page, CAPTIONS.intro, 4200);
  await captionOff(page);
  await caption(page, CAPTIONS.pipeline, 1200);
  const events = page.locator('#events table');
  await events.waitFor({ state: 'visible', timeout: 10000 });
  await glide(page, events);
  await page.waitForTimeout(2200);
  await captionOff(page);

  console.log('· scène 2 : la carte du flow, règles métier');
  const flowCard = () => page.locator('.card', { has: page.locator('h2', { hasText: 'order_dispatch' }) });
  const card = flowCard();
  await card.scrollIntoViewIfNeeded();
  await caption(page, CAPTIONS.rules, 3000);
  await glide(page, card.locator('.sect', { hasText: 'transcoding table' }));
  await page.waitForTimeout(1200);
  await captionOff(page);

  console.log('· scène 3 : correction n°1 — entrepôt par défaut, shadow-replay, promote');
  await correctConstant(page, card, 'DEFAULT_WAREHOUSE', 'WH-EXP',
    CAPTIONS.fix1, CAPTIONS.replay1, CAPTIONS.promote1);

  console.log('· scène 4 : correction n°2 — seuil d’alerte, shadow-replay, promote');
  const card2 = flowCard();
  await card2.scrollIntoViewIfNeeded();
  await correctConstant(page, card2, 'ALERT_THRESHOLD_EUR', 200,
    CAPTIONS.fix2, CAPTIONS.replay1, CAPTIONS.promote2);

  console.log('· scène 5 : une nouvelle commande vit la nouvelle règle');
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'smooth' }));
  await page.waitForTimeout(900);
  await caption(page, CAPTIONS.live, 1500);
  await ingest(LATE_ORDER);
  await page.locator('#events td[title*="SO-1004"]').first()
    .waitFor({ state: 'visible', timeout: 15000 });
  await glide(page, page.locator('#events td[title*="SO-1004"]').first());
  await page.waitForTimeout(2600);
  await captionOff(page);

  console.log('· vérifications finales (E2E)');
  const file = await (await fetch(`${BASE}/file?path=flows/order_dispatch.vjs`)).json();
  assert.ok(file.content.includes('DEFAULT_WAREHOUSE = "WH-EXP"'), 'promotion n°1 écrite');
  assert.ok(file.content.includes('ALERT_THRESHOLD_EUR = 200'), 'promotion n°2 écrite');
  const late = await (await fetch(`${BASE}/events?flow=flow:order_dispatch`)).json();
  const so4 = late.events.find(e => (e.preview || '').includes('SO-1004'));
  assert.ok(so4 && so4.emits.includes('vx.slack.out'), 'SO-1004 (260 €) alerte avec le seuil corrigé');

  await caption(page, CAPTIONS.outro, 5000);
  await context.close(); // flush la vidéo
  await browser.close();

  const webm = readdirSync(OUT).find(f => f.endsWith('.webm'));
  const target = `${OUT}vejas-demo-metier.webm`;
  if (webm && `${OUT}${webm}` !== target) renameSync(`${OUT}${webm}`, target);
  console.log(`✓ démo E2E verte — vidéo : ${target}`);
}

main().catch(e => { console.error('✗', e.message); process.exit(1); });
