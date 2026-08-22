// Vejas — the public Act-1 film: SAP ⇄ Salesforce, directed.
//
// Same machinery as e2e/demo.mjs (captions, glide cursor, in-panel correction,
// E2E assertions), pointed at the real bridge on the SAP box. Every second on
// screen either shows an action or carries a subtitle — no dead air, no raw
// JSON, no scrolling text.
//
// Run (laptop side), with the tunnel up:
//   ssh -N -L 8686:127.0.0.1:8686 cpo@<box> &
//   node bridge-film.mjs                    (video in ./film-out/)
// Env: VEJAS_URL (default http://localhost:8686), BOX (required: user@sap-host)

import { chromium } from 'playwright';
import { mkdirSync, renameSync, readdirSync, rmSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import assert from 'node:assert';

const BASE = process.env.VEJAS_URL || 'http://localhost:8686';
const BOX = process.env.BOX || (() => { throw new Error('set BOX=user@sap-host'); })();
const OUT = new URL('./film-out/', import.meta.url).pathname;
const FLOW_FILE = 'flows/sap_idoc_to_sf.vjs';

const CAPTIONS = {
  intro: 'SAP on one side. Salesforce on the other. Two flows an agent wrote — one Rust binary, no JVM, no builder.',
  pipeline: 'The pipeline — derived from the code itself.',
  idoc: 'A real IDoc leaves SAP: ten accounts, streamed onto the bus…',
  arrived: '…bulk-inserted into Salesforce. Every step traced.',
  rules: "The business rules, extracted from the code. This is the expert's screen.",
  fix: 'One label is wrong. The expert fixes the meaning — no code, no redeploy.',
  replay: 'Before it lands, the change is replayed on the real events just processed.',
  promote: 'The diff is exactly the intent. Promote.',
  live: 'The next IDoc lives the corrected rule.',
  outro: 'The agent writes the code. You keep what it means.   —   vejas.dev · Apache-2.0',
};

// ─────────────────────── box-side actions ───────────────────────

function ssh(cmd) {
  return execFileSync('ssh', ['-o', 'BatchMode=yes', BOX, cmd], { encoding: 'utf8' });
}
function triggerIdoc() { ssh('cd ~/bridge-demo && ./trigger-idoc.sh >/dev/null 2>&1 || true'); }

async function events() {
  const res = await fetch(`${BASE}/events?flow=flow:sap_idoc_to_sf`);
  return (await res.json()).events;
}
async function waitForEvents(min, timeoutMs = 40_000) {
  const t0 = Date.now();
  for (;;) {
    const ev = await events();
    if (ev.length >= min) return ev;
    if (Date.now() - t0 > timeoutMs) throw new Error(`only ${ev.length}/${min} events`);
    await new Promise(r => setTimeout(r, 800));
  }
}

async function setup() {
  console.log('· setup: literal reset + stack sanity');
  ssh(`sed -i 's/DEFAULT_INDUSTRY = "SAP-sourced"/DEFAULT_INDUSTRY = "From SAP IDoc"/' ~/bridge-demo/bridge-demo-root/${FLOW_FILE}`);
  const health = await fetch(`${BASE}/healthz`);
  assert.equal(health.status, 200, 'runtime up (tunnel + box)');
  // the runtime reloads the flow on mtime change; give it a beat
  await new Promise(r => setTimeout(r, 2500));
}

// ─────────────────────── staging (from demo.mjs) ───────────────────────

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

// ─────────────────────── the film ───────────────────────

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

  console.log('· scene 1: title over the panel, then the pipeline');
  await page.goto(BASE, { waitUntil: 'networkidle' });
  await stage(page);
  await caption(page, CAPTIONS.intro, 4600);
  await captionOff(page);
  const graph = page.locator('#graph, svg').first();
  await graph.scrollIntoViewIfNeeded();
  await caption(page, CAPTIONS.pipeline, 2800);
  await captionOff(page);

  console.log('· scene 2: a real IDoc arrives on camera');
  const before = (await events()).length;
  await page.locator('#events table').scrollIntoViewIfNeeded();
  await caption(page, CAPTIONS.idoc, 1400);
  triggerIdoc();
  await waitForEvents(before + 1);
  const row = page.locator('#events tbody tr, #events tr').nth(1);
  await glide(page, row);
  await caption(page, CAPTIONS.arrived, 3400);
  await captionOff(page);

  console.log('· scene 3: the expert corrects the meaning — replay — promote');
  const card = page.locator('.card', { has: page.locator('h2', { hasText: 'sap_idoc_to_sf' }) });
  await card.scrollIntoViewIfNeeded();
  await caption(page, CAPTIONS.rules, 3200);
  await captionOff(page);
  const box = card.locator('.const', { hasText: 'DEFAULT_INDUSTRY' });
  const input = box.locator('input.rule');
  await caption(page, CAPTIONS.fix, 2600);
  await glide(page, input);
  await input.click();
  await input.fill('');
  await input.pressSequentially('SAP-sourced', { delay: 90 });
  await page.waitForTimeout(500);
  await click(page, box.locator('button.apply'));
  const strip = card.locator('.replay');
  await strip.waitFor({ state: 'visible', timeout: 15000 });
  assert.ok(/would change/.test(await strip.textContent()), 'replay strip shows a diff');
  await caption(page, CAPTIONS.replay, 3600);
  await captionOff(page);
  await caption(page, CAPTIONS.promote, 2200);
  await click(page, strip.locator('button', { hasText: 'Promote' }));
  await page.waitForSelector('#toast.show', { timeout: 10000 });
  await page.waitForTimeout(1000);
  await captionOff(page);

  console.log('· scene 4: the next IDoc lives the corrected rule');
  await page.locator('#events table').scrollIntoViewIfNeeded();
  await caption(page, CAPTIONS.live, 1400);
  const n2 = (await events()).length;
  triggerIdoc();
  await waitForEvents(n2 + 1);
  const fresh = page.locator('#events tbody tr, #events tr').nth(1);
  await glide(page, fresh);
  // open its payload: the corrected label, on camera
  try { await fresh.locator('td.payload summary').click({ timeout: 3000 }); } catch {}
  await page.waitForTimeout(3200);
  await captionOff(page);

  console.log('· E2E assertions');
  const file = await (await fetch(`${BASE}/file?path=${FLOW_FILE}`)).json();
  assert.ok(file.content.includes('DEFAULT_INDUSTRY = "SAP-sourced"'), 'promotion written to the file');
  // the corrected label lives in the EMITTED payload — visible on the sink's ring
  const sink = await (await fetch(`${BASE}/events?flow=connector:sf_ingest`)).json();
  assert.ok(sink.events.some(e => (e.preview || '').includes('SAP-sourced')),
    'a live event carries the corrected label (sf_ingest ring)');

  await caption(page, CAPTIONS.outro, 5200);
  await context.close();
  await browser.close();

  const webm = readdirSync(OUT).find(f => f.endsWith('.webm'));
  const target = `${OUT}vejas-bridge-film.webm`;
  if (webm && `${OUT}${webm}` !== target) renameSync(`${OUT}${webm}`, target);
  console.log(`✓ film green — video: ${target}`);
}

main().catch(e => { console.error('✗', e.message); process.exit(1); });
