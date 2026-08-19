# E2E demo (Playwright)

`node e2e/demo.mjs` drives the whole business-expert journey against a running
stack and records it as a video (`e2e/out/vejas-demo-metier.webm`):

1. setup over MCP — an agent-written flow (`order_dispatch`) plus real traffic
   through the webhook connector;
2. in the browser — the pipeline and live events, the business rules extracted
   from the code, two corrections made without touching code, each
   **shadow-replayed on the flow's last real events** before promotion, and a
   new order picking up the corrected rules immediately.

Every step asserts, so this is also the panel's end-to-end regression test —
it fails loudly if any beat breaks.

## Run

```bash
docker compose down && docker compose up -d --build   # fresh stack (fresh trace ring)
cd e2e && npm install && npx playwright install chromium   # once
node demo.mjs
```

Reset between runs (the demo writes `flows/order_dispatch.vjs` + its fixture):

```bash
rm -f flows/order_dispatch.vjs flows/fixtures/order_dispatch.json
docker compose down
```

Share it: `ffmpeg -i e2e/out/vejas-demo-metier.webm vejas-demo.mp4`.
Captions are French (a business audience); the strings live in one `CAPTIONS`
object in `demo.mjs`.
