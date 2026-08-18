"""slack-out connector: bus -> Slack incoming webhook.

Consumes `vx.slack.out` (durable pull consumer). Payload: {"text": "..."}.
Without SLACK_WEBHOOK_URL set it prints a DRY-RUN line instead, so the
demo works with no Slack workspace at hand.
"""

import asyncio
import json
import os
import urllib.request

import nats
from vejas import ensure_stream, SUBJECT_ROOT, NATS_URL

SUBJECT = f"{SUBJECT_ROOT}.slack.out"
WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")

# Declared surface for the pipeline graph.
SUBJECTS_IN = ["vx.slack.out"]


def _post(text):
    req = urllib.request.Request(
        WEBHOOK,
        data=json.dumps({"text": text}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


async def main():
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream(js)
    sub = await js.pull_subscribe(SUBJECT, durable="slack_out")
    mode = "webhook" if WEBHOOK else "DRY-RUN (set SLACK_WEBHOOK_URL to post)"
    print(f"[slack-out] consuming {SUBJECT} -> {mode}", flush=True)
    while True:
        try:
            msgs = await sub.fetch(10, timeout=5)
        except (asyncio.TimeoutError, nats.errors.TimeoutError):
            continue
        for msg in msgs:
            try:
                text = json.loads(msg.data.decode()).get("text", msg.data.decode())
                if WEBHOOK:
                    await asyncio.to_thread(_post, text)
                    print(f"[slack-out] posted: {text}", flush=True)
                else:
                    print(f"[slack-out] DRY-RUN would post: {text}", flush=True)
                await msg.ack()
            except Exception as exc:
                print(f"[slack-out] error: {exc!r} -> nak", flush=True)
                try:
                    await msg.nak(delay=5)
                except Exception:
                    pass


if __name__ == "__main__":
    asyncio.run(main())
