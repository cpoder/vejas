"""Vejas flow SDK.

A flow is a plain Python file. No DSL, no builder, nothing to export:

    from vejas import flow, emit

    @flow(source="vx.stripe.events")
    def stripe_alerts(event):
        if event["amount"] > 50000:
            emit("vx.slack.out", {"text": f"big payment: {event['amount']/100} EUR"})

Semantics (the whole contract, on purpose):
  - one durable JetStream pull consumer per flow function (durable = function name)
  - handler runs per message; emits are published BEFORE the ack, so a crash
    means redelivery, never a lost emit (at-least-once end to end)
  - an exception naks the message with a delay; the runtime restarts dead flows
"""

import asyncio
import contextvars
import importlib.util
import inspect
import json
import os
import sys

STREAM = os.environ.get("VEJAS_STREAM", "VEJAS")
SUBJECT_ROOT = os.environ.get("VEJAS_SUBJECT_ROOT", "vx")
NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")

_FLOWS = []  # (fn, source, durable)
_pending = contextvars.ContextVar("vejas_pending")


def flow(source, durable=None):
    """Register a function as a flow fed by `source` (a vx.* subject)."""

    def register(fn):
        _FLOWS.append((fn, source, durable or fn.__name__))
        return fn

    return register


def emit(subject, payload):
    """Queue a message for publication. Flushed before the incoming ack."""
    _pending.get().append((subject, json.dumps(payload).encode()))


async def ensure_stream(js):
    from nats.js.api import StreamConfig

    try:
        await js.add_stream(StreamConfig(name=STREAM, subjects=[f"{SUBJECT_ROOT}.>"]))
    except Exception as exc:  # already exists (possibly with same config) -> fine
        if "already in use" not in str(exc) and "exists" not in str(exc):
            raise


async def _run_flow(js, nats_errors, fn, source, durable):
    sub = await js.pull_subscribe(source, durable=durable, stream=STREAM)
    print(f"[vejas-sdk] flow {durable!r} consuming {source!r}", flush=True)
    while True:
        try:
            msgs = await sub.fetch(10, timeout=5)
        except (asyncio.TimeoutError, nats_errors.TimeoutError):
            continue
        for msg in msgs:
            token = _pending.set([])
            try:
                event = json.loads(msg.data.decode())
                result = fn(event)
                if inspect.isawaitable(result):
                    await result
                for subject, data in _pending.get():
                    await js.publish(subject, data)
                await msg.ack()
            except Exception as exc:
                print(f"[vejas-sdk] {durable}: {exc!r} -> nak", file=sys.stderr, flush=True)
                try:
                    await msg.nak(delay=5)
                except Exception:
                    pass
            finally:
                _pending.reset(token)


async def _main(path):
    import nats
    import nats.errors as nats_errors

    spec = importlib.util.spec_from_file_location("vejas_flow", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not _FLOWS:
        print(f"[vejas-sdk] {path}: no @flow found", file=sys.stderr)
        sys.exit(2)
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream(js)
    await asyncio.gather(*[_run_flow(js, nats_errors, fn, src, dur) for fn, src, dur in _FLOWS])


def run(path):
    asyncio.run(_main(path))
