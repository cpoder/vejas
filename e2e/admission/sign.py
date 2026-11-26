#!/usr/bin/env python3
"""The headers a sender adds to sign a fixture, for a webhook recipe whose
http-in manifest sets SIGNATURE (ADR-0037): one `Name: value` per line, nothing
when the manifest asks for no signature. The secret is the dummy one admission
exported from overrides.json, read the way the env secret store reads it.

    sign.py <manifest> <fixture>

Written apart from the runtime, from the senders' documentation, so that a
recipe passes only if the runtime checks what the sender really signs."""
import base64
import hashlib
import hmac
import os
import re
import sys
import time

manifest = open(sys.argv[1], encoding="utf-8").read()
body = open(sys.argv[2], "rb").read()


def literal(name):
    found = re.search(rf'^{name}\s*=\s*"([^"]*)"', manifest, re.M)
    return found.group(1) if found else None


scheme = (literal("SIGNATURE") or "").lower()
if not scheme:
    sys.exit(0)
ref = re.search(r'^SIGNING_SECRET\s*=\s*secret\("([^"]+)"\)', manifest, re.M)
if not ref:
    sys.exit("SIGNATURE is set and SIGNING_SECRET is not secret(\"…\")")
secret = os.environ["VEJAS_SECRET_" + re.sub(r"[^A-Za-z0-9]", "_", ref.group(1)).upper()]
key, now = secret.encode(), str(int(time.time()))


def mac(k, message):
    return hmac.new(k, message, hashlib.sha256).digest()


if scheme == "github":
    print("X-Hub-Signature-256: sha256=" + mac(key, body).hex())
elif scheme == "stripe":
    print(f"Stripe-Signature: t={now},v1=" + mac(key, now.encode() + b"." + body).hex())
elif scheme == "slack":
    print(f"X-Slack-Request-Timestamp: {now}")
    print("X-Slack-Signature: v0=" + mac(key, b"v0:" + now.encode() + b":" + body).hex())
elif scheme == "shopify":
    print("X-Shopify-Hmac-Sha256: " + base64.b64encode(mac(key, body)).decode())
elif scheme in ("standard-webhooks", "svix"):
    raw = base64.b64decode(secret.removeprefix("whsec_"))
    signed = b"msg_admission." + now.encode() + b"." + body
    print("webhook-id: msg_admission")
    print(f"webhook-timestamp: {now}")
    print("webhook-signature: v1," + base64.b64encode(mac(raw, signed)).decode())
elif scheme == "gitlab":
    print("X-Gitlab-Token: " + secret)
elif scheme == "token":
    print(f"{literal('SIGNATURE_HEADER')}: {secret}")
elif scheme == "hmac-sha256":
    digest = mac(key, body)
    encoded = (base64.b64encode(digest).decode()
               if (literal("SIGNATURE_ENCODING") or "hex").lower() == "base64" else digest.hex())
    print(f"{literal('SIGNATURE_HEADER')}: {literal('SIGNATURE_PREFIX') or ''}{encoded}")
else:
    sys.exit(f"SIGNATURE {scheme!r}: no sender this check knows")
