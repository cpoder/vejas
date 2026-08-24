#!/usr/bin/env bash
# Deploy the vejas.dev landing (site/) to the host that serves it.
#
# vejas.dev is a Caddy `file_server` over a static directory on the box
# (a `caddy:2-alpine` container, bind-mounted from $DEST). There is NO CI for
# it — this script is the reproducible path, so a landing change is one command,
# not an ad-hoc scp. The mdBook under $DEST/docs is built + deployed separately
# and is deliberately left untouched here (no --delete).
set -euo pipefail

BOX="${VEJAS_SITE_BOX:-cpo@95.216.191.129}"
DEST="${VEJAS_SITE_DEST:-/home/cpo/vejas-site}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

echo "[deploy-site] $HERE/site → $BOX:$DEST"
ssh -o BatchMode=yes "$BOX" "mkdir -p '$DEST/demo'"
scp "$HERE/site/index.html" "$BOX:$DEST/index.html"
if [ -d "$HERE/site/demo" ] && [ -n "$(ls -A "$HERE/site/demo" 2>/dev/null)" ]; then
  scp "$HERE"/site/demo/* "$BOX:$DEST/demo/"
fi
echo "[deploy-site] done — https://vejas.dev/ (Caddy serves it fresh; clients cache ~5 min)"
