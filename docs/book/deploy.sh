#!/bin/bash
# Build the book and deploy it to vejas.dev/docs (static file server on the box).
#   BOX=user@host docs/book/deploy.sh
set -euo pipefail
cd "$(dirname "$0")"
: "${BOX:?set BOX=user@host (the vejas.dev box)}"
mdbook build .
rsync -az --delete book/ "$BOX":~/vejas-site/docs/
echo "deployed: https://vejas.dev/docs/"
