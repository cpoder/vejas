#!/usr/bin/env bash
# Publish the book to vejas.dev/docs, from a commit.
#   scripts/deploy-docs.sh [ref]        (default HEAD; a release passes its tag)
#
# GitHub Pages carries site/ only and the book has no CI deployment on purpose:
# that would put an SSH key for a box shared with production into repository
# secrets, for a documentation page. So this is a release step (RELEASING.md).
#
# The book is built from `git archive`, never from the working tree: uncommitted
# pages must not go live. It lands next to the live directory, and the two swap in
# one move, so a reader never sees a half-copied book. The previous one stays in
# $BACKUPS, outside $DEST, because everything under $DEST is served publicly.
set -euo pipefail

BOX="${VEJAS_SITE_BOX:-cpo@95.216.191.129}"
DEST="${VEJAS_SITE_DEST:-/home/cpo/vejas-site}"
BACKUPS="${VEJAS_SITE_BACKUPS:-/home/cpo/vejas-site-backups}"
REF="${1:-HEAD}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
git -C "$HERE" archive "$REF" | tar -x -C "$WORK"
mdbook build "$WORK/docs/book"

STAMP=$(date -u +%Y%m%dT%H%M%S)
NEW="$BACKUPS/docs.new-$STAMP"
echo "[deploy-docs] $REF → $BOX:$DEST/docs"
tar -C "$WORK/docs/book/book" -czf - . | ssh -o BatchMode=yes "$BOX" \
  "set -e; mkdir -p '$NEW'; tar -xzf - -C '$NEW'; chmod -R a+rX '$NEW'"
ssh -o BatchMode=yes "$BOX" "set -e; mv '$DEST/docs' '$BACKUPS/docs-$STAMP'; mv '$NEW' '$DEST/docs'"
curl -fsS -m 15 -o /dev/null https://vejas.dev/docs/
echo "[deploy-docs] done — previous book in $BOX:$BACKUPS/docs-$STAMP"
