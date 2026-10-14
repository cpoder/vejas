#!/usr/bin/env bash
# Audit every Cargo.lock in the repo against the RustSec advisory database.
#
# The runtime (core/) and each connector crate carry their own lockfile, and
# cargo-audit reads .cargo/audit.toml from the current directory only, so this
# runs from the repo root with --file for each lock: one ignore list for all.
# CI runs it on every push and once a day, since advisories land without any
# commit here (.github/workflows/audit.yml).
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
while IFS= read -r lock; do
  echo "[audit] $lock"
  cargo audit --file "$lock" || fail=1
done < <(git ls-files '*Cargo.lock')
exit $fail
