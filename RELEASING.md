# Releasing

Versions follow SemVer; the git tag `vX.Y.Z` is the release trigger.

## Steps

1. Update `CHANGELOG.md`: move `[Unreleased]` entries under a new
   `[X.Y.Z] — <date>` heading, and refresh the compare links at the bottom.
2. Bump `version` in `core/Cargo.toml` (and any connector crate that
   changed) to `X.Y.Z`; commit `Cargo.lock`.
3. Commit: `release: vX.Y.Z`.
4. Tag and push:
   ```bash
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin master --follow-tags
   ```

Pushing the tag runs `.github/workflows/release.yml`, which:
- builds the release binary (`vejas-runtime`, Linux x86_64), stripped, and
  attaches it plus its SHA-256 to the GitHub Release;
- builds and pushes the container image to
  `ghcr.io/cpoder/vejas:X.Y.Z` and `:latest`;
- creates the GitHub Release with the CHANGELOG section as its notes.

## Version discipline (ADR-0030)

The public HTTP API, `docs/CONTROL.md` and `docs/SUBJECTS.md` are a
**contract**: the enterprise tier and external integrators pin a version of
them. A breaking change to any of these is at least a minor bump while `0.x`,
called out in the changelog.
