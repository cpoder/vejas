# A public demo, read only

A runtime can face the public and show its console without letting anyone
change it (ADR-0041). Start it with `VEJAS_PUBLIC=1`: its flows run as on any
runtime, and the public sees them run.

## What it serves

GET on the reads the console makes, and nothing else:

| Read | For |
|---|---|
| `/`, `/panel` | the console |
| `/healthz` | the probe a proxy or an orchestrator polls |
| `/topology`, `/graph` | the map: the units, their state, how they connect |
| `/surface` | the business values each flow carries, shown, never changed, masked by the runtime as the trace record masks (ADR-0017) |
| `/events` | what went through the bus, masked as the console masks it |
| `/dlq` | the dead letters, the newest 50, without replay or purge |
| `/traces?search=`, `/traces/{id}` | the trace record |
| `/file`, `/shape`, `/rules` | a flow's, a service's or a connector's code (`.vjs` in `flows/`, `services/`, `connectors/`) and what its page derives from it; a file whose literals or code those masks would change is not served |
| `/drivers`, `/evictions` | the drivers there are, the versions a promote evicted |

Everything else answers `404 not found`, the same for a route that exists and
one that does not, and whatever credential comes with it:

- every write (`POST`, and any other method that is not a read), with
  `VEJAS_TOKEN` or the approval token as well;
- `/mcp`, so no agent reaches the runtime;
- the `api` flows and their OpenAPI document;
- the secrets, the samples, the previews and every shown run;
- the proposals and the metrics.

The list lives in one place, `core/src/public.rs`. The suite
`e2e/public-mode.sh` reads every route the runtime serves from its code and
checks that each one outside the list answers 404, with and without the
tokens: a route added later is refused until it is listed.

## What else changes

- The runtime starts no agent, whatever `VEJAS_AGENT_CMD` says, and no
  control channel, even with `VEJAS_TENANT` set.
- A read asks for little: a trace search is 200 characters and 20 traces at
  most, a trace id 64 characters, and the runtime handles 32 requests at once
  at most (`VEJAS_HTTP_MAX_INFLIGHT` can lower it, not raise it).
- The console shows no action: no edit, replay, purge, approval, test or new
  flow, and a line at the top says it is a read-only demo. It reads the code,
  without saving it.

## Keep the history short

A demo runs for days. Set `VEJAS_STREAM_MAX_AGE_SECS` so the bus keeps an hour
of it:

```bash
VEJAS_PUBLIC=1 VEJAS_STREAM_MAX_AGE_SECS=3600 vejas-runtime
```

The setting is not tied to the public mode. Any runtime can bound its stream
this way; unset, or `0`, the stream keeps its messages for ever, as before. A
stream made by an earlier start takes the new age at the next start.

## In front of it

The mode is the barrier that does not depend on anything else. Put a second
one in front:

- a reverse proxy with TLS that passes only GET and HEAD on the same reads,
  and limits each client's rate;
- a container that runs as a non-root user, on a read-only file system, with
  CPU and memory limits;
- a host apart from any production, with no secret configured.

An independent security review tries every route of the mode before it faces
the public.
