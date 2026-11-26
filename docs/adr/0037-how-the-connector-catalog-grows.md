# 0037 — How the connector catalog grows: vendors as certified recipes on protocol drivers

- Status: Accepted (partial). Driver 1, signed webhooks on `http-in`, is
  built; the rest is not. The founder answered the three open questions on
  2026-11-26, and the answers close this record.
- Date: 2026-11-26

## Context

**What exists.** The runtime has 12 built-in drivers: `http-in`, `http-out`,
`http-poll`, `oauth-poll`, `slack-out`, `timer`, `mqtt-in`, `mqtt-out`, and
four exec drivers (`exec-source`, `exec-sink`, `exec-stream-source`,
`exec-rpc`). On top of them, 21 recipes are certified by the admission test
(ADR-0017) on every push: SaaS APIs over the HTTP drivers (ServiceNow, Jira,
Workday RaaS, Stripe, Shopify, GitHub, PagerDuty, SendGrid, Slack, Discord)
and brokers (Kafka through kcat, MQTT, MQ, AMQP). Four standalone binaries
cover what needs a native client or a transaction in one process: SAP RFC,
Salesforce, IBM MQ, AMQP. Neither the release nor the image ships those four;
an operator builds them from source.

**What a team evaluating an integration platform looks for first, and does
not find:**

- a relational database: there is no SQL driver at all;
- files: a directory, SFTP, S3-compatible object storage;
- mail in (IMAP) and plain SMTP out;
- verified webhooks: Stripe, Shopify, GitHub and Slack sign their requests
  with an HMAC, and `http-in` checks none of it;
- user-delegated OAuth (refresh tokens: Google Workspace, Microsoft 365,
  HubSpot) and service-account JWTs: `oauth-poll` knows only the
  client-credentials grant;
- AWS-signed requests (SigV4), which SQS, S3 and every AWS API want.

**What November showed.** Nine connector PRs were opened on 2026-11-23 (#33
to #41). Eight came within one minute, each with the right shape (a manifest,
a fixture, a mock, overrides), and every one was red on the admission job;
their author closed them a minute later "for proper testing before
submission". One of them sent arbitrary SQL from the message payload to
`/opt/vejas/vejas-postgresql`, a binary that exists nowhere. The ninth passed
admission, but it duplicated a certified recipe with a binary that started
`curl` per request and committed its build output; it was closed in review.
The gate did its job. The lesson is about where the cost now sits: an agent writes
a connector in minutes, and what takes the time is getting it certified and
shipped. A catalog grows at the speed of its gate, so the gate has to be in
the writer's hands before the pull request, not after.

**What Vejas can do that a catalog built by hand cannot.** The premise of the
product is that an agent writes the integration (VISION) and the runtime is
its MCP server. The same agent can write the connector, if the runtime gives
it the protocol it needs and a loop that proves the result. That turns "is X
in your catalog?" into "X is certified in minutes, by your agent, under your
review", which is the adoption argument a smaller catalog can make.

## Decision

1. **A vendor is a recipe; a driver is a protocol.** A new driver, built-in or
   standalone, exists for a protocol or an authentication scheme the existing
   drivers cannot speak, or for a delivery guarantee that must live in one
   process (ADR-0023). No vendor gets a driver of its own. The question asked
   of a proposed driver is which recipes it unlocks; one that unlocks a single
   vendor is a recipe missing an auth scheme.

2. **The drivers this calls for, in this order.**
   1. **Signed webhooks on `http-in`.** The manifest declares the signature
      header, the HMAC over the raw body, the encoding and a timestamp
      tolerance, with presets for the common schemes (Stripe, Slack, GitHub,
      Shopify). A request that fails the check gets 401 before anything is
      published. Small, and a security baseline for any public ingest.
      **Built** on 2026-11-26: `SIGNATURE` takes `github`, `stripe`, `slack`,
      `shopify`, `standard-webhooks` and `gitlab`, and `hmac-sha256` or
      `token` for any other sender; the guide *Expose an API* says how.
   2. **SQL, Postgres first** (`sql-poll`, `sql-sink`, `rpc:sql`). Statements
      are declared in the manifest, named and parameterized; a flow passes
      parameters, never SQL text, so a flow an agent wrote cannot become an
      injection path, and governance reviews a statement once. `sql-poll`
      reads incrementally on a cursor column and commits the cursor in KV
      after the publish is confirmed (the OFFSET_KV pattern of ADR-0022).
      `sql-sink` runs a declared upsert keyed for idempotence, so a
      redelivery is harmless. MySQL and SQL Server follow on the same
      contract; change data capture is a later increment; Oracle and DB2 need
      vendor C clients and get standalone binaries, as SAP did (ADR-0014).
   3. **OAuth2 refresh tokens and JWT bearer** on the HTTP drivers. The
      operator obtains the first token once, out of band; the runtime
      refreshes it and stores the rotated token in the secret store
      (ADR-0008).
   4. **Files**: a local directory, SFTP, and S3-compatible storage with
      SigV4 (which then serves SQS and the other AWS APIs as recipes). A file
      is published once read and moved or deleted only after the publish is
      confirmed. A file over the bus limit goes to the NATS object store and
      the message carries its reference, so no new infrastructure (ADR-0002).
   5. **Mail**: `imap-poll` and `smtp-out`.

   A family goes into the runtime when its client is pure Rust and its size is
   measured and published (binary size and cold start, on the benchmarks
   page). The stock binary keeps a budget of 20 MB (11.3 MB today); a family
   that would break it ships as a standalone binary instead.

3. **The agent writes recipes, and the proof loop is in its hands.**
   - The MCP server gains a tool that runs the admission test on one recipe,
     so any agent reaches green before it opens a pull request. That needs
     the mock as data (recorded request and answer pairs the runtime can
     serve) rather than a Node script, so the loop runs wherever the runtime
     runs.
   - A recipe is written from the vendor's OpenAPI description: the
     manifest, a fixture from the specification's examples, the mock from its
     schemas, and the overrides. The integration agent of ADR-0036 gets
     "write me a connector for X" as a scope, ending in a proposal with its
     evidence, not in code.
   - A recipe records whether it was checked against a live tenant, by whom
     and when. The mock proves the contract; only a tenant proves the vendor.

4. **The gate, written down.** A connector change lands only if:
   - its recipe directory is admission-complete, or carries an `EXCEPTION.md`
     saying why a real system is needed (ADR-0017);
   - it does not duplicate a certified recipe;
   - it carries trace context in and out where the protocol allows
     (ADR-0034);
   - a standalone binary ships in the release and the image, which makes MQ
     and AMQP the first to fix;
   - it commits no build output (`target/` is ignored for every connector
     directory) and no report stands in for a test.

   CI enforces what it can; review checks the rest.

5. **Extensibility stays as ADR-0011 decided**: external processes in any
   language, WASM when its triggers are met, no native dynamic libraries. A
   connector written in Rust is a standalone binary sharing the runtime's
   `bus.rs`, as MQ and AMQP already do. That module gets a crate of its own
   when a third connector needs it, not a plugin ABI.

## Consequences

- The catalog grows mostly as data (recipes) on a few protocols, so each new
  driver is paid for once and multiplied by the recipes it unlocks.
- The runtime grows: four families add dependencies and megabytes. The size
  budget makes that a measured trade, and the standalone path stays open.
- SQL through declared statements is less flexible than SQL text: a flow that
  needs a new query needs a manifest change, which is the point under
  governance (ADR-0024) and a cost for a quick prototype.
- The mock as data is new work in the admission test, and it changes how
  existing recipes are certified; the Node mocks keep working until each
  recipe moves.
- A recipe certified against a mock can still break against the real vendor.
  The live-tenant record makes that visible instead of implied.
- Shipping the standalone binaries in the image makes it larger, and each of
  them becomes a release artifact with its own audit lockfile (the audit job
  already covers five).

## Alternatives considered

- **A driver per vendor**, as the closed PRs did: fast to start, and every
  driver is code to maintain and ship. It does not scale past a few
  dozen and it duplicates what the HTTP drivers already do.
- **Native plugins** (`.so`/`.dll`, or a Rust plugin ABI): rejected again for
  the reasons of ADR-0011. A crash or a vulnerable dependency in a plugin
  takes the runtime down with it, and Rust has no stable ABI.
- **Arbitrary SQL from the flow**: the shape of the closed Postgres PR. It
  makes every flow that touches a database an injection risk, and moves the
  review of SQL from the manifest to every payload.
- **Buying breadth through a third-party connector hub or embedding a
  generic connector runtime**: a second runtime to operate and a dependency
  on someone else's catalog, against ADR-0002 and the one-binary line.
- **Waiting for WASM**: it answers isolation for third-party code, not the
  missing protocols; the triggers of ADR-0011 still decide when it comes.

## Open questions for the founder

Answered on 2026-11-26: the founder kept the recommendation on all three.

1. Is the order right? This proposal puts signed webhooks first (small,
   security), then Postgres (the most asked-for system of record), then the
   agent's proof loop. The proof loop could come first if the catalog story
   matters more than the database story for the next prospects.
   **Answer:** the order stands: signed webhooks, then Postgres, then the
   proof loop.
2. Is a 20 MB budget for the stock binary the right line, or should the
   families ship as standalone binaries from the start and keep the runtime
   at its current size?
   **Answer:** the 20 MB budget. A family with a pure Rust client goes into
   the runtime once its size and cold start are measured; a vendor C client
   (Oracle, DB2) ships as a standalone binary, as SAP did.
3. Should the recipe-writing scope wait for the integration agent of
   ADR-0036, or ship first as an MCP tool and a guide that any agent follows?
   **Answer:** the MCP tool and the guide come first. The agent of ADR-0036
   uses them when it is built.
