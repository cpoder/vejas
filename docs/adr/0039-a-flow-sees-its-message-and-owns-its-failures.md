# 0039 — A flow sees its message as it came, and owns its failures

- Status: Accepted (partial). Points 1 and 2 are built. Points 3 and 4 are proposed: their syntax waits for the founder's review.
- Date: 2026-11-27

## Context

A bench of fourteen realistic integration cases ran a webhook intake: route,
drop the duplicates, enrich by two calls to an HTTP API, publish the result,
or publish a business reject. After `invoke` learned to call a system
(ADR-0032), Vejas alone passed ten. Four cases failed for reasons the
language could not get past:

- **An array body.** An `api` flow that received a JSON array saw an empty
  event. The runtime kept a body only when it was an object, and wrote the
  path parameters and the query string into it, over the body's own fields:
  a body field named `query` was lost.
- **A message that is not JSON.** It went to the dead letter queue before the
  flow ran. The flow could not publish its own reject, with the original
  value.
- **A business reject when a call keeps failing.** The run failed, the
  message came back, and after its deliveries it was a dead letter. The flow
  had no way to say "this call will not succeed, publish the reject".
- **A duplicate.** The sender delivers an event twice. The flow has no state,
  so it cannot tell.

The bench names no client and ships nothing: what it taught is written here.

## Decision

### 1. An `api` flow's event is the body as sent (built)

- `event` is the JSON body as it came: an object, an array, a scalar, or
  null when there is none. An object's fields are variables, as for a bus
  event.
- A path parameter is a variable of its own, over a body field of the same
  name. `event.id` stays the body's.
- `request` holds the rest of the request: `{method, path, params, query,
  headers}`. `headers` holds the few that say what the request is (`accept`,
  `content-type`, `idempotency-key`, `user-agent`, `x-correlation-id`,
  `x-request-id`) and those the flow names in an `API_HEADERS` list. Names
  are in lower case, a repeated header is joined with `, `. A header whose
  name reads as a credential is never given, even when the list names it:
  a name the record's mask takes for one (ADR-0017), as written or with
  camelCase read as snake_case, one with a word headers name a credential
  by (`token`, `session`, `jwt`, `signature`, `hmac`), one with a part
  that is a credential's (`key`, `pin`, `mfa`), or one with a part that
  starts with `auth` (`x-auth-code`). The name of a key (`x-key-id`), a key
  that names an event or a route (`x-event-key`), a dedupe key
  (`idempotency-key`) and who a proxy says the caller is
  (`x-auth-request-user`) are given. A name is read in each spelling the
  request sends and the list has: one that reads as a credential withholds
  the header. A list of what is given, since a credential header can have
  any name. The trace record is open to read: it keeps the value of the six
  alone, masks the value of each header the list names, since a name can be
  missed, and lists those the list named and the flow was not given in
  `request.withheld_headers`. What the flow does with a value it was given,
  an answer or an emit, the record shows as the flow wrote it, masked by its
  key as any value is.
- A path parameter named `event` or `request` is not a variable, since those
  two names are the runtime's: `request.params` holds it.
- `query` is no longer a variable. It is `request.query`. Breaking, with no
  flow in the repo reading it.
- A body that is not JSON answers 400. Before, the flow ran on an empty
  event. It is the caller's error: it fails no hop.
- The trace record keeps what the flow was given, the event and the
  request, with the headers as above.

### 2. A flow can take a message that is not JSON (built)

- A flow that reads the variable `unreadable` (found statically, anywhere in
  its code) runs such a message. Its event is `{unreadable: {error, text,
  base64}}`: why it is not JSON, and the message once, its text when its
  bytes are UTF-8 text, its bytes in base64 when they are not, the other
  null. Once, so that the masks read the text. The masks read base64 under
  a key that says it is (one that ends or starts with `base64`, or
  `contentBytes`), an array of them included: bytes that read as a text
  are masked as a text is, and the value whole when a mask finds a
  credential in them, or ends in the start of a value handed out when it
  is cut short in a preview; bytes that read as no text are masked whole,
  since no mask can read them and base64 keeps them exact. Text is UTF-8
  with few control characters besides the delimiters text formats use
  (FIX's SOH, the separators FS to US, a coloured log's ESC), a run of them
  counted once as padding is, or an encoding in which ASCII stays ASCII and
  no three bytes past it come in a row, as Latin-1 writes accents. UTF-16
  and UTF-32, four ASCII characters of which anywhere make it no text,
  EBCDIC, however many spaces pad it, and compressed bytes are not. Whitespace and padding are
  read past, as producers fold and join base64. A flow that forwards the
  bytes keeps them under such a key (`raw_base64`): under another one, or
  in a form the masks do not decode (base64 of base64, a field beside an
  `isBase64Encoded` flag), no mask reads them. A message that is not JSON
  and reads as no text is masked whole wherever the record, the events and
  the dead letters show it as text.
- It is a field like another, so a replay of the recorded event runs the
  same way, and no engine change was needed. A JSON message with its own
  field `unreadable` sets the variable too: the language reference says so.
- A flow that does not read it keeps the dead letter. An `api` flow that
  reads it takes such a body instead of answering 400.
- Time travel, the canary and a literal's replay run such a message in the
  versions that read `unreadable`. A version that does not leaves it to the
  dead letters, which counts as a change.

### 3. `try` / `catch` (proposed)

```
try:
    o = invoke get_quote(id: quoteId)
    emit "vx.quote.refined", {id: quoteId, price: o.body.price}
catch e:
    emit "vx.quote.rejected", {id: quoteId, code: f"API_HTTP_{e.status}"}
end
```

- `catch` runs only for a failure no later delivery may change: an error the
  service does not retry, or its attempts spent. Before that the run fails
  and the message comes back after the service's delay, as today: retries
  stay deliveries (ADR-0032).
- `e` is `{error, service, status, attempts}`, `status` when the system
  answered.
- The emits held in the `try` before the failure are dropped: the run's
  emits stay all or nothing.
- The language stays pure: this is control flow, with no new effect.

### 4. `seen` / `remember` (proposed)

```
if seen(event.dedupKey):
    emit "vx.quote.log", {level: "DUPLICATE", key: event.dedupKey}
else:
    o = invoke get_quote(id: quoteId)
    emit "vx.quote.refined", {id: quoteId, price: o.body.price}
    remember event.dedupKey for "7d"
end
```

- `seen(key)` is true when a run of this flow remembered the key within the
  window it gave. The runtime looks the key up in a JetStream key-value
  bucket of the flow's. It is read before the run's calls, so a duplicate
  makes none.
- `remember key for "7d"` marks the key once the run's emits are stored
  (the stream answers for them, ADR-0035 amendment). A run that failed or
  went through a `catch` marks nothing, so its replay is processed again,
  and a business reject that does not say `remember` leaves the key free.
  The read and the mark are apart for that reason: a flow marks only what
  it published.
- The lookup and the mark sit at the edges of the run, as the event and the
  ack do, so the run itself stays pure. `remember` is a write for the write
  gate of a read-method `api` flow. A shown run (preview, replay, time
  travel) reads `seen` from what the record kept.
- Limit: two copies that arrive at the same instant on two instances can
  both pass. A sender that needs exactly once keeps its own key.

## Consequences

- An `api` flow that read `query`, or a path parameter through `event`,
  changes. The changelog says so.
- A message that is not JSON reaches a flow only when the flow asks for it,
  so no existing flow sees a new kind of event.
- Points 3 and 4 add `try`/`catch`, a builtin, `seen`, and a statement,
  `remember`. They wait for the founder's review of their syntax.
