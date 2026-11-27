# 0039 — A flow sees its message as it came, and owns its failures

- Status: Accepted. Points 1 to 4 are built. The founder chose the syntax of points 3 and 4 (2026-11-27), over an `on error` block for the whole flow and a declared `DEDUPE` setting.
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
- A builtin, `base64()`, gives the base64 of a text's UTF-8 bytes, so
  `unreadable.base64 ?? base64(unreadable.text)` is the message in base64
  whatever its bytes are: a reject that must carry the original value in
  base64 can. It is pure, as every builtin is. What it gives is read by no
  mask unless it sits under a key that says it is base64; the base64 of a
  short text that holds a secret, a credential pair, is masked as the
  secret is, under any key. The last 64 such values are kept apart from
  the values the stores hand out, so that none pushes those out.
- It is a field like another, so a replay of the recorded event runs the
  same way, and no engine change was needed. A JSON message with its own
  field `unreadable` sets the variable too: the language reference says so.
- A flow that does not read it keeps the dead letter. An `api` flow that
  reads it takes such a body instead of answering 400.
- Time travel, the canary and a literal's replay run such a message in the
  versions that read `unreadable`. A version that does not leaves it to the
  dead letters, which counts as a change.

### 3. `try` / `catch` (built)

```
try:
    o = invoke get_quote(id: quoteId)
    emit "vx.quote.refined", {id: quoteId, price: o.body.price}
catch e:
    emit "vx.quote.rejected", {id: quoteId, code: f"API_HTTP_{e.status}"}
end
```

- `catch` runs only for a failure no later delivery may change: an error in
  the code, a call the service does not try again, or a call on the
  delivery that spends the service's `ATTEMPTS`, and at the latest on the
  last of the five deliveries any message gets. Before that the run fails
  and the message comes back after the service's delay, as without `try`:
  retries stay deliveries (ADR-0032). A `secret()` or a `seen()` whose
  store does not answer is a failure a later delivery may change, at the
  pace a message comes back when nothing says otherwise: the catch takes it
  on the last delivery. A secret the store says it does not have is an
  error in the code, which no delivery changes; in a service's declaration,
  it fails the call to that service without a retry. An `api` flow's request has
  no later delivery, so
  its catch takes the failure at once, and answers 503 when nothing catches
  such a failure.
- `e` is `{error, service, status, attempts}`: `service` null for an error
  in the code, `status` null when the system gave no answer, `attempts` the
  delivery's number (1 for a request). `catch:` without a name is allowed.
- What the `try` emitted, remembered or responded before the failure is
  dropped: the run's effects stay all or nothing, and the trace record
  shows those statements as run, not as sent. The variables it set stay, as
  a branch's do; a filter whose condition failed gives the flow its own
  back. A failure in the catch fails the run, or goes to the
  `try` around it.
- A run that is shown makes no call (ADR-0032), so its catch takes no
  call's failure: such a run fails there and says why, as before.
- The trace record writes the step that failed as `caught` and the run as
  succeeded: the steps that follow are the catch's.
- `try` and `catch` are words of the language only where a statement
  starts, so a flow that uses them as a variable or a field is unchanged.
- The language stays pure: this is control flow, with no new effect.

### 4. `seen` / `remember` (built)

```
if seen(event.dedupKey):
    emit "vx.quote.log", {level: "DUPLICATE", key: event.dedupKey}
else:
    o = invoke get_quote(id: quoteId)
    emit "vx.quote.refined", {id: quoteId, price: o.body.price}
    remember event.dedupKey for "7d"
end
```

- `seen(key)` is true when a run of this flow remembered the key and the
  window it gave has not passed. The runtime looks the key up in a
  JetStream key-value bucket of the flow's, `VEJAS_SEEN_` and the flow's
  name and the start of its hash, under the key's SHA-256: any value a key
  holds fits, and two flows never share a bucket. It is read where the flow
  reads it, before the run's calls, so a duplicate makes none.
- `remember key for "7d"` marks the key once the run's emits are stored
  (the stream answers for them, ADR-0035 amendment). A run that failed marks
  nothing, so the message that comes back is processed again. A mark made in
  a `try` before its failure is dropped; a `catch` marks what it remembers,
  so a business reject that does not say `remember` leaves the key free.
  The read and the mark are apart for that reason: a flow marks only what
  it published.
- The window is a number and `s`, `m`, `h` or `d`, up to ten years, written
  in the statement or in an UPPERCASE constant, so the parser checks it. A
  constant's window is its literal, wherever the constant is written: an
  event's field of the same name never sets it, and the parser refuses a
  flow that sets the constant anywhere else, since that value would not be
  the window. The stored value is when the window passes. The bucket keeps
  a key as long as the flow's longest window: a longer one raises its
  limit, a shorter one never lowers it. A mark reads the limit again once
  a minute and raises it to the longest window the instance has written,
  should two instances have raised it at once.
- A key is any value but null; a number or a document is read as its JSON
  text.
- The runtime holds a run's marks while its emits are confirmed, so a copy
  later in the same batch reads them. A mark that cannot be written fails
  nothing: the emits are stored, so the message is acked. The log, the
  unit's last error and the trace record say `vejas.remember.error`, whether
  the run was sampled or not. The next copy is then processed again.
- A bucket that does not answer fails the run, which the next delivery may
  change, as a call's failure; a bus that answers is asked whether the
  bucket exists, and an error is never read as "no bucket". An `api` flow
  answers 503, and opens its bucket only when a run reads or marks a key.
  Until a bucket exists, `seen` asks for it once a second at most.
- The lookup and the mark sit at the edges of the run, as the event and the
  ack do, so the run itself stays pure. `remember` is a write for the write
  gate of a read-method `api` flow. A shown run (preview, replay, time
  travel) reads every key as new: it reads no state, and its steps show the
  branch it took.
- Limit: two copies that run at the same instant, on two instances or as
  two requests, can both pass. So can a copy on another instance in the
  second after a flow's first mark, while that instance still takes the
  bucket as absent. A sender that needs exactly once keeps its own key.

## Consequences

- An `api` flow that read `query`, or a path parameter through `event`,
  changes. The changelog says so.
- A message that is not JSON reaches a flow only when the flow asks for it,
  so no existing flow sees a new kind of event.
- Points 3 and 4 add `try`/`catch`, a builtin, `seen`, and a statement,
  `remember`. A flow that does not use them is unchanged, and opens no
  bucket.
- A flow that publishes a business reject or drops duplicates says so in
  its own lines, where it needed code outside Vejas, and the trace shows the
  catch and the marks.
