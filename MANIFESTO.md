# Integration doesn't need an IDE anymore

I have spent most of my career inside integration platforms. webMethods flows back when an ESB was the center of the world, IoT integrations on Cumulocity, and lately the usual mix of iPaaS and homemade glue that every company runs somewhere. Fifteen years of drawing boxes and arrows, honestly.

## Why the canvas existed

Visual designers exist for one reason. Developers were expensive, and integration logic is 80% repetitive (map this field, call that API, retry on failure). So the industry built UIs that let fewer people, or cheaper people, produce more flows. It worked. MuleSoft, Boomi, n8n, Zapier, all variations of the same trade: you give up code, you get a canvas.

The price of that trade is well known to anyone who has operated these platforms at scale. The logic ends up locked in proprietary JSON or XML. You cannot diff it properly. You cannot review it in a pull request. You cannot unit test it without spinning up the whole platform. And migrating away means rewriting everything, which is precisely the point (for the vendor).

## The constraint is gone

In 2026 the constraint that justified all this has disappeared. An agent writes correct glue code for cents. And I mean working flows, not demo snippets: parse this webhook, enrich from that database, push to this queue, handle the retry. Glue is the most specification-shaped, most commoditized category of code there is, which is exactly why it was the first to fall.

Watch what the incumbent platforms are doing with that fact. They are adding assistants that generate their own proprietary format. An AI that writes the JSON that feeds the canvas that compiles to an execution you still cannot test. I understand why they do it (the format is the lock-in, they will not give it up), but from the buyer's side it is backwards. The abstraction existed to protect humans from code. Agents do not need protection from code. Code is their native interface, and ours too when it comes to review.

So the honest question is what remains of an integration platform once you delete the designer. My answer is the runtime, the transport, the connectors, and the observability. That is the whole product. Everything else was scaffolding around a constraint that no longer exists.

## Vejas

Vejas is my attempt at that reduced platform. Vėjas is the old Baltic god of the wind (a sibling of Varpulis, the storm spirit whose name my CEP engine already carries, which tells you where the monitoring layer will come from). Wind moves things without anyone drawing the route. Same idea here: you do not draw flows anymore. You state the intent, an agent writes the flow as plain code, and the platform's job is to carry it well and show you what it does.

The architecture is deliberately boring:

- One Rust binary for the runtime. It runs the flows in-process, wires them to the bus, enforces retries and backpressure, and exports traces. No subprocess, no Python.
- NATS as the only infrastructure dependency. JetStream covers persistence, key-value and object storage, so there is no Redis and no Postgres to start with. Two processes, one docker-compose. That is the deployment story.
- Connectors are just services on the bus. The bundled ones ship in the binary; the "plugin interface" is a subject convention, not a linker trick, so a connector can be written in any language, is isolated by construction, and can be replaced live.
- Flows are VejasScript files in your own git repository — a small, readable language (I reimplemented my old WmScript in Rust for it). An agent writes them, a human reviews the pull request, the tests run, GitOps deploys. If you leave, you take your code with you. There is nothing to export because nothing was captured.
- No builder UI. Two screens survive: monitoring (live topology, a per-flow feed of the last processed events, shadow-replay on real traffic) for whoever operates, and a business panel where a domain user reviews and corrects the business surface of a flow (mappings, thresholds). Neither screen can draw a flow. (OpenTelemetry export is on the roadmap, not in the binary yet — I'd rather tell you that here than have you grep for it.)
- An MCP server exposes the platform to whatever agent you already use: list connectors, scaffold a flow, run it against fixtures, deploy, read traces. Bring your own agent. Claude Code, Codex, a cron job with an API key, I don't care.

Everything under Apache-2.0. A platform that argues against proprietary formats has to start with itself.

## Where humans stay in charge

The dividing line became obvious while watching agents work. They are better than us at the algorithmic side: parsing, retries, pagination, idempotency. And they have no idea whether amounts arrive in cents or euros, whether "customer" means the billing account or the ship-to party, or what threshold makes an alert worth a human's attention. Business meaning is not in the repository; it is in someone's head.

So a Vejas flow has two surfaces. The algorithmic body is code, written and rewritten by agents, reviewed like code. The business surface is data declared inside that same code: literal mapping tables (dotted source paths, named transforms), thresholds, constants. Literals are statically extractable, so the platform renders them as the two-column mapping view business users have always known, filled with live sample values from the traces. That view is the one screen a non-technical user needs: they validate, they correct a path or a threshold, or they tell the agent in plain language what is wrong ("amounts are cents, not euros"). Every correction comes back as the same thing, a patch, shown as before and after on real events. The domain user approves behavior without reading a diff.

Nothing about this reintroduces a builder. You still cannot draw a flow, and there is still nothing proprietary to export. The split is simply honest about who is good at what: the agent owns how, the human owns what it means.

## What this is not

This is not another durable-execution engine. Temporal and Restate solve hard state problems and solve them well; Vejas happily runs beside them. It is not Windmill either, which is excellent but still centers on a builder UI. And it is not low-code. If anything it runs in the opposite direction, all code and no builder.

## Three ways this fails

Now the part where I argue against myself, because this bet has three well-known failure modes.

Connectors. The value of an iPaaS is its catalog, and catalogs take years. My counter-argument is that connector code is exactly the kind of code agents produce well (it is specification-shaped), so a small catalog plus a good SDK plus agents might compound faster than catalogs used to. That part is unproven.

Buyers. Enterprises buy governance, support, and someone to blame when it breaks. They do not buy architectural elegance; I sold to them long enough to know. Vejas starts with people who run their own infrastructure and read the code they deploy. Whether it can climb from there is an open question.

Timing. Maybe letting agents write production glue is two years early for most shops. Fine by me, the demo was cheap and git does not expire. My own conviction, built from watching agents write integration code all year, is that it is not early at all.

The demo repo is at https://github.com/cpoder/vejas: one Rust runtime, a handful of connectors, and Stripe-to-Slack running in plain VejasScript that an agent writes from one sentence. A recorded end-to-end session (agent builds it, tests it, ships it, live) is the next thing I am putting up. If you run integrations for a living and think this is wrong, I want the strongest version of your objection. If you think it is obvious, even better, come help with the connector SDK :)
