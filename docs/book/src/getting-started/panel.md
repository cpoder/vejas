# The panel

One HTML page embedded in the runtime, at `http://localhost:8686`. No build
step, no dependency. It is the human side of the platform: a domain expert
reads a flow and corrects its meaning, an operator follows events and handles
what failed, and neither has to become a flow developer.

Everything it shows is derived from the code. Change a file and the page
follows.

## Where things are

A menu on the left lists the pages:

- **Map**: every connector, subject, flow and service, from where events come
  in to where they go. Pick a subject to follow its path. The link
  `#focus=f:<flow>` opens the map on a flow's path.
- **Flows**: one page per flow, for the domain expert.
- **Watch**: Activity and To review, for the operator. The number next to To
  review counts the dead letters and the changes waiting for a decision.
- **Setup**: connectors, services and secrets.

## A flow's page

A flow reads top to bottom, in the order it runs:

1. **Receives**: the subject it listens to, who publishes it, and the event
   fields it reads, with their values in the sample event.
2. **Computes**: each value it makes, what it gives with the sample, and how
   it is made.
3. **Calls**: the services it uses and what they give back.
4. **Checks**: its conditions, as sentences ([the rules view](../guides/rules-view.md)).
5. **Sends**: the subject it publishes on, with which fields, and who reads it.

The business values (thresholds, transcoding tables, lists) appear in yellow,
with a pencil, where the flow uses them. A click opens the value with the
comment written above it in the code. Change one thing, then choose **Check on
recent events**: the runtime replays the last real events with the new value
and shows which ones would change, and how. **Make it live** writes the value
through the same audited path agents use (`/surface/set`) and the flow
reloads. Credential-shaped names are masked (single-sourced pattern,
[ADR-0017](../decisions.md)). With `VEJAS_TOKEN` set, the panel asks for the
token on the first write and keeps it in the page's memory only.

Values that wire the flow to the bus, such as the subject it sends to, sit
under **Technical settings**. **Open the code** shows the whole script, and
**Edit the sample** the event the page computes with.

## Activity

Each event the runtime handled, newest first, one row per event: the time,
whether it went through, and the path it took through connectors and flows,
with retries counted as attempts. Open a row to follow the event hop by hop,
with what each hop received and the statements it ran
([ADR-0034](../decisions.md)). Find an event by any value it carried, or by its
trace id. A hop that went through can be kept as a golden test
([golden traffic](../guides/golden-traffic.md)).

## To review

- **Dead letters**: events a connector or a flow could not handle, kept
  instead of lost. Fix the cause, then replay them
  ([DLQ & replay](../guides/dlq-replay.md)).
- **Changes proposed by agents**: in governed mode, each proposal with its
  evidence, to approve or reject with the approval token
  ([governed mode](../guides/governed-mode.md)).

## Setup

**Connectors** lists how events come in and go out. A connector's page holds
its settings (saving one restarts it), the secrets it uses and a connection
test. **Services** shows what each service receives, computes and gives back,
and which flows call it. **Secrets** lists every reference with its status;
values are write-only. **New flow** and **New connector** take a sentence, and
an agent writes the file.
