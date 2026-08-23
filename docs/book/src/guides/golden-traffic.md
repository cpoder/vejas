# Golden-traffic curation

Fixtures written by hand go stale; real traffic is the truth. Golden
curation turns **one real event into a permanent test** in one click:

- In the panel's event ring, star ★ an event (or `POST /events/golden`).
- The runtime re-runs the flow on that event **now** and captures the
  actual emits as the expectation.
- What lands is a file: `tests/vjs/curated_<flow>_<n>.vjs` — input event +
  expected emits — which `vejas-runtime vjs-test` (and CI) runs forever.

That closes the expert→CI loop: the domain expert who *recognizes* a
representative case doesn't write a test — they point at reality, and the
platform freezes it. When a flow's behavior must change, time-travel and
canary say *what* changes against history
([changing safely](change-safely.md)); the curated cases say *what must
never change*.

In a cluster, curation is refused on instances that hold no write access to
the root (the cluster guard) — curate where the files live.
