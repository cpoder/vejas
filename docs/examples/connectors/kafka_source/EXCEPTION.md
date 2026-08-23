# No broker in CI — the mechanism is CI-tested, the matrix rides kcat
A meaningful mock of a Kafka broker for `kcat` would be a broker. What CI
verifies instead: the credential lint and parse (above), and the generic
offset-resume mechanism (exec-stream-source + OFFSET_KV) via
`e2e/offset-resume.sh` — a kill -9 of the runtime mid-stream, then restart:
zero gap (every offset reaches the bus), resume from the committed offset,
duplicates bounded by the commit cadence (OFFSET_COMMIT_MS).
What to verify against a dev broker before production: the `CMD` auth flags
for YOUR cluster (kcat carries the full librdkafka matrix — TLS, SASL
PLAIN/SCRAM, Kerberos), and one end-to-end message each way. Upgrade path:
a redpanda-container CI leg if real usage warrants it (noted, not built).
