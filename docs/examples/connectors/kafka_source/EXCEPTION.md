# No broker in CI — the mechanism is CI-tested, the matrix rides kcat
A meaningful mock of a Kafka broker for `kcat` would be a broker. What CI
verifies instead: the credential lint and parse (above), and the generic
offset-resume mechanism (exec-stream-source + OFFSET_KV) which has its own
mock-child test — resume across restarts with zero gap and zero duplicate.
What to verify against a dev broker before production: the `CMD` auth flags
for YOUR cluster (kcat carries the full librdkafka matrix — TLS, SASL
PLAIN/SCRAM, Kerberos), and one end-to-end message each way. Upgrade path:
a redpanda-container CI leg if real usage warrants it (noted, not built).
