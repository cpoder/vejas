# No broker in CI — the mechanism is CI-tested, the matrix rides kcat
A meaningful mock of a Kafka broker for `kcat` would be a broker. What CI
verifies instead: the credential lint and parse (above), and the exec-sink
stdio contract (one JSON per line to the child), covered by the transport
suite. What to verify against a dev broker before production: the `CMD`
auth flags for YOUR cluster (kcat carries the full librdkafka matrix — TLS,
SASL PLAIN/SCRAM, Kerberos), and one end-to-end message each way. Upgrade
path: a redpanda-container CI leg if real usage warrants it (noted, not
built).
