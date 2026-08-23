# No queue manager in CI — invariants are CI-tested against a fake broker
A meaningful mock of an IBM MQ queue manager would be a queue manager. What
CI verifies instead: the env-file credential lint (above), and — in the
crate's own tests (connectors/mq) — the transactional invariants against a
fault-injecting fake broker: no loss on crash-before-MQCMIT (message re-got),
consume-only-after-commit, put invisible until commit and discarded on
backout, failed commit leaves the bus message for redelivery; plus the FFI
layout proof (size_of each #[repr(C)] descriptor == its MQ*_LENGTH_1 from
cmqc.h) and an e2e source wiring test on real NATS (fake MQ -> bus, in
order, each commit only after its JetStream pub-ack).
What to verify against a real queue manager before production: the MQI
semantics the fake models (MQGET syncpoint / MQCMIT / MQBACK behavior on
YOUR qmgr version), channel auth + TLS for your channel, and one end-to-end
message each way. Upgrade path: an ibmcom/mq Developer-container CI leg if
real usage warrants it (noted in ADR-0023, not built).
