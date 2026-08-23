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

## Verified against a real queue manager (out of band)
2026-08-23, against the free MQ Developer container (icr.io/ibm-messaging/mq,
QM1): source drained real MQPUTs to the bus in order (CURDEPTH 5→0), sink
landed bus messages as real MQPUTs (contents read back with amqsget), and
the backout invariant held live — with the bus down, thousands of real
MQGET→MQBACK cycles lost nothing; the message reached the bus the moment it
recovered. Found and fixed, then re-verified live: password auth via MQCSP
(MQCNO v5 — the layout needs ConnectionId before SecurityParms; verified
end to end against CHCKCLNT(REQUIRED): valid password connects and drains,
wrong password and no-credentials both fail clean with 2035), and a
progressive backoff on repeated MQBACK. Packaging: the redistributable
client needs its full directory structure — lib64 alone segfaults inside
libmqic.
