# No queue manager in CI — invariants are CI-tested against a fake broker
A meaningful mock of an IBM MQ queue manager would be a queue manager. What
CI verifies instead: the env-file credential lint (above), and — in the
crate's own tests (connectors/mq) — the sink's transactional invariants
against a fault-injecting fake broker: put invisible until commit and
discarded on backout, failed MQCMIT leaves the bus message unacked for
redelivery (nothing lost, nothing phantom-delivered); plus the FFI layout
proof (size_of each #[repr(C)] descriptor == its MQ*_LENGTH_1 from cmqc.h).
What to verify against a real queue manager before production: the MQI
semantics the fake models (MQPUT syncpoint / MQCMIT / MQBACK behavior on
YOUR qmgr version), channel auth + TLS for your channel, the CorrelId dedup
field end-to-end, and one message each way. Upgrade path: an ibmcom/mq
Developer-container CI leg if real usage warrants it (noted in ADR-0023,
not built).

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
