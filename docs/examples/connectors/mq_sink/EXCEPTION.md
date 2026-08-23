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
