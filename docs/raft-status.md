# Distributed Consensus Scope & Raft Status

## 1. Honest Scope & Current Implementation Status

AetherDB V2 operates as a **reliable single-node hybrid storage engine**.

The consensus subsystem (`com.aetherdb.consensus.raft`) contains an **experimental consensus prototype and simulation** of the Raft algorithm:
- Leader election timing and vote solicitation protocol.
- Heartbeat dispatch and term step-down logic.
- MurmurHash3-32 virtual node consistent hash router (`ConsistentHashRouter`).

---

## 2. Implemented vs. Incomplete Capabilities

### Implemented:
- In-memory candidate election timers with randomized backoff.
- In-memory `RequestVote` RPC arguments and replies.
- In-memory `AppendEntries` heartbeat exchange.
- Collision-safe virtual node ring representation with deterministic key assignment.

### Not Implemented (Future Roadmap):
- Ordinary Key-Value client writes (`/api/v1/kv/put`) currently write directly to the local LSM storage engine; **they are not quorum-replicated across nodes**.
- Raft term, vote, and replicated log state are maintained in memory; they are not yet persisted to disk across node reboots.
- Log payloads currently exchange heartbeat metadata; full payload batch streaming, previous-log conflict truncation, and `nextIndex`/`matchIndex` replication vectors are not yet complete.
- For complete architecture and specifications for future distributed Raft replication, see [`docs/raft-design.md`](raft-design.md).

