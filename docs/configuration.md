# AetherDB Configuration Reference

## Overview
AetherDB loads its settings from property files, default settings, and explicit runtime property overrides.

---

## Configuration Properties

### Node & Server Settings
| Property | Default | Description |
| :--- | :--- | :--- |
| `node.id` | `node-1` | Unique node identifier. |
| `node.host` | `127.0.0.1` | IP address to bind HTTP server to (defaults to loopback for security). |
| `node.port` | `8080` | Port for the HTTP REST gateway and dashboard. |
| `server.auth.enabled` | `false` | Enable Bearer token authentication on HTTP endpoints. |
| `server.auth.token` | `""` | Secret token required in `Authorization: Bearer <token>` header. |
| `server.cors.allowed.origins` | `*` | Allowed origins for CORS responses. |
| `server.max.request.body.bytes` | `10485760` | Maximum allowed request body size in bytes (10 MB). |
| `server.dashboard.enabled` | `true` | Serve the administrative HTML dashboard on `/dashboard`. |

---

### LSM-Tree Storage Engine Settings
| Property | Default | Description |
| :--- | :--- | :--- |
| `storage.data.dir` | `./data/node-1/storage` | Root directory for local node data. |
| `storage.wal.dir` | `./data/node-1/wal` | Directory for Write-Ahead Log segments (`.wal`). |
| `storage.sstable.dir` | `./data/node-1/sstable` | Directory for SSTable data blocks (`.sst`). |
| `lsm.memtable.max.bytes` | `33554432` | Maximum byte size of the active MemTable before triggering rotation & flush (32 MB). |
| `lsm.memtable.flush.queue.capacity` | `8` | Maximum queue capacity for background flush worker pool. |
| `lsm.wal.sync.mode` | `PERIODIC` | Durability mode: `IMMEDIATE` (fsync per write), `PERIODIC` (timer sync), `OS_BUFFER` (OS page cache). |
| `lsm.wal.periodic.sync.ms` | `50` | Interval in milliseconds between background fsync calls in `PERIODIC` mode. |
| `lsm.bloom.false.positive.rate` | `0.01` | Target false-positive probability for SSTable Bloom filters (1%). |
| `lsm.sstable.block.size` | `65536` | Target data block size in SSTable files (64 KB). |
| `lsm.compaction.min.threshold` | `4` | Minimum number of Level 0 SSTables required to trigger compaction. |
| `lsm.compaction.max.threshold` | `16` | Maximum number of SSTables evaluated in a single compaction pass. |

---

### Columnar Analytics Settings
| Property | Default | Description |
| :--- | :--- | :--- |
| `storage.columnar.dir` | `./data/node-1/columnar` | Directory for columnar chunk files (`.achunk`). |
| `columnar.chunk.max.rows` | `65536` | Maximum number of rows per columnar chunk file. |
| `columnar.dictionary.threshold.ratio` | `0.70` | Maximum cardinality ratio below which dictionary encoding is selected. |
| `columnar.vectorized.batch.size` | `1024` | Batch vector size for column evaluations. |

---

### Raft & Partitioning Settings
| Property | Default | Description |
| :--- | :--- | :--- |
| `raft.enabled` | `false` | Enable experimental Raft election and heartbeat background timers. |
| `raft.election.timeout.min.ms` | `150` | Minimum randomized election timeout in milliseconds. |
| `raft.election.timeout.max.ms` | `300` | Maximum randomized election timeout in milliseconds. |
| `raft.heartbeat.interval.ms` | `50` | Interval between leader heartbeat broadcasts. |
| `cluster.vnodes.per.node` | `64` | Virtual nodes allocated per physical instance in the consistent hash ring. |
