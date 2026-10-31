# Write-Ahead Log (WAL) Rotation & Recovery Mechanics

## 1. Monotonic Segment Lifecycle

WAL segments use monotonically increasing 64-bit numerical identifiers formatted as:
`wal_00000001.wal`, `wal_00000002.wal`, etc.

```
       [Active WAL]  ──(MemTable Sealed)──►  [Rotated/Sealed WAL]
            ▲                                      │
            │                                      │ (MemTableFlushTask executes)
    (Appends Writes)                               ▼
                                             [Flush to SSTable.tmp]
                                                   │
                                                   ▼ (Atomic Rename & Force)
                                             [Install SSTable.sst]
                                                   │
                                                   ▼
                                        [Retire Obsolete WAL Segment]
```

### Segment States:
1. **ACTIVE**: Appended to by incoming client writes.
2. **SEALED**: Associated with a sealed immutable MemTable currently being flushed to an SSTable.
3. **OBSOLETE**: Flushed durably to an installed and verified SSTable. Safely deleted.

---

## 2. Binary Framing & Checksum Format

Each record in the WAL is framed with:
`[CRC32 Checksum (4B)][Payload Length (4B)][Serialized DataRecord]`

- **CRC32**: Computed over the exact serialized payload bytes using `java.util.zip.CRC32`.
- **Payload Length**: Variable LEB128/VarInt or 4-byte big-endian integer bounded by `MAX_PAYLOAD_SIZE = 64 MB`.
- **DataRecord Payload**: `[type (1B)][keyLen (VarInt)][key][valLen (VarInt)][val][timestamp (8B)][seqNum (8B)]`.

---

## 3. Crash Recovery & Torn-Tail Truncation

Upon database restart:
1. **Multi-Segment Discovery**: All `.wal` files in `storage.wal.dir` are located and sorted in ascending order of segment ID.
2. **Iterative Bounded Replay**: Each file is read frame-by-frame using full-read loops without loading entire files into memory.
3. **Torn-Tail Detection**:
   - If an uncommitted write was interrupted mid-frame due to a power loss or crash, the remaining trailing bytes at EOF will fail either the 8-byte frame header check, the payload length bound check, or the CRC32 checksum.
   - The engine identifies the `lastValidOffset` of the last successfully committed record.
   - The file channel is demonstrably and cleanly truncated to `lastValidOffset` (`channel.truncate(lastValidOffset)`), and forced to disk.
   - Future appends resume cleanly from `lastValidOffset` without corrupting subsequent records.
4. **Non-Tail Corruption Detection**:
   - If CRC corruption occurs in the middle of a log file where subsequent data follows, the engine throws `CorruptedWalException` and halts startup rather than silently skipping records.
5. **Sequence Restoration**:
   - The maximum sequence number across all recovered WAL records and all existing SSTables is identified.
   - `sequenceNumberGenerator.set(maxSeq + 1)`.
   - Sequence numbers remain strictly monotonic across restart.
