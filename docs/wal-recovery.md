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
- **Payload Length**: 4-byte big-endian integer bounded by `MAX_PAYLOAD_SIZE = 64 MB`.
- **DataRecord Payload**: `[type (1B)][keyLen (VarInt)][key][valLen (VarInt)][val][timestamp (8B)][seqNum (8B)]`.

---

## 3. Crash Recovery, Corruption Classification & Safe Truncation

Upon database restart:
1. **Multi-Segment Discovery**: All `.wal` files in `storage.wal.dir` are located and sorted in ascending order of segment ID.
2. **Streaming Record Recovery**: Segments are replayed via a streaming record consumer (`recoverAllSegments(walDir, consumer)`), avoiding full in-memory list allocations.
3. **Corruption Classification**:
   - **Case A (Incomplete Trailing Frame at EOF)**: If a write was interrupted mid-frame (fewer than 8 bytes for header, or incomplete payload bytes before EOF), the engine safely truncates the channel to `lastValidOffset` via `channel.truncate(lastValidOffset)` and forces changes to disk. Future writes resume cleanly from `lastValidOffset`.
   - **Case B (Corrupted Complete Frame / CRC Mismatch)**: If all payload bytes are present on disk but the CRC32 checksum fails verification, the engine treats this as bit rot or on-disk tampering. It strictly throws `CorruptedWalException` and halts startup rather than silently skipping records or masking data corruption.
4. **Sequence Monotonicity Restoration**:
   - The maximum sequence number across all recovered WAL records and all existing SSTables is identified.
   - `sequenceNumberGenerator.set(maxSeq + 1)`.
   - Sequence numbers remain strictly monotonic across restarts and crashes.
5. **Atomic WAL Rotation**:
   - Rotation pre-allocates and initializes the new segment file channel *before* touching or closing the previous active log, preventing unrecoverable states during failure.
