# Columnar Storage & Ingestion Architecture

## 1. Columnar Format Specification

AetherDB columnar chunks (`.achunk`) store typed tabular data with dictionary and run-length encoding.

### File Layout:
`[Magic 4B: AECO][Version 2B][ChunkId 8B][RowCount 4B][Schema Definition][ZoneMaps][Encoded Columns][CRC32 Checksum 8B]`

- **Magic Bytes**: `0x4145434F` ("AECO").
- **Schema**: Table name, column names, and data types (`INT32`, `INT64`, `TIMESTAMP`, `FLOAT64`, `STRING`, `BOOLEAN`).
- **ZoneMaps**: Per-column Min-Max bounds, null counts, and row counts.
- **Encoded Columns**: Adaptive compressed binary streams (Raw, Dictionary, or RLE).
- **CRC32 Checksum**: Verifies the entire payload before accepting chunks on load.

---

## 2. Dynamic Capacity & Buffer Sizing

`ColumnEncoder` computes exact byte requirements before allocating serialization buffers, eliminating fixed-capacity buffer overflows for long strings (tested up to 5,000+ characters), empty strings, and Unicode characters.

---

## 3. Schema Validation & Path Traversal Security

1. **Table Name Sanitization**: Table names are validated against regex `^[a-zA-Z0-9_]{1,64}$`. Directory traversal attempts (e.g. `../`, `/`, `\`) throw `IllegalArgumentException`.
2. **Schema Ingestion Validation**: Ingested records are checked against the initial schema definition. Rows with missing columns receive typed null values; rows with incompatible types throw clear schema mismatch exceptions.
3. **Chunk Row Limits**: `columnar.chunk.max.rows` (default 65,536) automatically partitions large batch ingestions into appropriately sized chunk files.

---

## 4. HTAP Consistency Model Clarification

In AetherDB V2:
- The LSM-Tree Key-Value engine and the Columnar Analytics engine have **separate, explicit ingestion and query paths**.
- Key-Value writes (`put`, `delete`) write to the LSM engine (WAL, MemTable, SSTables).
- Analytical data is ingested via `ingestColumnar()`.
- Updates to Key-Value data do not automatically reflect in the columnar store without an explicit ETL / CDC event log mechanism.
