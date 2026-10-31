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

## 3. Schema Ingestion, Validation & Explicit Registration

1. **Table Name Sanitization**: Table names are validated against regex `^[a-zA-Z0-9_]{1,64}$`. Directory traversal attempts (e.g. `../`, `/`, `\`) throw `IllegalArgumentException`.
2. **Untyped All-Null Policy**: If an initial ingestion batch contains a column where every value is untyped null, AetherDB strictly rejects the batch with an `IllegalArgumentException` requiring explicit schema registration, rather than silently defaulting to `STRING`.
3. **Explicit Schema Registration**: Tables can be pre-registered via `registerTableSchema(schema)`. Table names must match `^[a-zA-Z0-9_]{1,128}$` and strictly match `schema.getTableName()`. Re-registering an identical schema is idempotent; incompatible schema replacements are rejected with `IllegalArgumentException`. Registering a schema for an existing table verifies compatibility across all pre-existing chunks on disk. When a schema is pre-registered, initial all-null batches are accepted and correctly typed according to the registered schema.
4. **Schema Ingestion Consistency**: Ingested records are checked against the initial schema definition. Rows with missing columns receive typed null values; rows with unexpected extra columns or incompatible types throw clear schema mismatch exceptions.
5. **Chunk Row Limits**: `columnar.chunk.max.rows` (default 65,536) automatically partitions large batch ingestions into appropriately sized chunk files.

---

## 4. Binary Deserialization Hardening & Defensive Immutability

1. **Schema Allocation Bounds**: `Schema.deserialize()` enforces strict bounds (`MAX_COLUMNS = 10,000`, table and column name length <= 255 bytes). Rejects duplicate column names and unsupported column types with `CorruptedSchemaException`.
2. **Defensive Immutability**: `ColumnarChunk` performs defensive copying of byte arrays upon construction and accessor calls (`getEncodedColumn()`, `getEncodedColumns()`). Decoded column lists returned by `getDecodedColumn()` are wrapped in unmodifiable lists, preventing external mutation of cached state.
3. **ZoneMap Immutability & Bounds**: `ZoneMap` instances are frozen upon incorporation into `ColumnarChunk`, rejecting subsequent mutations with `UnsupportedOperationException`. Deserialization rigorously verifies record counts (`rowCount >= 0`, `nullCount >= 0`, `rowCount >= nullCount`) and min/max consistency (`min <= max`), rejecting corrupted streams with `CorruptedSchemaException`.

---

## 5. HTAP Consistency Model Clarification

In AetherDB V2:
- The LSM-Tree Key-Value engine and the Columnar Analytics engine have **separate, explicit ingestion and query paths**.
- Key-Value writes (`put`, `delete`) write to the LSM engine (WAL, MemTable, SSTables).
- Analytical data is ingested via `ingestColumnar()`.
- Updates to Key-Value data do not automatically reflect in the columnar store without an explicit ETL / CDC event log mechanism.
