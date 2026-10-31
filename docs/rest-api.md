# REST API & Security Specification

## 1. Overview
The AetherDB HTTP Gateway runs on Java Virtual Threads, providing non-blocking JSON APIs for transactional Key-Value access, batch analytics ingestion, and analytical queries.

---

## 2. Security Controls

- **HTTP Method Enforcement**: Each endpoint enforces its designated method and responds with `405 Method Not Allowed` for invalid methods.
- **Payload Limits**: Requests exceeding `server.max.request.body.bytes` (default 10 MB) are rejected with `413 Payload Too Large`.
- **Authentication**: When `server.auth.enabled=true`, all incoming requests must supply `Authorization: Bearer <token>`. Unauthorized requests return `401 Unauthorized`.
- **CORS**: Configurable via `server.cors.allowed.origins` (default `*`).
- **Safe JSON Handling**: Handled via Jackson Core/Databind, properly parsing and escaping quotes, backslashes, control characters, and Unicode.

---

## 3. Endpoints

### 3.1 Key-Value Endpoints

#### `POST /api/v1/kv/put`
Inserts or updates a key-value pair.
```json
// Request Body:
{"key": "user:1001", "value": "active"}

// Response (200 OK):
{"status": "ok", "key": "user:1001"}
```

#### `GET /api/v1/kv/get?key=<key>`
Retrieves a key-value record.
```json
// Response (200 OK):
{
  "key": "user:1001",
  "value": "active",
  "timestamp": 1730347890123,
  "sequenceNumber": 42
}

// If key not found (404 Not Found):
{"error": "Key not found: user:1001"}
```

#### `DELETE /api/v1/kv/delete?key=<key>` (or `POST`)
Appends a tombstone deletion.
```json
// Response (200 OK):
{"status": "deleted", "key": "user:1001"}
```

---

### 3.2 Columnar Analytics Endpoints

#### `POST /api/v1/analytics/ingest`
Ingests a batch of structured records into a columnar table.
```json
// Request Body:
{
  "table": "events",
  "records": [
    {"id": 1, "region": "us-east", "latency": 14.5, "status": 200},
    {"id": 2, "region": "us-west", "latency": 22.1, "status": 200}
  ]
}

// Response (200 OK):
{"status": "ingested", "table": "events", "records": 2}
```

#### `POST /api/v1/analytics/query`
Executes an analytical query with selection expressions, predicates, and grouping.
```json
// Request Body:
{
  "table": "events",
  "select": ["COUNT(*)", "AVG(latency)"],
  "predicates": [
    {"column": "latency", "op": ">=", "value": 10.0}
  ],
  "groupBy": ["region"],
  "limit": 50
}

// Response (200 OK):
{
  "executionTimeMs": 1.25,
  "scannedChunks": 1,
  "skippedChunks": 0,
  "totalRowsProcessed": 2,
  "columns": ["region", "COUNT(*)", "AVG(latency)"],
  "rows": [
    ["us-east", 1, 14.5],
    ["us-west", 1, 22.1]
  ]
}
```

---

### 3.3 Cluster & Metrics Endpoints

#### `GET /api/v1/cluster/status`
Returns node ID, consensus role, and partition ring topology.

#### `GET /api/v1/metrics`
Returns real-time engine telemetry (puts, gets, hits, misses, SSTable counts, row counts).

