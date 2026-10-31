package com.aetherdb.network;

import com.aetherdb.analytics.FilterOperator;
import com.aetherdb.analytics.Predicate;
import com.aetherdb.analytics.QueryEngine;
import com.aetherdb.analytics.QueryRequest;
import com.aetherdb.analytics.QueryResult;
import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.Constants;
import com.aetherdb.consensus.raft.RaftNode;
import com.aetherdb.consensus.raft.messages.AppendEntriesArgs;
import com.aetherdb.consensus.raft.messages.AppendEntriesReply;
import com.aetherdb.consensus.raft.messages.RequestVoteArgs;
import com.aetherdb.consensus.raft.messages.RequestVoteReply;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.model.DataRecord;
import com.aetherdb.partition.ConsistentHashRouter;
import com.aetherdb.storage.engine.StorageEngine;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/**
 * Production REST API Handler for Key-Value, Columnar Analytics, and Raft Consensus.
 * Enforces strict HTTP methods, Jackson JSON parsing/serialization, bounded request sizes,
 * constant-time authentication, CORS origin validation, and clean error reporting.
 */
public class RestApiHandler implements HttpHandler {

    private static class PayloadTooLargeException extends RuntimeException {
        public PayloadTooLargeException(String message) {
            super(message);
        }
    }

    private final AetherConfig config;
    private final StorageEngine storageEngine;
    private final RaftNode raftNode;
    private final ConsistentHashRouter router;
    private final QueryEngine queryEngine;
    private final ObjectMapper mapper = new ObjectMapper();

    public RestApiHandler(AetherConfig config, StorageEngine storageEngine, RaftNode raftNode, ConsistentHashRouter router) {
        this.config = config;
        this.storageEngine = storageEngine;
        this.raftNode = raftNode;
        this.router = router;
        this.queryEngine = new QueryEngine(storageEngine, config.getVectorizedBatchSize());
    }

    public RestApiHandler(StorageEngine storageEngine, RaftNode raftNode, ConsistentHashRouter router) {
        this(new AetherConfig(), storageEngine, raftNode, router);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod().toUpperCase();

        // 1. CORS Preflight Handling
        if ("OPTIONS".equalsIgnoreCase(method)) {
            handleCorsPreflight(exchange);
            return;
        }

        // 2. Authentication Check
        if (config.isAuthEnabled() && !isAuthorized(exchange)) {
            sendResponse(exchange, 401, "{\"error\":\"Unauthorized: Invalid or missing authentication token\"}");
            return;
        }

        // 3. Raft endpoint gate
        if (path.startsWith("/api/v1/cluster") || path.startsWith("/api/v1/raft")) {
            if (!config.isRaftEnabled()) {
                sendResponse(exchange, 404, "{\"error\":\"Raft consensus module is disabled in configuration\"}");
                return;
            }
        }

        try {
            switch (path) {
                case "/api/v1/kv/put" -> {
                    if (!enforceMethod(exchange, "POST")) return;
                    handleKvPut(exchange);
                }
                case "/api/v1/kv/get" -> {
                    if (!enforceMethod(exchange, "GET")) return;
                    handleKvGet(exchange);
                }
                case "/api/v1/kv/delete" -> {
                    if (!enforceMethod(exchange, "DELETE", "POST")) return;
                    handleKvDelete(exchange);
                }
                case "/api/v1/analytics/ingest" -> {
                    if (!enforceMethod(exchange, "POST")) return;
                    handleAnalyticsIngest(exchange);
                }
                case "/api/v1/analytics/query" -> {
                    if (!enforceMethod(exchange, "POST")) return;
                    handleAnalyticsQuery(exchange);
                }
                case "/api/v1/cluster/status" -> {
                    if (!enforceMethod(exchange, "GET")) return;
                    handleClusterStatus(exchange);
                }
                case "/api/v1/metrics" -> {
                    if (!enforceMethod(exchange, "GET")) return;
                    handleMetrics(exchange);
                }
                case "/api/v1/raft/requestVote" -> {
                    if (!enforceMethod(exchange, "POST")) return;
                    handleRaftRequestVote(exchange);
                }
                case "/api/v1/raft/appendEntries" -> {
                    if (!enforceMethod(exchange, "POST")) return;
                    handleRaftAppendEntries(exchange);
                }
                default -> sendResponse(exchange, 404, "{\"error\":\"Endpoint not found: " + path + "\"}");
            }
        } catch (PayloadTooLargeException e) {
            sendResponse(exchange, 413, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        } catch (JsonProcessingException e) {
            sendResponse(exchange, 400, "{\"error\":\"Malformed JSON: " + escapeJson(e.getMessage()) + "\"}");
        } catch (IllegalArgumentException e) {
            sendResponse(exchange, 400, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        } catch (Exception e) {
            sendResponse(exchange, 500, "{\"error\":\"Internal server error: " + escapeJson(e.getMessage()) + "\"}");
        }
    }

    private boolean enforceMethod(HttpExchange exchange, String... allowedMethods) throws IOException {
        String method = exchange.getRequestMethod().toUpperCase();
        for (String m : allowedMethods) {
            if (m.equalsIgnoreCase(method)) {
                return true;
            }
        }
        exchange.getResponseHeaders().set("Allow", String.join(", ", allowedMethods));
        sendResponse(exchange, 405, "{\"error\":\"Method Not Allowed: " + method
                + ". Allowed methods: " + Arrays.toString(allowedMethods) + "\"}");
        return false;
    }

    private boolean isAuthorized(HttpExchange exchange) {
        String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
        if (authHeader == null) return false;
        String token = authHeader.startsWith("Bearer ") ? authHeader.substring(7).trim() : authHeader.trim();
        if (token.isEmpty() || config.getAuthToken().trim().isEmpty()) {
            return false;
        }
        byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);
        byte[] expectedBytes = config.getAuthToken().getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(tokenBytes, expectedBytes);
    }

    private void handleCorsPreflight(HttpExchange exchange) throws IOException {
        String allowed = config.getCorsAllowedOrigins();
        String origin = exchange.getRequestHeaders().getFirst("Origin");
        if (!"*".equals(allowed) && origin != null) {
            String[] allowedList = allowed.split(",");
            boolean matched = false;
            for (String a : allowedList) {
                if (a.trim().equalsIgnoreCase(origin.trim())) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                sendResponse(exchange, 403, "{\"error\":\"CORS origin not allowed\"}");
                return;
            }
        }
        setCorsHeaders(exchange);
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, Authorization");
        exchange.sendResponseHeaders(204, -1);
    }

    private void setCorsHeaders(HttpExchange exchange) {
        String allowed = config.getCorsAllowedOrigins();
        if ("*".equals(allowed)) {
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        } else {
            String origin = exchange.getRequestHeaders().getFirst("Origin");
            if (origin != null) {
                String[] allowedList = allowed.split(",");
                for (String a : allowedList) {
                    if (a.trim().equalsIgnoreCase(origin.trim())) {
                        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", origin.trim());
                        exchange.getResponseHeaders().set("Vary", "Origin");
                        break;
                    }
                }
            }
        }
    }

    private void handleKvPut(HttpExchange exchange) throws IOException {
        byte[] bodyBytes = readBody(exchange);
        if (bodyBytes.length == 0) {
            sendResponse(exchange, 400, "{\"error\":\"Empty request body\"}");
            return;
        }

        JsonNode root = mapper.readTree(bodyBytes);
        JsonNode keyNode = root.get("key");
        JsonNode valNode = root.get("value");

        if (keyNode == null || valNode == null || keyNode.isNull() || valNode.isNull()) {
            sendResponse(exchange, 400, "{\"error\":\"Missing required 'key' or 'value' field in JSON request\"}");
            return;
        }

        String key = keyNode.asText();
        String value = valNode.asText();

        storageEngine.put(key, value);

        ObjectNode response = mapper.createObjectNode();
        response.put("status", "ok");
        response.put("key", key);
        sendResponse(exchange, 200, mapper.writeValueAsString(response));
    }

    private void handleKvGet(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getRawQuery();
        String key = extractQueryParam(query, "key");

        if (key == null) {
            sendResponse(exchange, 400, "{\"error\":\"Missing 'key' query parameter\"}");
            return;
        }

        DataRecord record = storageEngine.get(key);
        if (record == null) {
            sendResponse(exchange, 404, "{\"error\":\"Key not found: " + escapeJson(key) + "\"}");
        } else {
            ObjectNode res = mapper.createObjectNode();
            res.put("key", record.getKeyAsString());
            res.put("value", record.getValueAsString());
            res.put("timestamp", record.getTimestamp());
            res.put("sequenceNumber", record.getSequenceNumber());
            sendResponse(exchange, 200, mapper.writeValueAsString(res));
        }
    }

    private void handleKvDelete(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getRawQuery();
        String key = extractQueryParam(query, "key");

        if (key == null) {
            byte[] bodyBytes = readBody(exchange);
            if (bodyBytes.length > 0) {
                try {
                    JsonNode root = mapper.readTree(bodyBytes);
                    if (root.has("key")) {
                        key = root.get("key").asText();
                    }
                } catch (Exception ignored) {}
            }
        }

        if (key == null) {
            sendResponse(exchange, 400, "{\"error\":\"Missing 'key' in query parameter or JSON body\"}");
            return;
        }

        storageEngine.delete(key);

        ObjectNode response = mapper.createObjectNode();
        response.put("status", "ok");
        response.put("deletedKey", key);
        sendResponse(exchange, 200, mapper.writeValueAsString(response));
    }

    private void handleAnalyticsIngest(HttpExchange exchange) throws IOException {
        byte[] bodyBytes = readBody(exchange);
        if (bodyBytes.length == 0) {
            sendResponse(exchange, 400, "{\"error\":\"Empty request body\"}");
            return;
        }

        JsonNode root = mapper.readTree(bodyBytes);
        if (!root.has("table") || !root.get("table").isTextual()) {
            sendResponse(exchange, 400, "{\"error\":\"Missing required string field 'table'\"}");
            return;
        }
        if (!root.has("records") || !root.get("records").isArray()) {
            sendResponse(exchange, 400, "{\"error\":\"Missing required array field 'records'\"}");
            return;
        }

        String table = root.get("table").asText();
        ArrayNode recordsArray = (ArrayNode) root.get("records");
        List<Map<String, ColumnValue>> records = new ArrayList<>(recordsArray.size());

        for (int i = 0; i < recordsArray.size(); i++) {
            JsonNode rowNode = recordsArray.get(i);
            if (!rowNode.isObject()) {
                sendResponse(exchange, 400, "{\"error\":\"Each item in records array must be a JSON object, row: " + i + "\"}");
                return;
            }

            Map<String, ColumnValue> rowMap = new LinkedHashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = rowNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String colName = field.getKey();
                JsonNode val = field.getValue();

                ColumnValue cv;
                if (val.isNull()) {
                    cv = ColumnValue.ofNull(); // Untyped null
                } else if (val.isBoolean()) {
                    cv = ColumnValue.ofBoolean(val.asBoolean());
                } else if (val.isIntegralNumber()) {
                    cv = ColumnValue.ofLong(val.asLong());
                } else if (val.isFloatingPointNumber()) {
                    cv = ColumnValue.ofDouble(val.asDouble());
                } else {
                    cv = ColumnValue.ofString(val.asText());
                }
                rowMap.put(colName, cv);
            }
            records.add(rowMap);
        }

        storageEngine.ingestColumnar(table, records);

        ObjectNode res = mapper.createObjectNode();
        res.put("status", "ingested");
        res.put("table", table);
        res.put("records", records.size());
        res.put("rowsIngested", records.size());
        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleAnalyticsQuery(HttpExchange exchange) throws IOException {
        byte[] bodyBytes = readBody(exchange);
        if (bodyBytes.length == 0) {
            sendResponse(exchange, 400, "{\"error\":\"Empty request body\"}");
            return;
        }

        JsonNode root = mapper.readTree(bodyBytes);
        if (!root.has("table") || !root.get("table").isTextual()) {
            sendResponse(exchange, 400, "{\"error\":\"Missing required string field 'table'\"}");
            return;
        }
        if (!root.has("select") || !root.get("select").isArray() || root.get("select").isEmpty()) {
            sendResponse(exchange, 400, "{\"error\":\"Missing required non-empty array 'select'\"}");
            return;
        }

        String table = root.get("table").asText();
        List<String> selectExprs = new ArrayList<>();
        for (JsonNode sel : root.get("select")) {
            selectExprs.add(sel.asText());
        }

        List<Predicate> predicates = new ArrayList<>();
        if (root.has("predicates") && root.get("predicates").isArray()) {
            for (JsonNode pNode : root.get("predicates")) {
                if (!pNode.has("column") || !pNode.has("op")) {
                    sendResponse(exchange, 400, "{\"error\":\"Predicate must specify both 'column' and 'op'\"}");
                    return;
                }
                String col = pNode.get("column").asText();
                String opStr = pNode.get("op").asText();
                FilterOperator op = FilterOperator.fromString(opStr);
                JsonNode vNode = pNode.get("value");

                ColumnValue targetVal;
                if (vNode == null || vNode.isNull()) {
                    targetVal = ColumnValue.ofNull();
                } else if (vNode.isIntegralNumber()) {
                    targetVal = ColumnValue.ofLong(vNode.asLong());
                } else if (vNode.isFloatingPointNumber()) {
                    targetVal = ColumnValue.ofDouble(vNode.asDouble());
                } else if (vNode.isBoolean()) {
                    targetVal = ColumnValue.ofBoolean(vNode.asBoolean());
                } else {
                    targetVal = ColumnValue.ofString(vNode.asText());
                }

                predicates.add(new Predicate(col, op, targetVal));
            }
        }

        List<String> groupByCols = new ArrayList<>();
        if (root.has("groupBy") && root.get("groupBy").isArray()) {
            for (JsonNode g : root.get("groupBy")) {
                groupByCols.add(g.asText());
            }
        }

        int limit = root.has("limit") ? root.get("limit").asInt() : 1000;
        QueryRequest request = new QueryRequest(table, selectExprs, predicates, groupByCols, limit);
        QueryResult result = queryEngine.execute(request);

        ObjectNode res = mapper.createObjectNode();
        ArrayNode colsArr = res.putArray("columns");
        for (String c : result.getColumns()) {
            colsArr.add(c);
        }

        ArrayNode rowsArr = res.putArray("rows");
        for (List<Object> row : result.getRows()) {
            ArrayNode rowArr = rowsArr.addArray();
            for (Object v : row) {
                if (v == null) rowArr.addNull();
                else if (v instanceof Number n) {
                    if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) rowArr.add(n.longValue());
                    else rowArr.add(n.doubleValue());
                }
                else if (v instanceof Boolean b) rowArr.add(b);
                else rowArr.add(v.toString());
            }
        }

        res.put("executionTimeMs", result.getExecutionTimeMs());
        res.put("rowsProcessed", result.getTotalRowsProcessed());
        res.put("scannedChunks", result.getScannedChunks());
        res.put("skippedChunks", result.getSkippedChunks());

        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleClusterStatus(HttpExchange exchange) throws IOException {
        ObjectNode res = mapper.createObjectNode();
        res.put("status", "ok");
        res.put("nodeId", raftNode != null ? raftNode.getNodeId() : "standalone");
        res.put("state", raftNode != null ? raftNode.getCurrentRole().name() : "STANDALONE");
        res.put("term", raftNode != null ? raftNode.getState().getCurrentTerm() : 0);
        res.put("totalVNodes", router != null ? router.getRingSize() : 0);

        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        Map<String, Object> storageMetrics = storageEngine.getMetrics();
        ObjectNode res = mapper.createObjectNode();
        res.put("status", "ok");

        ObjectNode st = res.putObject("storage");
        for (Map.Entry<String, Object> e : storageMetrics.entrySet()) {
            if (e.getValue() instanceof Number n) {
                st.put(e.getKey(), n.longValue());
            } else {
                st.put(e.getKey(), String.valueOf(e.getValue()));
            }
        }

        if (raftNode != null) {
            ObjectNode rf = res.putObject("raft");
            rf.put("nodeId", raftNode.getNodeId());
            rf.put("state", raftNode.getCurrentRole().name());
            rf.put("currentTerm", raftNode.getState().getCurrentTerm());
            rf.put("commitIndex", raftNode.getState().getCommitIndex());
        }

        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleRaftRequestVote(HttpExchange exchange) throws IOException {
        byte[] bodyBytes = readBody(exchange);
        RequestVoteArgs args = mapper.readValue(bodyBytes, RequestVoteArgs.class);
        RequestVoteReply reply = raftNode.handleRequestVote(args);

        ObjectNode res = mapper.createObjectNode();
        res.put("term", reply.getTerm());
        res.put("voteGranted", reply.isVoteGranted());
        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleRaftAppendEntries(HttpExchange exchange) throws IOException {
        byte[] bodyBytes = readBody(exchange);
        AppendEntriesArgs args = mapper.readValue(bodyBytes, AppendEntriesArgs.class);
        AppendEntriesReply reply = raftNode.handleAppendEntries(args);

        ObjectNode res = mapper.createObjectNode();
        res.put("term", reply.getTerm());
        res.put("success", reply.isSuccess());
        res.put("matchIndex", reply.getMatchIndex());
        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private byte[] readBody(HttpExchange exchange) throws IOException {
        int maxBytes = config.getMaxRequestBodyBytes();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int total = 0;
        try (InputStream in = exchange.getRequestBody()) {
            int read;
            while ((read = in.read(buf)) != -1) {
                total += read;
                if (total > maxBytes) {
                    throw new PayloadTooLargeException("Payload too large: request body exceeds maximum limit of " + maxBytes + " bytes");
                }
                baos.write(buf, 0, read);
            }
        }
        return baos.toByteArray();
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String responseText) throws IOException {
        byte[] bytes = responseText.getBytes(StandardCharsets.UTF_8);
        setCorsHeaders(exchange);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private String extractQueryParam(String query, String paramName) {
        if (query == null || query.isEmpty()) return null;
        for (String pair : query.split("&")) {
            String[] parts = pair.split("=", 2);
            if (parts.length == 2 && parts[0].equalsIgnoreCase(paramName)) {
                return URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}
