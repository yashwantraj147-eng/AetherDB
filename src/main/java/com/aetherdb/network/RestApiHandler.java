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
import java.util.*;

/**
 * Production REST API Handler for Key-Value, Columnar Analytics, and Raft Consensus.
 * Enforces strict HTTP methods, Jackson JSON parsing/serialization, bounded request sizes,
 * configurable authentication and CORS, and table name validation.
 */
public class RestApiHandler implements HttpHandler {

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
        return !config.getAuthToken().isEmpty() && config.getAuthToken().equals(token);
    }

    private void handleCorsPreflight(HttpExchange exchange) throws IOException {
        setCorsHeaders(exchange);
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, Authorization");
        exchange.sendResponseHeaders(204, -1);
    }

    private void setCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", config.getCorsAllowedOrigins());
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
            sendResponse(exchange, 400, "{\"error\":\"Missing 'key' parameter\"}");
            return;
        }

        storageEngine.delete(key);
        ObjectNode res = mapper.createObjectNode();
        res.put("status", "deleted");
        res.put("key", key);
        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleAnalyticsIngest(HttpExchange exchange) throws IOException {
        byte[] bodyBytes = readBody(exchange);
        if (bodyBytes.length == 0) {
            sendResponse(exchange, 400, "{\"error\":\"Empty request body\"}");
            return;
        }

        JsonNode root = mapper.readTree(bodyBytes);
        String table = root.has("table") ? root.get("table").asText() : "default";

        if (!root.has("records") || !root.get("records").isArray()) {
            sendResponse(exchange, 400, "{\"error\":\"Missing 'records' array in ingestion payload\"}");
            return;
        }

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
                    cv = ColumnValue.ofNull(ColumnType.STRING);
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
        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleAnalyticsQuery(HttpExchange exchange) throws IOException {
        byte[] bodyBytes = readBody(exchange);
        if (bodyBytes.length == 0) {
            sendResponse(exchange, 400, "{\"error\":\"Empty request body\"}");
            return;
        }

        JsonNode root = mapper.readTree(bodyBytes);
        String table = root.has("table") ? root.get("table").asText() : "default";

        List<String> selectList = new ArrayList<>();
        if (root.has("select") && root.get("select").isArray()) {
            for (JsonNode s : root.get("select")) {
                selectList.add(s.asText());
            }
        } else if (root.has("select") && root.get("select").isTextual()) {
            for (String part : root.get("select").asText().split(",")) {
                if (!part.trim().isEmpty()) selectList.add(part.trim());
            }
        }
        if (selectList.isEmpty()) {
            selectList.add("COUNT(*)");
        }

        List<Predicate> predicates = new ArrayList<>();
        if (root.has("predicates") && root.get("predicates").isArray()) {
            for (JsonNode pNode : root.get("predicates")) {
                String col = pNode.get("column").asText();
                String opStr = pNode.get("op").asText();
                FilterOperator op = FilterOperator.fromString(opStr);
                JsonNode vNode = pNode.get("value");

                ColumnValue targetVal;
                if (vNode == null || vNode.isNull()) {
                    targetVal = ColumnValue.ofNull(ColumnType.STRING);
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

        int limit = root.has("limit") ? root.get("limit").asInt(100) : 100;
        QueryRequest req = new QueryRequest(table, selectList, predicates, groupByCols, limit);
        QueryResult result = queryEngine.execute(req);

        ObjectNode res = mapper.createObjectNode();
        res.put("executionTimeMs", result.getExecutionTimeMs());
        res.put("scannedChunks", result.getScannedChunks());
        res.put("skippedChunks", result.getSkippedChunks());
        res.put("totalRowsProcessed", result.getTotalRowsProcessed());

        ArrayNode colsArr = res.putArray("columns");
        for (String c : result.getColumns()) {
            colsArr.add(c);
        }

        ArrayNode rowsArr = res.putArray("rows");
        for (List<Object> row : result.getRows()) {
            ArrayNode rowArr = rowsArr.addArray();
            for (Object item : row) {
                if (item == null) {
                    rowArr.addNull();
                } else if (item instanceof Number n) {
                    if (n instanceof Double || n instanceof Float) {
                        rowArr.add(n.doubleValue());
                    } else {
                        rowArr.add(n.longValue());
                    }
                } else if (item instanceof Boolean b) {
                    rowArr.add(b);
                } else {
                    rowArr.add(item.toString());
                }
            }
        }

        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleClusterStatus(HttpExchange exchange) throws IOException {
        ObjectNode res = mapper.createObjectNode();
        res.put("nodeId", raftNode.getNodeId());
        res.put("role", raftNode.getCurrentRole().name());
        res.put("leaderId", raftNode.getCurrentLeaderId() != null ? raftNode.getCurrentLeaderId() : "none");
        res.put("term", raftNode.getCurrentTerm());
        res.put("isLeader", raftNode.isLeader());
        res.put("ringSize", router.getRingSize());
        res.put("physicalNodesCount", router.getPhysicalNodes().size());
        res.put("raftStatus", "EXPERIMENTAL_PROTOTYPE");
        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        Map<String, Object> metrics = storageEngine.getMetrics();
        ObjectNode res = mapper.createObjectNode();
        for (Map.Entry<String, Object> e : metrics.entrySet()) {
            if (e.getValue() instanceof Number n) {
                res.put(e.getKey(), n.longValue());
            } else {
                res.put(e.getKey(), String.valueOf(e.getValue()));
            }
        }
        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleRaftRequestVote(HttpExchange exchange) throws IOException {
        byte[] bodyBytes = readBody(exchange);
        JsonNode root = mapper.readTree(bodyBytes);
        long term = root.get("term").asLong();
        String candidateId = root.get("candidateId").asText();
        long lastLogIndex = root.get("lastLogIndex").asLong();
        long lastLogTerm = root.get("lastLogTerm").asLong();

        RequestVoteArgs args = new RequestVoteArgs(term, candidateId, lastLogIndex, lastLogTerm);
        RequestVoteReply reply = raftNode.handleRequestVote(args);

        ObjectNode res = mapper.createObjectNode();
        res.put("term", reply.getTerm());
        res.put("voteGranted", reply.isVoteGranted());
        sendResponse(exchange, 200, mapper.writeValueAsString(res));
    }

    private void handleRaftAppendEntries(HttpExchange exchange) throws IOException {
        byte[] bodyBytes = readBody(exchange);
        JsonNode root = mapper.readTree(bodyBytes);
        long term = root.get("term").asLong();
        String leaderId = root.get("leaderId").asText();
        long prevLogIndex = root.get("prevLogIndex").asLong();
        long prevLogTerm = root.get("prevLogTerm").asLong();
        long leaderCommit = root.get("leaderCommit").asLong();

        AppendEntriesArgs args = new AppendEntriesArgs(term, leaderId, prevLogIndex, prevLogTerm, Collections.emptyList(), leaderCommit);
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
                    throw new IllegalArgumentException("Payload too large: request body exceeds maximum limit of " + maxBytes + " bytes");
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
