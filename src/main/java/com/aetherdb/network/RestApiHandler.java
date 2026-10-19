package com.aetherdb.network;

import com.aetherdb.analytics.FilterOperator;
import com.aetherdb.analytics.Predicate;
import com.aetherdb.analytics.QueryEngine;
import com.aetherdb.analytics.QueryRequest;
import com.aetherdb.analytics.QueryResult;
import com.aetherdb.common.Constants;
import com.aetherdb.consensus.raft.LogPayload;
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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * REST API Handler for Key-Value, Columnar Analytics, Raft Consensus, and Cluster Management.
 */
public class RestApiHandler implements HttpHandler {

    private final StorageEngine storageEngine;
    private final RaftNode raftNode;
    private final ConsistentHashRouter router;
    private final QueryEngine queryEngine;

    public RestApiHandler(StorageEngine storageEngine, RaftNode raftNode, ConsistentHashRouter router) {
        this.storageEngine = storageEngine;
        this.raftNode = raftNode;
        this.router = router;
        this.queryEngine = new QueryEngine(storageEngine, 1024);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod().toUpperCase();

        try {
            switch (path) {
                case "/api/v1/kv/put" -> handleKvPut(exchange);
                case "/api/v1/kv/get" -> handleKvGet(exchange);
                case "/api/v1/kv/delete" -> handleKvDelete(exchange);
                case "/api/v1/analytics/ingest" -> handleAnalyticsIngest(exchange);
                case "/api/v1/analytics/query" -> handleAnalyticsQuery(exchange);
                case "/api/v1/cluster/status" -> handleClusterStatus(exchange);
                case "/api/v1/metrics" -> handleMetrics(exchange);
                case "/api/v1/raft/requestVote" -> handleRaftRequestVote(exchange);
                case "/api/v1/raft/appendEntries" -> handleRaftAppendEntries(exchange);
                default -> sendResponse(exchange, 404, "{\"error\":\"Endpoint not found: " + path + "\"}");
            }
        } catch (Exception e) {
            sendResponse(exchange, 500, "{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    private void handleKvPut(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        String key = extractJsonField(body, "key");
        String value = extractJsonField(body, "value");

        if (key == null || value == null) {
            sendResponse(exchange, 400, "{\"error\":\"Missing key or value in request\"}");
            return;
        }

        storageEngine.put(key, value);
        sendResponse(exchange, 200, "{\"status\":\"ok\",\"key\":\"" + key + "\"}");
    }

    private void handleKvGet(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String key = extractQueryParam(query, "key");

        if (key == null) {
            sendResponse(exchange, 400, "{\"error\":\"Missing key parameter\"}");
            return;
        }

        DataRecord record = storageEngine.get(key);
        if (record == null) {
            sendResponse(exchange, 404, "{\"error\":\"Key not found: " + key + "\"}");
        } else {
            String json = String.format("{\"key\":\"%s\",\"value\":\"%s\",\"timestamp\":%d,\"sequenceNumber\":%d}",
                    record.getKeyAsString(), record.getValueAsString(), record.getTimestamp(), record.getSequenceNumber());
            sendResponse(exchange, 200, json);
        }
    }

    private void handleKvDelete(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String key = extractQueryParam(query, "key");
        if (key == null) {
            String body = readBody(exchange);
            key = extractJsonField(body, "key");
        }

        if (key == null) {
            sendResponse(exchange, 400, "{\"error\":\"Missing key parameter\"}");
            return;
        }

        storageEngine.delete(key);
        sendResponse(exchange, 200, "{\"status\":\"deleted\",\"key\":\"" + key + "\"}");
    }

    private void handleAnalyticsIngest(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        String table = extractJsonField(body, "table");
        if (table == null) table = "default";

        // Generate synthetic or parsed batch
        List<Map<String, ColumnValue>> records = new ArrayList<>();
        Map<String, ColumnValue> r1 = new LinkedHashMap<>();
        r1.put("id", ColumnValue.ofLong(System.currentTimeMillis()));
        r1.put("region", ColumnValue.ofString("us-east"));
        r1.put("latency", ColumnValue.ofDouble(12.4));
        r1.put("status", ColumnValue.ofInt(200));
        records.add(r1);

        storageEngine.ingestColumnar(table, records);
        sendResponse(exchange, 200, "{\"status\":\"ingested\",\"table\":\"" + table + "\",\"records\":1}");
    }

    private void handleAnalyticsQuery(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        String table = extractJsonField(body, "table");
        if (table == null) table = "default";

        String select = extractJsonField(body, "select");
        List<String> selectList = (select != null && !select.isEmpty())
                ? List.of(select.split(","))
                : List.of("COUNT(*)");

        QueryRequest req = new QueryRequest(table, selectList, Collections.emptyList(), Collections.emptyList(), 100);
        QueryResult result = queryEngine.execute(req);

        StringBuilder sb = new StringBuilder();
        sb.append("{\"executionTimeMs\":").append(result.getExecutionTimeMs())
                .append(",\"scannedChunks\":").append(result.getScannedChunks())
                .append(",\"skippedChunks\":").append(result.getSkippedChunks())
                .append(",\"columns\":[");
        for (int i = 0; i < result.getColumns().size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(result.getColumns().get(i)).append("\"");
        }
        sb.append("],\"rows\":[");
        for (int i = 0; i < result.getRows().size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("[");
            List<Object> row = result.getRows().get(i);
            for (int j = 0; j < row.size(); j++) {
                if (j > 0) sb.append(",");
                Object val = row.get(j);
                if (val instanceof Number) sb.append(val);
                else sb.append("\"").append(val).append("\"");
            }
            sb.append("]");
        }
        sb.append("]}");

        sendResponse(exchange, 200, sb.toString());
    }

    private void handleClusterStatus(HttpExchange exchange) throws IOException {
        String json = String.format("{\"nodeId\":\"%s\",\"role\":\"%s\",\"leaderId\":\"%s\",\"term\":%d,\"commitIndex\":%d,\"nodes\":%d}",
                raftNode.getNodeId(), raftNode.getCurrentRole(), raftNode.getCurrentLeaderId(),
                raftNode.getCurrentTerm(), raftNode.getState().getCommitIndex(), router.getPhysicalNodes().size());
        sendResponse(exchange, 200, json);
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        Map<String, Object> metrics = storageEngine.getMetrics();
        StringBuilder sb = new StringBuilder("{");
        int count = 0;
        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            if (count++ > 0) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":").append(entry.getValue());
        }
        sb.append("}");
        sendResponse(exchange, 200, sb.toString());
    }

    private void handleRaftRequestVote(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        long term = Long.parseLong(extractJsonField(body, "term"));
        String candidateId = extractJsonField(body, "candidateId");
        long lastLogIndex = Long.parseLong(extractJsonField(body, "lastLogIndex"));
        long lastLogTerm = Long.parseLong(extractJsonField(body, "lastLogTerm"));

        RequestVoteArgs args = new RequestVoteArgs(term, candidateId, lastLogIndex, lastLogTerm);
        RequestVoteReply reply = raftNode.handleRequestVote(args);

        String json = String.format("{\"term\":%d,\"voteGranted\":%b}", reply.getTerm(), reply.isVoteGranted());
        sendResponse(exchange, 200, json);
    }

    private void handleRaftAppendEntries(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        long term = Long.parseLong(extractJsonField(body, "term"));
        String leaderId = extractJsonField(body, "leaderId");
        long prevLogIndex = Long.parseLong(extractJsonField(body, "prevLogIndex"));
        long prevLogTerm = Long.parseLong(extractJsonField(body, "prevLogTerm"));
        long leaderCommit = Long.parseLong(extractJsonField(body, "leaderCommit"));

        AppendEntriesArgs args = new AppendEntriesArgs(term, leaderId, prevLogIndex, prevLogTerm, Collections.emptyList(), leaderCommit);
        AppendEntriesReply reply = raftNode.handleAppendEntries(args);

        String json = String.format("{\"term\":%d,\"success\":%b,\"matchIndex\":%d}",
                reply.getTerm(), reply.isSuccess(), reply.getMatchIndex());
        sendResponse(exchange, 200, json);
    }

    private String readBody(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String responseText) throws IOException {
        byte[] bytes = responseText.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private String extractJsonField(String json, String field) {
        if (json == null) return null;
        int idx = json.indexOf("\"" + field + "\"");
        if (idx < 0) {
            idx = json.indexOf(field);
        }
        if (idx < 0) return null;
        int colon = json.indexOf(':', idx + field.length());
        if (colon < 0) return null;
        int start = colon + 1;
        while (start < json.length() && (json.charAt(start) == ' ' || json.charAt(start) == '\"')) {
            start++;
        }
        int end = start;
        while (end < json.length() && json.charAt(end) != '\"' && json.charAt(end) != ',' && json.charAt(end) != '}') {
            end++;
        }
        String res = json.substring(start, end).trim();
        return res.isEmpty() ? null : res;
    }

    private String extractQueryParam(String query, String param) {
        if (query == null) return null;
        for (String pair : query.split("&")) {
            String[] kv = pair.split("=");
            if (kv.length == 2 && kv[0].equalsIgnoreCase(param)) {
                return kv[1];
            }
        }
        return null;
    }
}
