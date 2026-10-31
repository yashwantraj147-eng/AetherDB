package com.aetherdb.test;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.consensus.raft.RaftNode;
import com.aetherdb.network.HttpApiServer;
import com.aetherdb.partition.ConsistentHashRouter;
import com.aetherdb.storage.engine.HybridStorageManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

public class RestApiSecurityTest {

    private Path tempDir;
    private AetherConfig config;
    private HybridStorageManager storage;
    private RaftNode raftNode;
    private ConsistentHashRouter router;
    private HttpApiServer server;
    private HttpClient client;
    private String baseUrl;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("aether_rest_test_");
        config = new AetherConfig();
        config.setProperty("node.host", "127.0.0.1");
        config.setProperty("node.port", "0"); // Ephemeral port
        config.setProperty("storage.wal.dir", tempDir.resolve("wal").toString());
        config.setProperty("storage.sstable.dir", tempDir.resolve("sstable").toString());
        config.setProperty("storage.columnar.dir", tempDir.resolve("columnar").toString());
        config.setProperty("storage.data.dir", tempDir.resolve("storage").toString());
        config.setProperty("server.auth.enabled", "false");
        config.validateAndCreateDirectories();

        storage = new HybridStorageManager(config);
        raftNode = new RaftNode(config, storage);
        router = new ConsistentHashRouter(16);
        server = new HttpApiServer(config, storage, raftNode, router);
        server.start();

        baseUrl = "http://127.0.0.1:" + server.getPort();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) server.close();
        if (raftNode != null) raftNode.close();
        if (storage != null) storage.close();

        if (Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("HTTP Method enforcement: rejecting invalid methods with 405 Method Not Allowed")
    void testMethodEnforcement() throws Exception {
        // GET on /api/v1/kv/put is not allowed (POST only)
        HttpRequest getOnPut = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/kv/put"))
                .GET()
                .build();
        HttpResponse<String> resp1 = client.send(getOnPut, HttpResponse.BodyHandlers.ofString());
        assertEquals(405, resp1.statusCode());
        assertTrue(resp1.body().contains("Method Not Allowed"));

        // POST on /api/v1/kv/get is not allowed (GET only)
        HttpRequest postOnGet = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/kv/get?key=foo"))
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();
        HttpResponse<String> resp2 = client.send(postOnGet, HttpResponse.BodyHandlers.ofString());
        assertEquals(405, resp2.statusCode());
    }

    @Test
    @DisplayName("Key-Value REST lifecycle: PUT, GET, and DELETE with Jackson JSON")
    void testKvRestLifecycle() throws Exception {
        // 1. PUT
        String putJson = "{\"key\":\"my_api_key\",\"value\":\"my_api_value_123\"}";
        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/kv/put"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(putJson))
                .build();
        HttpResponse<String> putResp = client.send(putReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, putResp.statusCode());

        // 2. GET
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/kv/get?key=my_api_key"))
                .GET()
                .build();
        HttpResponse<String> getResp = client.send(getReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, getResp.statusCode());

        JsonNode getJson = mapper.readTree(getResp.body());
        assertEquals("my_api_key", getJson.get("key").asText());
        assertEquals("my_api_value_123", getJson.get("value").asText());

        // 3. DELETE
        HttpRequest delReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/kv/delete?key=my_api_key"))
                .DELETE()
                .build();
        HttpResponse<String> delResp = client.send(delReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, delResp.statusCode());

        // 4. GET after DELETE should return 404
        HttpResponse<String> getAfterDel = client.send(getReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, getAfterDel.statusCode());
    }

    @Test
    @DisplayName("Analytics REST API: real batch ingestion and configurable query execution")
    void testAnalyticsBatchIngestAndQuery() throws Exception {
        String ingestPayload = """
                {
                  "table": "events",
                  "records": [
                    {"eventId": 1, "region": "US-EAST", "latency": 15.4},
                    {"eventId": 2, "region": "US-WEST", "latency": 25.1},
                    {"eventId": 3, "region": "US-EAST", "latency": 18.2}
                  ]
                }
                """;

        HttpRequest ingestReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/analytics/ingest"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(ingestPayload))
                .build();
        HttpResponse<String> ingestResp = client.send(ingestReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, ingestResp.statusCode());

        JsonNode ingestJson = mapper.readTree(ingestResp.body());
        assertEquals("ingested", ingestJson.get("status").asText());
        assertEquals(3, ingestJson.get("records").asInt());

        // Query: SELECT region, COUNT(*), AVG(latency) GROUP BY region
        String queryPayload = """
                {
                  "table": "events",
                  "select": ["COUNT(*)", "AVG(latency)"],
                  "groupBy": ["region"],
                  "limit": 10
                }
                """;

        HttpRequest queryReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/analytics/query"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(queryPayload))
                .build();
        HttpResponse<String> queryResp = client.send(queryReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, queryResp.statusCode());

        JsonNode queryJson = mapper.readTree(queryResp.body());
        JsonNode rows = queryJson.get("rows");
        assertEquals(2, rows.size()); // US-EAST and US-WEST
    }
}

