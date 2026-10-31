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
        config.setProperty("consensus.raft.enabled", "true");
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
    @DisplayName("Security: external network bind (0.0.0.0) without authentication throws SecurityException")
    void testExternalBindWithoutAuthThrows() {
        AetherConfig badConfig = new AetherConfig();
        badConfig.setProperty("node.host", "0.0.0.0");
        badConfig.setProperty("server.auth.enabled", "false");

        assertThrows(SecurityException.class, () -> {
            new HttpApiServer(badConfig, storage, raftNode, router);
        });
    }

    @Test
    @DisplayName("Security: empty configured auth token throws SecurityException")
    void testEmptyAuthTokenThrows() {
        AetherConfig badConfig = new AetherConfig();
        badConfig.setProperty("node.host", "127.0.0.1");
        badConfig.setProperty("server.auth.enabled", "true");
        badConfig.setProperty("server.auth.token", "   ");

        assertThrows(SecurityException.class, () -> {
            new HttpApiServer(badConfig, storage, raftNode, router);
        });
    }

    @Test
    @DisplayName("Security: Raft endpoints return 404 when Raft is disabled")
    void testRaftEndpointsGatedWhenDisabled() throws Exception {
        AetherConfig noRaftConfig = new AetherConfig();
        noRaftConfig.setProperty("node.host", "127.0.0.1");
        noRaftConfig.setProperty("node.port", "0");
        noRaftConfig.setProperty("consensus.raft.enabled", "false");
        noRaftConfig.setProperty("server.auth.enabled", "false");
        noRaftConfig.setProperty("storage.wal.dir", tempDir.resolve("wal_noraft").toString());
        noRaftConfig.setProperty("storage.sstable.dir", tempDir.resolve("sstable_noraft").toString());
        noRaftConfig.setProperty("storage.columnar.dir", tempDir.resolve("columnar_noraft").toString());
        noRaftConfig.setProperty("storage.data.dir", tempDir.resolve("storage_noraft").toString());
        noRaftConfig.validateAndCreateDirectories();

        try (HybridStorageManager eng = new HybridStorageManager(noRaftConfig);
             HttpApiServer noRaftServer = new HttpApiServer(noRaftConfig, eng, null, null)) {
            noRaftServer.start();

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + noRaftServer.getPort() + "/api/v1/cluster/status"))
                    .GET()
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            assertEquals(404, resp.statusCode());
            assertTrue(resp.body().contains("Raft consensus module is disabled"));
        }
    }

    @Test
    @DisplayName("Security: oversized request payload returns 413 Payload Too Large")
    void testPayloadTooLargeReturns413() throws Exception {
        // Default max request body is 10MB; send a payload with a very large string
        AetherConfig smallBodyConfig = new AetherConfig();
        smallBodyConfig.setProperty("node.host", "127.0.0.1");
        smallBodyConfig.setProperty("node.port", "0");
        smallBodyConfig.setProperty("server.auth.enabled", "false");
        smallBodyConfig.setProperty("server.max.request.body.bytes", "1024"); // 1 KB limit
        smallBodyConfig.setProperty("storage.wal.dir", tempDir.resolve("wal_small").toString());
        smallBodyConfig.setProperty("storage.sstable.dir", tempDir.resolve("sstable_small").toString());
        smallBodyConfig.setProperty("storage.columnar.dir", tempDir.resolve("columnar_small").toString());
        smallBodyConfig.setProperty("storage.data.dir", tempDir.resolve("storage_small").toString());
        smallBodyConfig.validateAndCreateDirectories();

        try (HybridStorageManager eng = new HybridStorageManager(smallBodyConfig);
             HttpApiServer smallServer = new HttpApiServer(smallBodyConfig, eng, null, null)) {
            smallServer.start();

            String hugePayload = "{\"key\":\"large\",\"value\":\"" + "X".repeat(5000) + "\"}";
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + smallServer.getPort() + "/api/v1/kv/put"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(hugePayload))
                    .build();

            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            assertEquals(413, resp.statusCode());
            assertTrue(resp.body().contains("Payload too large"));
        }
    }

    @Test
    @DisplayName("Security: malformed JSON returns 400 Bad Request")
    void testMalformedJsonReturns400() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/kv/put"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{malformed_json_not_quoted}"))
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, resp.statusCode());
        assertTrue(resp.body().contains("Malformed JSON"));
    }

    @Test
    @DisplayName("HTTP Method enforcement: rejecting invalid methods with 405 Method Not Allowed")
    void testMethodEnforcement() throws Exception {
        HttpRequest getOnPut = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/kv/put"))
                .GET()
                .build();
        HttpResponse<String> resp1 = client.send(getOnPut, HttpResponse.BodyHandlers.ofString());
        assertEquals(405, resp1.statusCode());
        assertTrue(resp1.body().contains("Method Not Allowed"));

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
        String putJson = "{\"key\":\"my_api_key\",\"value\":\"my_api_value_123\"}";
        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/kv/put"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(putJson))
                .build();
        HttpResponse<String> putResp = client.send(putReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, putResp.statusCode());

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/kv/get?key=my_api_key"))
                .GET()
                .build();
        HttpResponse<String> getResp = client.send(getReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, getResp.statusCode());

        JsonNode getJson = mapper.readTree(getResp.body());
        assertEquals("my_api_key", getJson.get("key").asText());
        assertEquals("my_api_value_123", getJson.get("value").asText());

        HttpRequest delReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/kv/delete?key=my_api_key"))
                .DELETE()
                .build();
        HttpResponse<String> delResp = client.send(delReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, delResp.statusCode());

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
                    {"region": "us-east", "latency": 15.5},
                    {"region": "us-east", "latency": 25.5},
                    {"region": "eu-west", "latency": 40.0}
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
        assertEquals(2, queryJson.get("rows").size());
    }
}
