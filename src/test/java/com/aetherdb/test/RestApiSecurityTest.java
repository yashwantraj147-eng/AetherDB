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
import java.util.List;

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
    @DisplayName("Security: external network bind (0.0.0.0) with valid authentication succeeds")
    void testExternalBindWithValidAuthSucceeds() throws Exception {
        AetherConfig validConfig = new AetherConfig();
        validConfig.setProperty("node.host", "0.0.0.0");
        validConfig.setProperty("node.port", "0");
        validConfig.setProperty("server.auth.enabled", "true");
        validConfig.setProperty("server.auth.token", "valid_secret_token_123");
        validConfig.setProperty("storage.wal.dir", tempDir.resolve("wal_ext").toString());
        validConfig.setProperty("storage.sstable.dir", tempDir.resolve("sstable_ext").toString());
        validConfig.setProperty("storage.columnar.dir", tempDir.resolve("columnar_ext").toString());
        validConfig.setProperty("storage.data.dir", tempDir.resolve("storage_ext").toString());
        validConfig.validateAndCreateDirectories();

        try (HttpApiServer extServer = new HttpApiServer(validConfig, storage, raftNode, router)) {
            assertNotNull(extServer);
            assertTrue(extServer.getPort() > 0);
        }
    }

    @Test
    @DisplayName("Auth: missing token, invalid token, and valid Bearer token behavior")
    void testAuthenticationTokenScenarios() throws Exception {
        AetherConfig authConfig = new AetherConfig();
        authConfig.setProperty("node.host", "127.0.0.1");
        authConfig.setProperty("node.port", "0");
        authConfig.setProperty("server.auth.enabled", "true");
        authConfig.setProperty("server.auth.token", "super_secret_auth_token_456");
        authConfig.setProperty("storage.wal.dir", tempDir.resolve("wal_auth").toString());
        authConfig.setProperty("storage.sstable.dir", tempDir.resolve("sstable_auth").toString());
        authConfig.setProperty("storage.columnar.dir", tempDir.resolve("columnar_auth").toString());
        authConfig.setProperty("storage.data.dir", tempDir.resolve("storage_auth").toString());
        authConfig.validateAndCreateDirectories();

        try (HybridStorageManager authStorage = new HybridStorageManager(authConfig);
             HttpApiServer authServer = new HttpApiServer(authConfig, authStorage, null, null)) {
            authServer.start();
            String serverUrl = "http://127.0.0.1:" + authServer.getPort();

            // 1. Missing authentication token returns 401
            HttpRequest reqNoAuth = HttpRequest.newBuilder()
                    .uri(URI.create(serverUrl + "/api/v1/kv/get?key=test"))
                    .GET()
                    .build();
            HttpResponse<String> respNoAuth = client.send(reqNoAuth, HttpResponse.BodyHandlers.ofString());
            assertEquals(401, respNoAuth.statusCode());
            assertTrue(respNoAuth.body().contains("Unauthorized"));

            // 2. Invalid authentication token returns 401
            HttpRequest reqWrongAuth = HttpRequest.newBuilder()
                    .uri(URI.create(serverUrl + "/api/v1/kv/get?key=test"))
                    .header("Authorization", "Bearer invalid_or_wrong_token")
                    .GET()
                    .build();
            HttpResponse<String> respWrongAuth = client.send(reqWrongAuth, HttpResponse.BodyHandlers.ofString());
            assertEquals(401, respWrongAuth.statusCode());
            assertTrue(respWrongAuth.body().contains("Unauthorized"));

            // 3. Valid Bearer token succeeds
            HttpRequest reqValidAuth = HttpRequest.newBuilder()
                    .uri(URI.create(serverUrl + "/api/v1/kv/put"))
                    .header("Authorization", "Bearer super_secret_auth_token_456")
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"key\":\"auth_key\",\"value\":\"auth_val\"}"))
                    .build();
            HttpResponse<String> respValidAuth = client.send(reqValidAuth, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, respValidAuth.statusCode());
            assertTrue(respValidAuth.body().contains("\"status\":\"ok\""));
        }
    }

    @Test
    @DisplayName("CORS: allowed origin, rejected origin, preflight, multiple origins, and Vary header")
    void testCorsAllowedRejectedPreflightAndVary() throws Exception {
        AetherConfig corsConfig = new AetherConfig();
        corsConfig.setProperty("node.host", "127.0.0.1");
        corsConfig.setProperty("node.port", "0");
        corsConfig.setProperty("server.auth.enabled", "false");
        corsConfig.setProperty("server.cors.allowed.origins", "https://app1.example.com, https://app2.example.com");
        corsConfig.setProperty("storage.wal.dir", tempDir.resolve("wal_cors").toString());
        corsConfig.setProperty("storage.sstable.dir", tempDir.resolve("sstable_cors").toString());
        corsConfig.setProperty("storage.columnar.dir", tempDir.resolve("columnar_cors").toString());
        corsConfig.setProperty("storage.data.dir", tempDir.resolve("storage_cors").toString());
        corsConfig.validateAndCreateDirectories();

        try (HybridStorageManager corsStorage = new HybridStorageManager(corsConfig);
             HttpApiServer corsServer = new HttpApiServer(corsConfig, corsStorage, null, null)) {
            corsServer.start();
            String serverUrl = "http://127.0.0.1:" + corsServer.getPort();

            // Scenario 1: Allowed CORS origin reflected and Vary: Origin header present
            HttpRequest reqAllowed1 = HttpRequest.newBuilder()
                    .uri(URI.create(serverUrl + "/api/v1/kv/get?key=nonexistent"))
                    .header("Origin", "https://app1.example.com")
                    .GET()
                    .build();
            HttpResponse<String> respAllowed1 = client.send(reqAllowed1, HttpResponse.BodyHandlers.ofString());
            assertEquals("https://app1.example.com", respAllowed1.headers().firstValue("Access-Control-Allow-Origin").orElse(null));
            assertEquals("Origin", respAllowed1.headers().firstValue("Vary").orElse(null));

            // Scenario 2: Second allowed origin from comma-separated list
            HttpRequest reqAllowed2 = HttpRequest.newBuilder()
                    .uri(URI.create(serverUrl + "/api/v1/kv/get?key=nonexistent"))
                    .header("Origin", "https://app2.example.com")
                    .GET()
                    .build();
            HttpResponse<String> respAllowed2 = client.send(reqAllowed2, HttpResponse.BodyHandlers.ofString());
            assertEquals("https://app2.example.com", respAllowed2.headers().firstValue("Access-Control-Allow-Origin").orElse(null));
            assertEquals("Origin", respAllowed2.headers().firstValue("Vary").orElse(null));

            // Scenario 3: Rejected CORS origin is NOT reflected in GET response
            HttpRequest reqRejectedGet = HttpRequest.newBuilder()
                    .uri(URI.create(serverUrl + "/api/v1/kv/get?key=nonexistent"))
                    .header("Origin", "https://evil.unauthorized.com")
                    .GET()
                    .build();
            HttpResponse<String> respRejectedGet = client.send(reqRejectedGet, HttpResponse.BodyHandlers.ofString());
            assertTrue(respRejectedGet.headers().firstValue("Access-Control-Allow-Origin").isEmpty());

            // Scenario 4: Rejected CORS origin returns 403 on OPTIONS preflight
            HttpRequest reqRejectedOptions = HttpRequest.newBuilder()
                    .uri(URI.create(serverUrl + "/api/v1/kv/put"))
                    .header("Origin", "https://evil.unauthorized.com")
                    .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<String> respRejectedOptions = client.send(reqRejectedOptions, HttpResponse.BodyHandlers.ofString());
            assertEquals(403, respRejectedOptions.statusCode());

            // Scenario 5: Allowed origin OPTIONS preflight returns 204 with methods and headers
            HttpRequest reqAllowedOptions = HttpRequest.newBuilder()
                    .uri(URI.create(serverUrl + "/api/v1/kv/put"))
                    .header("Origin", "https://app1.example.com")
                    .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<String> respAllowedOptions = client.send(reqAllowedOptions, HttpResponse.BodyHandlers.ofString());
            assertEquals(204, respAllowedOptions.statusCode());
            assertEquals("https://app1.example.com", respAllowedOptions.headers().firstValue("Access-Control-Allow-Origin").orElse(null));
            assertTrue(respAllowedOptions.headers().firstValue("Access-Control-Allow-Methods").orElse("").contains("POST"));
            assertTrue(respAllowedOptions.headers().firstValue("Access-Control-Allow-Headers").orElse("").contains("Authorization"));
        }
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
