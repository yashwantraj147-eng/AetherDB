package com.aetherdb.cli;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Interactive Command-Line Interface (REPL) for AetherDB.
 */
public class AetherCli {

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        String baseUrl = "http://" + host + ":" + port;

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        System.out.println("========================================================");
        System.out.println("  AetherDB Interactive Shell Connected to " + baseUrl);
        System.out.println("  Commands: PUT <k> <v> | GET <k> | DEL <k> | METRICS | EXIT");
        System.out.println("========================================================\n");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                System.out.print("aether> ");
                String line = reader.readLine();
                if (line == null) break;
                line = line.trim();
                if (line.isEmpty()) continue;

                if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                    System.out.println("Goodbye!");
                    break;
                }

                String[] tokens = line.split("\\s+", 3);
                String cmd = tokens[0].toUpperCase();

                switch (cmd) {
                    case "PUT" -> {
                        if (tokens.length < 3) {
                            System.out.println("Usage: PUT <key> <value>");
                            continue;
                        }
                        String json = String.format("{\"key\":\"%s\",\"value\":\"%s\"}", tokens[1], tokens[2]);
                        sendPost(client, baseUrl + "/api/v1/kv/put", json);
                    }
                    case "GET" -> {
                        if (tokens.length < 2) {
                            System.out.println("Usage: GET <key>");
                            continue;
                        }
                        sendGet(client, baseUrl + "/api/v1/kv/get?key=" + tokens[1]);
                    }
                    case "DEL" -> {
                        if (tokens.length < 2) {
                            System.out.println("Usage: DEL <key>");
                            continue;
                        }
                        sendPost(client, baseUrl + "/api/v1/kv/delete?key=" + tokens[1], "");
                    }
                    case "METRICS" -> sendGet(client, baseUrl + "/api/v1/metrics");
                    case "STATUS" -> sendGet(client, baseUrl + "/api/v1/cluster/status");
                    default -> System.out.println("Unknown command: " + cmd);
                }
            }
        } catch (Exception e) {
            System.err.println("CLI Error: " + e.getMessage());
        }
    }

    private static void sendGet(HttpClient client, String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            System.out.println(resp.body());
        } catch (Exception e) {
            System.out.println("Request failed: " + e.getMessage());
        }
    }

    private static void sendPost(HttpClient client, String url, String body) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            System.out.println(resp.body());
        } catch (Exception e) {
            System.out.println("Request failed: " + e.getMessage());
        }
    }
}
