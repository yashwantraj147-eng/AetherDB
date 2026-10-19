package com.aetherdb.network;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Serves the embedded single-page real-time monitoring and analytics dashboard.
 */
public class DashboardHandler implements HttpHandler {

    private final File dashboardFile;

    public DashboardHandler(File dashboardFile) {
        this.dashboardFile = dashboardFile;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        byte[] content;
        String contentType = "text/html; charset=UTF-8";

        if (dashboardFile != null && dashboardFile.exists()) {
            try (FileInputStream in = new FileInputStream(dashboardFile)) {
                content = in.readAllBytes();
            }
        } else {
            content = getFallbackDashboardHtml().getBytes(StandardCharsets.UTF_8);
        }

        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, content.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(content);
        }
    }

    private String getFallbackDashboardHtml() {
        return "<!DOCTYPE html><html><head><title>AetherDB</title></head>" +
               "<body style='font-family:sans-serif;background:#0d1117;color:#c9d1d9;padding:40px;'>" +
               "<h1>⚡ AetherDB Active</h1><p>Embedded dashboard file not found. API is fully running at /api/v1/metrics</p></body></html>";
    }
}
