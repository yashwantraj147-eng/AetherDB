package com.aetherdb.network;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.consensus.raft.RaftNode;
import com.aetherdb.partition.ConsistentHashRouter;
import com.aetherdb.storage.engine.StorageEngine;
import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Embedded HTTP Gateway for AetherDB powered by Java Virtual Threads.
 * Supports concurrent client connections with JSON REST and dashboard endpoints.
 * Enforces mandatory authentication for external network bindings.
 */
public class HttpApiServer implements AutoCloseable {

    private final HttpServer server;
    private final int port;

    public HttpApiServer(AetherConfig config, StorageEngine storageEngine,
                         RaftNode raftNode, ConsistentHashRouter router) throws IOException {
        this.port = config.getNodePort();
        String host = config.getNodeHost();

        // Enforce external bind authentication policy:
        // If server is bound to 0.0.0.0 or a non-loopback address, authentication is mandatory
        boolean isLoopback = "127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host);
        if (!isLoopback) {
            if (!config.isAuthEnabled()) {
                throw new SecurityException("Server bound to external interface (" + host
                        + ") but auth.enabled is false! Authentication is mandatory for external network access.");
            }
            if (config.getAuthToken() == null || config.getAuthToken().trim().isEmpty()) {
                throw new SecurityException("Server bound to external interface (" + host
                        + ") with empty auth token! A non-empty auth token is mandatory.");
            }
        } else if (config.isAuthEnabled()) {
            if (config.getAuthToken() == null || config.getAuthToken().trim().isEmpty()) {
                throw new SecurityException("auth.enabled is true but auth.token is empty!");
            }
        }

        this.server = HttpServer.create(new InetSocketAddress(host, port), 0);

        // Bind virtual thread executor for lightweight non-blocking connection handling
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        RestApiHandler restHandler = new RestApiHandler(config, storageEngine, raftNode, router);
        server.createContext("/api/", restHandler);

        if (config.isDashboardEnabled()) {
            File dashboardFile = new File("web/dashboard.html");
            if (!dashboardFile.exists()) {
                dashboardFile = new File("bin/web/dashboard.html");
            }
            server.createContext("/dashboard", new DashboardHandler(dashboardFile));
            server.createContext("/", new DashboardHandler(dashboardFile));
        }
    }

    public void start() {
        server.start();
        System.out.println("⚡ AetherDB HTTP Server listening on port " + getPort() + " (Virtual Threads Enabled)");
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(1);
    }
}
