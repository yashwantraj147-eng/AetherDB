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
 */
public class HttpApiServer implements AutoCloseable {

    private final HttpServer server;
    private final int port;

    public HttpApiServer(AetherConfig config, StorageEngine storageEngine,
                         RaftNode raftNode, ConsistentHashRouter router) throws IOException {
        this.port = config.getNodePort();
        String host = config.getNodeHost();

        if ("0.0.0.0".equals(host) && !config.isAuthEnabled()) {
            System.err.println("WARNING: Server bound to 0.0.0.0 with authentication disabled! Endpoints are open to external network access.");
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
